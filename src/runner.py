"""
Sharded Anti-Ban Queue Dispatcher (`src/runner.py`) for FlightPulse Matrix v4.0.

Responsibilities:
  1. Never scrapes all 200 date slots in a single burst.
  2. Queries up to `max_slots` due `date_slots` ordered by `last_checked_at ASC NULLS FIRST`.
  3. Prioritizes zero-cost network channels strictly:
       Nivel 1: Cloudflare WARP (`SOCKS5_PROXY`)
       Nivel 2: Direct (`None`)
       Nivel 3: `PROXY_LIST` (Webshare, interleaved across accounts with random run offset)
  4. Enforces a per-account circuit breaker (`ProxyCircuitBreaker`): HTTP 402/407, quota/auth
     errors, or 3 consecutive tunnel failures mark the entire account in `exhausted_accounts`
     and log `[PROXY_EXHAUSTED] account=<user>`.
  5. Pools a single `patchright` browser instance per active channel, resetting cookies
     between destinations, and emits `(tier, network_channel)` telemetry at run completion.
"""

from __future__ import annotations

import argparse
import collections
import logging
import random
import sys
import time
from typing import Any, Callable, Optional

from src.alerts import dispatch_destination_digest, evaluate_slot_alert_triggers
from src.config import (
    DEFAULT_GL,
    MAX_SLOTS_PER_RUN,
    get_proxy_account,
    get_proxy_pool,
    get_proxy_tier,
    redact_account_for_log,
    redact_proxy_for_log,
    sanitize_log_text,
)
from src.db import (
    fetch_due_date_slots,
    init_db,
    record_price_snapshot,
    touch_slot_checked,
)
from src.scraper import (
    PooledPatchrightSession,
    SlotScrapeResult,
    _is_proxy_or_tunnel_error,
    _is_quota_or_auth_error,
    scrape_date_slot,
)

logger = logging.getLogger("flightpulse.runner")


class ProxyCircuitBreaker:
    """
    Tracks per-account proxy failures and trips an account-wide circuit breaker on
    HTTP 402, HTTP 407, auth/quota errors, or 3 consecutive tunnel failures.
    """

    def __init__(self, max_consecutive_failures: int = 3) -> None:
        self.max_consecutive_failures = max_consecutive_failures
        self.exhausted_accounts: set[str] = set()
        self.consecutive_tunnel_failures: dict[str, int] = {}

    def is_available(self, proxy_url: Optional[str]) -> bool:
        """Return True if the channel/account has not been tripped as exhausted."""
        if proxy_url is None:
            return True
        account = get_proxy_account(proxy_url)
        return account not in self.exhausted_accounts

    def record_success(self, proxy_url: Optional[str]) -> None:
        """Reset consecutive tunnel failure counter on a successful scrape."""
        if proxy_url is None:
            return
        account = get_proxy_account(proxy_url)
        self.consecutive_tunnel_failures[account] = 0

    def record_failure(self, proxy_url: Optional[str], reason: Any = "") -> bool:
        """
        Record a proxy failure and trip the account circuit breaker if needed.
        Returns True if the account was newly marked in `exhausted_accounts`.
        """
        if proxy_url is None:
            return False

        account = get_proxy_account(proxy_url)
        if account in self.exhausted_accounts:
            return False

        reason_str = sanitize_log_text(reason or "")
        safe_account = redact_account_for_log(account)
        if _is_quota_or_auth_error(reason_str):
            self.exhausted_accounts.add(account)
            logger.warning("[PROXY_EXHAUSTED] account=%s reason=%s", safe_account, reason_str[:120])
            return True

        is_tunnel = _is_proxy_or_tunnel_error(Exception(reason_str)) or any(
            tok in reason_str.lower() for tok in ("tunnel", "timeout", "connect", "unreachable", "refused")
        )
        if is_tunnel:
            count = self.consecutive_tunnel_failures.get(account, 0) + 1
            self.consecutive_tunnel_failures[account] = count
            if count >= self.max_consecutive_failures:
                self.exhausted_accounts.add(account)
                logger.warning(
                    "[PROXY_EXHAUSTED] account=%s reason=consecutive_tunnel_failures(%d)",
                    safe_account,
                    count,
                )
                return True

        return False

    def filter_available(self, proxies: list[Optional[str]]) -> list[Optional[str]]:
        """Return proxies excluding any belonging to exhausted accounts."""
        return [p for p in proxies if self.is_available(p)]


def run_sharded_dispatcher(
    max_slots: int = MAX_SLOTS_PER_RUN,
    force_all: bool = False,
    destination_id: Optional[int] = None,
    slot_id: Optional[int] = None,
    progress_callback: Optional[Callable[[int, int, str, Optional[dict[str, Any]]], None]] = None,
) -> dict[str, Any]:
    """
    Execute a sharded anti-ban scraping batch across due `date_slots`,
    strictly prioritizing Nivel 1 (WARP) -> Nivel 2 (Direct) -> Nivel 3 (Interleaved Proxy Pool).
    """
    start_time = time.perf_counter()
    init_db()

    due_slots = fetch_due_date_slots(
        max_slots=max_slots,
        force_all=force_all,
        destination_id=destination_id,
        slot_id=slot_id,
    )

    total_due = len(due_slots)
    if total_due == 0:
        logger.info("No date_slots are currently due for checking.")
        return {
            "total_due": 0,
            "scraped_ok": 0,
            "failed": 0,
            "alerts_sent": 0,
            "elapsed_seconds": round(time.perf_counter() - start_time, 2),
            "tier_telemetry": {},
            "results": [],
        }

    # Group selected slots by destination_id while preserving due priority order
    slots_by_dest: dict[int, list[dict[str, Any]]] = collections.OrderedDict()
    dest_meta_map: dict[int, dict[str, Any]] = {}

    for slot in due_slots:
        d_id = int(slot["destination_id"])
        if d_id not in slots_by_dest:
            slots_by_dest[d_id] = []
            dest_meta_map[d_id] = {
                "id": d_id,
                "name": slot.get("destination_name"),
                "origin": slot.get("origin"),
                "destination": slot.get("destination_code"),
                "currency": slot.get("currency"),
                "gl": str(slot.get("gl") or DEFAULT_GL).strip().upper(),
                "target_price": slot.get("target_price"),
                "cooldown_hours": slot.get("cooldown_hours"),
                "filter_config": slot.get("filter_config") or {},
            }
        slots_by_dest[d_id].append(slot)

    # Strict zero-cost priority: [WARP?, Direct (None), ...interleaved Tier-3 proxies...]
    channel_chain = get_proxy_pool(verify_tunnel=True, random_offset=True)
    breaker = ProxyCircuitBreaker(max_consecutive_failures=3)
    tier_telemetry: collections.Counter[tuple[str, str]] = collections.Counter()

    active_idx = 0
    current_channel: Optional[str] = channel_chain[active_idx]

    logger.info(
        "Dispatching %d due date_slots across %d destination(s) | primary=(%s, %s) | pool_len=%d.",
        total_due,
        len(slots_by_dest),
        get_proxy_tier(current_channel),
        redact_proxy_for_log(current_channel),
        len(channel_chain),
    )

    scraped_ok = 0
    failed = 0
    alerts_sent = 0
    processed_count = 0
    detailed_results: list[dict[str, Any]] = []

    def _advance_channel(current_pos: int) -> tuple[int, Optional[str], bool]:
        for next_idx in range(current_pos + 1, len(channel_chain)):
            cand = channel_chain[next_idx]
            if breaker.is_available(cand):
                return next_idx, cand, True
        return current_pos, None, False

    session: Optional[PooledPatchrightSession] = None
    try:
        while active_idx < len(channel_chain):
            cand_channel = channel_chain[active_idx]
            if not breaker.is_available(cand_channel):
                nxt_idx, nxt_ch, ok_adv = _advance_channel(active_idx)
                if not ok_adv:
                    break
                active_idx, current_channel = nxt_idx, nxt_ch
                continue
            try:
                session = PooledPatchrightSession(proxy_url=cand_channel).start()
                current_channel = session.proxy_url
                break
            except Exception as browser_init_exc:
                logger.warning(
                    "Could not start Patchright browser on (%s, %s): %s",
                    get_proxy_tier(cand_channel),
                    redact_proxy_for_log(cand_channel),
                    sanitize_log_text(browser_init_exc),
                )
                breaker.record_failure(cand_channel, browser_init_exc)
                nxt_idx, nxt_ch, ok_adv = _advance_channel(active_idx)
                if not ok_adv:
                    current_channel = None
                    session = None
                    break
                active_idx, current_channel = nxt_idx, nxt_ch

        for dest_idx, (d_id, dest_slots) in enumerate(slots_by_dest.items()):
            if session is not None and dest_idx > 0:
                session.reset_for_new_destination()

            dest_meta = dest_meta_map[d_id]
            triggered_for_dest: list[dict[str, Any]] = []

            logger.info(
                "Scraping destination [%d] %s (%s -> %s): %d due slot(s) via (%s, %s)",
                d_id,
                dest_meta.get("name"),
                dest_meta.get("origin"),
                dest_meta.get("destination"),
                len(dest_slots),
                get_proxy_tier(current_channel),
                redact_proxy_for_log(current_channel),
            )

            for slot_idx, slot in enumerate(dest_slots):
                s_id = int(slot["id"])
                s_label = slot.get("label") or f"{slot.get('outbound_date')} -> {slot.get('return_date')}"
                url = str(slot["google_flights_url"])
                date_range_label = f"{slot.get('outbound_date')} -> {slot.get('return_date') or 'OW'}"

                if progress_callback:
                    progress_callback(
                        processed_count,
                        total_due,
                        f"Escaneando {dest_meta.get('origin')}->{dest_meta.get('destination')} ({s_label})...",
                        None,
                    )

                scrape_res: Optional[SlotScrapeResult] = None
                used_channel: Optional[str] = current_channel

                try:
                    scrape_res = scrape_date_slot(
                        url=url,
                        pooled_session=session,
                        proxy_url=current_channel,
                        date_range_label=date_range_label,
                        gl=dest_meta.get("gl"),
                    )
                    if session is not None and session.proxy_url != current_channel:
                        # Session switched in-place (e.g. proxy -> direct)
                        breaker.record_failure(current_channel, "proxy_switched_to_direct")
                        current_channel = session.proxy_url
                        used_channel = current_channel
                except Exception as slot_exc:
                    logger.warning(
                        "[%s] Slot id=%d scrape failed on (%s, %s): %s",
                        date_range_label,
                        s_id,
                        get_proxy_tier(current_channel),
                        redact_proxy_for_log(current_channel),
                        sanitize_log_text(slot_exc),
                    )
                    breaker.record_failure(current_channel, slot_exc)

                # If current channel failed to return prices, escalate once to next non-exhausted channel
                if (scrape_res is None or scrape_res.cheapest_tab_price <= 0) and len(channel_chain) > 1:
                    nxt_idx, nxt_ch, ok_adv = _advance_channel(active_idx)
                    if ok_adv:
                        logger.info(
                            "[%s] Escalating slot id=%d from (%s, %s) to (%s, %s).",
                            date_range_label,
                            s_id,
                            get_proxy_tier(current_channel),
                            redact_proxy_for_log(current_channel),
                            get_proxy_tier(nxt_ch),
                            redact_proxy_for_log(nxt_ch),
                        )
                        if session is not None:
                            session.close()
                            session = None
                        active_idx = nxt_idx
                        current_channel = nxt_ch
                        try:
                            session = PooledPatchrightSession(proxy_url=current_channel).start()
                            scrape_res = scrape_date_slot(
                                url=url,
                                pooled_session=session,
                                proxy_url=current_channel,
                                date_range_label=date_range_label,
                                gl=dest_meta.get("gl"),
                            )
                            used_channel = session.proxy_url if session is not None else current_channel
                        except Exception as esc_exc:
                            breaker.record_failure(current_channel, esc_exc)
                            logger.warning(
                                "[%s] Escalated scrape failed for slot id=%d: %s",
                                date_range_label,
                                s_id,
                                sanitize_log_text(esc_exc),
                            )

                processed_count += 1

                if scrape_res is not None and scrape_res.cheapest_tab_price > 0:
                    scraped_ok += 1
                    breaker.record_success(used_channel)
                    tier_key = (get_proxy_tier(used_channel), redact_account_for_log(get_proxy_account(used_channel)))
                    tier_telemetry[tier_key] += 1

                    res_dict = scrape_res.to_dict()
                    snap_stats = record_price_snapshot(
                        date_slot_id=s_id,
                        destination_id=d_id,
                        cheapest_tab_price=scrape_res.cheapest_tab_price,
                        best_tab_price=scrape_res.best_tab_price,
                        cheapest_airline=scrape_res.cheapest_airline,
                        cheapest_stops=scrape_res.cheapest_stops,
                        cheapest_duration=scrape_res.cheapest_duration,
                        cheapest_dep_arr=scrape_res.cheapest_dep_arr,
                        origin_airport=scrape_res.origin_airport,
                        destination_airport=scrape_res.destination_airport,
                        best_airline=scrape_res.best_airline,
                        best_stops=scrape_res.best_stops,
                        best_duration=scrape_res.best_duration,
                        best_origin_airport=scrape_res.best_origin_airport,
                        best_destination_airport=scrape_res.best_destination_airport,
                        is_self_transfer=scrape_res.is_self_transfer,
                        extraction_source=scrape_res.extraction_source,
                        raw_top_offers=scrape_res.raw_top_offers,
                    )

                    trigger_info = evaluate_slot_alert_triggers(
                        slot_meta=slot,
                        scrape_result=res_dict,
                        snapshot_stats=snap_stats,
                    )
                    if trigger_info is not None:
                        triggered_for_dest.append(trigger_info)

                    winning_route = (
                        f"{scrape_res.origin_airport}->{scrape_res.destination_airport}"
                        if scrape_res.origin_airport and scrape_res.destination_airport
                        else f"{dest_meta.get('origin')}->{dest_meta.get('destination')}"
                    )
                    item_summary = {
                        "slot_id": s_id,
                        "destination_id": d_id,
                        "destination_name": dest_meta.get("name"),
                        "label": s_label,
                        "outbound_date": slot.get("outbound_date"),
                        "return_date": slot.get("return_date"),
                        "status": "OK",
                        "winning_route": winning_route,
                        "origin_airport": scrape_res.origin_airport,
                        "destination_airport": scrape_res.destination_airport,
                        "cheapest_tab_price": scrape_res.cheapest_tab_price,
                        "best_tab_price": scrape_res.best_tab_price,
                        "cheapest_airline": scrape_res.cheapest_airline,
                        "cheapest_stops": scrape_res.cheapest_stops,
                        "cheapest_duration": scrape_res.cheapest_duration,
                        "is_self_transfer": scrape_res.is_self_transfer,
                        "extraction_source": scrape_res.extraction_source,
                        "cheapest_tab_status": scrape_res.cheapest_tab_status,
                        "network_tier": tier_key[0],
                        "network_channel": tier_key[1],
                        "triggered_alert": trigger_info is not None,
                    }
                    detailed_results.append(item_summary)

                    if progress_callback:
                        progress_callback(
                            processed_count,
                            total_due,
                            f"Completado {s_label}: Más Barato ${scrape_res.cheapest_tab_price:,.0f} | Mejor ${scrape_res.best_tab_price or scrape_res.cheapest_tab_price:,.0f}",
                            item_summary,
                        )
                else:
                    failed += 1
                    tier_key = (get_proxy_tier(used_channel), f"{redact_account_for_log(get_proxy_account(used_channel))}_failed")
                    tier_telemetry[tier_key] += 1
                    touch_slot_checked(s_id)
                    fail_summary = {
                        "slot_id": s_id,
                        "destination_id": d_id,
                        "destination_name": dest_meta.get("name"),
                        "label": s_label,
                        "outbound_date": slot.get("outbound_date"),
                        "return_date": slot.get("return_date"),
                        "status": "FAILED",
                        "cheapest_tab_price": None,
                        "best_tab_price": None,
                        "extraction_source": "failed",
                        "network_tier": tier_key[0],
                        "network_channel": tier_key[1],
                        "triggered_alert": False,
                    }
                    detailed_results.append(fail_summary)

                    if progress_callback:
                        progress_callback(
                            processed_count,
                            total_due,
                            f"Sin resultados en {s_label}",
                            fail_summary,
                        )

                # Anti-ban jitter between date slots
                if slot_idx < len(dest_slots) - 1:
                    time.sleep(random.uniform(2.5, 4.5))

            if triggered_for_dest:
                sent = dispatch_destination_digest(
                    destination_meta=dest_meta,
                    triggered_slots=triggered_for_dest,
                )
                if sent:
                    alerts_sent += 1
    finally:
        if session is not None:
            session.close()

    elapsed = round(time.perf_counter() - start_time, 2)
    telemetry_dict = {f"{tier}:{ch}": cnt for (tier, ch), cnt in sorted(tier_telemetry.items())}
    logger.info(
        "Batch finished in %.2fs | Due: %d | OK: %d | Failed: %d | Alerts: %d | Telemetry: %s",
        elapsed,
        total_due,
        scraped_ok,
        failed,
        alerts_sent,
        telemetry_dict,
    )
    return {
        "total_due": total_due,
        "scraped_ok": scraped_ok,
        "failed": failed,
        "alerts_sent": alerts_sent,
        "elapsed_seconds": elapsed,
        "tier_telemetry": telemetry_dict,
        "exhausted_accounts": sorted(redact_account_for_log(a) for a in breaker.exhausted_accounts),
        "results": detailed_results,
    }


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FlightPulse Matrix v4.0 — Sharded Anti-Ban Queue Dispatcher"
    )
    parser.add_argument(
        "--max-slots",
        type=int,
        default=MAX_SLOTS_PER_RUN,
        help=f"Maximum number of due date_slots to process in this run (default: {MAX_SLOTS_PER_RUN})",
    )
    parser.add_argument(
        "--force-all",
        action="store_true",
        help="Ignore check_interval_minutes and force-scrape active slots up to --max-slots",
    )
    parser.add_argument(
        "--destination-id",
        type=int,
        default=None,
        help="Only check date_slots belonging to this destination ID",
    )
    parser.add_argument(
        "--slot-id",
        type=int,
        default=None,
        help="Only check a single specific date_slot ID",
    )
    parser.add_argument(
        "--init-db-only",
        action="store_true",
        help="Initialize PostgreSQL tables/indexes and exit without scraping",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    if args.init_db_only:
        init_db()
        return 0

    summary = run_sharded_dispatcher(
        max_slots=args.max_slots,
        force_all=args.force_all,
        destination_id=args.destination_id,
        slot_id=args.slot_id,
    )
    if summary["total_due"] > 0 and summary["scraped_ok"] == 0:
        logger.error("All %d attempted date slots failed to return prices.", summary["total_due"])
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
