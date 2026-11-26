"""
Sharded Anti-Ban Queue Dispatcher (`src/runner.py`) for FlightPulse Matrix v5.0.

Architecture (Strict 3-Phase RAM-Buffered Execution):
  - Phase 1 (Single Read & Disconnect — ~1s):
      Captures `batch_started_at`, runs `init_db()` (protected by PostgreSQL advisory lock),
      calls `fetch_shard_due_work_package(...)` to deterministically partition active
      destinations into `total_shards` disjoint buckets via `assign_destinations_to_shard`,
      and immediately calls `close_db_connection()` (`[NEON_SLEEP]`) so Neon compute can sleep.
  - Phase 2 (100% In-RAM Multi-GL Scraping — 0 DB calls):
      Competes each due slot across `gl_pool` (e.g. MX, IT, DE) via `scrape_date_slot_multi_gl`,
      reusing a single `PooledPatchrightSession` and prioritizing Nivel 1 (WARP) -> Nivel 2
      (Direct) -> Nivel 3 (Interleaved Proxy Pool). Accumulates winners in `ram_results`.
      Registers SIGTERM/SIGINT handlers + `try ... finally` so partial RAM buffers always flush.
  - Phase 3 (Atomic Batch Flush + Consolidated Alerts + Close — ~2s):
      Reconnects once to Neon, flushes `price_snapshots` and `date_slots` (`last_checked_at =
      batch_started_at`, `winning_gl`, `last_winning_gl`, `all_time_low_gl`) via
      `flush_shard_results_batch`, evaluates triggers against Phase 1 baselines, sends
      consolidated Telegram digests, batch-inserts `alert_logs`, and closes the DB connection.
"""

from __future__ import annotations

import argparse
import collections
import datetime
import logging
import random
import signal
import sys
import threading
import time
from typing import Any, Callable, Optional

from src.alerts import (
    _is_slot_future_release,
    evaluate_slot_alert_triggers,
    format_consolidated_destination_digest,
    send_telegram_message,
)
from src.config import (
    DEFAULT_GL,
    DEFAULT_GL_POOL,
    DUE_GRACE_MINUTES,
    MAX_SLOTS_PER_RUN,
    get_proxy_account,
    get_proxy_pool,
    get_proxy_tier,
    parse_gl_pool,
    redact_account_for_log,
    redact_proxy_for_log,
    sanitize_log_text,
)
from src.db import (
    assign_destinations_to_shard,
    close_db_connection,
    fetch_shard_due_work_package,
    flush_shard_results_batch,
    init_db,
    record_alerts_batch,
)
from src.scraper import (
    PooledPatchrightSession,
    SlotScrapeResult,
    _effective_result_price,
    _is_proxy_or_tunnel_error,
    _is_quota_or_auth_error,
    scrape_date_slot_multi_gl,
)

logger = logging.getLogger("flightpulse.runner")

__all__ = [
    "ProxyCircuitBreaker",
    "assign_destinations_to_shard",
    "run_sharded_dispatcher",
    "parse_args",
    "main",
]


class _GracefulShutdown(BaseException):
    """Internal control-flow signal raised on SIGTERM/SIGINT to flush RAM buffer immediately."""


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
    shard_index: int = 0,
    total_shards: int = 1,
    dry_run: bool = False,
    progress_callback: Optional[Callable[[int, int, str, Optional[dict[str, Any]]], None]] = None,
) -> dict[str, Any]:
    """
    Execute a 3-phase RAM-buffered Multi-GL scraping batch across due `date_slots`
    for `shard_index` / `total_shards`.
    """
    start_time = time.perf_counter()

    # ==========================================================================
    # PHASE 1: SINGLE READ & IMMEDIATE DISCONNECT (~1s)
    # ==========================================================================
    batch_started_at = datetime.datetime.now(datetime.timezone.utc)
    init_db()
    work_pkg = fetch_shard_due_work_package(
        shard_index=shard_index,
        total_shards=total_shards,
        force_all=force_all,
        max_slots=max_slots,
        due_grace_minutes=DUE_GRACE_MINUTES,
        destination_id=destination_id,
        slot_id=slot_id,
    )
    close_db_connection()

    due_slots: list[dict[str, Any]] = work_pkg["due_slots"]
    total_due = len(due_slots)
    logger.info(
        "[NEON_SLEEP] Disconnected DB before scraping phase (%d due slots in RAM | shard=%d/%d).",
        total_due,
        shard_index,
        total_shards,
    )

    if total_due == 0:
        logger.info("No date_slots are currently due for checking in shard %d/%d.", shard_index, total_shards)
        return {
            "shard_index": shard_index,
            "total_shards": total_shards,
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
    slot_meta_by_id: dict[int, dict[str, Any]] = {}

    for slot in due_slots:
        s_id = int(slot["id"])
        d_id = int(slot["destination_id"])
        slot_meta_by_id[s_id] = slot
        if d_id not in slots_by_dest:
            slots_by_dest[d_id] = []
            pool_list = parse_gl_pool(slot.get("gl_pool") or slot.get("gl"), fallback=DEFAULT_GL_POOL)
            dest_meta_map[d_id] = {
                "id": d_id,
                "name": slot.get("destination_name"),
                "origin": slot.get("origin"),
                "destination": slot.get("destination_code"),
                "currency": slot.get("currency"),
                "gl": str(slot.get("gl") or pool_list[0]).strip().upper(),
                "gl_pool": pool_list,
                "target_price": slot.get("target_price"),
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
        "Shard %d/%d dispatching %d due date_slots across %d destination(s) | primary=(%s, %s) | pool_len=%d.",
        shard_index,
        total_shards,
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
    detailed_by_slot_id: dict[int, dict[str, Any]] = {}
    ram_results: list[dict[str, Any]] = []
    ram_failed_slot_ids: list[int] = []

    def _advance_channel(current_pos: int) -> tuple[int, Optional[str], bool]:
        for next_idx in range(current_pos + 1, len(channel_chain)):
            cand = channel_chain[next_idx]
            if breaker.is_available(cand):
                return next_idx, cand, True
        return current_pos, None, False

    # ==========================================================================
    # PHASE 2: 100% IN-RAM MULTI-GL SCRAPING (ZERO DB CALLS)
    # ==========================================================================
    is_main_thread = threading.current_thread() is threading.main_thread()
    prev_sigterm: Any = None
    prev_sigint: Any = None
    interrupted = False

    def _signal_handler(signum: int, frame: Any) -> None:
        logger.warning(
            "Received signal %d during Phase 2 scraping; aborting loop to flush %d RAM result(s).",
            signum,
            len(ram_results),
        )
        raise _GracefulShutdown()

    if is_main_thread:
        prev_sigterm = signal.getsignal(signal.SIGTERM)
        prev_sigint = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)

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
            dest_gl_pool: list[str] = dest_meta["gl_pool"]

            logger.info(
                "Scraping destination [%d] %s (%s -> %s, gl_pool=%s): %d due slot(s) via (%s, %s)",
                d_id,
                dest_meta.get("name"),
                dest_meta.get("origin"),
                dest_meta.get("destination"),
                ",".join(dest_gl_pool),
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
                    scrape_res = scrape_date_slot_multi_gl(
                        url=url,
                        gl_pool=dest_gl_pool,
                        pooled_session=session,
                        proxy_url=current_channel,
                        date_range_label=date_range_label,
                    )
                    if session is not None and session.proxy_url != current_channel:
                        breaker.record_failure(current_channel, "proxy_switched_to_direct")
                        current_channel = session.proxy_url
                        used_channel = current_channel
                except (_GracefulShutdown, KeyboardInterrupt):
                    raise
                except Exception as slot_exc:
                    logger.warning(
                        "[%s] Slot id=%d Multi-GL scrape failed on (%s, %s): %s",
                        date_range_label,
                        s_id,
                        get_proxy_tier(current_channel),
                        redact_proxy_for_log(current_channel),
                        sanitize_log_text(slot_exc),
                    )
                    breaker.record_failure(current_channel, slot_exc)

                eff_price = _effective_result_price(scrape_res)

                # If current channel failed to return prices, escalate once to next non-exhausted channel
                if (scrape_res is None or eff_price is None or eff_price <= 0) and len(channel_chain) > 1:
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
                            scrape_res = scrape_date_slot_multi_gl(
                                url=url,
                                gl_pool=dest_gl_pool,
                                pooled_session=session,
                                proxy_url=current_channel,
                                date_range_label=date_range_label,
                            )
                            used_channel = session.proxy_url if session is not None else current_channel
                            eff_price = _effective_result_price(scrape_res)
                        except (_GracefulShutdown, KeyboardInterrupt):
                            raise
                        except Exception as esc_exc:
                            breaker.record_failure(current_channel, esc_exc)
                            logger.warning(
                                "[%s] Escalated Multi-GL scrape failed for slot id=%d: %s",
                                date_range_label,
                                s_id,
                                sanitize_log_text(esc_exc),
                            )

                processed_count += 1

                if scrape_res is not None and eff_price is not None and eff_price > 0:
                    scraped_ok += 1
                    breaker.record_success(used_channel)
                    tier_key = (get_proxy_tier(used_channel), redact_account_for_log(get_proxy_account(used_channel)))
                    tier_telemetry[tier_key] += 1

                    winning_gl = str(
                        scrape_res.winning_gl or scrape_res.gl or dest_meta["gl"] or DEFAULT_GL
                    ).strip().upper()
                    scraped_at_ts = datetime.datetime.now(datetime.timezone.utc)

                    res_dict = scrape_res.to_dict()
                    res_dict["winning_gl"] = winning_gl

                    ram_results.append(
                        {
                            "date_slot_id": s_id,
                            "destination_id": d_id,
                            "scraped_at": scraped_at_ts,
                            "cheapest_tab_price": float(eff_price),
                            "best_tab_price": scrape_res.best_tab_price,
                            "cheapest_airline": scrape_res.cheapest_airline,
                            "cheapest_stops": scrape_res.cheapest_stops,
                            "cheapest_duration": scrape_res.cheapest_duration,
                            "cheapest_dep_arr": scrape_res.cheapest_dep_arr,
                            "origin_airport": scrape_res.origin_airport,
                            "destination_airport": scrape_res.destination_airport,
                            "best_airline": scrape_res.best_airline,
                            "best_stops": scrape_res.best_stops,
                            "best_duration": scrape_res.best_duration,
                            "best_origin_airport": scrape_res.best_origin_airport,
                            "best_destination_airport": scrape_res.best_destination_airport,
                            "is_self_transfer": scrape_res.is_self_transfer,
                            "winning_gl": winning_gl,
                            "extraction_source": scrape_res.extraction_source,
                            "raw_top_offers": scrape_res.raw_top_offers,
                            "scrape_dict": res_dict,
                        }
                    )

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
                        "winning_gl": winning_gl,
                        "winning_route": winning_route,
                        "origin_airport": scrape_res.origin_airport,
                        "destination_airport": scrape_res.destination_airport,
                        "cheapest_tab_price": float(eff_price),
                        "best_tab_price": scrape_res.best_tab_price,
                        "cheapest_airline": scrape_res.cheapest_airline,
                        "cheapest_stops": scrape_res.cheapest_stops,
                        "cheapest_duration": scrape_res.cheapest_duration,
                        "is_self_transfer": scrape_res.is_self_transfer,
                        "extraction_source": scrape_res.extraction_source,
                        "cheapest_tab_status": scrape_res.cheapest_tab_status,
                        "network_tier": tier_key[0],
                        "network_channel": tier_key[1],
                        "triggered_alert": False,
                    }
                    detailed_results.append(item_summary)
                    detailed_by_slot_id[s_id] = item_summary

                    if progress_callback:
                        progress_callback(
                            processed_count,
                            total_due,
                            f"Completado {s_label} ({winning_gl}): Más Barato ${eff_price:,.0f} | Mejor ${(scrape_res.best_tab_price or eff_price):,.0f}",
                            item_summary,
                        )
                else:
                    failed += 1
                    tier_key = (get_proxy_tier(used_channel), f"{redact_account_for_log(get_proxy_account(used_channel))}_failed")
                    tier_telemetry[tier_key] += 1
                    ram_failed_slot_ids.append(s_id)
                    fail_summary = {
                        "slot_id": s_id,
                        "destination_id": d_id,
                        "destination_name": dest_meta.get("name"),
                        "label": s_label,
                        "outbound_date": slot.get("outbound_date"),
                        "return_date": slot.get("return_date"),
                        "status": "FAILED",
                        "winning_gl": None,
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
    except (_GracefulShutdown, KeyboardInterrupt):
        interrupted = True
        logger.warning("Phase 2 interrupted; flushing %d buffered RAM results before exit.", len(ram_results))
    finally:
        if is_main_thread:
            try:
                if prev_sigterm is not None:
                    signal.signal(signal.SIGTERM, prev_sigterm)
                if prev_sigint is not None:
                    signal.signal(signal.SIGINT, prev_sigint)
            except Exception:
                pass
        if session is not None:
            session.close()

        # ======================================================================
        # PHASE 3: ATOMIC BATCH FLUSH + CONSOLIDATED ALERTS + CLOSE (~2s)
        # ======================================================================
        try:
            if not dry_run and (ram_results or ram_failed_slot_ids):
                flush_stats = flush_shard_results_batch(
                    ram_results=ram_results,
                    batch_started_at=batch_started_at,
                    failed_slot_ids=ram_failed_slot_ids,
                )
                logger.info(
                    "[NEON_FLUSH] Batch flushed: snapshots=%d, slots_updated=%d, failed_touched=%d",
                    flush_stats["snapshots_inserted"],
                    flush_stats["slots_updated"],
                    flush_stats["failed_touched"],
                )

                results_by_dest: dict[int, list[dict[str, Any]]] = collections.OrderedDict()
                for item in ram_results:
                    results_by_dest.setdefault(int(item["destination_id"]), []).append(item)

                sent_alert_batches: list[dict[str, Any]] = []
                for d_id, dest_ram_items in results_by_dest.items():
                    dest_meta = dest_meta_map[d_id]
                    triggered_for_dest: list[dict[str, Any]] = []

                    for item in dest_ram_items:
                        s_id = int(item["date_slot_id"])
                        slot_meta = slot_meta_by_id[s_id]
                        baseline_stats = {
                            "historical_min_price": slot_meta.get("historical_min_price"),
                            "last_alerted_at": slot_meta.get("last_alerted_at"),
                            "last_alerted_price": slot_meta.get("last_alerted_price"),
                            "prev_effective_price": slot_meta.get("prev_effective_price"),
                            "last_atl_alerted_price": slot_meta.get("last_atl_alerted_price"),
                        }
                        trig = evaluate_slot_alert_triggers(
                            slot_meta=slot_meta,
                            scrape_result=item["scrape_dict"],
                            snapshot_stats=baseline_stats,
                        )
                        if trig is not None:
                            triggered_for_dest.append(trig)
                            if s_id in detailed_by_slot_id:
                                detailed_by_slot_id[s_id]["triggered_alert"] = True

                    eligible_slots = [s for s in triggered_for_dest if not _is_slot_future_release(s)]
                    if eligible_slots:
                        msg_html = format_consolidated_destination_digest(
                            destination_meta=dest_meta,
                            triggered_slots=eligible_slots,
                            is_test=False,
                        )
                        if msg_html:
                            sent_ok, status_detail = send_telegram_message(msg_html)
                            if sent_ok:
                                alerts_sent += 1
                                sent_alert_batches.append(
                                    {
                                        "destination_id": d_id,
                                        "triggered_slots": eligible_slots,
                                        "message_preview": msg_html,
                                    }
                                )
                            else:
                                logger.error(
                                    "Failed to send Telegram digest for destination_id=%d: %s",
                                    d_id,
                                    status_detail,
                                )

                if sent_alert_batches:
                    record_alerts_batch(sent_alert_batches)
        finally:
            close_db_connection()

    elapsed = round(time.perf_counter() - start_time, 2)
    telemetry_dict = {f"{tier}:{ch}": cnt for (tier, ch), cnt in sorted(tier_telemetry.items())}
    logger.info(
        "Shard %d/%d finished in %.2fs | Due: %d | OK: %d | Failed: %d | Alerts: %d | Interrupted: %s | Telemetry: %s",
        shard_index,
        total_shards,
        elapsed,
        total_due,
        scraped_ok,
        failed,
        alerts_sent,
        interrupted,
        telemetry_dict,
    )
    return {
        "shard_index": shard_index,
        "total_shards": total_shards,
        "total_due": total_due,
        "scraped_ok": scraped_ok,
        "failed": failed,
        "alerts_sent": alerts_sent,
        "interrupted": interrupted,
        "elapsed_seconds": elapsed,
        "tier_telemetry": telemetry_dict,
        "exhausted_accounts": sorted(redact_account_for_log(a) for a in breaker.exhausted_accounts),
        "results": detailed_results,
    }


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="FlightPulse Matrix v5.0 — 6-Shard Multi-GL RAM-Buffered Queue Dispatcher"
    )
    parser.add_argument(
        "--max-slots",
        type=int,
        default=MAX_SLOTS_PER_RUN,
        help=f"Maximum number of due date_slots to process in this shard (default: {MAX_SLOTS_PER_RUN})",
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
        "--shard-index",
        type=int,
        default=0,
        help="0-based index of this runner shard (default: 0)",
    )
    parser.add_argument(
        "--total-shards",
        type=int,
        default=1,
        help="Total number of parallel shards (default: 1; GitHub Actions matrix uses 6)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run scraping without writing to Neon DB or sending Telegram alerts",
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
        close_db_connection()
        return 0

    summary = run_sharded_dispatcher(
        max_slots=args.max_slots,
        force_all=args.force_all,
        destination_id=args.destination_id,
        slot_id=args.slot_id,
        shard_index=args.shard_index,
        total_shards=args.total_shards,
        dry_run=args.dry_run,
    )
    if summary["total_due"] > 0 and summary["scraped_ok"] == 0:
        logger.error("All %d attempted date slots failed to return prices.", summary["total_due"])
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
