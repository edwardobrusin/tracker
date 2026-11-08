"""
Sharded Anti-Ban Queue Dispatcher (`src/runner.py`) for FlightPulse Matrix v4.0.

Responsibilities:
  1. Never scrapes all 100 date slots in a single burst.
  2. Queries up to `max_slots` due `date_slots` ordered by `last_checked_at ASC NULLS FIRST`.
  3. Groups selected slots by `destination_id` and shards destinations across
     `PROXY_LIST` / `SOCKS5_PROXY` (Cloudflare WARP / Webshare) with direct fallback.
  4. Pools a single `patchright` browser instance per proxy shard, resetting cookies
     and re-injecting the `SOCS` consent cookie between destinations.
  5. Persists dual-tab (`cheapest_tab_price` + `best_tab_price`) snapshots and sends
     ONE consolidated Telegram digest alert per destination.
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
from src.config import MAX_SLOTS_PER_RUN, get_proxy_pool
from src.db import (
    fetch_due_date_slots,
    init_db,
    record_price_snapshot,
    touch_slot_checked,
)
from src.scraper import PooledPatchrightSession, SlotScrapeResult, scrape_date_slot

logger = logging.getLogger("flightpulse.runner")


def run_sharded_dispatcher(
    max_slots: int = MAX_SLOTS_PER_RUN,
    force_all: bool = False,
    destination_id: Optional[int] = None,
    slot_id: Optional[int] = None,
    progress_callback: Optional[Callable[[int, int, str, Optional[dict[str, Any]]], None]] = None,
) -> dict[str, Any]:
    """
    Execute a sharded anti-ban scraping batch across due `date_slots`.

    Returns a summary dictionary containing counts of checked slots, successful scrapes,
    triggered alerts, and detailed per-slot results.
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
                "target_price": slot.get("target_price"),
                "cooldown_hours": slot.get("cooldown_hours"),
                "filter_config": slot.get("filter_config") or {},
            }
        slots_by_dest[d_id].append(slot)

    # Assign each destination to a proxy from the verified pool (round-robin sharding)
    proxy_pool = get_proxy_pool()
    proxy_groups: dict[Optional[str], list[int]] = collections.OrderedDict()
    for idx, d_id in enumerate(slots_by_dest.keys()):
        assigned_proxy = proxy_pool[idx % len(proxy_pool)]
        if assigned_proxy not in proxy_groups:
            proxy_groups[assigned_proxy] = []
        proxy_groups[assigned_proxy].append(d_id)

    logger.info(
        "Dispatching %d due date_slots across %d destination(s) using %d proxy shard(s).",
        total_due,
        len(slots_by_dest),
        len(proxy_groups),
    )

    scraped_ok = 0
    failed = 0
    alerts_sent = 0
    processed_count = 0
    detailed_results: list[dict[str, Any]] = []

    for proxy_url, dest_ids in proxy_groups.items():
        session: Optional[PooledPatchrightSession] = None
        try:
            try:
                session = PooledPatchrightSession(proxy_url=proxy_url).start()
            except Exception as browser_init_exc:
                logger.warning(
                    "Could not start Patchright browser on proxy=%s (%s). Trying direct browser or Layer 3 SSR.",
                    proxy_url,
                    browser_init_exc,
                )
                if proxy_url is not None:
                    try:
                        session = PooledPatchrightSession(proxy_url=None).start()
                    except Exception:
                        session = None

            for dest_idx, d_id in enumerate(dest_ids):
                if session is not None and dest_idx > 0:
                    session.reset_for_new_destination()

                dest_slots = slots_by_dest[d_id]
                dest_meta = dest_meta_map[d_id]
                triggered_for_dest: list[dict[str, Any]] = []

                logger.info(
                    "Scraping destination [%d] %s (%s -> %s): %d due slot(s) via proxy=%s",
                    d_id,
                    dest_meta.get("name"),
                    dest_meta.get("origin"),
                    dest_meta.get("destination"),
                    len(dest_slots),
                    proxy_url or "direct",
                )

                for slot_idx, slot in enumerate(dest_slots):
                    s_id = int(slot["id"])
                    s_label = slot.get("label") or f"{slot.get('outbound_date')} -> {slot.get('return_date')}"
                    url = str(slot["google_flights_url"])

                    if progress_callback:
                        progress_callback(
                            processed_count,
                            total_due,
                            f"Escaneando {dest_meta.get('origin')}->{dest_meta.get('destination')} ({s_label})...",
                            None,
                        )

                    scrape_res: Optional[SlotScrapeResult] = scrape_date_slot(
                        url=url,
                        pooled_session=session,
                        proxy_url=proxy_url,
                    )

                    processed_count += 1

                    if scrape_res is not None and scrape_res.cheapest_tab_price > 0:
                        scraped_ok += 1
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
                            best_airline=scrape_res.best_airline,
                            best_stops=scrape_res.best_stops,
                            best_duration=scrape_res.best_duration,
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

                        item_summary = {
                            "slot_id": s_id,
                            "destination_id": d_id,
                            "destination_name": dest_meta.get("name"),
                            "label": s_label,
                            "outbound_date": slot.get("outbound_date"),
                            "return_date": slot.get("return_date"),
                            "status": "OK",
                            "cheapest_tab_price": scrape_res.cheapest_tab_price,
                            "best_tab_price": scrape_res.best_tab_price,
                            "cheapest_airline": scrape_res.cheapest_airline,
                            "cheapest_stops": scrape_res.cheapest_stops,
                            "cheapest_duration": scrape_res.cheapest_duration,
                            "is_self_transfer": scrape_res.is_self_transfer,
                            "extraction_source": scrape_res.extraction_source,
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

                    # Anti-ban jitter between date slots inside the same shard
                    if slot_idx < len(dest_slots) - 1:
                        time.sleep(random.uniform(1.1, 2.4))

                # Consolidated Destination Digest Alert (ONE grouped Telegram card per destination)
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
    logger.info(
        "Batch finished in %.2fs | Due: %d | OK: %d | Failed: %d | Consolidated Digest Alerts: %d",
        elapsed,
        total_due,
        scraped_ok,
        failed,
        alerts_sent,
    )
    return {
        "total_due": total_due,
        "scraped_ok": scraped_ok,
        "failed": failed,
        "alerts_sent": alerts_sent,
        "elapsed_seconds": elapsed,
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
