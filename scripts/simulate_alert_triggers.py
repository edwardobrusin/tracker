"""
Audit active Neon DB price bounds (MIN / MAX / Meta) and run an end-to-end,
100% lossless simulation of Telegram alert triggers (`Meta`, `Bajo Histórico`,
and `<2%` anti-noise suppression) with guaranteed rollback in `finally`.
"""

from __future__ import annotations

import datetime
import logging
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.alerts import (
    _is_slot_future_release,
    evaluate_slot_alert_triggers,
    format_consolidated_destination_digest,
    send_telegram_message,
)
from src.db import (
    clear_db_cache,
    get_connection,
    list_date_slots,
    list_destinations,
    record_alert_and_update_slots,
    record_price_snapshot,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("flightpulse.sim")


def audit_db_summary() -> list[dict[str, Any]]:
    """Read-only audit of all destinations and <=330d active slots in Neon PostgreSQL."""
    dests = list_destinations(active_only=False)
    all_slots = list_date_slots(active_only=False)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    date_slot_id,
                    MIN(cheapest_tab_price) AS snap_min,
                    MAX(cheapest_tab_price) AS snap_max,
                    COUNT(*) AS snap_count
                FROM price_snapshots
                GROUP BY date_slot_id;
                """
            )
            snap_stats_by_slot = {int(r["date_slot_id"]): dict(r) for r in cur.fetchall()}

    rows: list[dict[str, Any]] = []
    print("\n" + "=" * 128)
    print(
        f"{'ID':>3} | {'DESTINO':<28} | {'META':>10} | {'ACT':>3} | {'GL POOL':<9} | "
        f"{'MIN HIST BD':>12} | {'MAX HIST BD':>12} | {'ULT CHEAPEST':>12} | {'SLOTS c/DATOS':>13}"
    )
    print("-" * 128)

    for d in dests:
        d_id = int(d["id"])
        active_slots = [
            s for s in all_slots
            if int(s["destination_id"]) == d_id
            and bool(s.get("is_active", True))
            and not _is_slot_future_release(s)
        ]

        mins: list[float] = []
        maxs: list[float] = []
        latests: list[float] = []
        slots_with_data = 0

        for s in active_slots:
            sid = int(s["id"])
            st = snap_stats_by_slot.get(sid)
            s_min = float(st["snap_min"]) if st and st.get("snap_min") is not None else None
            s_max = float(st["snap_max"]) if st and st.get("snap_max") is not None else None
            atl = float(s["all_time_low_price"]) if s.get("all_time_low_price") is not None else None
            latest = float(s["latest_cheapest_price"]) if s.get("latest_cheapest_price") is not None else None

            if atl is not None and (s_min is None or atl < s_min):
                s_min = atl
            if s_min is not None:
                mins.append(s_min)
            if s_max is not None:
                maxs.append(s_max)
            elif latest is not None:
                maxs.append(latest)
            if latest is not None:
                latests.append(latest)
            if (st and int(st.get("snap_count") or 0) > 0) or latest is not None:
                slots_with_data += 1

        d_min = min(mins) if mins else None
        d_max = max(maxs) if maxs else None
        d_last_min = min(latests) if latests else None
        alerts_on = bool(d.get("is_active", True))
        target_p = float(d.get("target_price") or 0)
        gl_pool_str = str(d.get("gl_pool") or d.get("gl") or "MX,IT,DE")

        print(
            f"{d_id:>3} | {str(d.get('name') or '')[:28]:<28} | "
            f"${target_p:>9,.0f} | {'YES' if alerts_on else 'NO ':>3} | {gl_pool_str:<9} | "
            f"{f'${d_min:,.0f}' if d_min is not None else 'N/A':>12} | "
            f"{f'${d_max:,.0f}' if d_max is not None else 'N/A':>12} | "
            f"{f'${d_last_min:,.0f}' if d_last_min is not None else 'N/A':>12} | "
            f"{slots_with_data:>5}/{len(active_slots):<7}"
        )
        rows.append(
            {
                "destination": d,
                "active_slots": active_slots,
                "min_hist": d_min,
                "max_hist": d_max,
                "latest_min": d_last_min,
                "slots_with_data": slots_with_data,
            }
        )
    print("=" * 128)
    return rows


def _fetch_table_counts() -> tuple[int, int]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM price_snapshots;")
            snaps = int((cur.fetchone() or {}).get("c") or 0)
            cur.execute("SELECT COUNT(*) AS c FROM alert_logs;")
            logs = int((cur.fetchone() or {}).get("c") or 0)
    return snaps, logs


def _backup_slot_and_dest(slot_id: int, dest_id: int) -> tuple[dict[str, Any], dict[str, Any]]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM date_slots WHERE id = %s;", (slot_id,))
            slot_row = dict(cur.fetchone() or {})
            cur.execute("SELECT * FROM destinations WHERE id = %s;", (dest_id,))
            dest_row = dict(cur.fetchone() or {})
    return slot_row, dest_row


def _restore_slot_and_dest(slot_backup: dict[str, Any], dest_backup: dict[str, Any]) -> None:
    cols = [
        "last_checked_at",
        "last_alerted_at",
        "last_alerted_price",
        "latest_cheapest_price",
        "latest_best_price",
        "latest_airline",
        "latest_stops",
        "latest_duration",
        "latest_origin_airport",
        "latest_destination_airport",
        "latest_best_origin_airport",
        "latest_best_destination_airport",
        "latest_is_self_transfer",
        "all_time_low_price",
    ]
    assign_sql = ", ".join(f"{c} = %s" for c in cols)
    vals = [slot_backup.get(c) for c in cols]
    vals.append(slot_backup["id"])

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE date_slots SET {assign_sql} WHERE id = %s;", tuple(vals))
            cur.execute(
                "UPDATE destinations SET target_price = %s WHERE id = %s;",
                (dest_backup.get("target_price"), dest_backup["id"]),
            )
        conn.commit()
    clear_db_cache()


def run_trigger_simulations(audit_rows: list[dict[str, Any]]) -> None:
    """Execute Cases 1 (Meta), 2 (Bajo Histórico), and 3 (-0.5% anti-noise) with guaranteed DB restore."""
    chosen_dest: Optional[dict[str, Any]] = None
    chosen_slot: Optional[dict[str, Any]] = None

    # Prefer a <=330d active slot that already has snapshot history
    for entry in audit_rows:
        d = entry["destination"]
        if not d.get("is_active"):
            continue
        for s in entry["active_slots"]:
            if s.get("all_time_low_price") is not None and s.get("latest_cheapest_price") is not None:
                chosen_dest = d
                chosen_slot = s
                break
        if chosen_slot is not None:
            break

    if not chosen_dest or not chosen_slot:
        raise RuntimeError("No active <=330d slot with snapshot history found for simulation.")

    dest_id = int(chosen_dest["id"])
    slot_id = int(chosen_slot["id"])

    counts_before = _fetch_table_counts()
    slot_backup, dest_backup = _backup_slot_and_dest(slot_id, dest_id)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(al.cheapest_price, al.best_price) AS last_atl
                FROM alert_logs al
                WHERE al.date_slot_id = %s
                  AND POSITION('ALL_TIME_LOW' IN al.trigger_type) > 0
                ORDER BY al.sent_at DESC
                LIMIT 1;
                """,
                (slot_id,),
            )
            r_atl = cur.fetchone()
            real_last_atl = float(r_atl["last_atl"]) if r_atl and r_atl.get("last_atl") is not None else None
            cur.execute(
                "SELECT MIN(cheapest_tab_price) AS smin FROM price_snapshots WHERE date_slot_id = %s;",
                (slot_id,),
            )
            r_min = cur.fetchone()
            real_snap_min = float(r_min["smin"]) if r_min and r_min.get("smin") is not None else None

    real_atl = float(slot_backup["all_time_low_price"])
    hist_min = min(real_atl, real_snap_min) if real_snap_min is not None else real_atl
    ref_low = real_last_atl if (real_last_atl is not None and real_last_atl > 0) else hist_min
    orig_target = float(dest_backup["target_price"])

    inserted_snapshot_ids: list[int] = []
    inserted_alert_ids: list[int] = []

    print(
        f"\nSelected simulation target: dest_id={dest_id} ('{chosen_dest['name']}'), "
        f"slot_id={slot_id} ({chosen_slot['outbound_date']} -> {chosen_slot['return_date']}), "
        f"real_target=${orig_target:,.0f}, hist_min=${hist_min:,.0f}, ref_low=${ref_low:,.0f}"
    )

    try:
        def _run_case(
            case_name: str,
            sim_price: float,
            temp_target: float,
            clear_last_alert: bool,
        ) -> dict[str, Any]:
            # Reset slot & destination state to exact baseline before each scenario
            _restore_slot_and_dest(slot_backup, dest_backup)
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE destinations SET target_price = %s WHERE id = %s;", (Decimal(str(temp_target)), dest_id))
                    if clear_last_alert:
                        cur.execute(
                            "UPDATE date_slots SET last_alerted_at = NULL, last_alerted_price = NULL WHERE id = %s;",
                            (slot_id,),
                        )
                conn.commit()
            clear_db_cache()

            snap_stats = record_price_snapshot(
                date_slot_id=slot_id,
                destination_id=dest_id,
                cheapest_tab_price=sim_price,
                best_tab_price=round(sim_price + 1850.0, 2),
                cheapest_airline=str(slot_backup.get("latest_airline") or "Aerolínea Demo"),
                cheapest_stops=str(slot_backup.get("latest_stops") or "1 escala"),
                cheapest_duration=str(slot_backup.get("latest_duration") or "14 h 10 min"),
                origin_airport=str(slot_backup.get("latest_origin_airport") or "MEX"),
                destination_airport=str(slot_backup.get("latest_destination_airport") or "MAD"),
                best_airline="Iberia",
                best_stops="Directo",
                best_duration="10 h 35 min",
                best_origin_airport="MEX",
                best_destination_airport="MAD",
                extraction_source="sim_trigger_test",
            )
            inserted_snapshot_ids.append(int(snap_stats["snapshot_id"]))

            slot_meta = {
                **chosen_slot,
                "target_price": temp_target,
                "filter_config": chosen_dest.get("filter_config") or {},
            }
            if clear_last_alert:
                slot_meta["last_alerted_at"] = None
                slot_meta["last_alerted_price"] = None

            scrape_dict = {
                "cheapest_tab_price": sim_price,
                "best_tab_price": round(sim_price + 1850.0, 2),
                "cheapest_airline": str(slot_backup.get("latest_airline") or "Aerolínea Demo"),
                "cheapest_stops": str(slot_backup.get("latest_stops") or "1 escala"),
                "cheapest_duration": str(slot_backup.get("latest_duration") or "14 h 10 min"),
                "origin_airport": str(slot_backup.get("latest_origin_airport") or "MEX"),
                "destination_airport": str(slot_backup.get("latest_destination_airport") or "MAD"),
                "best_airline": "Iberia",
                "best_stops": "Directo",
                "best_duration": "10 h 35 min",
                "best_origin_airport": "MEX",
                "best_destination_airport": "MAD",
                "is_self_transfer": False,
            }
            trig = evaluate_slot_alert_triggers(
                slot_meta=slot_meta,
                scrape_result=scrape_dict,
                snapshot_stats=snap_stats,
            )

            sent_ok = False
            sent_detail = "NOT_TRIGGERED"
            msg_html = ""
            if trig is not None:
                sim_dest_meta = {
                    **chosen_dest,
                    "name": f"[SIMULACIÓN] {chosen_dest['name']}",
                    "target_price": temp_target,
                }
                msg_html = format_consolidated_destination_digest(
                    destination_meta=sim_dest_meta,
                    triggered_slots=[trig],
                    is_test=False,
                )
                sent_ok, sent_detail = send_telegram_message(msg_html)
                if sent_ok:
                    log_ids = record_alert_and_update_slots(
                        destination_id=dest_id,
                        triggered_slots=[trig],
                        message_preview=msg_html,
                    )
                    inserted_alert_ids.extend(log_ids)

            return {
                "case": case_name,
                "sim_price": sim_price,
                "temp_target": temp_target,
                "trigger_type": trig.get("trigger_type") if trig else None,
                "sent_ok": sent_ok,
                "sent_detail": sent_detail,
                "first_line": msg_html.splitlines()[0] if msg_html else "",
            }

        # Case 1 — Solo Meta (<= target_price AND > hist_min / ref_low)
        temp_target_case1 = orig_target if orig_target > hist_min else round(hist_min * 1.15, 2)
        sim_price_case1 = round(hist_min * 1.05, 2)
        if sim_price_case1 > temp_target_case1:
            sim_price_case1 = round((hist_min + temp_target_case1) / 2.0, 2)
        res1 = _run_case("Case 1 (Solo Meta)", sim_price_case1, temp_target_case1, clear_last_alert=True)

        # Case 2 — Solo Bajo Histórico (-5% below ref_low, > target_price)
        sim_price_case2 = round(ref_low * 0.95, 2)
        temp_target_case2 = orig_target if orig_target < sim_price_case2 else round(sim_price_case2 * 0.70, 2)
        res2 = _run_case("Case 2 (Solo Bajo Histórico)", sim_price_case2, temp_target_case2, clear_last_alert=False)

        # Case 3 — Anti-ruido (-0.5% below ref_low, <2% threshold, > target_price)
        sim_price_case3 = round(ref_low * 0.995, 2)
        temp_target_case3 = orig_target if orig_target < sim_price_case3 else round(sim_price_case3 * 0.70, 2)
        res3 = _run_case("Case 3 (Anti-ruido -0.5%)", sim_price_case3, temp_target_case3, clear_last_alert=False)

        print("\n=== SIMULATION RESULTS ===")
        for r in (res1, res2, res3):
            print(
                f"{r['case']} | price=${r['sim_price']:,.2f} | target=${r['temp_target']:,.2f} "
                f"| trigger={r['trigger_type']} | telegram_sent={r['sent_ok']} ({r['sent_detail']}) "
                f"| header={r['first_line']}"
            )

        assert res1["trigger_type"] == "TARGET_BREACH", f"Expected TARGET_BREACH, got {res1['trigger_type']}"
        assert res2["trigger_type"] == "ALL_TIME_LOW", f"Expected ALL_TIME_LOW, got {res2['trigger_type']}"
        assert res3["trigger_type"] is None, f"Expected None for -0.5%, got {res3['trigger_type']}"
    finally:
        with get_connection() as conn:
            with conn.cursor() as cur:
                if inserted_alert_ids:
                    cur.execute("DELETE FROM alert_logs WHERE id = ANY(%s);", (inserted_alert_ids,))
                if inserted_snapshot_ids:
                    cur.execute("DELETE FROM price_snapshots WHERE id = ANY(%s);", (inserted_snapshot_ids,))
            conn.commit()
        _restore_slot_and_dest(slot_backup, dest_backup)

    counts_after = _fetch_table_counts()
    slot_after, dest_after = _backup_slot_and_dest(slot_id, dest_id)
    assert counts_before == counts_after, f"Row count mismatch! before={counts_before} after={counts_after}"
    assert slot_after["all_time_low_price"] == slot_backup["all_time_low_price"]
    assert slot_after["last_checked_at"] == slot_backup["last_checked_at"]
    assert slot_after["last_alerted_at"] == slot_backup["last_alerted_at"]
    assert slot_after["last_alerted_price"] == slot_backup["last_alerted_price"]
    assert dest_after["target_price"] == dest_backup["target_price"]
    print(
        f"\n[DB CLEANUP VERIFIED] counts_before={counts_before} == counts_after={counts_after} | "
        f"deleted snaps={len(inserted_snapshot_ids)}, logs={len(inserted_alert_ids)} | "
        f"all_time_low_price & target_price restored identically."
    )


def main() -> int:
    audit_rows = audit_db_summary()
    run_trigger_simulations(audit_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
