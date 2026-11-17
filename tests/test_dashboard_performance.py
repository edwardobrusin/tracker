"""
Comprehensive Functional, Regression, and Millisecond Performance Benchmark Suite
for FlightPulse Matrix v4.0 (`tests/test_dashboard_performance.py`).

Measures `elapsed_ms`, `sql_queries`, and `db_connects` across every action
available in the Streamlit dashboard and validates the scraper/parser fixes
and 20-proxy pool configuration.
"""

from __future__ import annotations

import base64
import datetime
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

# Ensure project root is on sys.path when executed directly via `python tests/...`
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from streamlit.testing.v1 import AppTest

from src.config import get_flight_window_status, get_proxy_pool
from src.db import (
    bulk_create_date_slots,
    clear_db_cache,
    connection_stats,
    create_destination,
    delete_destination,
    get_connection,
    get_dashboard_summary,
    get_recent_alert_logs,
    get_snapshots_for_destination,
    init_db,
    list_date_slots,
    list_destinations,
    reset_connection_stats,
    set_destination_active,
    update_destination,
)
from src.ds1_parser import (
    _parse_single_itinerary_entry,
    parse_flight_card_text,
    parse_price_number,
)
from src.tfs_builder import (
    FilterProfile,
    _parse_protobuf_fields,
    build_google_flights_url,
    decode_tfs_parameter,
    encode_tfs_protobuf,
    format_date_slot_label,
    generate_date_matrix,
    parse_pasted_urls,
)


@dataclass
class BenchmarkRow:
    """Single measured dashboard action benchmark result."""

    action: str
    elapsed_ms: float
    sql_queries: int
    db_connects: int
    status: str
    detail: str


RESULTS: list[BenchmarkRow] = []


def measure(action: str, fn: Callable[[], str]) -> Any:
    """Execute `fn`, capturing wall-clock milliseconds, SQL queries, and TCP connects."""
    before = connection_stats()
    t0 = time.perf_counter()
    detail = fn()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    after = connection_stats()
    sql_queries = after["total_queries"] - before["total_queries"]
    db_connects = after["total_connects"] - before["total_connects"]
    RESULTS.append(
        BenchmarkRow(
            action=action,
            elapsed_ms=elapsed_ms,
            sql_queries=sql_queries,
            db_connects=db_connects,
            status="PASS",
            detail=detail,
        )
    )
    return detail


def bench_1_db_connection_reuse() -> None:
    """Verify persistent thread-local connection reuse vs. cold TCP+TLS connect."""
    reset_connection_stats()

    def _cold_ping() -> str:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return "Cold TCP+TLS handshake + SELECT 1"

    def _warm_ping() -> str:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return "Warm pooled socket SELECT 1"

    measure("01a. db_connection_reuse (cold)", _cold_ping)
    measure("01b. db_connection_reuse (warm)", _warm_ping)
    warm_row = RESULTS[-1]
    assert warm_row.db_connects == 0, f"Expected 0 new connects on warm query, got {warm_row.db_connects}"


def bench_2_dashboard_summary_kpi() -> None:
    """Verify single-CTE cold summary (1 query) and cached summary (0 queries)."""
    init_db()
    clear_db_cache()

    def _cold_summary() -> str:
        summary = get_dashboard_summary()
        return f"dests={summary['active_destinations']}, slots={summary['active_date_slots']}"

    def _cached_summary() -> str:
        summary = get_dashboard_summary()
        return f"cached dests={summary['active_destinations']}, best_deal={bool(summary['best_deal'])}"

    measure("02a. dashboard_summary_kpi (cold CTE)", _cold_summary)
    cold_row = RESULTS[-1]
    assert cold_row.sql_queries == 1, f"Expected 1 CTE query on cold summary, got {cold_row.sql_queries}"

    measure("02b. dashboard_summary_kpi (cached)", _cached_summary)
    cached_row = RESULTS[-1]
    assert cached_row.sql_queries == 0, f"Expected 0 SQL queries on cached summary, got {cached_row.sql_queries}"


def bench_3_tab1_switch_destination() -> None:
    """Verify switching destinations in Tab 1 executes 0 extra SQL queries after initial load."""
    clear_db_cache()

    def _initial_load() -> str:
        dests = list_destinations(active_only=False)
        slots = list_date_slots(destination_id=None, active_only=False)
        return f"loaded {len(dests)} dests, {len(slots)} slots"

    def _switch_destinations() -> str:
        dests = list_destinations(active_only=False)
        total_rows = 0
        for d in dests:
            s_list = list_date_slots(destination_id=int(d["id"]), active_only=False)
            total_rows += len(s_list)
        all_slots = list_date_slots(destination_id=None, active_only=False)
        return f"switched {len(dests)} dests + global ({len(all_slots)} slots, {total_rows} filtered)"

    measure("03a. tab1_initial_load (cold)", _initial_load)
    measure("03b. tab1_switch_destination (all)", _switch_destinations)
    switch_row = RESULTS[-1]
    assert switch_row.sql_queries == 0, f"Expected 0 SQL queries switching destinations, got {switch_row.sql_queries}"


def bench_4_tab1_chart_modes_and_lookback() -> None:
    """Verify Tab 1 evolution snapshot queries across lookback windows and chart modes."""
    dests = list_destinations(active_only=False)
    target_id = int(dests[0]["id"]) if dests else 1

    def _cold_charts() -> str:
        snaps_30 = get_snapshots_for_destination(target_id, days=30)
        snaps_90 = get_snapshots_for_destination(target_id, days=90)
        return f"30d={len(snaps_30)} snaps, 90d={len(snaps_90)} snaps"

    def _cached_charts() -> str:
        snaps_30 = get_snapshots_for_destination(target_id, days=30)
        snaps_90 = get_snapshots_for_destination(target_id, days=90)
        return f"cached 30d={len(snaps_30)}, 90d={len(snaps_90)}"

    measure("04a. tab1_chart_lookback (cold)", _cold_charts)
    measure("04b. tab1_chart_lookback (cached)", _cached_charts)
    assert RESULTS[-1].sql_queries == 0, "Expected 0 SQL queries on cached chart lookback"


def bench_5_tab2_crud_duplicate_and_delete() -> None:
    """Exercise full destination CRUD: create, duplicate block, bulk slots, update+URL sync, toggle, delete."""
    unique_name = f"__PerfTest_Dest_{int(time.time() * 1000)}"
    created_id: Optional[int] = None

    try:
        def _crud_lifecycle() -> str:
            nonlocal created_id
            created_id = create_destination(
                name=unique_name,
                origin="MEX, NLU",
                destination="MAD, BCN",
                target_price=15000.0,
                trip_type="round-trip",
                currency="MXN",
                filter_config=FilterProfile(max_stops=1).to_dict(),
                is_active=False,
            )

            # Verify duplicate name blocking (case-insensitive + whitespace)
            dup_blocked = False
            try:
                create_destination(
                    name=f"  {unique_name.lower()}  ",
                    origin="MEX",
                    destination="MAD",
                    target_price=14000.0,
                    is_active=False,
                )
            except ValueError:
                dup_blocked = True
            assert dup_blocked, "Duplicate destination creation must raise ValueError"

            # Bulk-insert 4 date slots + verify duplicate slot skipping
            today = datetime.date.today()
            sample_slots = []
            for wk in range(4):
                out_d = (today + datetime.timedelta(days=30 + wk * 7)).strftime("%Y-%m-%d")
                ret_d = (today + datetime.timedelta(days=40 + wk * 7)).strftime("%Y-%m-%d")
                sample_slots.append(
                    {
                        "label": format_date_slot_label(out_d, ret_d),
                        "outbound_date": out_d,
                        "return_date": ret_d,
                        "google_flights_url": build_google_flights_url("MEX,NLU", "MAD,BCN", out_d, ret_d),
                    }
                )
            inserted = bulk_create_date_slots(created_id, sample_slots, replace_existing=False)
            assert len(inserted) == 4, f"Expected 4 inserted slots, got {len(inserted)}"

            dup_inserted = bulk_create_date_slots(created_id, sample_slots[:2], replace_existing=False)
            assert len(dup_inserted) == 0, f"Expected 0 duplicate slots inserted, got {len(dup_inserted)}"

            # Update destination & batch-regenerate child slot URLs
            update_destination(
                destination_id=created_id,
                name=f"{unique_name}_Updated",
                origin="MEX,NLU",
                destination="MAD",
                target_price=14500.0,
                trip_type="round-trip",
                currency="MXN",
                filter_config=FilterProfile(max_stops=0).to_dict(),
                is_active=False,
            )
            updated_slots = list_date_slots(destination_id=created_id, active_only=False)
            assert all("tfs=CBwQAh" in str(s["google_flights_url"]) for s in updated_slots)

            set_destination_active(created_id, False)
            delete_destination(created_id)
            created_id = None
            return "create+dup_block+4_slots_batch+update_batch+delete OK"

        measure("05. tab2_crud_dup_batch_delete", _crud_lifecycle)
    finally:
        if created_id is not None:
            delete_destination(created_id)


def bench_6_tab2_calendar_staging_and_shift() -> None:
    """Benchmark Tab 2 calendar staging, +7 días stepping, +4 semanas, and duplicate prevention."""
    def _calendar_staging() -> str:
        today = datetime.date.today()
        base_out = today + datetime.timedelta(days=21)
        base_ret = base_out + datetime.timedelta(days=9)
        profile = FilterProfile()
        staged: list[dict[str, Any]] = []
        seen_pairs: set[tuple[str, Optional[str]]] = set()

        def _stage(o_dt: datetime.date, r_dt: Optional[datetime.date]) -> bool:
            o_str = o_dt.strftime("%Y-%m-%d")
            r_str = r_dt.strftime("%Y-%m-%d") if r_dt else None
            if (o_str, r_str) in seen_pairs:
                return False
            seen_pairs.add((o_str, r_str))
            staged.append(
                {
                    "label": format_date_slot_label(o_str, r_str),
                    "outbound_date": o_str,
                    "return_date": r_str,
                    "google_flights_url": build_google_flights_url("MEX,NLU", "KEF,RKV", o_str, r_str, "round-trip", profile),
                    "window_info": get_flight_window_status(o_str, r_str),
                }
            )
            return True

        assert _stage(base_out, base_ret)
        assert not _stage(base_out, base_ret), "Duplicate staging must be blocked"

        # Simulate +7 días from last staged
        last_out = datetime.date.fromisoformat(staged[-1]["outbound_date"])
        last_ret = datetime.date.fromisoformat(staged[-1]["return_date"])
        assert _stage(last_out + datetime.timedelta(days=7), last_ret + datetime.timedelta(days=7))

        # Simulate +4 semanas
        cursor_out = datetime.date.fromisoformat(staged[-1]["outbound_date"]) + datetime.timedelta(days=7)
        cursor_ret = datetime.date.fromisoformat(staged[-1]["return_date"]) + datetime.timedelta(days=7)
        for _ in range(4):
            assert _stage(cursor_out, cursor_ret)
            cursor_out += datetime.timedelta(days=7)
            cursor_ret += datetime.timedelta(days=7)

        assert len(staged) == 6
        return f"staged {len(staged)} unique weekly slots"

    measure("06. tab2_calendar_staging_+7d_+4w", _calendar_staging)


def bench_7_tab2_auto_matrix_generator() -> None:
    """Benchmark Tab 2 Option A automatic 10-date matrix generator."""
    def _auto_gen() -> str:
        today = datetime.date.today()
        slots = generate_date_matrix(
            start_date=today + datetime.timedelta(days=14),
            end_date=today + datetime.timedelta(days=74),
            stay_days_list=[7, 8, 9, 10],
            weekdays_only=[3, 4, 5],
            max_slots=10,
            origin="MEX,NLU",
            destination="KEF,RKV,AEY,EGS",
            trip_type="round-trip",
        )
        assert len(slots) == 10, f"Expected 10 generated slots, got {len(slots)}"
        assert all("tfs=CBwQAh" in s["google_flights_url"] for s in slots)
        return f"generated {len(slots)} N:M slots"

    measure("07. tab2_auto_matrix_generator", _auto_gen)


def bench_8_tab2_paste_urls_decoder() -> None:
    """Benchmark Tab 2 Option C N:M URL paste parser & Protobuf decoder."""
    def _paste_decode() -> str:
        urls = "\n".join(
            [
                build_google_flights_url("MEX,NLU", "KEF,RKV", "2027-07-23", "2027-08-08"),
                f"[Flight 2]({build_google_flights_url('MEX,NLU', 'LIM,CUZ', '2027-08-06', '2027-08-22')})",
            ]
        )
        parsed = parse_pasted_urls(urls, max_slots=10)
        assert len(parsed) == 2
        assert parsed[0]["outbound_date"] == "2027-07-23" and parsed[0]["return_date"] == "2027-08-08"
        assert parsed[1]["outbound_date"] == "2027-08-06" and parsed[1]["return_date"] == "2027-08-22"
        return f"decoded {len(parsed)} URLs cleanly"

    measure("08. tab2_paste_urls_decoder", _paste_decode)


def bench_9_tab3_scanner_scopes_and_logs() -> None:
    """Benchmark Tab 3 data dependencies: slot picker list, alert_logs, and 20-proxy pool."""
    clear_db_cache()

    def _tab3_load() -> str:
        slots = list_date_slots(active_only=False)
        logs = get_recent_alert_logs(limit=40)
        pool = get_proxy_pool(verify_tunnel=False)
        non_direct = [p for p in pool if p is not None]
        assert len(non_direct) == 20, f"Expected 20 proxies in pool, got {len(non_direct)}"
        return f"slots={len(slots)}, logs={len(logs)}, proxies={len(non_direct)}"

    measure("09. tab3_scopes_logs_20_proxies", _tab3_load)


def bench_10_scraper_parser_and_db_integrity() -> None:
    """Verify Protobuf Field 2/19 fix, promo/year/metadata rejection, and 0 corrupted DB rows."""
    def _regressions() -> str:
        # 1. Verify Field 2 = 2 and Field 19 = trip_enum on both RT & OW
        tfs_rt = encode_tfs_protobuf("MEX,NLU", "KEF,RKV", "2027-07-23", "2027-08-08", "round-trip")
        tfs_ow = encode_tfs_protobuf("MEX", "NRT", "2027-07-23", None, "one-way")
        assert tfs_rt.startswith("CBwQAh"), f"Expected CBwQAh prefix on RT, got {tfs_rt[:8]}"
        assert tfs_ow.startswith("CBwQAh"), f"Expected CBwQAh prefix on OW, got {tfs_ow[:8]}"

        fields_rt = dict((f[0], f[2]) for f in _parse_protobuf_fields(base64.urlsafe_b64decode(tfs_rt + "==")))
        fields_ow = dict((f[0], f[2]) for f in _parse_protobuf_fields(base64.urlsafe_b64decode(tfs_ow + "==")))
        assert fields_rt[2] == 2 and fields_rt[19] == 1
        assert fields_ow[2] == 2 and fields_ow[19] == 2
        assert decode_tfs_parameter(tfs_rt)["trip_type"] == "round-trip"
        assert decode_tfs_parameter(tfs_ow)["trip_type"] == "one-way"

        # 2. Reject landing-page promo card ("1.839 MXN Viva") and bare years ("2027")
        promo_card = "Ciudad de México\nMonterrey\n28 ene-4 feb\nDirecto\ndesde 1.839\xa0MXN"
        assert parse_flight_card_text("", promo_card) is None, "Landing-page promo card must be rejected"
        assert parse_price_number("Salida en 2027") is None, "Bare year 2027 must not parse as price"
        assert parse_price_number("Desde 2027") is None, "Bare 'Desde 2027' must not parse as price"
        assert parse_price_number("33.703\xa0MXN") == 33703.0

        # 3. Real Google Flights DOM card must still parse cleanly
        real_card = (
            "8:40\n\xa0–\xa0\n6:30+1\nDelta, Air France\n15\xa0h 50\xa0min\nMEX–KEF\n"
            "2 escalas\nATL, BOS\n714 kg CO2e\n33.703\xa0MXN\nida y vuelta"
        )
        parsed_real = parse_flight_card_text("", real_card)
        assert parsed_real is not None and parsed_real.price == 33703.0
        assert parsed_real.origin_airport == "MEX" and parsed_real.destination_airport == "KEF"
        assert "15" in parsed_real.duration

        # 4. Reject ds:1 airport metadata nodes lacking legs/duration/valid airline
        fake_meta_node = [["Aeropuerto Internacional Felipe Ángeles, MX", ["Aeropuerto"]], [None, 985]]
        assert _parse_single_itinerary_entry(fake_meta_node) is None

        # 5. Verify DB contains 0 corrupted 1839 rows
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) AS c FROM price_snapshots WHERE cheapest_tab_price = 1839 AND cheapest_airline = 'Viva';"
                )
                assert cur.fetchone()["c"] == 0, "Corrupted 1839 price_snapshots still present"
                cur.execute("SELECT COUNT(*) AS c FROM alert_logs WHERE cheapest_price = 1839;")
                assert cur.fetchone()["c"] == 0, "Corrupted 1839 alert_logs still present"
                cur.execute(
                    "SELECT COUNT(*) AS c FROM date_slots WHERE latest_cheapest_price = 1839 OR all_time_low_price = 1839;"
                )
                assert cur.fetchone()["c"] == 0, "Corrupted 1839 date_slots still present"

        return "tfs_fields + promo/year/ds1_reject + 0 corrupted DB rows OK"

    measure("10. scraper_parser_db_integrity", _regressions)


def bench_11_full_streamlit_apptest() -> None:
    """End-to-end Streamlit AppTest: initial load + Tab 1/Tab 2/Tab 3 interactions."""
    clear_db_cache()
    os.environ["PROXY_LIST"] = os.getenv("PROXY_LIST", "")

    at = AppTest.from_file(str(ROOT_DIR / "app.py"), default_timeout=20)
    at.session_state["authenticated"] = True

    def _app_initial_render() -> str:
        at.run()
        assert not at.exception, f"Initial render exceptions: {[e.value for e in at.exception]}"
        assert not at.error, f"Initial render errors: {[e.value for e in at.error]}"
        assert len(at.tabs) >= 3
        return f"tabs={[t.label for t in at.tabs[:3]]}"

    def _app_tab1_interaction() -> str:
        sel = at.selectbox(key="tab1_dest_select")
        if len(sel.options) > 1:
            sel.select_index(1).run()
        at.radio(key="tab1_chart_mode").set_value(
            "Inspección Dual-Tab (Más Baratos vs. Mejores para una fecha)"
        ).run()
        assert not at.exception, f"Tab 1 exceptions: {[e.value for e in at.exception]}"
        return "Tab 1 dest switch + chart mode switch OK"

    def _app_tab2_interaction() -> str:
        at.text_input(key="dest_name_new").set_value("  Islandia  ")
        create_btn = [b for b in at.button if b.label == "Crear destino"][0]
        create_btn.click().run()
        assert any("Ya existe un destino llamado" in e.value for e in at.error), "Expected duplicate error in UI"
        assert not at.exception, f"Tab 2 exceptions: {[e.value for e in at.exception]}"
        return "Tab 2 duplicate block verified"

    def _app_tab3_interaction() -> str:
        at.radio(key="tab3_scan_scope").set_value("Ejecutar Lote de Cola Vencida (Due Queue Batch)").run()
        assert not at.exception, f"Tab 3 exceptions: {[e.value for e in at.exception]}"
        return "Tab 3 scanner scope switch OK"

    measure("11a. apptest_initial_render", _app_initial_render)
    measure("11b. apptest_tab1_interactions", _app_tab1_interaction)
    measure("11c. apptest_tab2_duplicate_guard", _app_tab2_interaction)
    measure("11d. apptest_tab3_scope_switch", _app_tab3_interaction)


def print_summary_table() -> None:
    print("\n" + "=" * 108)
    print(f"{'ACTION':<38} | {'TIME (ms)':>10} | {'SQL':>5} | {'CONNS':>5} | {'STATUS':<6} | {'DETAIL'}")
    print("-" * 108)
    for row in RESULTS:
        print(
            f"{row.action:<38} | {row.elapsed_ms:10.2f} | {row.sql_queries:5d} | "
            f"{row.db_connects:5d} | {row.status:<6} | {row.detail}"
        )
    print("=" * 108)
    print("ALL CHECKS PASSED")


def main() -> int:
    bench_1_db_connection_reuse()
    bench_2_dashboard_summary_kpi()
    bench_3_tab1_switch_destination()
    bench_4_tab1_chart_modes_and_lookback()
    bench_5_tab2_crud_duplicate_and_delete()
    bench_6_tab2_calendar_staging_and_shift()
    bench_7_tab2_auto_matrix_generator()
    bench_8_tab2_paste_urls_decoder()
    bench_9_tab3_scanner_scopes_and_logs()
    bench_10_scraper_parser_and_db_integrity()
    bench_11_full_streamlit_apptest()
    print_summary_table()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
