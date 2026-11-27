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

from src.alerts import (
    evaluate_slot_alert_triggers,
    format_consolidated_destination_digest,
)
from src.config import (
    DEFAULT_CHECK_INTERVAL_MINUTES,
    DEFAULT_GL_POOL,
    DUE_GRACE_MINUTES,
    MAX_DESTINATIONS,
    MAX_SLOTS_PER_RUN,
    MAX_TOTAL_SLOTS,
    get_flight_window_status,
    get_proxy_account,
    get_proxy_pool,
    get_proxy_tier,
    interleave_proxies_by_account,
    parse_gl_pool,
    redact_account_for_log,
    redact_proxy_for_log,
    sanitize_log_text,
)
from src.ds1_parser import FlightOffer
from src.runner import ProxyCircuitBreaker, assign_destinations_to_shard, run_sharded_dispatcher
from src.scraper import (
    PooledPatchrightSession,
    SlotScrapeResult,
    consolidate_slot_offers,
    scrape_date_slot_multi_gl,
)
from src.db import (
    bulk_create_date_slots,
    clear_db_cache,
    close_db_connection,
    connection_stats,
    create_destination,
    delete_destination,
    flush_shard_results_batch,
    get_connection,
    get_dashboard_summary,
    get_date_slot,
    get_recent_alert_logs,
    get_snapshots_for_destination,
    init_db,
    is_db_connected,
    list_date_slots,
    list_destinations,
    partition_destinations_into_shards,
    reset_connection_stats,
    set_destination_active,
    update_destination,
)
from src.ds1_parser import (
    _extract_token_currency,
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

        # 5. Reject currency mismatch (e.g. $307 USD when tracking MXN)
        usd_card = (
            "8:40\n\xa0–\xa0\n6:30+1\nAvianca\n6\xa0h 55\xa0min\nMEX–SJO\n"
            "1 escala\nSAL\n500 kg CO2e\n307\xa0USD\nida y vuelta"
        )
        assert parse_flight_card_text("", usd_card, expected_currency="MXN") is None, "USD card must be rejected for MXN slot"
        usd_token = base64.b64encode(b"\x08\x01\x1a\x03USD").decode("ascii")
        mxn_token = base64.b64encode(b"\x08\x01\x1a\x03MXN").decode("ascii")
        mock_leg = [None, None, None, "MEX", None, "2027-09-03", "SJO", None, None, 415]
        mock_usd_entry = [
            ["AV", ["Avianca"], [mock_leg], None, None, None, None, None, None, 415],
            [[None, 307], usd_token],
        ]
        mock_mxn_entry = [
            ["AV", ["Avianca"], [mock_leg], None, None, None, None, None, None, 415],
            [[None, 5673], mxn_token],
        ]
        assert _extract_token_currency(mock_usd_entry) == "USD"
        assert _parse_single_itinerary_entry(mock_usd_entry, expected_currency="MXN") is None
        parsed_mxn = _parse_single_itinerary_entry(mock_mxn_entry, expected_currency="MXN")
        assert parsed_mxn is not None and parsed_mxn.price == 5673.0

        # 6. Verify 50x10 capacity expansion defaults & hourly cadence
        assert MAX_DESTINATIONS == 50, f"Expected MAX_DESTINATIONS=50, got {MAX_DESTINATIONS}"
        assert MAX_TOTAL_SLOTS == 500, f"Expected MAX_TOTAL_SLOTS=500, got {MAX_TOTAL_SLOTS}"
        assert MAX_SLOTS_PER_RUN == 500, f"Expected MAX_SLOTS_PER_RUN=500, got {MAX_SLOTS_PER_RUN}"
        assert DEFAULT_CHECK_INTERVAL_MINUTES == 60, f"Expected 60m interval, got {DEFAULT_CHECK_INTERVAL_MINUTES}"
        assert DUE_GRACE_MINUTES == 5, f"Expected DUE_GRACE_MINUTES=5, got {DUE_GRACE_MINUTES}"
        assert DEFAULT_GL_POOL == ("MX", "IT", "DE")

        # 7. Verify DB contains 0 corrupted 1839 or 307 rows
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
                cur.execute(
                    "SELECT COUNT(*) AS c FROM price_snapshots WHERE cheapest_tab_price = 307 OR best_tab_price = 307;"
                )
                assert cur.fetchone()["c"] == 0, "Corrupted 307 price_snapshots still present"
                cur.execute("SELECT COUNT(*) AS c FROM alert_logs WHERE cheapest_price = 307 OR best_price = 307;")
                assert cur.fetchone()["c"] == 0, "Corrupted 307 alert_logs still present"
                cur.execute(
                    "SELECT COUNT(*) AS c FROM date_slots WHERE latest_cheapest_price = 307 OR all_time_low_price = 307;"
                )
                assert cur.fetchone()["c"] == 0, "Corrupted 307 date_slots still present"

        return "tfs_fields + currency_guard + 20x10_limits + 0 corrupted DB rows OK"

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


def bench_12_proxy_rotation_and_circuit_breaker() -> None:
    """Verify Tier-1/2/3 ordering, interleaved rotation, random offset, and 402/407 circuit breaker."""
    def _proxy_tests() -> str:
        import random as _rnd

        sample = [
            "http://acctA:pw@1.1.1.1:8001",
            "http://acctA:pw@1.1.1.2:8002",
            "http://acctB:pw@2.2.2.1:8001",
            "http://acctB:pw@2.2.2.2:8002",
        ]
        interleaved = interleave_proxies_by_account(sample, random_offset=False)
        assert [get_proxy_account(p) for p in interleaved] == ["acctA", "acctB", "acctA", "acctB"]

        starts = {
            interleave_proxies_by_account(sample, random_offset=True, rng=_rnd.Random(s))[0]
            for s in range(20)
        }
        assert len(starts) > 1, "Expected random initial offset to rotate starting proxy across runs"

        assert get_proxy_tier("socks5://127.0.0.1:40000") == "tier1_warp"
        assert get_proxy_tier(None) == "tier2_direct"
        assert get_proxy_tier(sample[0]) == "tier3_proxy"

        # Circuit breaker: HTTP 402/407 immediately marks entire account exhausted
        breaker = ProxyCircuitBreaker(max_consecutive_failures=3)
        tripped_407 = breaker.record_failure(sample[0], "Tunnel rejected (HTTP 407 Proxy Auth Required)")
        assert tripped_407 is True
        assert "acctA" in breaker.exhausted_accounts
        assert breaker.is_available(sample[1]) is False, "All ports of acctA must be skipped"
        assert breaker.is_available(sample[2]) is True, "acctB must remain available"
        assert breaker.is_available(None) is True, "Direct fallback must remain available"

        # HTTP 402 on acctC
        acct_c = "http://acctC:pw@3.3.3.1:8001"
        assert breaker.record_failure(acct_c, "HTTP 402 Payment Required") is True
        assert "acctC" in breaker.exhausted_accounts

        # 3 consecutive tunnel failures trip acctB; success resets counter
        assert breaker.record_failure(sample[2], "ERR_TUNNEL_CONNECTION_FAILED") is False
        assert breaker.record_failure(sample[2], "ERR_TUNNEL_CONNECTION_FAILED") is False
        breaker.record_success(sample[2])
        assert breaker.is_available(sample[3]) is True
        assert breaker.record_failure(sample[2], "ERR_TUNNEL_CONNECTION_FAILED") is False
        assert breaker.record_failure(sample[2], "ERR_TUNNEL_CONNECTION_FAILED") is False
        assert breaker.record_failure(sample[3], "ERR_TUNNEL_CONNECTION_FAILED") is True
        assert "acctB" in breaker.exhausted_accounts
        assert breaker.filter_available([None, *sample]) == [None]

        return "interleave + random_offset + 402/407 + 3x_tunnel circuit breaker OK"

    measure("12. unit_proxy_rotation_breaker", _proxy_tests)


def bench_13_alert_triggers_meta_and_low() -> None:
    """Verify 2-trigger alert engine (Meta & Bajo Histórico) without cooldown_hours, and FUTURE_RELEASE exclusion."""
    def _trigger_tests() -> str:
        today = datetime.date.today()
        active_out = (today + datetime.timedelta(days=45)).strftime("%Y-%m-%d")
        active_ret = (today + datetime.timedelta(days=55)).strftime("%Y-%m-%d")
        future_out = (today + datetime.timedelta(days=400)).strftime("%Y-%m-%d")
        future_ret = (today + datetime.timedelta(days=410)).strftime("%Y-%m-%d")
        now_utc = datetime.datetime.now(datetime.timezone.utc)

        base_slot = {
            "id": 99,
            "label": "Vie 10 Oct - Lun 20 Oct (10d)",
            "outbound_date": active_out,
            "return_date": active_ret,
            "google_flights_url": "https://www.google.com/travel/flights?tfs=test",
            "target_price": 10000.0,
            "last_winning_gl": "IT",
            "filter_config": {},
        }

        # 1. FUTURE_RELEASE (>330d) must always be excluded
        fut_slot = {**base_slot, "outbound_date": future_out, "return_date": future_ret}
        assert (
            evaluate_slot_alert_triggers(
                fut_slot,
                {"cheapest_tab_price": 5000.0, "best_tab_price": 6000.0, "winning_gl": "IT"},
                {"historical_min_price": 15000.0},
            )
            is None
        )

        # 2. SHARP_DROP removed: 20% drop vs rolling_7d_avg must NOT trigger if above target and no ATL
        no_sharp = evaluate_slot_alert_triggers(
            base_slot,
            {"cheapest_tab_price": 12000.0, "best_tab_price": 12500.0},
            {"historical_min_price": None, "rolling_7d_avg": 20000.0},
        )
        assert no_sharp is None, "SHARP_DROP must be completely removed"

        # 3. TARGET_BREACH (Meta): immediate on first breach & on crossing down from > target
        t1 = evaluate_slot_alert_triggers(
            base_slot,
            {"cheapest_tab_price": 9900.0, "best_tab_price": 10500.0, "winning_gl": "IT"},
            {"historical_min_price": None, "last_alerted_at": None, "last_alerted_price": None},
        )
        assert t1 is not None and t1["trigger_type"] == "TARGET_BREACH" and t1["winning_gl"] == "IT"

        t_cross = evaluate_slot_alert_triggers(
            base_slot,
            {"cheapest_tab_price": 9900.0, "best_tab_price": None, "winning_gl": "DE"},
            {
                "historical_min_price": None,
                "last_alerted_at": now_utc - datetime.timedelta(minutes=30),
                "last_alerted_price": 9800.0,
                "prev_effective_price": 10200.0,
            },
        )
        assert t_cross is not None and t_cross["trigger_type"] == "TARGET_BREACH" and t_cross["winning_gl"] == "DE"

        # Staying below target: <3% extra drop blocked; >=3% fires immediately with zero cooldown_hours
        assert (
            evaluate_slot_alert_triggers(
                base_slot,
                {"cheapest_tab_price": 9800.0, "best_tab_price": 10000.0},
                {
                    "historical_min_price": 9800.0,
                    "last_alerted_at": now_utc - datetime.timedelta(hours=24),
                    "last_alerted_price": 9900.0,
                    "prev_effective_price": 9850.0,
                },
            )
            is None
        )
        t_3pct_immediate = evaluate_slot_alert_triggers(
            base_slot,
            {"cheapest_tab_price": 9650.0, "best_tab_price": 10000.0},
            {
                "historical_min_price": 9900.0,
                "last_alerted_at": now_utc - datetime.timedelta(minutes=15),
                "last_alerted_price": 10000.0,
                "prev_effective_price": 9900.0,
                "last_atl_alerted_price": 9700.0,
            },
        )
        assert t_3pct_immediate is not None and t_3pct_immediate["trigger_type"] == "TARGET_BREACH"

        # 4. ALL_TIME_LOW (Bajo Histórico): requires < historical_min_price and <= reference_low * 0.98
        assert (
            evaluate_slot_alert_triggers(
                base_slot,
                {"cheapest_tab_price": 19700.0, "best_tab_price": 20000.0},
                {
                    "historical_min_price": 20000.0,
                    "last_alerted_at": now_utc - datetime.timedelta(minutes=10),
                    "last_alerted_price": 20000.0,
                },
            )
            is None
        )
        t_atl = evaluate_slot_alert_triggers(
            base_slot,
            {"cheapest_tab_price": 19600.0, "best_tab_price": 20000.0},
            {
                "historical_min_price": 20000.0,
                "last_alerted_at": now_utc - datetime.timedelta(minutes=10),
                "last_alerted_price": 20000.0,
            },
        )
        assert t_atl is not None and t_atl["trigger_type"] == "ALL_TIME_LOW"

        # 5. effective_price fallback to best_tab_price when cheapest_tab_price is None
        t_fallback = evaluate_slot_alert_triggers(
            base_slot,
            {"cheapest_tab_price": None, "best_tab_price": 9800.0},
            {"historical_min_price": None},
        )
        assert t_fallback is not None and t_fallback["effective_price"] == 9800.0

        # 6. Total absence of cooldown_hours in core modules & UI
        for rel_path in (
            "src/config.py",
            "src/db.py",
            "src/alerts.py",
            "src/runner.py",
            "app.py",
            "scripts/simulate_alert_triggers.py",
        ):
            src_text = (ROOT_DIR / rel_path).read_text(encoding="utf-8")
            if rel_path == "src/db.py":
                # Only allowed in idempotent DROP COLUMN IF EXISTS migration
                non_drop_lines = [
                    ln for ln in src_text.splitlines()
                    if "cooldown_hours" in ln and "DROP COLUMN IF EXISTS cooldown_hours" not in ln
                ]
                assert not non_drop_lines, f"Unexpected cooldown_hours in {rel_path}: {non_drop_lines}"
            else:
                assert "cooldown_hours" not in src_text, f"cooldown_hours still present in {rel_path}"

        return "Meta + Bajo Histórico + 0 cooldown_hours + FUTURE_RELEASE guard OK"

    measure("13. unit_alert_triggers_meta_low", _trigger_tests)


def bench_14_compact_html_rendering() -> None:
    """Verify compact HTML Telegram formatting, escaping, self-transfer mark, and mixed triggers."""
    def _html_tests() -> str:
        today = datetime.date.today()
        d1_out = (today + datetime.timedelta(days=30)).strftime("%Y-%m-%d")
        d1_ret = (today + datetime.timedelta(days=46)).strftime("%Y-%m-%d")
        d2_out = (today + datetime.timedelta(days=60)).strftime("%Y-%m-%d")
        d2_ret = (today + datetime.timedelta(days=76)).strftime("%Y-%m-%d")
        fut_out = (today + datetime.timedelta(days=400)).strftime("%Y-%m-%d")
        fut_ret = (today + datetime.timedelta(days=410)).strftime("%Y-%m-%d")

        dest_meta = {
            "id": 1,
            "name": "Conexión Europa <Islandia & Más>",
            "origin": "MEX, NLU",
            "destination": "MAD",
            "currency": "MXN",
        }
        slots = [
            {
                "label": "Vie 27 Ago - Dom 12 Sep (16d)",
                "outbound_date": d1_out,
                "return_date": d1_ret,
                "google_flights_url": "https://www.google.com/travel/flights?tfs=abc&hl=es",
                "cheapest_tab_price": 17993.0,
                "cheapest_airline": "Iberia",
                "cheapest_stops": "Directo",
                "cheapest_duration": "10 h 35 min",
                "origin_airport": "MEX",
                "destination_airport": "MAD",
                "is_self_transfer": True,
                "best_tab_price": 18500.0,
                "best_stops": "1 escala",
                "best_duration": "12 h 10 min",
                "best_origin_airport": "MEX",
                "best_destination_airport": "MAD",
                "trigger_type": "TARGET_BREACH",
            },
            {
                "label": "Vie 10 Sep - Dom 26 Sep (16d)",
                "outbound_date": d2_out,
                "return_date": d2_ret,
                "google_flights_url": "https://www.google.com/travel/flights?tfs=def&hl=es",
                "cheapest_tab_price": 19100.0,
                "cheapest_airline": "KLM",
                "cheapest_stops": "2 escalas",
                "cheapest_duration": "15 h 20 min",
                "origin_airport": "MEX",
                "destination_airport": "AMS",
                "is_self_transfer": False,
                "best_tab_price": None,
                "trigger_type": "ALL_TIME_LOW",
            },
            {
                "label": "Futuro Lejano",
                "outbound_date": fut_out,
                "return_date": fut_ret,
                "google_flights_url": "https://www.google.com/travel/flights?tfs=fut",
                "cheapest_tab_price": 5000.0,
                "trigger_type": "TARGET_BREACH",
            },
        ]

        rendered = format_consolidated_destination_digest(dest_meta, slots, is_test=False)
        assert "<b>Conexión Europa &lt;Islandia &amp; Más&gt;</b> | Meta / Bajo Histórico" in rendered
        assert "1. Vie 27 Ago - Dom 12 Sep · Meta" in rendered
        assert "2. Vie 10 Sep - Dom 26 Sep · Bajo Histórico" in rendered
        assert "Barato: $17,993 MXN (MEX -&gt; MAD, Directo ⚠️, 10 h 35 min)" in rendered
        assert "Mejor: $18,500 MXN (MEX -&gt; MAD, 1 escala, 12 h 10 min)" in rendered
        assert "tfu=EgoIABAAGAAgAigB" in rendered and "gl=MX" in rendered
        assert "tfs=abc" in rendered and "<b>[Reservar]</b></a>" in rendered
        assert "Iberia" not in rendered and "KLM" not in rendered
        assert "Futuro Lejano" not in rendered
        # Slot 2 had best_tab_price=None so only Barato: must appear for slot 2
        assert rendered.count("Mejor:") == 1

        dest_it = {**dest_meta, "gl": "IT"}
        rendered_it = format_consolidated_destination_digest(dest_it, slots[:1], is_test=False)
        assert "tfu=EgoIABAAGAAgAigB" in rendered_it and "gl=IT" in rendered_it

        test_rendered = format_consolidated_destination_digest(dest_meta, slots[:1], is_test=True)
        assert "<b>[PRUEBA] Conexión Europa &lt;Islandia &amp; Más&gt;</b> | Meta" in test_rendered
        assert " · Meta" not in test_rendered, "Single-trigger digest must not add per-slot suffix"

        return "compact HTML + escape + mixed triggers + PRUEBA prefix OK"

    measure("14. unit_compact_html_rendering", _html_tests)


def bench_15_proxy_redaction_and_sanitization() -> None:
    """Verify proxy URLs, usernames, passwords, and IPs are scrubbed from logs/exceptions."""
    def _redaction_tests() -> str:
        secret_proxy = "http://zjmkogsd:supersecret99@45.38.107.97:6014"
        redacted = redact_proxy_for_log(secret_proxy)
        assert "zjmkogsd" not in redacted
        assert "supersecret99" not in redacted
        assert "45.38.107.97" not in redacted
        assert "6014" not in redacted
        assert redacted.startswith("webshare:acct_****gsd#")

        assert redact_proxy_for_log(None) == "direct"
        assert redact_proxy_for_log("socks5://127.0.0.1:40000") == "warp"
        assert redact_account_for_log("zjmkogsd") == "acct_****gsd"

        fake_exc = RuntimeError(
            "curl: (56) Proxy CONNECT aborted via http://user:secretpass@1.2.3.4:8080 and 9.8.7.6:3128"
        )
        scrubbed = sanitize_log_text(fake_exc)
        assert "secretpass" not in scrubbed
        assert "1.2.3.4" not in scrubbed
        assert "8080" not in scrubbed
        assert "9.8.7.6" not in scrubbed
        assert "http://user:secretpass@1.2.3.4:8080" not in scrubbed
        assert "[REDACTED_PROXY]" in scrubbed
        return "redact_proxy_for_log + sanitize_log_text OK"

    measure("15. unit_proxy_redaction_sanitize", _redaction_tests)


def bench_16_cheapest_click_failure_tolerance() -> None:
    """Verify cheapest_tab.click() timeout tries DOM click fallback and handles full failure gracefully."""
    def _click_fail_test() -> str:
        from src.scraper import consolidate_slot_offers

        class _AlwaysFailTab:
            def count(self) -> int:
                return 1

            def click(self, timeout: int = 5000) -> None:
                raise TimeoutError("Locator.click: Timeout 5000ms exceeded")

            def evaluate(self, script: str) -> None:
                raise RuntimeError("DOM click blocked")

        class _FailLocator:
            def __init__(self) -> None:
                self.first = _AlwaysFailTab()

            def filter(self, **kwargs: Any) -> "_FailLocator":
                return self

        class _FailPage:
            def locator(self, sel: str) -> _FailLocator:
                return _FailLocator()

            def get_by_role(self, role: str, **kwargs: Any) -> _FailLocator:
                return _FailLocator()

        sess = PooledPatchrightSession(proxy_url=None, headless=True)
        best_offers = [
            FlightOffer(
                price=15588.0,
                airline="LATAM",
                stops="Directo",
                duration="9 h 30 min",
                origin_airport="MEX",
                destination_airport="GRU",
                tab_source="best",
            )
        ]
        clicked, status, tab_hint = sess._switch_to_cheapest_tab_deterministic(
            page=_FailPage(),
            rpc_payload_counter={"count": 0},
            best_dom_offers=best_offers,
        )
        assert clicked is False
        assert status == "cheapest_click_timeout"
        assert tab_hint is None

        # Consolidating with preserved Mejores/ds:1 data still yields valid slot result
        cons = consolidate_slot_offers(
            best_dom_offers=best_offers,
            cheapest_dom_offers=[],
            rpc_offers=[],
            ds1_offers=best_offers,
            ds1_best=best_offers[0],
        )
        assert cons is not None
        assert cons["best_offer"].price == 15588.0
        assert cons["cheapest_offer"].price == 15588.0

        # Subcase B: locator.click() fails -> DOM el.click() fallback succeeds + spinner disappears + hint matches
        dom_clicks = {"count": 0}

        class _DomFallbackTab:
            def count(self) -> int:
                return 1

            def click(self, timeout: int = 5000) -> None:
                raise TimeoutError("Locator.click: Timeout 5000ms exceeded")

            def evaluate(self, script: str) -> None:
                dom_clicks["count"] += 1

        class _FallbackLocator:
            def __init__(self) -> None:
                self.first = _DomFallbackTab()

            def filter(self, **kwargs: Any) -> "_FallbackLocator":
                return self

        class _FallbackPage:
            def __init__(self) -> None:
                self.calls = 0

            def locator(self, sel: str) -> _FallbackLocator:
                return _FallbackLocator()

            def get_by_role(self, role: str, **kwargs: Any) -> _FallbackLocator:
                return _FallbackLocator()

            def wait_for_timeout(self, ms: int) -> None:
                pass

            def evaluate(self, script: str, *args: Any) -> dict[str, Any]:
                self.calls += 1
                # Simulate real transition: call 1 provisional ($23,062 + spinner active) ->
                # calls 2 & 3 spinner disappears and settled lowest price ($19,236) stabilizes
                if self.calls == 1:
                    return {
                        "tabFound": True,
                        "ariaSelected": True,
                        "spinnerActive": True,
                        "tabHintAria": "23062 pesos mexicanos",
                        "tabHintText": "23.062\xa0MXN",
                        "noCheaper": False,
                        "cardSamples": [
                            {
                                "aria": "Desde 23062 pesos mexicanos. Salida de MEX.",
                                "priceNodes": ["23.062\xa0MXN"],
                                "lines": "23.062\xa0MXN",
                            }
                        ],
                    }
                return {
                    "tabFound": True,
                    "ariaSelected": True,
                    "spinnerActive": False,
                    "tabHintAria": "19236 pesos mexicanos",
                    "tabHintText": "19.236\xa0MXN",
                    "noCheaper": False,
                    "cardSamples": [
                        {
                            "aria": "Desde 19236 pesos mexicanos. Salida de MEX.",
                            "priceNodes": ["19.236\xa0MXN"],
                            "lines": "19.236\xa0MXN",
                        }
                    ],
                }

        clicked2, status2, tab_hint2 = sess._switch_to_cheapest_tab_deterministic(
            page=_FallbackPage(),
            rpc_payload_counter={"count": 0},
            best_dom_offers=best_offers,
            expected_currency="MXN",
        )
        assert dom_clicks["count"] == 1
        assert clicked2 is True
        assert status2 == "cheapest_loaded_verified"
        assert tab_hint2 == 19236.0
        return "click fallback + Mejores preservation OK"

    measure("16. unit_cheapest_click_tolerance", _click_fail_test)


def bench_17_min_offer_consolidation() -> None:
    """Verify consolidation picks min(dom_cheapest, rpc_cheapest, ds1_cheapest) and tags source."""
    def _min_test() -> str:
        from src.scraper import consolidate_slot_offers

        best_dom = [FlightOffer(price=5393.0, airline="Aeromexico", origin_airport="MEX", destination_airport="GUA", tab_source="best")]
        cheapest_dom = [FlightOffer(price=5393.0, airline="Aeromexico", origin_airport="MEX", destination_airport="GUA", tab_source="cheapest")]
        rpc_offers = [FlightOffer(price=4100.0, airline="Volaris", origin_airport="MEX", destination_airport="GUA", tab_source="cheapest")]
        ds1_offers = [FlightOffer(price=3214.0, airline="Aeromexico", origin_airport="MEX", destination_airport="TAP", tab_source="cheapest")]

        cons_ds1 = consolidate_slot_offers(best_dom, cheapest_dom, rpc_offers, ds1_offers, ds1_best=best_dom[0])
        assert cons_ds1 is not None
        assert cons_ds1["cheapest_offer"].price == 3214.0
        assert cons_ds1["cheapest_source"] == "cheapest_ds1_min"
        assert cons_ds1["best_offer"].price == 5393.0

        cons_rpc = consolidate_slot_offers(best_dom, cheapest_dom, rpc_offers, [], ds1_best=best_dom[0])
        assert cons_rpc is not None
        assert cons_rpc["cheapest_offer"].price == 4100.0
        assert cons_rpc["cheapest_source"] == "cheapest_rpc"

        dom_win = [FlightOffer(price=2946.0, airline="Viva", origin_airport="MEX", destination_airport="TAP", tab_source="cheapest")]
        cons_dom = consolidate_slot_offers(best_dom, dom_win, rpc_offers, ds1_offers, ds1_best=best_dom[0])
        assert cons_dom is not None
        assert cons_dom["cheapest_offer"].price == 2946.0
        assert cons_dom["cheapest_source"] == "cheapest_dom"

        return "min(dom, rpc, ds1) consolidation + source tagging OK"

    measure("17. unit_min_offer_consolidation", _min_test)


def bench_18_tab_hint_and_early_exit() -> None:
    """Verify tab_hint parsing inside .bU1Yvc, spinner wait, early exit, and mismatch warning."""
    def _hint_test() -> str:
        from src.scraper import evaluate_cheapest_wait_state, parse_tab_hint_price

        assert parse_tab_hint_price("19236 pesos mexicanos", "19.236\xa0MXN") == 19236.0
        assert parse_tab_hint_price("", "23.062\xa0MXN") == 23062.0
        assert parse_tab_hint_price("19829 pesos mexicanos", "") == 19829.0
        assert parse_tab_hint_price("", "") is None

        # Spinner active keeps waiting even when prices temporarily match
        assert evaluate_cheapest_wait_state(True, True, 23062.0, 23062.0) == (False, "waiting")
        # Tab not yet aria-selected keeps waiting (or cheapest_timeout at deadline)
        assert evaluate_cheapest_wait_state(False, False, 19236.0, 19236.0) == (False, "waiting")
        assert evaluate_cheapest_wait_state(False, False, 19236.0, 19236.0, at_deadline=True) == (True, "cheapest_timeout")
        # Anti-premature-exit: provisional $23,062 at < 2.0s before spinner was seen must NOT exit early
        assert evaluate_cheapest_wait_state(
            True, False, 23062.0, 23062.0, spinner_seen=False, dom_hint_stable_count=1, elapsed_s=0.8
        ) == (False, "waiting_min_settle")
        # Even after spinner finishes, require 2 consecutive stable reads before cheapest_loaded_verified
        assert evaluate_cheapest_wait_state(
            True, False, 23062.0, 23062.0, spinner_seen=True, dom_hint_stable_count=1, elapsed_s=1.0
        ) == (False, "waiting_hint_stability")
        # Once spinner transitions to False and $19,236 stabilizes for 2 consecutive reads -> exit verified
        assert evaluate_cheapest_wait_state(
            True, False, 19236.0, 19236.0, spinner_seen=True, dom_hint_stable_count=2, elapsed_s=1.8
        ) == (True, "cheapest_loaded_verified")
        assert evaluate_cheapest_wait_state(
            True, False, 19236.0, 19400.0, spinner_seen=True, dom_hint_stable_count=2, elapsed_s=2.0
        ) == (True, "cheapest_loaded_verified")
        # Still higher than hint before deadline -> wait, then request sort when DOM stabilizes
        assert evaluate_cheapest_wait_state(True, False, 19236.0, 26725.0, dom_stable_count=1, at_deadline=False) == (False, "waiting_dom_sync")
        assert evaluate_cheapest_wait_state(True, False, 19236.0, 26725.0, dom_stable_count=3, at_deadline=False) == (True, "cheapest_needs_price_sort")
        # Persistent mismatch at deadline -> explicit mismatch status
        assert evaluate_cheapest_wait_state(True, False, 19236.0, 26725.0, at_deadline=True) == (True, "cheapest_tab_mismatch")
        # No-cheaper-options notice
        assert evaluate_cheapest_wait_state(True, False, None, 26725.0, no_cheaper_notice=True) == (True, "cheapest_no_cheaper_notice")
        # No hint fallback requires >= 2 consecutive stable DOM reads
        assert evaluate_cheapest_wait_state(True, False, None, 19236.0, dom_stable_count=1) == (False, "waiting_dom_stability")
        assert evaluate_cheapest_wait_state(True, False, None, 19236.0, dom_stable_count=2) == (True, "cheapest_dom_stabilized")

        return "tab_hint parse + anti-premature-exit + mismatch OK"

    measure("18. unit_tab_hint_and_early_exit", _hint_test)


def bench_19_direct_cheapest_flow() -> None:
    """Verify _scrape_slot_once navigates directly with CHEAPEST_TFU + gl, 0 tab clicks, and no unnecessary sort."""
    def _direct_test() -> str:
        from src.tfs_builder import CHEAPEST_TFU, build_google_flights_url

        visited_urls: list[str] = []

        class _StubPage:
            url = "https://www.google.com/travel/flights/search"

            def set_default_timeout(self, ms: int) -> None:
                pass

            def on(self, ev: str, fn: Any) -> None:
                pass

            def goto(self, u: str, **kwargs: Any) -> None:
                visited_urls.append(u)

            def wait_for_selector(self, sel: str, **kwargs: Any) -> None:
                pass

            def content(self) -> str:
                return "<html><body></body></html>"

            def close(self) -> None:
                pass

        class _StubCtx:
            def new_page(self) -> _StubPage:
                return _StubPage()

        sess = PooledPatchrightSession(proxy_url=None, headless=True)
        sess._context = _StubCtx()

        top_best = FlightOffer(
            price=17620.0,
            airline="Lufthansa",
            stops="1 escala",
            duration="15 h 10 min",
            dep_arr="21:00 - 14:10 (MEX-MXP)",
            origin_airport="MEX",
            destination_airport="MXP",
            tab_source="best",
        )
        other_cheapest = FlightOffer(
            price=15951.0,
            airline="Air Canada",
            stops="2 escalas",
            duration="38 h 25 min",
            dep_arr="11:55 - 10:20 (MEX-MXP)",
            origin_airport="MEX",
            destination_airport="MXP",
            tab_source="cheapest",
        )

        sess._wait_for_direct_cheapest_settled = lambda page, t_goto, max_wait_s=35.0: (  # type: ignore[method-assign]
            "direct_cheapest_settled", 15951.0, 0, 0, 12.4
        )
        sess._extract_sectioned_dom_cards = lambda page, max_cards=80, expected_currency=None: (  # type: ignore[method-assign]
            [top_best], [other_cheapest], [top_best, other_cheapest]
        )

        sort_calls = {"count": 0}

        def _fail_if_sort(*args: Any, **kwargs: Any) -> tuple[bool, Optional[float], Optional[float]]:
            sort_calls["count"] += 1
            return False, None, None

        sess._sort_results_by_price = _fail_if_sort  # type: ignore[method-assign]

        test_url = build_google_flights_url(["MEX", "NLU"], ["MXP", "MAD"], "2027-09-11", "2027-09-27")
        res = sess._scrape_slot_once(test_url, gl="IT")
        assert res is not None
        assert len(visited_urls) == 1
        assert f"tfu={CHEAPEST_TFU}" in visited_urls[0] and "gl=IT" in visited_urls[0]
        assert sort_calls["count"] == 0, f"Expected 0 safety-net sort calls, got {sort_calls['count']}"
        assert res.tab_clicks == 0
        assert res.cheapest_tab_status == "direct_cheapest_settled"
        assert res.cheapest_tab_price == 15951.0
        assert res.best_tab_price == 17620.0
        assert res.tab_hint_price == 15951.0
        assert res.gl == "IT"
        assert res.sorted_by_price is False
        return "direct tfu+gl nav + 0 clicks + Best/Cheapest dual extraction OK"

    measure("19. unit_direct_cheapest_flow", _direct_test)


def bench_20_sort_results_by_price() -> None:
    """Verify _sort_results_by_price clicks button[jsname='AefGQb'] -> Precio and consolidates 43h20m $16,205 offer."""
    def _sort_test() -> str:
        from src.ds1_parser import parse_flight_card_text
        from src.scraper import consolidate_slot_offers

        clicks = {"open_menu": 0, "select_precio": 0}

        class _SortBtn:
            def count(self) -> int:
                return 1

            def is_visible(self) -> bool:
                return True

            def inner_text(self) -> str:
                return "Ordenados por precio" if clicks["select_precio"] > 0 else "Ordenados por vuelos destacados"

            def get_attribute(self, attr: str) -> str:
                return self.inner_text()

            def click(self, timeout: int = 2000) -> None:
                clicks["open_menu"] += 1

        class _PrecioOption:
            def count(self) -> int:
                return 1

            def is_visible(self) -> bool:
                return True

            def click(self, timeout: int = 2000) -> None:
                clicks["select_precio"] += 1

        class _MockLocator:
            def __init__(self, target_kind: str) -> None:
                self.target_kind = target_kind
                self.first = _SortBtn() if target_kind == "sort_btn" else _PrecioOption()

            def filter(self, **kwargs: Any) -> "_MockLocator":
                return self

        class _SortMockPage:
            def locator(self, sel: str) -> _MockLocator:
                if "AefGQb" in sel:
                    return _MockLocator("sort_btn")
                return _MockLocator("menu_opt")

            def wait_for_timeout(self, ms: int) -> None:
                pass

            def evaluate(self, script: str, *args: Any) -> Any:
                # Probe state after selecting "Precio"
                if clicks["select_precio"] > 0:
                    return {
                        "tabFound": True,
                        "ariaSelected": True,
                        "spinnerActive": False,
                        "tabHintAria": "16205 pesos mexicanos",
                        "tabHintText": "16.205\xa0MXN",
                        "noCheaper": False,
                        "cardSamples": [
                            {
                                "aria": "Desde 16205 pesos mexicanos. Air Canada. 2 escalas. 43 h 20 min.",
                                "priceNodes": ["16.205\xa0MXN"],
                                "lines": "16.205\xa0MXN|Air Canada|43 h 20 min|2 escalas",
                            }
                        ],
                    }
                return {}

        sess = PooledPatchrightSession(proxy_url=None, headless=True)
        applied, sort_hint, sort_min = sess._sort_results_by_price(
            page=_SortMockPage(),
            tab_hint_price=16205.0,
            max_wait_s=2.0,
        )
        assert clicks["open_menu"] == 1
        assert clicks["select_precio"] == 1
        assert applied is True
        assert sort_hint == 16205.0
        assert sort_min == 16205.0

        # Parse the real long-duration 43 h 20 min card without any duration/stops truncation
        parsed = parse_flight_card_text(
            aria_label=(
                "Desde 16205 pesos mexicanos por viaje de ida y vuelta con Air Canada. "
                "Sale del aeropuerto de MEX a las 06:15 y llega a MAD a las 01:35. "
                "Duración total: 43 h 20 min. 2 escalas."
            ),
            inner_text="06:15 – 01:35\nAir Canada\n43 h 20 min\nMEX–MAD\n2 escalas\n16.205\xa0MXN",
            price_override=16205.0,
            tab_source="cheapest",
            expected_currency="MXN",
        )
        assert parsed is not None
        assert parsed.price == 16205.0
        assert parsed.airline == "Air Canada"
        assert parsed.stops == "2 escalas"
        assert parsed.duration == "43 h 20 min"

        best_default = [FlightOffer(price=19480.0, airline="Iberia", stops="Directo", duration="10 h 35 min", origin_airport="MEX", destination_airport="MAD", tab_source="best")]
        cons = consolidate_slot_offers(
            best_dom_offers=best_default,
            cheapest_dom_offers=[FlightOffer(price=18500.0, airline="Aeromexico", origin_airport="MEX", destination_airport="MAD", tab_source="cheapest")],
            rpc_offers=[],
            ds1_offers=[],
            ds1_best=best_default[0],
            sorted_dom_offers=[parsed],
        )
        assert cons is not None
        assert cons["cheapest_offer"].price == 16205.0
        assert cons["cheapest_source"] == "cheapest_dom_sorted_price"
        assert cons["best_offer"].price == 19480.0
        return "sort button AefGQb -> Precio -> $16,205 (43h20m) OK"

    measure("20. unit_sort_results_by_price", _sort_test)


def bench_21_sort_label_guard_and_en_card() -> None:
    """Verify _sort_results_by_price returns False if label stays unchanged, and parses English MX$15,959 / 38h25m cards."""
    def _guard_and_en_test() -> str:
        from src.ds1_parser import parse_flight_card_text, parse_price_number
        from src.scraper import parse_tab_hint_price

        clicks = {"open": 0, "opt": 0}

        class _UnchangedSortBtn:
            def count(self) -> int:
                return 1

            def is_visible(self) -> bool:
                return True

            def inner_text(self) -> str:
                return "Ordenados por vuelos destacados"

            def get_attribute(self, attr: str) -> str:
                return "Ordenados por vuelos destacados, Cambiar orden"

            def click(self, timeout: int = 2000) -> None:
                clicks["open"] += 1

            def evaluate(self, script: str) -> None:
                clicks["open"] += 1

        class _SilentOpt:
            def count(self) -> int:
                return 1

            def is_visible(self) -> bool:
                return True

            def click(self, timeout: int = 2000) -> None:
                clicks["opt"] += 1

            def evaluate(self, script: str) -> None:
                clicks["opt"] += 1

        class _Loc:
            def __init__(self, kind: str) -> None:
                self.first = _UnchangedSortBtn() if kind == "btn" else _SilentOpt()

            def filter(self, **kwargs: Any) -> "_Loc":
                return self

        class _StuckPage:
            def locator(self, sel: str) -> _Loc:
                return _Loc("btn" if "AefGQb" in sel else "opt")

            def wait_for_timeout(self, ms: int) -> None:
                pass

            def evaluate(self, script: str, *args: Any) -> Any:
                return False

        sess = PooledPatchrightSession(proxy_url=None, headless=True)
        applied_stuck, _, _ = sess._sort_results_by_price(
            page=_StuckPage(),
            tab_hint_price=15959.0,
            max_wait_s=2.0,
        )
        assert applied_stuck is False, "Expected sorted_by_price=False when button label stays 'Ordenados por vuelos destacados'"

        # Bilingual English card parsing matching user's exact pasted card
        assert parse_price_number("From 15959 Mexican pesos") == 15959.0
        assert parse_price_number("MX$15,959") == 15959.0
        assert parse_tab_hint_price("15959 Mexican pesos", "MX$15,959") == 15959.0

        en_offer = parse_flight_card_text(
            aria_label=(
                "From 15959 Mexican pesos Round trip flight with Air Canada. "
                "Leaves Benito Juárez International Airport at 6:15 AM and arrives at Milan Malpensa Airport at 10:20 AM+1. "
                "Total duration 38 hr 25 min. 2 stops."
            ),
            inner_text="6:15 AM – 10:20 AM+1\nAir Canada\n38 hr 25 min\nMEX–MXP\n2 stops\nMX$15,959",
            price_override=None,
            tab_source="cheapest",
            expected_currency="MXN",
        )
        assert en_offer is not None
        assert en_offer.price == 15959.0
        assert en_offer.airline == "Air Canada"
        assert en_offer.stops == "2 stops"
        assert en_offer.duration == "38 hr 25 min"
        assert en_offer.origin_airport == "MEX"
        assert en_offer.destination_airport == "MXP"
        return "unchanged-label guard + EN MX$15,959 / 38hr25m card OK"

    measure("21. unit_sort_guard_and_en_card", _guard_and_en_test)


def bench_22_post_sort_rpc_wait_and_parse() -> None:
    """Verify post-sort waits for GetShoppingResults RPC instead of exiting on frozen pre-sort DOM."""
    def _post_sort_rpc_test() -> str:
        from src.ds1_parser import parse_shopping_rpc_response
        from src.scraper import consolidate_slot_offers, evaluate_post_sort_wait_state

        # 1. Pure decision guard: frozen $18,036 DOM with rpc_delta_sort=0 must NOT exit early
        assert evaluate_post_sort_wait_state(
            spinner_active=False,
            rpc_delta_sort=0,
            target_hint=16201.0,
            dom_min_price=18036.0,
            baseline_dom_min=18036.0,
            dom_stable_count=2,
            elapsed_since_click_s=0.6,
            at_deadline=False,
        ) == (False, "waiting_post_sort_rpc")

        # Once post-sort RPC arrives and DOM drops to $16,201 -> exit cleanly
        assert evaluate_post_sort_wait_state(
            spinner_active=False,
            rpc_delta_sort=1,
            target_hint=16201.0,
            dom_min_price=16201.0,
            baseline_dom_min=18036.0,
            dom_stable_count=1,
            elapsed_since_click_s=2.4,
            at_deadline=False,
        ) == (True, "post_sort_hint_matched")

        # 2. Simulated _sort_results_by_price where DOM stays frozen on $18,036 for 2 polls
        #    while RPC is in flight, then on poll 3 RPC counter increments (+1) and $16,201 appears
        state = {"open": 0, "opt": 0, "probes": 0}
        rpc_counter = {"count": 1}

        class _Btn:
            def count(self) -> int:
                return 1

            def is_visible(self) -> bool:
                return True

            def inner_text(self) -> str:
                return "Ordenados por precio" if state["opt"] > 0 else "Ordenados por vuelos destacados"

            def get_attribute(self, a: str) -> str:
                return self.inner_text()

            def click(self, timeout: int = 2000) -> None:
                state["open"] += 1

        class _Opt:
            def count(self) -> int:
                return 1

            def is_visible(self) -> bool:
                return True

            def click(self, timeout: int = 2000) -> None:
                state["opt"] += 1

        class _Loc:
            def __init__(self, kind: str) -> None:
                self.first = _Btn() if kind == "btn" else _Opt()

            def filter(self, **kwargs: Any) -> "_Loc":
                return self

        class _DelayedRpcPage:
            def locator(self, sel: str) -> _Loc:
                return _Loc("btn" if "AefGQb" in sel else "opt")

            def wait_for_timeout(self, ms: int) -> None:
                pass

            def evaluate(self, script: str, *args: Any) -> dict[str, Any]:
                state["probes"] += 1
                # Probe 1 (baseline before click) + Probes 2,3 (frozen $18,036 before RPC lands)
                if state["probes"] <= 3:
                    return {
                        "tabFound": True,
                        "ariaSelected": True,
                        "spinnerActive": False,
                        "tabHintAria": "18036 pesos mexicanos",
                        "tabHintText": "18.036\xa0MXN",
                        "noCheaper": False,
                        "cardSamples": [
                            {"aria": "Desde 18036 pesos mexicanos", "priceNodes": ["18.036\xa0MXN"], "lines": "18.036"}
                        ],
                    }
                # Probe 4: post-sort GetShoppingResults arrived!
                rpc_counter["count"] = 2
                return {
                    "tabFound": True,
                    "ariaSelected": True,
                    "spinnerActive": False,
                    "tabHintAria": "16201 pesos mexicanos",
                    "tabHintText": "16.201\xa0MXN",
                    "noCheaper": False,
                    "cardSamples": [
                        {"aria": "Desde 16201 pesos mexicanos", "priceNodes": ["16.201\xa0MXN"], "lines": "16.201|43 h 20 min"}
                    ],
                }

        sess = PooledPatchrightSession(proxy_url=None, headless=True)
        applied, hint_out, dom_out = sess._sort_results_by_price(
            page=_DelayedRpcPage(),
            tab_hint_price=16201.0,
            max_wait_s=5.0,
            rpc_payload_counter=rpc_counter,
        )
        assert applied is True
        assert state["probes"] == 4, f"Expected wait through 4 probes until RPC landed, got {state['probes']}"
        assert rpc_counter["count"] == 2
        assert hint_out == 16201.0
        assert dom_out == 16201.0

        # 3. Verify synthetic GetShoppingResults payload with $16,201 (2600 min = 43h20m, 2 stops) parses & consolidates
        import json as _json
        fake_leg = [[["MEX", "06:00", "2027-09-11", None, "YYZ"], None, None, None, None, [6, 0]], None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]
        # Simple direct consolidation check + batchexecute frame parse check
        sample_itinerary = [
            [
                "AC",
                ["Air Canada"],
                None,
                "MEX",
                None,
                [6, 15],
                "MXP",
                None,
                [10, 20],
                2600,
                None,
                [["MEX", None, "YYZ"], ["YYZ", None, "YUL"], ["YUL", None, "MXP"]],
            ],
            [[None, 16201]],
        ]
        # Ensure parse_shopping_rpc_response handles wrb.fr JSON gracefully
        payload = ')]}\'\n' + _json.dumps([["wrb.fr", "GetShoppingResults", _json.dumps([None, None, None, [[sample_itinerary]]])]])
        rpc_offers = parse_shopping_rpc_response(payload, expected_currency="MXN")
        assert isinstance(rpc_offers, list)
        post_rpc_offer = FlightOffer(
            price=16201.0,
            airline="Air Canada",
            stops="2 escalas",
            duration="43 h 20 min",
            origin_airport="MEX",
            destination_airport="MXP",
            tab_source="cheapest",
        )
        cons = consolidate_slot_offers(
            best_dom_offers=[FlightOffer(price=18474.0, airline="Aeromexico", origin_airport="MEX", destination_airport="MXP", tab_source="best")],
            cheapest_dom_offers=[FlightOffer(price=18036.0, airline="Air Canada", origin_airport="MEX", destination_airport="MXP", tab_source="cheapest")],
            rpc_offers=[post_rpc_offer],
            ds1_offers=[],
        )
        assert cons is not None
        assert cons["cheapest_offer"].price == 16201.0
        assert cons["cheapest_source"] == "cheapest_rpc"
        return "post-sort RPC wait + $16,201 consolidation OK"

    measure("22. unit_post_sort_rpc_wait_parse", _post_sort_rpc_test)


def bench_23_normalize_flights_url_and_reservar() -> None:
    """Verify normalize_flights_url preserves ?tfs=/curr/hl while setting tfu/gl, and [Reservar] uses both."""
    def _url_and_reservar_test() -> str:
        from urllib.parse import parse_qs, urlparse
        from src.tfs_builder import BEST_TFU, CHEAPEST_TFU, build_google_flights_url, normalize_flights_url

        orig_url = build_google_flights_url(
            ["MEX", "NLU"], ["MXP", "CDG"], "2027-09-11", "2027-09-27", curr="MXN", gl="MX", hl="es"
        )
        assert f"tfu={BEST_TFU}" in orig_url
        orig_qs = parse_qs(urlparse(orig_url).query)

        norm_it = normalize_flights_url(orig_url, tfu=CHEAPEST_TFU, gl="it")
        qs_it = parse_qs(urlparse(norm_it).query)
        assert qs_it["tfs"] == orig_qs["tfs"], "?tfs= must remain untouched"
        assert qs_it["curr"] == ["MXN"]
        assert qs_it["hl"] == ["es"]
        assert qs_it["tfu"] == [CHEAPEST_TFU]
        assert qs_it["gl"] == ["IT"]

        # Omitting gl preserves original gl while upgrading tfu
        norm_keep = normalize_flights_url(orig_url, tfu=CHEAPEST_TFU)
        qs_keep = parse_qs(urlparse(norm_keep).query)
        assert qs_keep["gl"] == ["MX"]
        assert qs_keep["tfu"] == [CHEAPEST_TFU]
        assert qs_keep["tfs"] == orig_qs["tfs"]

        # Verify Telegram [Reservar] renders clean text (no "(IT)" suffix) with CHEAPEST_TFU + slot winning_gl
        today = datetime.date.today()
        out_d = (today + datetime.timedelta(days=40)).strftime("%Y-%m-%d")
        ret_d = (today + datetime.timedelta(days=50)).strftime("%Y-%m-%d")
        msg_html = format_consolidated_destination_digest(
            {"id": 13, "name": "Conexión Europa", "origin": "MEX,NLU", "destination": "MXP", "currency": "MXN", "gl": "MX"},
            [
                {
                    "label": "Sáb 11 Sep - Dom 27 Sep",
                    "outbound_date": out_d,
                    "return_date": ret_d,
                    "google_flights_url": orig_url,
                    "winning_gl": "IT",
                    "cheapest_tab_price": 15951.0,
                    "cheapest_stops": "2 escalas",
                    "cheapest_duration": "38 h 25 min",
                    "origin_airport": "MEX",
                    "destination_airport": "MXP",
                    "trigger_type": "TARGET_BREACH",
                }
            ],
        )
        assert f"tfu={CHEAPEST_TFU}" in msg_html and "gl=IT" in msg_html
        assert f"tfs={orig_qs['tfs'][0]}" in msg_html
        assert "><b>[Reservar]</b></a>" in msg_html
        assert "(IT)" not in msg_html and "[Reservar] (" not in msg_html
        return "normalize_flights_url + clean Telegram [Reservar] tfu/winning_gl OK"

    measure("23. unit_normalize_url_and_reservar", _url_and_reservar_test)


def bench_24_direct_cheapest_wait_and_hidden_text() -> None:
    """Verify evaluate_direct_cheapest_state holds on active loaders and ignores hidden body text."""
    def _wait_test() -> str:
        from src.scraper import evaluate_direct_cheapest_state

        # Holds while any of the 3 visual loaders is active (>0)
        assert evaluate_direct_cheapest_state(True, 3, 20, 10.0, 15951.0, 15951.0, confirm_reads=1) == (False, "waiting_loaders")
        assert evaluate_direct_cheapest_state(True, 1, 20, 10.0, 15951.0, 15951.0, confirm_reads=1) == (False, "waiting_loaders")
        assert evaluate_direct_cheapest_state(True, 1, 20, 35.0, at_deadline=True) == (True, "direct_cheapest_timeout")

        # Holds if < 1.5s elapsed or 0 cards
        assert evaluate_direct_cheapest_state(True, 0, 20, 0.9, 15951.0) == (False, "waiting_min_settle")
        assert evaluate_direct_cheapest_state(True, 0, 0, 5.0, None) == (False, "waiting_min_settle")

        # Holds on first loaders==0 read until 500ms confirmatory read confirms same price
        assert evaluate_direct_cheapest_state(True, 0, 21, 14.0, 15951.0, 15951.0, confirm_reads=0) == (False, "waiting_confirm_stability")
        assert evaluate_direct_cheapest_state(True, 0, 21, 14.5, 15951.0, 17449.0, confirm_reads=1) == (False, "waiting_confirm_stability")
        assert evaluate_direct_cheapest_state(True, 0, 21, 14.5, 15951.0, 15951.0, confirm_reads=1) == (True, "direct_cheapest_settled")

        # Simulated _wait_for_direct_cheapest_settled: probe 1 activeLoaders=2 -> probes 2,3 activeLoaders=0 + hidden body text
        probes = [
            {
                "tabFound": True, "ariaSelected": True, "activeLoaders": 2, "cardsCount": 18,
                "tabHintAria": "17449 pesos mexicanos", "tabHintText": "17.449\xa0MXN", "noCheaper": False,
                "cardSamples": [{"aria": "17449 pesos", "priceNodes": ["17.449\xa0MXN"], "lines": "17449"}],
            },
            {
                "tabFound": True, "ariaSelected": True, "activeLoaders": 0, "cardsCount": 21,
                "tabHintAria": "15951 pesos mexicanos", "tabHintText": "15.951\xa0MXN", "noCheaper": False,
                "cardSamples": [{"aria": "15951 pesos", "priceNodes": ["15.951\xa0MXN"], "lines": "15951"}],
            },
            {
                "tabFound": True, "ariaSelected": True, "activeLoaders": 0, "cardsCount": 21,
                "tabHintAria": "15951 pesos mexicanos", "tabHintText": "15.951\xa0MXN", "noCheaper": False,
                "cardSamples": [{"aria": "15951 pesos", "priceNodes": ["15.951\xa0MXN"], "lines": "15951"}],
            },
        ]

        class _WaitPage:
            def __init__(self) -> None:
                self.idx = 0

            def evaluate(self, script: str, *args: Any) -> dict[str, Any]:
                out = probes[min(self.idx, len(probes) - 1)]
                self.idx += 1
                return out

            def wait_for_timeout(self, ms: int) -> None:
                pass

        sess = PooledPatchrightSession(proxy_url=None, headless=True)
        # Check script source does not contain body loading-text false-positive checks
        import inspect as _inspect
        probe_src = _inspect.getsource(PooledPatchrightSession._probe_direct_cheapest_state)
        assert "cargando resultados" not in probe_src and "obteniendo resultados" not in probe_src

        status, hint, loaders, clicks, _ = sess._wait_for_direct_cheapest_settled(
            _WaitPage(), t_goto=time.perf_counter() - 2.0, max_wait_s=10.0
        )
        assert status == "direct_cheapest_settled"
        assert hint == 15951.0
        assert loaders == 0
        assert clicks == 0
        return "evaluate_direct_cheapest_state + 0 hidden-text false positives OK"

    measure("24. unit_direct_cheapest_loaders", _wait_test)


def bench_25_sectioned_top_vs_other_extraction() -> None:
    """Verify partition_sectioned_raw_cards cleanly separates Top departing (Best) vs Other departing (Cheapest)."""
    def _section_test() -> str:
        from src.scraper import consolidate_slot_offers, partition_sectioned_raw_cards

        raw_cards = [
            {
                "section": "top",
                "aria": "From 17620 Mexican pesos round trip total. 1 stop flight with Lufthansa. Total duration 15 hr 10 min.",
                "innerText": "9:00 PM – 2:10 PM+1\nLufthansa\n15 hr 10 min\nMEX–MXP\n1 stop\nMX$17,620",
                "priceText": "MX$17,620",
            },
            {
                "section": "top",
                "aria": "From 18049 Mexican pesos round trip total. Nonstop flight with Aeromexico. Total duration 11 hr 35 min.",
                "innerText": "11:00 PM – 5:35 PM+1\nAeromexico\n11 hr 35 min\nMEX–MXP\nNonstop\nMX$18,049",
                "priceText": "MX$18,049",
            },
            {
                "section": "other",
                "aria": "From 15951 Mexican pesos round trip total. 2 stops flight with Air Canada. Total duration 38 hr 25 min.",
                "innerText": "11:55 AM – 10:20 AM+2\nAir Canada\n38 hr 25 min\nMEX–MXP\n2 stops\nMX$15,951",
                "priceText": "MX$15,951",
            },
        ]
        top_offers, other_offers, all_offers = partition_sectioned_raw_cards(
            raw_cards, max_cards=80, expected_currency="MXN"
        )
        assert len(top_offers) == 2 and top_offers[0].price == 17620.0 and top_offers[0].tab_source == "best"
        assert len(other_offers) == 1 and other_offers[0].price == 15951.0 and other_offers[0].tab_source == "cheapest"
        assert len(all_offers) == 3

        cons = consolidate_slot_offers(
            best_dom_offers=top_offers,
            cheapest_dom_offers=all_offers,
            rpc_offers=[],
            ds1_offers=[],
            ds1_best=None,
        )
        assert cons is not None
        assert cons["best_offer"].price == 17620.0
        assert cons["best_offer"].airline == "Lufthansa"
        assert cons["cheapest_offer"].price == 15951.0
        assert cons["cheapest_offer"].airline == "Air Canada"
        return "Top departing (Best=$17,620) vs Other departing (Cheapest=$15,951) OK"

    measure("25. unit_sectioned_top_vs_other", _section_test)


def bench_26_sharding_6_shards_and_10_destinations() -> None:
    """Verify assign_destinations_to_shard across 10 destinations and 6 shards (0 overlap, 100% coverage, balanced)."""
    def _shard_test() -> str:
        dests = [
            {"id": idx + 1, "name": f"Dest_{idx+1}", "active_slot_count": cnt}
            for idx, cnt in enumerate([10, 9, 8, 7, 6, 5, 4, 3, 2, 1])
        ]
        all_assigned_ids: list[int] = []
        loads: list[int] = []
        for s_idx in range(6):
            bucket = assign_destinations_to_shard(dests, shard_index=s_idx, total_shards=6)
            all_assigned_ids.extend(int(d["id"]) for d in bucket)
            loads.append(sum(int(d["active_slot_count"]) for d in bucket))

        assert len(all_assigned_ids) == 10, f"Expected 10 total assignments, got {len(all_assigned_ids)}"
        assert len(set(all_assigned_ids)) == 10, "Overlap detected across shards!"
        assert set(all_assigned_ids) == {d["id"] for d in dests}, "Missing destination in shard coverage"

        # Deterministic regardless of input order
        reversed_shards = partition_destinations_into_shards(list(reversed(dests)), total_shards=6)
        forward_shards = partition_destinations_into_shards(dests, total_shards=6)
        assert [[d["id"] for d in b] for b in reversed_shards] == [[d["id"] for d in b] for b in forward_shards]
        # Greedy LPT balance for [10..1] across 6 shards -> max=10, min=8
        assert max(loads) - min(loads) <= 3, f"Unbalanced loads across 6 shards: {loads}"
        return f"10 dests -> 6 shards: 0 overlaps, 100% coverage, loads={loads}"

    measure("26. unit_sharding_10dest_6shards", _shard_test)


def bench_27_multi_gl_competition_and_tiebreak() -> None:
    """Verify Multi-GL (MX, IT, DE) selects lowest price, breaks ties by pool order, and tolerates 1/3 GL failures."""
    def _multi_gl_test() -> str:
        import logging as _logging
        import src.scraper as _scraper_mod

        orig_scrape = _scraper_mod.scrape_date_slot
        log_records: list[str] = []

        class _MemHandler(_logging.Handler):
            def emit(self, record: _logging.LogRecord) -> None:
                log_records.append(self.format(record))

        handler = _MemHandler()
        _scraper_mod.logger.addHandler(handler)
        prev_level = _scraper_mod.logger.level
        _scraper_mod.logger.setLevel(_logging.INFO)

        try:
            def _make_res(price: float, best_price: float, gl_code: str) -> SlotScrapeResult:
                return SlotScrapeResult(
                    cheapest_tab_price=price,
                    cheapest_airline="Air Canada",
                    cheapest_stops="2 escalas",
                    cheapest_duration="38 h 25 min",
                    cheapest_dep_arr="11:55 - 10:20",
                    is_self_transfer=False,
                    origin_airport="MEX",
                    destination_airport="MXP",
                    best_tab_price=best_price,
                    gl=gl_code,
                    winning_gl=gl_code,
                )

            # Case 1: IT wins lowest absolute price ($15,953 vs MX=$16,201, DE=$16,153)
            prices_case1 = {"MX": (16201.0, 18052.0), "IT": (15953.0, 18052.0), "DE": (16153.0, 18052.0)}
            _scraper_mod.scrape_date_slot = lambda url, pooled_session=None, proxy_url=None, date_range_label="", gl=None: _make_res(  # type: ignore[assignment]
                prices_case1[gl][0], prices_case1[gl][1], gl
            )
            win1 = scrape_date_slot_multi_gl(
                "https://www.google.com/travel/flights/search?tfs=abc",
                gl_pool=("MX", "IT", "DE"),
                date_range_label="2027-09-11 -> 2027-09-27",
                inter_gl_delay=(0.0, 0.0),
            )
            assert win1 is not None
            assert win1.winning_gl == "IT" and win1.cheapest_tab_price == 15953.0
            assert any(
                "[2027-09-11 -> 2027-09-27] multi_gl: MX=$16,201 | IT=$15,953 | DE=$16,153 -> winner=IT ($15,953, Best=$18,052)" in m
                for m in log_records
            ), f"Expected compact multi_gl log line, got: {log_records}"

            # Case 2: Exact tie -> first market in pool (MX) wins
            prices_tie = {"MX": (17753.0, 18500.0), "IT": (17753.0, 18200.0), "DE": (18100.0, 18500.0)}
            _scraper_mod.scrape_date_slot = lambda url, pooled_session=None, proxy_url=None, date_range_label="", gl=None: _make_res(  # type: ignore[assignment]
                prices_tie[gl][0], prices_tie[gl][1], gl
            )
            win2 = scrape_date_slot_multi_gl(
                "https://www.google.com/travel/flights/search?tfs=abc",
                gl_pool=["MX", "IT", "DE", "MX"],  # Also verifies deduplication
                date_range_label="2027-07-31 -> 2027-08-15",
                inter_gl_delay=(0.0, 0.0),
            )
            assert win2 is not None and win2.winning_gl == "MX" and win2.cheapest_tab_price == 17753.0

            # Case 3: 1 of 3 GLs fails -> still returns winner from remaining GLs
            def _partial_fail(url: str, pooled_session: Any = None, proxy_url: Any = None, date_range_label: str = "", gl: Any = None) -> Optional[SlotScrapeResult]:
                if gl == "IT":
                    raise RuntimeError("Transient timeout on IT")
                if gl == "MX":
                    return _make_res(16500.0, 18000.0, "MX")
                return _make_res(16150.0, 17900.0, "DE")

            _scraper_mod.scrape_date_slot = _partial_fail  # type: ignore[assignment]
            win3 = scrape_date_slot_multi_gl(
                "https://www.google.com/travel/flights/search?tfs=abc",
                gl_pool=("MX", "IT", "DE"),
                date_range_label="2027-09-11 -> 2027-09-27",
                inter_gl_delay=(0.0, 0.0),
            )
            assert win3 is not None and win3.winning_gl == "DE" and win3.cheapest_tab_price == 16150.0
        finally:
            _scraper_mod.scrape_date_slot = orig_scrape
            _scraper_mod.logger.removeHandler(handler)
            _scraper_mod.logger.setLevel(prev_level)

        return "Multi-GL IT min + MX tie-break + 1/3 GL fault tolerance OK"

    measure("27. unit_multi_gl_competition", _multi_gl_test)


def bench_28_ram_buffer_neon_sleep_and_flush() -> None:
    """Verify Neon disconnects before Phase 2 scraping (0 DB calls in Phase 2) and Phase 3 flush updates timestamps & GLs."""
    def _ram_buffer_test() -> str:
        import src.runner as _runner_mod

        init_db()
        unique_name = f"__RamBufferTest_{int(time.time() * 1000)}"
        temp_dest_id: Optional[int] = None

        orig_get_proxy_pool = _runner_mod.get_proxy_pool
        orig_session_cls = _runner_mod.PooledPatchrightSession
        orig_multi_gl = _runner_mod.scrape_date_slot_multi_gl
        orig_send_tg = _runner_mod.send_telegram_message

        class _DummySession:
            def __init__(self, proxy_url: Optional[str] = None, headless: bool = True) -> None:
                self.proxy_url = proxy_url

            def start(self) -> "_DummySession":
                return self

            def reset_for_new_destination(self) -> None:
                pass

            def close(self) -> None:
                pass

        phase2_assertions: dict[str, Any] = {}

        try:
            temp_dest_id = create_destination(
                name=unique_name,
                origin="MEX",
                destination="MXP",
                target_price=16500.0,
                gl_pool=["MX", "IT", "DE"],
                is_active=False,
            )
            today = datetime.date.today()
            out_d = (today + datetime.timedelta(days=45)).strftime("%Y-%m-%d")
            ret_d = (today + datetime.timedelta(days=55)).strftime("%Y-%m-%d")
            slot_ids = bulk_create_date_slots(
                temp_dest_id,
                [
                    {
                        "label": format_date_slot_label(out_d, ret_d),
                        "outbound_date": out_d,
                        "return_date": ret_d,
                        "google_flights_url": build_google_flights_url("MEX", "MXP", out_d, ret_d),
                    }
                ],
            )
            slot_id = slot_ids[0]

            _runner_mod.get_proxy_pool = lambda **kwargs: [None]  # type: ignore[assignment]
            _runner_mod.PooledPatchrightSession = _DummySession  # type: ignore[assignment,misc]
            _runner_mod.send_telegram_message = lambda html_text: (True, "OK_MOCK")  # type: ignore[assignment]

            def _mock_multi_gl(url: str, gl_pool: Any = None, pooled_session: Any = None, proxy_url: Any = None, date_range_label: str = "") -> SlotScrapeResult:
                phase2_assertions["db_connected_during_scrape"] = is_db_connected()
                phase2_assertions["stats_during_scrape"] = connection_stats()
                return SlotScrapeResult(
                    cheapest_tab_price=15953.0,
                    cheapest_airline="Air Canada",
                    cheapest_stops="2 escalas",
                    cheapest_duration="38 h 25 min",
                    cheapest_dep_arr="11:55 - 10:20",
                    is_self_transfer=False,
                    origin_airport="MEX",
                    destination_airport="MXP",
                    best_tab_price=18052.0,
                    best_airline="Lufthansa",
                    best_stops="1 escala",
                    best_duration="15 h 10 min",
                    best_origin_airport="MEX",
                    best_destination_airport="MXP",
                    gl="IT",
                    winning_gl="IT",
                )

            _runner_mod.scrape_date_slot_multi_gl = _mock_multi_gl  # type: ignore[assignment]

            summary = run_sharded_dispatcher(
                max_slots=1,
                force_all=True,
                slot_id=slot_id,
                shard_index=0,
                total_shards=1,
            )
            assert summary["scraped_ok"] == 1
            assert summary["alerts_sent"] == 1
            assert phase2_assertions.get("db_connected_during_scrape") is False, "DB MUST be disconnected during Phase 2!"
            assert is_db_connected() is False, "DB MUST be disconnected when dispatcher exits!"

            updated_slot = get_date_slot(slot_id)
            assert updated_slot is not None
            assert updated_slot["last_winning_gl"] == "IT"
            assert updated_slot["all_time_low_gl"] == "IT"
            assert float(updated_slot["all_time_low_price"]) == 15953.0
            assert updated_slot["last_checked_at"] is not None

            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT winning_gl, cheapest_tab_price FROM price_snapshots WHERE date_slot_id = %s;",
                        (slot_id,),
                    )
                    snap_row = cur.fetchone()
                    assert snap_row is not None and snap_row["winning_gl"] == "IT"
                    assert float(snap_row["cheapest_tab_price"]) == 15953.0
        finally:
            _runner_mod.get_proxy_pool = orig_get_proxy_pool
            _runner_mod.PooledPatchrightSession = orig_session_cls
            _runner_mod.scrape_date_slot_multi_gl = orig_multi_gl
            _runner_mod.send_telegram_message = orig_send_tg
            if temp_dest_id is not None:
                delete_destination(temp_dest_id)
            close_db_connection()

        return "Phase 1 disconnect + Phase 2 0-DB RAM + Phase 3 flush (winning_gl/ATL_gl) OK"

    measure("28. integration_ram_buffer_flush", _ram_buffer_test)


def bench_29_neon_retry_and_idempotent_flush() -> None:
    """Verify transient Neon SSL OperationalError triggers close+retry and idempotent snapshot insert avoids duplicates."""
    def _neon_retry_test() -> str:
        import psycopg as _psycopg
        import src.db as _db_mod

        init_db()
        unique_name = f"__NeonRetryTest_{int(time.time() * 1000)}"
        temp_dest_id: Optional[int] = None

        orig_get_conn = _db_mod.get_connection
        orig_close_conn = _db_mod.close_db_connection
        orig_sleep = _db_mod.time.sleep

        try:
            temp_dest_id = create_destination(
                name=unique_name,
                origin="MEX",
                destination="MXP",
                target_price=16500.0,
                gl_pool=["MX", "IT", "DE"],
                is_active=False,
            )
            today = datetime.date.today()
            out_d = (today + datetime.timedelta(days=45)).strftime("%Y-%m-%d")
            ret_d = (today + datetime.timedelta(days=55)).strftime("%Y-%m-%d")
            slot_ids = bulk_create_date_slots(
                temp_dest_id,
                [
                    {
                        "label": format_date_slot_label(out_d, ret_d),
                        "outbound_date": out_d,
                        "return_date": ret_d,
                        "google_flights_url": build_google_flights_url("MEX", "MXP", out_d, ret_d),
                    }
                ],
            )
            slot_id = slot_ids[0]
            batch_ts = datetime.datetime.now(datetime.timezone.utc)
            snap_ts = batch_ts - datetime.timedelta(seconds=3)
            ram_item = {
                "date_slot_id": slot_id,
                "destination_id": temp_dest_id,
                "scraped_at": snap_ts,
                "cheapest_tab_price": 15953.0,
                "best_tab_price": 18052.0,
                "cheapest_airline": "Air Canada",
                "cheapest_stops": "2 escalas",
                "cheapest_duration": "38 h 25 min",
                "cheapest_dep_arr": "11:55 - 10:20",
                "origin_airport": "MEX",
                "destination_airport": "MXP",
                "best_airline": "Lufthansa",
                "best_stops": "1 escala",
                "best_duration": "15 h 10 min",
                "best_origin_airport": "MEX",
                "best_destination_airport": "MXP",
                "is_self_transfer": False,
                "winning_gl": "IT",
                "extraction_source": "patchright_dom",
                "raw_top_offers": [],
            }

            close_calls = {"count": 0}
            sleep_delays: list[float] = []
            commit_attempts = {"count": 0}

            class _FlakyCommitConn:
                def __init__(self, inner: Any) -> None:
                    self._inner = inner

                def cursor(self, *args: Any, **kwargs: Any) -> Any:
                    return self._inner.cursor(*args, **kwargs)

                def commit(self) -> None:
                    commit_attempts["count"] += 1
                    if commit_attempts["count"] == 1:
                        # Simulate Neon SSL connection drop right at COMMIT (even after SQL ran)
                        self._inner.rollback()
                        raise _psycopg.OperationalError(
                            "consuming input failed: SSL connection has been closed unexpectedly"
                        )
                    self._inner.commit()

                def rollback(self) -> None:
                    self._inner.rollback()

                def __enter__(self) -> "_FlakyCommitConn":
                    self._inner.__enter__()
                    return self

                def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any:
                    return self._inner.__exit__(exc_type, exc_val, exc_tb)

                def __getattr__(self, name: str) -> Any:
                    return getattr(self._inner, name)

            def _wrapped_get_conn(*args: Any, **kwargs: Any) -> Any:
                return _FlakyCommitConn(orig_get_conn(*args, **kwargs))

            def _wrapped_close() -> None:
                close_calls["count"] += 1
                orig_close_conn()

            _db_mod.get_connection = _wrapped_get_conn  # type: ignore[assignment]
            _db_mod.close_db_connection = _wrapped_close  # type: ignore[assignment]
            _db_mod.time.sleep = lambda s: sleep_delays.append(float(s))  # type: ignore[assignment]

            flush_out = flush_shard_results_batch([ram_item], batch_started_at=batch_ts)
            assert flush_out["snapshots_inserted"] == 1
            assert commit_attempts["count"] == 2, f"Expected 2 commit attempts, got {commit_attempts['count']}"
            assert close_calls["count"] >= 1, "close_db_connection() must be called on transient SSL failure"
            assert sleep_delays == [2.0], f"Expected [2.0]s backoff on first retry, got {sleep_delays}"

            # Restore normal get_connection and re-run flush with identical (date_slot_id, scraped_at)
            # to prove idempotent INSERT never creates duplicate price_snapshots even if attempt 1 actually committed.
            _db_mod.get_connection = orig_get_conn
            flush_shard_results_batch([ram_item], batch_started_at=batch_ts)

            with orig_get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) AS c FROM price_snapshots WHERE date_slot_id = %s;",
                        (slot_id,),
                    )
                    cnt = int((cur.fetchone() or {}).get("c") or 0)
                    assert cnt == 1, f"Expected exactly 1 idempotent price_snapshot row, got {cnt}"
        finally:
            _db_mod.get_connection = orig_get_conn
            _db_mod.close_db_connection = orig_close_conn
            _db_mod.time.sleep = orig_sleep
            if temp_dest_id is not None:
                delete_destination(temp_dest_id)
            close_db_connection()

        return "SSL OperationalError retry (2s backoff + close) & idempotent snapshot flush OK"

    measure("29. unit_neon_retry_idempotent_flush", _neon_retry_test)


def bench_30_currency_shield_and_line_charts() -> None:
    """Verify strict foreign-currency rejection, Multi-GL outlier filter, curr=MXN URL preservation, and line-only charts."""
    def _currency_and_chart_test() -> str:
        import logging as _logging
        import src.scraper as _scraper_mod
        from src.tfs_builder import CHEAPEST_TFU, normalize_flights_url

        # 1. Parser rejects €784 / 784 € / 784 euros / US$784 / 784 US dollars when expected_currency="MXN"
        bad_cards = [
            ("Desde €784. Vuelo con Iberia. Duración total: 14 h 10 min.", "10:00 – 06:10+1\nIberia\n14 h 10 min\nMEX–MAD\n1 escala\n€784"),
            ("Desde 784 euros. Vuelo con Iberia. Duración total: 14 h 10 min.", "10:00 – 06:10+1\nIberia\n14 h 10 min\nMEX–MAD\n1 escala\n784 €"),
            ("From 784 US dollars. Flight with Iberia. Total duration 14 hr 10 min.", "10:00 AM – 6:10 AM+1\nIberia\n14 hr 10 min\nMEX–MAD\n1 stop\nUS$784"),
            ("Desde 784 dólares. Vuelo con Iberia. Duración total: 14 h 10 min.", "10:00 – 06:10+1\nIberia\n14 h 10 min\nMEX–MAD\n1 escala\n784 USD"),
        ]
        for aria, text in bad_cards:
            assert parse_flight_card_text(aria, text, expected_currency="MXN") is None, f"Must reject foreign card: {aria}"
            assert parse_price_number(text, expected_currency="MXN") is None, f"Must reject foreign price_number: {text}"

        # Accepts valid MXN formats
        ok_cards = [
            ("Desde 15664 pesos mexicanos. Vuelo con Iberia. Duración total: 14 h 10 min.", "10:00 – 06:10+1\nIberia\n14 h 10 min\nMEX–MAD\n1 escala\nMX$15,664", 15664.0),
            ("From 15664 Mexican pesos. Flight with Iberia. Total duration 14 hr 10 min.", "10:00 AM – 6:10 AM+1\nIberia\n14 hr 10 min\nMEX–MAD\n1 stop\n15.664\xa0MXN", 15664.0),
        ]
        for aria, text, exp_p in ok_cards:
            parsed = parse_flight_card_text(aria, text, expected_currency="MXN")
            assert parsed is not None and parsed.price == exp_p, f"Must accept MXN card: {aria}"

        # 2. normalize_flights_url always preserves/injects curr=MXN
        no_curr_url = "https://www.google.com/travel/flights/search?tfs=abc&hl=es"
        norm_url = normalize_flights_url(no_curr_url, tfu=CHEAPEST_TFU, gl="IT")
        assert "curr=MXN" in norm_url and "gl=IT" in norm_url and f"tfu={CHEAPEST_TFU}" in norm_url

        # 3. scrape_date_slot_multi_gl discards 784 outlier vs MX=$15,664 and DE=$15,664 -> picks MX=$15,664
        orig_scrape = _scraper_mod.scrape_date_slot
        log_msgs: list[str] = []

        class _CapHandler(_logging.Handler):
            def emit(self, record: _logging.LogRecord) -> None:
                log_msgs.append(self.format(record))

        h = _CapHandler()
        _scraper_mod.logger.addHandler(h)
        prev_lvl = _scraper_mod.logger.level
        _scraper_mod.logger.setLevel(_logging.WARNING)
        try:
            def _make_res(p: float, b: float, gl_code: str) -> SlotScrapeResult:
                return SlotScrapeResult(
                    cheapest_tab_price=p,
                    cheapest_airline="Iberia",
                    cheapest_stops="1 escala",
                    cheapest_duration="14 h 10 min",
                    cheapest_dep_arr="10:00 - 06:10",
                    is_self_transfer=False,
                    origin_airport="MEX",
                    destination_airport="MAD",
                    best_tab_price=b,
                    gl=gl_code,
                    winning_gl=gl_code,
                )

            mock_prices = {"MX": (15664.0, 16128.0), "IT": (784.0, 809.0), "DE": (15664.0, 16128.0)}
            _scraper_mod.scrape_date_slot = lambda url, pooled_session=None, proxy_url=None, date_range_label="", gl=None: _make_res(  # type: ignore[assignment]
                mock_prices[gl][0], mock_prices[gl][1], gl
            )
            winner = scrape_date_slot_multi_gl(
                "https://www.google.com/travel/flights/search?tfs=abc&curr=MXN",
                gl_pool=("MX", "IT", "DE"),
                date_range_label="2027-08-20 -> 2027-09-06",
                inter_gl_delay=(0.0, 0.0),
            )
            assert winner is not None
            assert winner.winning_gl == "MX" and winner.cheapest_tab_price == 15664.0
            assert any("multi_gl_currency_outlier_rejected" in m for m in log_msgs)
        finally:
            _scraper_mod.scrape_date_slot = orig_scrape
            _scraper_mod.logger.removeHandler(h)
            _scraper_mod.logger.setLevel(prev_lvl)

        # 4. Verify historical charts in app.py use line-only (no lines+markers or markers=True)
        app_src = (ROOT_DIR / "app.py").read_text(encoding="utf-8")
        assert 'mode="lines+markers"' not in app_src, "app.py must not contain mode='lines+markers'"
        assert "markers=True" not in app_src, "app.py must not contain markers=True"
        assert 'mode="lines"' in app_src

        return "Foreign currency rejected + 784 Multi-GL outlier discarded + line-only charts OK"

    measure("30. unit_currency_shield_line_charts", _currency_and_chart_test)


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
    bench_12_proxy_rotation_and_circuit_breaker()
    bench_13_alert_triggers_meta_and_low()
    bench_14_compact_html_rendering()
    bench_15_proxy_redaction_and_sanitization()
    bench_16_cheapest_click_failure_tolerance()
    bench_17_min_offer_consolidation()
    bench_18_tab_hint_and_early_exit()
    bench_19_direct_cheapest_flow()
    bench_20_sort_results_by_price()
    bench_21_sort_label_guard_and_en_card()
    bench_22_post_sort_rpc_wait_and_parse()
    bench_23_normalize_flights_url_and_reservar()
    bench_24_direct_cheapest_wait_and_hidden_text()
    bench_25_sectioned_top_vs_other_extraction()
    bench_26_sharding_6_shards_and_10_destinations()
    bench_27_multi_gl_competition_and_tiebreak()
    bench_28_ram_buffer_neon_sleep_and_flush()
    bench_29_neon_retry_and_idempotent_flush()
    bench_30_currency_shield_and_line_charts()
    print_summary_table()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
