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
    MAX_DESTINATIONS,
    MAX_SLOTS_PER_RUN,
    MAX_TOTAL_SLOTS,
    get_flight_window_status,
    get_proxy_account,
    get_proxy_pool,
    get_proxy_tier,
    interleave_proxies_by_account,
    redact_account_for_log,
    redact_proxy_for_log,
    sanitize_log_text,
)
from src.ds1_parser import FlightOffer
from src.runner import ProxyCircuitBreaker
from src.scraper import PooledPatchrightSession, consolidate_slot_offers
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

        # 6. Verify 20x10 capacity expansion defaults
        assert MAX_DESTINATIONS == 20, f"Expected MAX_DESTINATIONS=20, got {MAX_DESTINATIONS}"
        assert MAX_TOTAL_SLOTS == 200, f"Expected MAX_TOTAL_SLOTS=200, got {MAX_TOTAL_SLOTS}"
        assert MAX_SLOTS_PER_RUN >= 40, f"Expected MAX_SLOTS_PER_RUN>=40, got {MAX_SLOTS_PER_RUN}"

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
    """Verify 2-trigger alert engine (Meta & Bajo Histórico), no SHARP_DROP, and FUTURE_RELEASE exclusion."""
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
            "cooldown_hours": 12,
            "filter_config": {},
        }

        # 1. FUTURE_RELEASE (>330d) must always be excluded
        fut_slot = {**base_slot, "outbound_date": future_out, "return_date": future_ret}
        assert (
            evaluate_slot_alert_triggers(
                fut_slot,
                {"cheapest_tab_price": 5000.0, "best_tab_price": 6000.0},
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
            {"cheapest_tab_price": 9900.0, "best_tab_price": 10500.0},
            {"historical_min_price": None, "last_alerted_at": None, "last_alerted_price": None},
        )
        assert t1 is not None and t1["trigger_type"] == "TARGET_BREACH"

        t_cross = evaluate_slot_alert_triggers(
            base_slot,
            {"cheapest_tab_price": 9900.0, "best_tab_price": None},
            {
                "historical_min_price": None,
                "last_alerted_at": now_utc - datetime.timedelta(hours=1),
                "last_alerted_price": 9800.0,
                "prev_effective_price": 10200.0,
            },
        )
        assert t_cross is not None and t_cross["trigger_type"] == "TARGET_BREACH"

        # Staying below target: <3% extra drop blocked; >=3% needs cooldown; >=5% bypasses cooldown
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
        assert (
            evaluate_slot_alert_triggers(
                base_slot,
                {"cheapest_tab_price": 9650.0, "best_tab_price": 10000.0},
                {
                    "historical_min_price": 9900.0,
                    "last_alerted_at": now_utc - datetime.timedelta(hours=2),
                    "last_alerted_price": 10000.0,
                    "prev_effective_price": 9900.0,
                    "last_atl_alerted_price": 9700.0,
                },
            )
            is None
        )
        t_3pct_after_cd = evaluate_slot_alert_triggers(
            base_slot,
            {"cheapest_tab_price": 9650.0, "best_tab_price": 10000.0},
            {
                "historical_min_price": 9900.0,
                "last_alerted_at": now_utc - datetime.timedelta(hours=13),
                "last_alerted_price": 10000.0,
                "prev_effective_price": 9900.0,
                "last_atl_alerted_price": 9700.0,
            },
        )
        assert t_3pct_after_cd is not None and t_3pct_after_cd["trigger_type"] == "TARGET_BREACH"

        t_5pct_bypass = evaluate_slot_alert_triggers(
            base_slot,
            {"cheapest_tab_price": 9500.0, "best_tab_price": 10000.0},
            {
                "historical_min_price": 9900.0,
                "last_alerted_at": now_utc - datetime.timedelta(hours=1),
                "last_alerted_price": 10000.0,
                "prev_effective_price": 9900.0,
                "last_atl_alerted_price": 9600.0,
            },
        )
        assert t_5pct_bypass is not None and t_5pct_bypass["trigger_type"] == "TARGET_BREACH"

        # 4. ALL_TIME_LOW (Bajo Histórico): requires >=2% drop vs reference_low and bypasses cooldown
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

        return "Meta + Bajo Histórico + no SHARP_DROP + FUTURE_RELEASE guard OK"

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
        assert '<a href="https://www.google.com/travel/flights?tfs=abc&amp;hl=es"><b>[Reservar]</b></a>' in rendered
        assert "Iberia" not in rendered and "KLM" not in rendered
        assert "Futuro Lejano" not in rendered
        # Slot 2 had best_tab_price=None so only Barato: must appear for slot 2
        assert rendered.count("Mejor:") == 1

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
        # Still higher than hint before deadline -> wait
        assert evaluate_cheapest_wait_state(True, False, 19236.0, 26725.0, at_deadline=False) == (False, "waiting_dom_sync")
        # Persistent mismatch at deadline -> explicit mismatch status
        assert evaluate_cheapest_wait_state(True, False, 19236.0, 26725.0, at_deadline=True) == (True, "cheapest_tab_mismatch")
        # No-cheaper-options notice
        assert evaluate_cheapest_wait_state(True, False, None, 26725.0, no_cheaper_notice=True) == (True, "cheapest_no_cheaper_notice")
        # No hint fallback requires >= 2 consecutive stable DOM reads
        assert evaluate_cheapest_wait_state(True, False, None, 19236.0, dom_stable_count=1) == (False, "waiting_dom_stability")
        assert evaluate_cheapest_wait_state(True, False, None, 19236.0, dom_stable_count=2) == (True, "cheapest_dom_stabilized")

        return "tab_hint parse + anti-premature-exit + mismatch OK"

    measure("18. unit_tab_hint_and_early_exit", _hint_test)


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
    print_summary_table()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
