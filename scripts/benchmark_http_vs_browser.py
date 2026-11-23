"""
Benchmark A/B Empírico: Mode A (Browser Patchright + espera determinística) vs. Mode B (HTTP curl_cffi ds:1).

Reglas estrictas:
  - 100% solo lectura en Neon PostgreSQL (solo SELECT, cero INSERT/UPDATE).
  - Cero consumo de Webshare (ignora PROXY_LIST; usa exclusivamente WARP si está activo o conexión directa).
  - Exporta reporte detallado en consola y CSV local en tmp/benchmark_ab.csv.
"""

from __future__ import annotations

import csv
import os
import random
import sys
import time
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from curl_cffi import requests as curl_requests

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.config import (  # noqa: E402
    DEFAULT_CURRENCY,
    GOOGLE_CONSENT_COOKIES,
    GOOGLE_SOCS_COOKIE_VALUE,
    SOCKS5_PROXY,
    get_proxy_account,
    is_proxy_reachable,
    sanitize_log_text,
)
from src.db import init_db, list_date_slots  # noqa: E402
from src.ds1_parser import parse_ds1_from_html  # noqa: E402
from src.scraper import (  # noqa: E402
    PooledPatchrightSession,
    SlotScrapeResult,
    _extract_expected_corridor_iatas,
    _filter_offers_for_corridor,
    consolidate_slot_offers,
)


def _select_zero_cost_proxy() -> Optional[str]:
    """Use local Cloudflare WARP if reachable, otherwise direct connection (never Webshare)."""
    if SOCKS5_PROXY and is_proxy_reachable(SOCKS5_PROXY):
        return SOCKS5_PROXY
    return None


def _measure_browser_slot(
    session: PooledPatchrightSession,
    url: str,
) -> tuple[Optional[SlotScrapeResult], float, float, str]:
    """
    Run Mode A (Patchright with deterministic Cheapest wait) and measure real wire bytes
    downloaded via CDP `Network.loadingFinished` (`encodedDataLength`).
    Returns `(result, elapsed_s, kb_downloaded, status_note)`.
    """
    t0 = time.perf_counter()
    if not session._context:
        session.start()

    expected_origins, expected_destinations = _extract_expected_corridor_iatas(url)
    expected_curr = (
        parse_qs(urlparse(url).query).get("curr", [DEFAULT_CURRENCY])[0] or DEFAULT_CURRENCY
    ).strip().upper()

    page = session._context.new_page()
    cdp = None
    total_encoded_bytes = 0
    rpc_cheapest_offers: list[Any] = []
    rpc_payload_counter = {"count": 0}

    try:
        try:
            cdp = session._context.new_cdp_session(page)
            cdp.send("Network.enable")

            def _on_loading_finished(params: dict[str, Any]) -> None:
                nonlocal total_encoded_bytes
                total_encoded_bytes += int(params.get("encodedDataLength") or 0)

            cdp.on("Network.loadingFinished", _on_loading_finished)
        except Exception:
            cdp = None

        from src.ds1_parser import parse_ds1_from_html as _parse_ds1, parse_shopping_rpc_response

        def _on_response(response: Any) -> None:
            nonlocal total_encoded_bytes
            try:
                resp_url = response.url
                if cdp is None:
                    hdr_len = response.headers.get("content-length")
                    if hdr_len and str(hdr_len).isdigit():
                        total_encoded_bytes += int(hdr_len)
                if (
                    "FlightsFrontendService/GetShoppingResults" in resp_url
                    or "batchexecute" in resp_url
                ) and response.status == 200:
                    body = response.text()
                    if body and len(body) > 200:
                        rpc_payload_counter["count"] += 1
                    parsed_rpc = parse_shopping_rpc_response(body, expected_currency=expected_curr)
                    if parsed_rpc:
                        rpc_cheapest_offers.extend(parsed_rpc)
            except Exception:
                pass

        page.on("response", _on_response)

        page.goto(url, wait_until="domcontentloaded", timeout=35000)
        session._dismiss_consent_if_present(page)

        import re

        try:
            page.wait_for_selector("li.pIav2d", timeout=8000)
        except Exception:
            try:
                search_btn = page.get_by_role("button", name=re.compile(r"^Buscar$|^Search$", re.I)).first
                if search_btn.count() > 0 and search_btn.is_visible():
                    search_btn.click(timeout=3000)
                    page.wait_for_selector("li.pIav2d", timeout=12000)
            except Exception:
                pass
            if page.locator("li.pIav2d").count() == 0:
                reload_btn = page.locator(
                    "button:has-text('Volver a cargar'), button:has-text('Retry'), button:has-text('Reload')"
                ).first
                if reload_btn.count() > 0 and reload_btn.is_visible():
                    try:
                        reload_btn.click(timeout=3000)
                        page.wait_for_selector("li.pIav2d", timeout=10000)
                    except Exception:
                        page.wait_for_timeout(1500)

        html_content = page.content()
        ds1_extraction = _parse_ds1(html_content, expected_currency=expected_curr)

        best_dom_offers = _filter_offers_for_corridor(
            session._extract_dom_cards(page, tab_source="best", max_cards=8, expected_currency=expected_curr),
            expected_origins,
            expected_destinations,
        )

        _, cheapest_status = session._switch_to_cheapest_tab_deterministic(
            page=page,
            rpc_payload_counter=rpc_payload_counter,
            best_dom_offers=best_dom_offers,
        )

        cheapest_dom_offers = _filter_offers_for_corridor(
            session._extract_dom_cards(page, tab_source="cheapest", max_cards=8, expected_currency=expected_curr),
            expected_origins,
            expected_destinations,
        )
        filtered_rpc_offers = _filter_offers_for_corridor(
            rpc_cheapest_offers,
            expected_origins,
            expected_destinations,
        )

        ds1_offers: list[Any] = []
        ds1_best = None
        if ds1_extraction:
            ds1_raw = [ds1_extraction.cheapest_offer, *ds1_extraction.top_offers]
            ds1_offers = _filter_offers_for_corridor(ds1_raw, expected_origins, expected_destinations)
            if ds1_extraction.best_offer:
                best_f = _filter_offers_for_corridor([ds1_extraction.best_offer], expected_origins, expected_destinations)
                ds1_best = best_f[0] if best_f else None

        consolidated = consolidate_slot_offers(
            best_dom_offers=best_dom_offers,
            cheapest_dom_offers=cheapest_dom_offers,
            rpc_offers=filtered_rpc_offers,
            ds1_offers=ds1_offers,
            ds1_best=ds1_best,
            expected_origins=expected_origins,
            expected_destinations=expected_destinations,
        )
        if not consolidated:
            return None, time.perf_counter() - t0, total_encoded_bytes / 1024.0, cheapest_status

        cheapest_offer = consolidated["cheapest_offer"]
        best_offer = consolidated["best_offer"]

        res = SlotScrapeResult(
            cheapest_tab_price=cheapest_offer.price,
            cheapest_airline=cheapest_offer.airline,
            cheapest_stops=cheapest_offer.stops,
            cheapest_duration=cheapest_offer.duration,
            cheapest_dep_arr=cheapest_offer.dep_arr,
            is_self_transfer=cheapest_offer.is_self_transfer,
            origin_airport=consolidated["c_orig"],
            destination_airport=consolidated["c_dest"],
            best_tab_price=best_offer.price,
            best_airline=best_offer.airline,
            best_stops=best_offer.stops,
            best_duration=best_offer.duration,
            best_origin_airport=consolidated["b_orig"],
            best_destination_airport=consolidated["b_dest"],
            extraction_source=consolidated["extraction_source"],
            cheapest_tab_status=cheapest_status,
            cheapest_source=consolidated["cheapest_source"],
        )
        return res, time.perf_counter() - t0, total_encoded_bytes / 1024.0, cheapest_status
    except Exception as exc:
        return None, time.perf_counter() - t0, total_encoded_bytes / 1024.0, f"error:{sanitize_log_text(exc)[:60]}"
    finally:
        try:
            if cdp is not None:
                cdp.detach()
        except Exception:
            pass
        try:
            page.close()
        except Exception:
            pass


def _measure_http_slot(
    url: str,
    proxy_url: Optional[str],
) -> tuple[Optional[SlotScrapeResult], float, float, str]:
    """
    Run Mode B (HTTP curl_cffi parsing AF_initDataCallback(ds:1) over best + other flights).
    Returns `(result, elapsed_s, kb_downloaded, status_note)`.
    """
    t0 = time.perf_counter()
    expected_curr = (
        parse_qs(urlparse(url).query).get("curr", [DEFAULT_CURRENCY])[0] or DEFAULT_CURRENCY
    ).strip().upper()
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-MX,es;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
    }
    cookies = {
        "SOCS": GOOGLE_SOCS_COOKIE_VALUE,
        "CONSENT": "YES+cb.20240514-07-p0.es+FX+999",
    }
    kb = 0.0
    try:
        resp = curl_requests.get(
            url,
            headers=headers,
            cookies=cookies,
            proxy=proxy_url,
            impersonate="chrome",
            timeout=20,
            allow_redirects=True,
        )
        kb = len(resp.content or b"") / 1024.0
        if resp.status_code != 200 or not resp.text:
            return None, time.perf_counter() - t0, kb, f"http_{resp.status_code}"

        extraction = parse_ds1_from_html(resp.text, expected_currency=expected_curr)
        if not extraction:
            return None, time.perf_counter() - t0, kb, "ds1_no_offers"

        expected_origins, expected_destinations = _extract_expected_corridor_iatas(url)
        filtered_top = _filter_offers_for_corridor(
            [extraction.cheapest_offer, *extraction.top_offers],
            expected_origins,
            expected_destinations,
        )
        if not filtered_top:
            return None, time.perf_counter() - t0, kb, "ds1_filtered_empty"

        filtered_top.sort(key=lambda o: o.price)
        cheapest = filtered_top[0]
        best_filtered = (
            _filter_offers_for_corridor([extraction.best_offer], expected_origins, expected_destinations)
            if extraction.best_offer
            else []
        )
        best = best_filtered[0] if best_filtered else cheapest

        res = SlotScrapeResult(
            cheapest_tab_price=cheapest.price,
            cheapest_airline=cheapest.airline,
            cheapest_stops=cheapest.stops,
            cheapest_duration=cheapest.duration,
            cheapest_dep_arr=cheapest.dep_arr,
            is_self_transfer=cheapest.is_self_transfer,
            origin_airport=cheapest.origin_airport,
            destination_airport=cheapest.destination_airport,
            best_tab_price=best.price,
            best_airline=best.airline,
            best_stops=best.stops,
            best_duration=best.duration,
            best_origin_airport=best.origin_airport or cheapest.origin_airport,
            best_destination_airport=best.destination_airport or cheapest.destination_airport,
            extraction_source="curl_cffi_ssr",
        )
        return res, time.perf_counter() - t0, kb, "ok"
    except Exception as exc:
        return None, time.perf_counter() - t0, kb, f"error:{sanitize_log_text(exc)[:60]}"


def main() -> int:
    # Ensure zero Webshare consumption regardless of environment
    os.environ["PROXY_LIST"] = ""
    init_db()

    all_active = list_date_slots(active_only=True)
    searchable_slots = [
        s
        for s in all_active
        if s.get("destination_is_active") and s.get("window_info", {}).get("is_searchable")
    ]

    zero_cost_proxy = _select_zero_cost_proxy()
    print(
        f"Benchmark A/B starting | searchable_active_slots={len(searchable_slots)} | "
        f"network_channel={get_proxy_account(zero_cost_proxy)} | read_only_db=True | webshare=DISABLED",
        flush=True,
    )

    tmp_dir = ROOT_DIR / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    csv_path = tmp_dir / "benchmark_ab.csv"

    rows: list[dict[str, Any]] = []
    last_dest_id: Optional[int] = None

    with PooledPatchrightSession(proxy_url=zero_cost_proxy, headless=False) as session:
        for idx, slot in enumerate(searchable_slots, start=1):
            d_id = int(slot["destination_id"])
            if last_dest_id is not None and d_id != last_dest_id:
                session.reset_for_new_destination()
            last_dest_id = d_id

            url = str(slot["google_flights_url"])
            b_res, b_sec, b_kb, b_note = _measure_browser_slot(session, url)
            h_res, h_sec, h_kb, h_note = _measure_http_slot(url, zero_cost_proxy)

            db_cheapest = slot.get("latest_cheapest_price")
            db_best = slot.get("latest_best_price")

            b_c_price = b_res.cheapest_tab_price if b_res else None
            b_b_price = b_res.best_tab_price if b_res else None
            h_c_price = h_res.cheapest_tab_price if h_res else None
            h_b_price = h_res.best_tab_price if h_res else None

            diff_c_abs = (
                (h_c_price - b_c_price)
                if (h_c_price is not None and b_c_price is not None)
                else None
            )
            diff_c_pct = (
                ((h_c_price - b_c_price) / b_c_price * 100.0)
                if (diff_c_abs is not None and b_c_price and b_c_price > 0)
                else None
            )

            reason = "exact_match"
            if b_res is None and h_res is None:
                reason = f"both_failed({b_note}|{h_note})"
            elif b_res is not None and h_res is None:
                reason = f"http_failed({h_note})"
            elif b_res is None and h_res is not None:
                reason = f"browser_failed({b_note})"
            elif b_c_price != h_c_price or b_b_price != h_b_price:
                parts = []
                if b_c_price != h_c_price:
                    parts.append("cheapest_diff")
                if b_b_price != h_b_price:
                    parts.append("best_diff")
                reason = "+".join(parts)

            row = {
                "slot_id": slot["id"],
                "destination": slot.get("destination_name"),
                "dates": f"{slot.get('outbound_date')} -> {slot.get('return_date') or 'OW'}",
                "db_latest_cheapest": db_cheapest,
                "db_latest_best": db_best,
                "browser_best_price": b_b_price,
                "browser_best_route": f"{b_res.best_origin_airport}->{b_res.best_destination_airport}" if b_res else "",
                "browser_best_stops": b_res.best_stops if b_res else "",
                "browser_cheapest_price": b_c_price,
                "browser_cheapest_route": f"{b_res.origin_airport}->{b_res.destination_airport}" if b_res else "",
                "browser_cheapest_stops": b_res.cheapest_stops if b_res else "",
                "browser_self_transfer": b_res.is_self_transfer if b_res else False,
                "browser_cheapest_status": b_note,
                "browser_sec": round(b_sec, 2),
                "browser_kb": round(b_kb, 1),
                "http_best_price": h_b_price,
                "http_best_route": f"{h_res.best_origin_airport}->{h_res.best_destination_airport}" if h_res else "",
                "http_best_stops": h_res.best_stops if h_res else "",
                "http_cheapest_price": h_c_price,
                "http_cheapest_route": f"{h_res.origin_airport}->{h_res.destination_airport}" if h_res else "",
                "http_cheapest_stops": h_res.cheapest_stops if h_res else "",
                "http_self_transfer": h_res.is_self_transfer if h_res else False,
                "http_status": h_note,
                "http_sec": round(h_sec, 2),
                "http_kb": round(h_kb, 1),
                "cheapest_diff_mxn": round(diff_c_abs, 2) if diff_c_abs is not None else "",
                "cheapest_diff_pct": round(diff_c_pct, 2) if diff_c_pct is not None else "",
                "reason": reason,
            }
            rows.append(row)
            print(
                f"[{idx:02d}/{len(searchable_slots)}] {row['destination']} | {row['dates']} | "
                f"Browser: C={b_c_price} B={b_b_price} ({b_sec:.1f}s, {b_kb:.0f}KB, {b_note}) | "
                f"HTTP: C={h_c_price} B={h_b_price} ({h_sec:.1f}s, {h_kb:.0f}KB, {h_note})",
                flush=True,
            )

            if idx < len(searchable_slots):
                time.sleep(random.uniform(1.5, 3.0))

    if rows:
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    total = len(rows)
    b_ok = [r for r in rows if r["browser_cheapest_price"] is not None]
    h_ok = [r for r in rows if r["http_cheapest_price"] is not None]
    both_ok = [
        r
        for r in rows
        if r["browser_cheapest_price"] is not None and r["http_cheapest_price"] is not None
    ]
    exact_best = [
        r for r in both_ok if r["browser_best_price"] == r["http_best_price"]
    ]
    exact_cheapest = [
        r for r in both_ok if r["browser_cheapest_price"] == r["http_cheapest_price"]
    ]

    avg_b_kb = sum(r["browser_kb"] for r in rows) / max(1, total)
    avg_b_sec = sum(r["browser_sec"] for r in rows) / max(1, total)
    avg_h_kb = sum(r["http_kb"] for r in rows) / max(1, total)
    avg_h_sec = sum(r["http_sec"] for r in rows) / max(1, total)

    print("\n" + "=" * 100)
    print("RESUMEN GLOBAL BENCHMARK A/B")
    print("=" * 100)
    print(f"Total slots evaluados:         {total}")
    print(f"Tasa éxito Modo A (Browser):   {len(b_ok)}/{total} ({len(b_ok)/max(1,total)*100:.1f}%)")
    print(f"Tasa éxito Modo B (HTTP):      {len(h_ok)}/{total} ({len(h_ok)/max(1,total)*100:.1f}%)")
    print(f"Coincidencia exacta best:      {len(exact_best)}/{len(both_ok)} ({len(exact_best)/max(1,len(both_ok))*100:.1f}%)")
    print(f"Coincidencia exacta cheapest:  {len(exact_cheapest)}/{len(both_ok)} ({len(exact_cheapest)/max(1,len(both_ok))*100:.1f}%)")
    print(f"Promedio Modo A (Browser):     {avg_b_kb:.1f} KB ({avg_b_kb/1024:.2f} MB) | {avg_b_sec:.2f}s / slot")
    print(f"Promedio Modo B (HTTP):        {avg_h_kb:.1f} KB ({avg_h_kb/1024:.2f} MB) | {avg_h_sec:.2f}s / slot")
    print(f"CSV guardado en:               {csv_path}")

    discrepancies = [
        r
        for r in rows
        if r["browser_cheapest_price"] != r["http_cheapest_price"]
        or r["browser_best_price"] != r["http_best_price"]
    ]
    print("\n" + "=" * 100)
    print(f"TABLA DE DISCREPANCIAS ({len(discrepancies)}/{total})")
    print("=" * 100)
    for r in discrepancies:
        diff_str = (
            f"${r['cheapest_diff_mxn']:+.0f} ({r['cheapest_diff_pct']:+.1f}%)"
            if r["cheapest_diff_mxn"] != ""
            else "N/A"
        )
        print(
            f"{r['destination']} | {r['dates']} | "
            f"Best B={r['browser_best_price']} vs H={r['http_best_price']} | "
            f"Cheapest B={r['browser_cheapest_price']} vs H={r['http_cheapest_price']} | "
            f"Diff={diff_str} | self_trf={r['browser_self_transfer']} | {r['reason']}"
        )

    db_diffs = [
        r
        for r in rows
        if r["browser_cheapest_price"] is not None
        and r["db_latest_cheapest"] is not None
        and abs(float(r["browser_cheapest_price"]) - float(r["db_latest_cheapest"])) >= 1.0
    ]
    print("\n" + "=" * 100)
    print(f"BROWSER DETERMINÍSTICO VS ÚLTIMO SNAPSHOT EN BD ({len(db_diffs)}/{total})")
    print("=" * 100)
    for r in db_diffs:
        delta = float(r["browser_cheapest_price"]) - float(r["db_latest_cheapest"])
        print(
            f"Slot {r['slot_id']} | {r['destination']} | {r['dates']} | "
            f"BD={r['db_latest_cheapest']} -> Nuevo={r['browser_cheapest_price']} (delta={delta:+.0f}, {r['browser_cheapest_status']})"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
