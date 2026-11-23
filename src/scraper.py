"""
Two-Tier Hybrid Extraction Engine (`src/scraper.py`) for FlightPulse Matrix v4.0.

Architecture:
  - Browser Instance Pooling: Reuses a single `patchright` (`channel="chrome"`,
    `headless=False` under `xvfb-run`, `no_viewport=True`) browser context per proxy
    group across multiple destinations and date slots.
  - Between destinations: Clears cookies & storage and re-injects Google's `SOCS`
    consent cookie (`CAISHAgBEhJnd3NfMjAyNDA1MTQtMF9SQzIaAmVzIAEaBgiA_LyaBg`).
  - Resource & Telemetry Blocking: Aborts `image`, `media`, `font`, and Google
    Analytics/DoubleClick requests (<100 KB per date slot check).
  - Triple-Layer Resilient Parsing (Best Tab + Cheapest Tab):
      Layer 1: Inline `AF_initDataCallback` `ds:1` SSR parser + `page.on("response")`
               interceptor for `FlightsFrontendService/GetShoppingResults`.
      Layer 2: Semantic ARIA & DOM card extraction on initial "Best" (`Mejores`) tab,
               followed by clicking the "Cheapest" (`Más baratos` / `Cheapest`) tab,
               waiting 1,500ms, and extracting the top 3 cheapest flight cards.
      Layer 3: Fast HTTP SSR fallback via `curl_cffi` TLS impersonation + `fast-flights`.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    curl_requests = None  # type: ignore[assignment]

from src.config import (
    BLOCKED_RESOURCE_TYPES,
    BLOCKED_TELEMETRY_DOMAINS,
    CHEAPEST_TAB_WAIT_MS,
    DEFAULT_CURRENCY,
    DEFAULT_HL,
    GOOGLE_CONSENT_COOKIES,
    GOOGLE_SOCS_COOKIE_VALUE,
    HEADLESS_MODE,
    NAVIGATION_TIMEOUT_MS,
    redact_proxy_for_log,
    sanitize_log_text,
)
from src.ds1_parser import (
    FlightOffer,
    detect_self_transfer,
    extract_ds1_currency_from_html,
    parse_ds1_from_html,
    parse_flight_card_text,
    parse_price_number,
    parse_shopping_rpc_response,
)
from src.tfs_builder import decode_tfs_parameter

logger = logging.getLogger("flightpulse.scraper")


@dataclass
class SlotScrapeResult:
    """Complete dual-tab extraction result for a single `date_slot` with N:M winning airports."""

    cheapest_tab_price: float
    cheapest_airline: str
    cheapest_stops: str
    cheapest_duration: str
    cheapest_dep_arr: str
    is_self_transfer: bool
    origin_airport: Optional[str] = None
    destination_airport: Optional[str] = None
    best_tab_price: Optional[float] = None
    best_airline: Optional[str] = None
    best_stops: Optional[str] = None
    best_duration: Optional[str] = None
    best_origin_airport: Optional[str] = None
    best_destination_airport: Optional[str] = None
    extraction_source: str = "patchright_dom"
    cheapest_tab_status: str = "not_checked"
    cheapest_source: str = "cheapest_dom"
    tab_hint_price: Optional[float] = None
    raw_top_offers: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def format_playwright_proxy(proxy_url: Optional[str]) -> Optional[dict[str, str]]:
    """
    Format proxy URL into Playwright-compatible proxy dict,
    properly extracting username and password authentication if present.
    Supports SOCKS5, HTTP, and HTTPS proxies.
    """
    if not proxy_url:
        return None
    try:
        parsed = urlparse(proxy_url)
        if not parsed.hostname:
            return {"server": proxy_url}
        scheme = parsed.scheme or "http"
        port_part = f":{parsed.port}" if parsed.port else ""
        server_str = f"{scheme}://{parsed.hostname}{port_part}"
        cfg: dict[str, str] = {"server": server_str}
        if parsed.username:
            cfg["username"] = parsed.username
        if parsed.password:
            cfg["password"] = parsed.password
        return cfg
    except Exception:
        return {"server": proxy_url}


def parse_tab_hint_price(aria_label: str = "", text: str = "") -> Optional[float]:
    """
    Extract the numeric price announced in the Google Flights 'Los más bajos' tab header
    (e.g. `aria-label="19236 pesos mexicanos"` or `"19.236\\xa0MXN"` -> `19236.0`).
    """
    for candidate in (aria_label or "", text or ""):
        clean = str(candidate).strip()
        if not clean:
            continue
        parsed = parse_price_number(clean)
        if parsed is not None and parsed > 0:
            return parsed
        # Fallback for bare numeric aria-labels or strings without currency suffix
        m = re.search(r"(\d{1,3}(?:[.,\s\u00a0]\d{3})+|\d{3,7})", clean)
        if m:
            num_raw = re.sub(r"[\s,\u00a0]", "", m.group(1))
            if re.match(r"^\d{1,3}(\.\d{3})+$", num_raw):
                num_raw = num_raw.replace(".", "")
            try:
                val = float(num_raw)
                if 150 <= val <= 1_500_000:
                    return round(val, 2)
            except ValueError:
                pass
    return None


def evaluate_cheapest_wait_state(
    aria_selected: bool,
    spinner_active: bool,
    tab_hint_price: Optional[float],
    dom_min_price: Optional[float],
    no_cheaper_notice: bool = False,
    dom_stable_count: int = 0,
    at_deadline: bool = False,
    spinner_seen: bool = True,
    dom_hint_stable_count: int = 2,
    elapsed_s: float = 2.5,
) -> tuple[bool, str]:
    """
    Decide whether the deterministic 'Los más bajos' wait loop can exit early,
    must keep waiting for DOM synchronization, or timed out / mismatched.
    Prevents premature <2s exits on provisional prices when the spinner has not
    yet mounted/completed or before `tab_hint` + `dom_min` stabilize across 2 reads.
    Returns `(should_exit, status_reason)`.
    """
    if not aria_selected or spinner_active:
        if at_deadline:
            return True, "cheapest_timeout"
        return False, "waiting"

    if no_cheaper_notice:
        return True, "cheapest_no_cheaper_notice"

    if not spinner_seen and elapsed_s < 2.0:
        if at_deadline:
            return True, "cheapest_timeout"
        return False, "waiting_min_settle"

    if tab_hint_price is not None and tab_hint_price > 0:
        if dom_min_price is not None and dom_min_price > 0 and dom_min_price <= tab_hint_price * 1.01:
            if dom_hint_stable_count >= 2 or at_deadline:
                return True, "cheapest_loaded_verified"
            return False, "waiting_hint_stability"
        if at_deadline:
            return True, "cheapest_tab_mismatch"
        return False, "waiting_dom_sync"

    if dom_stable_count >= 2:
        return True, "cheapest_dom_stabilized"
    if at_deadline:
        return True, "cheapest_timeout"
    return False, "waiting_dom_stability"


def consolidate_slot_offers(
    best_dom_offers: list[FlightOffer],
    cheapest_dom_offers: list[FlightOffer],
    rpc_offers: list[FlightOffer],
    ds1_offers: list[FlightOffer],
    ds1_best: Optional[FlightOffer] = None,
    expected_origins: Optional[set[str]] = None,
    expected_destinations: Optional[set[str]] = None,
) -> Optional[dict[str, Any]]:
    """
    Consolidate Best and Cheapest offers across DOM, GetShoppingResults RPC, and initial ds:1.
    Always selects `min(dom_cheapest, rpc_cheapest, ds1_cheapest)` and records which source won
    (`cheapest_dom`, `cheapest_rpc`, `cheapest_ds1_min`).
    """
    best_offer: Optional[FlightOffer] = None
    if best_dom_offers:
        best_offer = best_dom_offers[0]
    elif ds1_best:
        best_offer = ds1_best

    tagged_candidates: list[tuple[FlightOffer, str]] = []
    for offer in cheapest_dom_offers:
        tagged_candidates.append((offer, "cheapest_dom"))
    for offer in rpc_offers:
        tagged_candidates.append((offer, "cheapest_rpc"))
    for offer in ds1_offers:
        tagged_candidates.append((offer, "cheapest_ds1_min"))
    for offer in best_dom_offers:
        tagged_candidates.append((offer, "cheapest_dom"))

    if not tagged_candidates:
        return None

    # Sort by price ASC; prefer DOM/RPC over ds1_min on exact tie
    source_rank = {"cheapest_dom": 0, "cheapest_rpc": 1, "cheapest_ds1_min": 2}
    tagged_candidates.sort(key=lambda pair: (pair[0].price, source_rank.get(pair[1], 3)))
    cheapest_offer, cheapest_source = tagged_candidates[0]

    if best_offer is None:
        best_offer = cheapest_offer

    if cheapest_source == "cheapest_dom" or best_dom_offers:
        source_tag = "patchright_dom" if cheapest_source == "cheapest_dom" else (
            "patchright_rpc" if cheapest_source == "cheapest_rpc" else "patchright_ds1"
        )
    elif cheapest_source == "cheapest_rpc":
        source_tag = "patchright_rpc"
    else:
        source_tag = "patchright_ds1"

    exp_o = expected_origins or set()
    exp_d = expected_destinations or set()
    fallback_orig = next(iter(exp_o)) if len(exp_o) == 1 else None
    fallback_dest = next(iter(exp_d)) if len(exp_d) == 1 else None

    c_orig = cheapest_offer.origin_airport or fallback_orig
    c_dest = cheapest_offer.destination_airport or fallback_dest
    b_orig = (best_offer.origin_airport if best_offer else None) or c_orig
    b_dest = (best_offer.destination_airport if best_offer else None) or c_dest

    return {
        "cheapest_offer": cheapest_offer,
        "best_offer": best_offer,
        "cheapest_source": cheapest_source,
        "extraction_source": source_tag,
        "c_orig": c_orig,
        "c_dest": c_dest,
        "b_orig": b_orig,
        "b_dest": b_dest,
        "top_3_dicts": [pair[0].to_dict() for pair in tagged_candidates[:3]],
    }


def _extract_expected_corridor_iatas(url: str) -> tuple[set[str], set[str]]:
    """Decode expected origin and destination IATA sets from a Google Flights URL's `?tfs=` parameter."""
    try:
        qs_tfs = parse_qs(urlparse(url).query).get("tfs", [""])[0]
        if not qs_tfs:
            return set(), set()
        dec = decode_tfs_parameter(qs_tfs)
        origins = {c.strip().upper() for c in (dec.get("origins") or []) if c and len(c.strip()) == 3}
        destinations = {c.strip().upper() for c in (dec.get("destinations") or []) if c and len(c.strip()) == 3}
        return origins, destinations
    except Exception:
        return set(), set()


def _filter_offers_for_corridor(
    offers: list[FlightOffer],
    expected_origins: set[str],
    expected_destinations: set[str],
) -> list[FlightOffer]:
    """Filter out offers whose detected origin or destination IATA falls outside the expected corridor."""
    if not offers:
        return []
    if not expected_origins and not expected_destinations:
        return list(offers)
    filtered: list[FlightOffer] = []
    for offer in offers:
        if expected_origins and offer.origin_airport and offer.origin_airport.upper() not in expected_origins:
            continue
        if expected_destinations and offer.destination_airport and offer.destination_airport.upper() not in expected_destinations:
            continue
        filtered.append(offer)
    return filtered


def _is_quota_or_auth_error(exc_or_msg: Any) -> bool:
    """Return True if an exception/message indicates proxy HTTP 402/407 or quota/auth exhaustion."""
    msg = str(exc_or_msg).upper()
    markers = (
        "402",
        "407",
        "ERR_PROXY_AUTH",
        "PROXY_AUTH",
        "PAYMENT REQUIRED",
        "QUOTA",
        "BANDWIDTH",
        "CREDENCIALES DE PROXY RECHAZADAS",
        "CUOTA DE PROXY AGOTADA",
    )
    return any(m in msg for m in markers)


def _is_proxy_or_tunnel_error(exc: Exception) -> bool:
    """Return True if an exception indicates a proxy/tunnel failure (e.g. FortiGate 403, 402/407, or unreachable proxy)."""
    msg = str(exc).upper()
    markers = (
        "ERR_PROXY_CONNECTION_FAILED",
        "ERR_TUNNEL_CONNECTION_FAILED",
        "ERR_PROXY_AUTH",
        "ERR_PROXY_TUNNEL_BLOCKED",
        "402",
        "403",
        "407",
        "FORTIGATE",
        "PROXY",
    )
    return any(m in msg for m in markers)


class PooledPatchrightSession:
    """
    Pooled `patchright` Chrome session bound to a single proxy (or direct connection).
    Reuses one browser process across all destinations/date_slots assigned to its shard.
    """

    def __init__(self, proxy_url: Optional[str] = None, headless: bool = HEADLESS_MODE) -> None:
        self.proxy_url = proxy_url
        self.headless = headless
        self._pw: Any = None
        self._browser: Any = None
        self._context: Any = None

    def _launch_browser_and_context(self) -> None:
        """Launch Chromium/Chrome and bind a fresh stealth context with route blocking."""
        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-background-networking",
            "--disable-sync",
            "--mute-audio",
            "--window-size=1920,1080",
        ]

        proxy_cfg = format_playwright_proxy(self.proxy_url)

        try:
            self._browser = self._pw.chromium.launch(
                channel="chrome",
                headless=self.headless,
                args=launch_args,
                proxy=proxy_cfg,
            )
            logger.info(
                "Launched Patchright Chrome (channel='chrome', headless=%s, proxy=%s)",
                self.headless,
                redact_proxy_for_log(self.proxy_url),
            )
        except Exception as exc:
            logger.warning(
                "Chrome channel launch (headless=%s) failed (%s); trying fallback headless=True.",
                self.headless,
                sanitize_log_text(exc),
            )
            try:
                self._browser = self._pw.chromium.launch(
                    channel="chrome",
                    headless=True,
                    args=launch_args,
                    proxy=proxy_cfg,
                )
            except Exception as exc2:
                logger.warning(
                    "Chrome channel unavailable (%s); falling back to bundled Patchright Chromium.",
                    sanitize_log_text(exc2),
                )
                try:
                    self._browser = self._pw.chromium.launch(
                        headless=True,
                        args=launch_args,
                        proxy=proxy_cfg,
                    )
                except Exception as exc3:
                    err_msg = str(exc3)
                    if "Executable doesn't exist" in err_msg or "patchright install" in err_msg:
                        logger.info(
                            "Patchright Chromium binary missing (%s). Running 'patchright install chromium' automatically...",
                            err_msg[:80],
                        )
                        import subprocess
                        import sys
                        subprocess.run(
                            [sys.executable, "-m", "patchright", "install", "chromium"],
                            check=True,
                            timeout=180,
                        )
                        self._browser = self._pw.chromium.launch(
                            headless=True,
                            args=launch_args,
                            proxy=proxy_cfg,
                        )
                    else:
                        raise

        self._context = self._browser.new_context(
            no_viewport=True,
            locale="es-MX",
            timezone_id="America/Mexico_City",
        )
        self._context.add_cookies(GOOGLE_CONSENT_COOKIES)
        self._context.route("**/*", self._handle_route)

    def _switch_to_direct_in_place(self) -> None:
        """Close a failing proxy browser and re-open one reusable direct browser on this session."""
        logger.warning(
            "Proxy %s failed or was blocked; switching pooled browser session in-place to direct connection.",
            redact_proxy_for_log(self.proxy_url),
        )
        for resource in (self._context, self._browser):
            if resource is not None:
                try:
                    resource.close()
                except Exception:
                    pass
        self._context = None
        self._browser = None
        self.proxy_url = None
        self._launch_browser_and_context()

    def start(self) -> "PooledPatchrightSession":
        """Start Patchright and create a stealth browser context with route blocking."""
        from patchright.sync_api import sync_playwright

        self._pw = sync_playwright().start()
        self._launch_browser_and_context()
        return self

    def _handle_route(self, route: Any) -> None:
        """
        Block heavy static resources (`image`, `media`, `font`) and telemetry/analytics
        domains to keep per-slot bandwidth under ~100 KB.
        """
        try:
            req = route.request
            if req.resource_type in BLOCKED_RESOURCE_TYPES:
                route.abort()
                return
            url_lower = req.url.lower()
            if any(domain in url_lower for domain in BLOCKED_TELEMETRY_DOMAINS):
                route.abort()
                return
            route.continue_()
        except Exception:
            try:
                route.continue_()
            except Exception:
                pass

    def reset_for_new_destination(self) -> None:
        """
        Clear cookies & storage between destinations while re-injecting the Google
        `SOCS` consent cookie so consent walls never render.
        """
        if not self._context:
            return
        try:
            self._context.clear_cookies()
            self._context.add_cookies(GOOGLE_CONSENT_COOKIES)
            logger.debug("Cleared browser cookies and re-injected Google SOCS consent cookie.")
        except Exception as exc:
            logger.warning("Failed to reset cookies between destinations: %s", exc)

    def _extract_dom_cards(
        self,
        page: Any,
        tab_source: str,
        max_cards: int = 5,
        expected_currency: Optional[str] = None,
    ) -> list[FlightOffer]:
        """
        Layer 2: Extract flight offers from visible Google Flights DOM cards in a single
        atomic `page.evaluate(...)` call (avoids multi-second Playwright locator stalls).
        """
        offers: list[FlightOffer] = []
        limit = max(1, max_cards * 2)
        try:
            raw_cards = page.evaluate(
                """(maxItems) => {
                    const cards = Array.from(document.querySelectorAll('li.pIav2d')).slice(0, maxItems);
                    return cards.map(card => {
                        let aria = card.getAttribute('aria-label') || '';
                        if (!aria) {
                            const innerLabeled = card.querySelector('[aria-label]');
                            if (innerLabeled) aria = innerLabeled.getAttribute('aria-label') || '';
                        }
                        const innerText = card.innerText || '';
                        const priceEl = card.querySelector('.YMlIz, span[data-gs]');
                        const priceText = priceEl ? (priceEl.innerText || '') : '';
                        return { aria, innerText, priceText };
                    });
                }""",
                limit,
            )
        except Exception:
            raw_cards = []

        if not isinstance(raw_cards, list):
            return offers

        for item in raw_cards:
            try:
                aria_label = str(item.get("aria") or "")
                inner_text = str(item.get("innerText") or "")
                price_text = str(item.get("priceText") or "")
                price_override = parse_price_number(price_text) if price_text else None

                offer = parse_flight_card_text(
                    aria_label=aria_label,
                    inner_text=inner_text,
                    price_override=price_override,
                    tab_source=tab_source,
                    expected_currency=expected_currency,
                )
                if offer and offer.price > 0:
                    if not any(
                        abs(o.price - offer.price) < 1.0
                        and o.airline == offer.airline
                        and o.dep_arr == offer.dep_arr
                        for o in offers
                    ):
                        offers.append(offer)
                        if len(offers) >= max_cards:
                            break
            except Exception:
                continue

        return offers

    def _dismiss_consent_if_present(self, page: Any) -> None:
        """
        Safety net in case a regional Google consent prompt still appears despite SOCS cookie.
        """
        try:
            if "consent.google.com" in page.url:
                btn = page.get_by_role(
                    "button",
                    name=re.compile(r"Aceptar todo|Accept all|Reject all|Rechazar todo", re.I),
                ).first
                if btn.count() > 0:
                    btn.click(timeout=3000)
                    page.wait_for_load_state("domcontentloaded", timeout=10000)
        except Exception:
            pass

    def _scrape_slot_once(self, url: str) -> Optional[SlotScrapeResult]:
        """Run a single browser page navigation and dual-tab extraction on `self._context`."""
        expected_origins, expected_destinations = _extract_expected_corridor_iatas(url)
        expected_curr = (
            parse_qs(urlparse(url).query).get("curr", [DEFAULT_CURRENCY])[0] or DEFAULT_CURRENCY
        ).strip().upper()
        page = self._context.new_page()
        page.set_default_timeout(15000)
        rpc_cheapest_offers: list[FlightOffer] = []
        rpc_payload_counter = {"count": 0}

        def _on_response(response: Any) -> None:
            try:
                resp_url = response.url
                if ("FlightsFrontendService/GetShoppingResults" in resp_url or "batchexecute" in resp_url) and response.status == 200:
                    body = response.text()
                    if body and len(body) > 200:
                        rpc_payload_counter["count"] += 1
                    parsed_rpc = parse_shopping_rpc_response(body, expected_currency=expected_curr)
                    if parsed_rpc:
                        rpc_cheapest_offers.extend(parsed_rpc)
            except Exception:
                pass

        page.on("response", _on_response)

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT_MS)
            self._dismiss_consent_if_present(page)

            if self.proxy_url is not None:
                cur_url = (page.url or "").lower()
                if cur_url.startswith("chrome-error://") or cur_url == "about:blank":
                    raise RuntimeError(f"ERR_PROXY_TUNNEL_BLOCKED: landed on {page.url}")

            # Wait up to 5s for real itinerary cards (`li.pIav2d`)
            try:
                page.wait_for_selector("li.pIav2d", timeout=5000)
            except Exception:
                if self.proxy_url is not None:
                    body_snippet = ""
                    try:
                        body_snippet = page.content()[:2500].lower()
                    except Exception:
                        pass
                    if "fortigate" in body_snippet or "proxy.http" in body_snippet:
                        raise RuntimeError("ERR_PROXY_TUNNEL_BLOCKED: FortiGate blocked Proxy.HTTP")

                # If Google Flights rendered the search/explore landing page with Buscar/Search unclicked, click it
                try:
                    search_btn = page.get_by_role(
                        "button",
                        name=re.compile(r"^Buscar$|^Search$", re.I),
                    ).first
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
                        logger.debug("Google Flights showed transient reload prompt after timeout. Clicking reload button...")
                        try:
                            reload_btn.click(timeout=3000)
                            page.wait_for_selector("li.pIav2d", timeout=10000)
                        except Exception:
                            page.wait_for_timeout(1500)
                    else:
                        page.wait_for_timeout(1500)

            # ------------------------------------------------------------------
            # LAYER 1A: Parse inline SSR `AF_initDataCallback({key: 'ds:1'})`
            # ------------------------------------------------------------------
            html_content = page.content()
            ds1_extraction = parse_ds1_from_html(html_content, expected_currency=expected_curr)

            # ------------------------------------------------------------------
            # LAYER 2A-PRE: Let the initial GetShoppingResults stream finish
            # before clicking "Los más bajos", so low multi-carrier fares in
            # initial full RPC / "Otros vuelos" DOM are never aborted mid-flight.
            # ------------------------------------------------------------------
            pre_tab_hint_price = self._wait_for_initial_results_settle(page, max_wait_s=16.0)

            # Extract top "Mejores opciones" cards + full pre-click DOM offers
            best_dom_offers = _filter_offers_for_corridor(
                self._extract_dom_cards(
                    page, tab_source="best", max_cards=5, expected_currency=expected_curr
                ),
                expected_origins,
                expected_destinations,
            )
            pre_dom_offers = _filter_offers_for_corridor(
                self._extract_dom_cards(
                    page, tab_source="cheapest", max_cards=45, expected_currency=expected_curr
                ),
                expected_origins,
                expected_destinations,
            )

            # ------------------------------------------------------------------
            # LAYER 2B: Deterministic Switch to Cheapest ("Los más bajos") Tab
            # ------------------------------------------------------------------
            cheapest_tab_clicked, cheapest_tab_status, post_tab_hint_price = self._switch_to_cheapest_tab_deterministic(
                page=page,
                rpc_payload_counter=rpc_payload_counter,
                best_dom_offers=best_dom_offers,
                expected_currency=expected_curr,
            )
            hint_candidates = [h for h in (pre_tab_hint_price, post_tab_hint_price) if h is not None and h > 0]
            tab_hint_price = min(hint_candidates) if hint_candidates else None

            cheapest_dom_offers = _filter_offers_for_corridor(
                self._extract_dom_cards(
                    page, tab_source="cheapest", max_cards=20, expected_currency=expected_curr
                ),
                expected_origins,
                expected_destinations,
            )
            combined_cheapest_dom = [*cheapest_dom_offers, *pre_dom_offers]
            filtered_rpc_offers = _filter_offers_for_corridor(
                rpc_cheapest_offers,
                expected_origins,
                expected_destinations,
            )
            ds1_offers: list[FlightOffer] = []
            ds1_best: Optional[FlightOffer] = None
            if ds1_extraction:
                ds1_raw = [ds1_extraction.cheapest_offer, *ds1_extraction.top_offers]
                ds1_offers = _filter_offers_for_corridor(ds1_raw, expected_origins, expected_destinations)
                if ds1_extraction.best_offer:
                    best_filtered = _filter_offers_for_corridor(
                        [ds1_extraction.best_offer], expected_origins, expected_destinations
                    )
                    ds1_best = best_filtered[0] if best_filtered else None

            # ------------------------------------------------------------------
            # MERGE & SELECT BEST + MIN(DOM, RPC, DS:1) CHEAPEST OFFERS
            # ------------------------------------------------------------------
            consolidated = consolidate_slot_offers(
                best_dom_offers=best_dom_offers,
                cheapest_dom_offers=combined_cheapest_dom,
                rpc_offers=filtered_rpc_offers,
                ds1_offers=ds1_offers,
                ds1_best=ds1_best,
                expected_origins=expected_origins,
                expected_destinations=expected_destinations,
            )
            if not consolidated:
                return None

            cheapest_offer = consolidated["cheapest_offer"]
            best_offer = consolidated["best_offer"]

            return SlotScrapeResult(
                cheapest_tab_price=cheapest_offer.price,
                cheapest_airline=cheapest_offer.airline,
                cheapest_stops=cheapest_offer.stops,
                cheapest_duration=cheapest_offer.duration,
                cheapest_dep_arr=cheapest_offer.dep_arr,
                is_self_transfer=cheapest_offer.is_self_transfer,
                origin_airport=consolidated["c_orig"],
                destination_airport=consolidated["c_dest"],
                best_tab_price=best_offer.price if best_offer else cheapest_offer.price,
                best_airline=best_offer.airline if best_offer else cheapest_offer.airline,
                best_stops=best_offer.stops if best_offer else cheapest_offer.stops,
                best_duration=best_offer.duration if best_offer else cheapest_offer.duration,
                best_origin_airport=consolidated["b_orig"],
                best_destination_airport=consolidated["b_dest"],
                extraction_source=consolidated["extraction_source"],
                cheapest_tab_status=cheapest_tab_status,
                cheapest_source=consolidated["cheapest_source"],
                tab_hint_price=tab_hint_price,
                raw_top_offers=consolidated["top_3_dicts"],
            )
        finally:
            try:
                page.close()
            except Exception:
                pass

    def _probe_cheapest_dom_state(self, page: Any) -> dict[str, Any]:
        """
        Atomically read tab selection, the exact `.bU1Yvc` spinner state (by DOM
        presence inside `.bU1Yvc` without `offsetParent` false negatives plus any active
        results-panel loading bar), the tab header's announced price (`span[role="text"]`),
        no-cheaper notice, and visible card prices in a single `page.evaluate(...)` call.
        """
        try:
            state = page.evaluate(
                """() => {
                    const tabs = Array.from(document.querySelectorAll('[role="tab"]'));
                    const tabRe = /Los más bajos|Más bajos|Cheapest|Más baratos|Baratos/i;
                    const cTab = tabs.find(t => tabRe.test(t.innerText || ''));
                    const tabFound = !!cTab;
                    const ariaSelected = cTab ? (cTab.getAttribute('aria-selected') === 'true') : false;

                    const bSpan = cTab ? cTab.querySelector('span.bU1Yvc, .bU1Yvc') : null;
                    const tabTextLower = cTab ? (cTab.innerText || '').toLowerCase() : '';
                    let spinnerActive = tabTextLower.includes('obteniendo resultados') || tabTextLower.includes('getting results');

                    if (cTab && !spinnerActive) {
                        const spinScope = bSpan || cTab;
                        const spinEls = spinScope.querySelectorAll('div.DU29of, [jscontroller="DFTXbf"], [jsname="LbNpof"], [role="progressbar"], [aria-busy="true"]');
                        for (const el of spinEls) {
                            const cs = window.getComputedStyle(el);
                            // Treat DOM presence inside .bU1Yvc as active unless explicitly hidden via CSS
                            if (cs.display !== 'none' && cs.visibility !== 'hidden') {
                                spinnerActive = true;
                                break;
                            }
                        }
                    }

                    if (!spinnerActive) {
                        const extBars = document.querySelectorAll('[role="progressbar"], [aria-busy="true"]');
                        for (const bar of extBars) {
                            if (bSpan && bSpan.contains(bar)) continue;
                            const label = (bar.getAttribute('aria-label') || '').toLowerCase();
                            if (!label.includes('obteniendo') && !label.includes('cargando') && !label.includes('getting') && !label.includes('loading')) {
                                continue;
                            }
                            const cs = window.getComputedStyle(bar);
                            const r = bar.getBoundingClientRect();
                            if (cs.display !== 'none' && cs.visibility !== 'hidden' && parseFloat(cs.opacity || '1') > 0 && r.width > 0 && r.height > 0) {
                                spinnerActive = true;
                                break;
                            }
                        }
                    }

                    const priceSpan = bSpan ? bSpan.querySelector('span[role="text"][aria-label], span[role="text"]') : null;
                    const tabHintAria = priceSpan ? (priceSpan.getAttribute('aria-label') || '') : '';
                    const tabHintText = priceSpan ? (priceSpan.innerText || '') : (bSpan ? (bSpan.innerText || '') : '');

                    const bodyLower = (document.body ? (document.body.innerText || '') : '').toLowerCase();
                    const noCheaper = (
                        bodyLower.includes('no hay opciones más baratas') ||
                        bodyLower.includes('no hay vuelos más baratos') ||
                        bodyLower.includes('no cheaper options') ||
                        bodyLower.includes('no cheaper flights')
                    );

                    const cards = Array.from(document.querySelectorAll('li.pIav2d')).slice(0, 8);
                    const cardSamples = cards.map(c => {
                        const aria = c.getAttribute('aria-label') || (c.querySelector('[aria-label]')?.getAttribute('aria-label') || '');
                        const priceNodes = Array.from(c.querySelectorAll('.YMlIz, span[data-gs]')).map(n => (n.innerText || '').trim()).filter(Boolean);
                        const lines = (c.innerText || '').split('\\n').map(s => s.trim()).filter(Boolean).slice(0, 4).join('|');
                        return { aria, priceNodes, lines };
                    });

                    return {
                        tabFound,
                        ariaSelected,
                        spinnerActive,
                        tabHintAria,
                        tabHintText,
                        noCheaper,
                        cardSamples,
                    };
                }"""
            )
            if isinstance(state, dict):
                return state
        except Exception:
            pass
        return {
            "tabFound": False,
            "ariaSelected": False,
            "spinnerActive": False,
            "tabHintAria": "",
            "tabHintText": "",
            "noCheaper": False,
            "cardSamples": [],
        }

    def _wait_for_initial_results_settle(
        self,
        page: Any,
        max_wait_s: float = 16.0,
    ) -> Optional[float]:
        """
        Allow the initial full `GetShoppingResults` stream (and the pre-click `.bU1Yvc`
        spinner) to finish before clicking 'Los más bajos', preventing premature click
        abortions of low multi-carrier fares.
        """
        t0 = time.perf_counter()
        deadline = t0 + max_wait_s
        last_pre_hint: Optional[float] = None
        stable_count = 0
        while True:
            st = self._probe_cheapest_dom_state(page)
            hp = parse_tab_hint_price(st.get("tabHintAria", ""), st.get("tabHintText", ""))
            if hp is not None:
                last_pre_hint = hp
            tab_found = bool(st.get("tabFound"))
            spinner_active = bool(st.get("spinnerActive"))
            elapsed = time.perf_counter() - t0
            if not tab_found and elapsed >= 2.0:
                break
            if tab_found and not spinner_active:
                stable_count += 1
                if stable_count >= 2:
                    break
            else:
                stable_count = 0
            if time.perf_counter() >= deadline:
                break
            page.wait_for_timeout(400)
        return last_pre_hint

    def _switch_to_cheapest_tab_deterministic(
        self,
        page: Any,
        rpc_payload_counter: dict[str, int],
        best_dom_offers: list[FlightOffer],
        expected_currency: Optional[str] = None,
        hard_timeout_s: float = 25.0,
    ) -> tuple[bool, str, Optional[float]]:
        """
        Deterministically switch from 'Mejores opciones' to 'Los más bajos' / 'Cheapest':
          1. Try `cheapest_tab.click(timeout=5000)` with immediate DOM `el.click()` fallback.
          2. Poll every 400 ms (hard deadline 25s) via single `page.evaluate(...)` reading:
             `ariaSelected`, `.bU1Yvc` spinner presence, `tab_hint_price`, and `dom_min_price`.
          3. Require `spinner_seen` (or >= 2.0 s settle) AND 2 consecutive stable readings
             where `dom_min_price <= tab_hint_price * 1.01` before early exit.
        """
        t0 = time.perf_counter()
        hard_deadline = t0 + hard_timeout_s
        tab_pattern = re.compile(r"Los más bajos|Más bajos|Cheapest|Más baratos|Baratos", re.I)

        def _locate_tab() -> Any:
            loc = page.locator('[role="tab"]').filter(has_text=tab_pattern).first
            if loc.count() == 0:
                loc = page.get_by_role("tab", name=tab_pattern).first
            return loc

        try:
            cheapest_tab = _locate_tab()
            if cheapest_tab.count() == 0:
                logger.info("Cheapest tab transition: status=cheapest_tab_missing")
                return False, "cheapest_tab_missing", None
        except Exception as find_exc:
            logger.warning(
                "Cheapest tab locate failed (%s); preserving Mejores/ds:1 offers.",
                sanitize_log_text(find_exc),
            )
            return False, "cheapest_click_timeout", None

        rpc_before = rpc_payload_counter.get("count", 0)

        # Primary click with immediate DOM click() fallback
        clicked_ok = False
        try:
            cheapest_tab.click(timeout=5000)
            clicked_ok = True
        except Exception as click_exc:
            logger.warning(
                "cheapest_tab.click(5000ms) failed (%s); trying DOM el.click() fallback.",
                sanitize_log_text(click_exc),
            )
            try:
                cheapest_tab.evaluate("el => el.click()")
                clicked_ok = True
            except Exception as dom_click_exc:
                logger.warning(
                    "Cheapest tab DOM click fallback failed (%s); preserving Mejores/ds:1 offers.",
                    sanitize_log_text(dom_click_exc),
                )
                return False, "cheapest_click_timeout", None

        if not clicked_ok:
            return False, "cheapest_click_timeout", None

        # Verify aria-selected="true" promptly; retry DOM click once if needed
        dom_reclick_attempted = False
        prev_card_sig: Optional[tuple[str, ...]] = None
        dom_stable_count = 0
        spinner_seen = False
        prev_hint_dom_pair: Optional[tuple[float, float]] = None
        dom_hint_stable_count = 0
        final_status = "cheapest_timeout"
        last_tab_hint: Optional[float] = None
        last_dom_min: Optional[float] = None

        while True:
            dom_state = self._probe_cheapest_dom_state(page)
            aria_selected = bool(dom_state.get("ariaSelected"))

            if not aria_selected and not dom_reclick_attempted and (time.perf_counter() - t0) >= 0.8:
                dom_reclick_attempted = True
                try:
                    _locate_tab().evaluate("el => el.click()")
                except Exception:
                    pass
                page.wait_for_timeout(300)
                dom_state = self._probe_cheapest_dom_state(page)
                aria_selected = bool(dom_state.get("ariaSelected"))

            spinner_active = bool(dom_state.get("spinnerActive"))
            if spinner_active:
                spinner_seen = True

            tab_hint_price = parse_tab_hint_price(
                dom_state.get("tabHintAria", ""),
                dom_state.get("tabHintText", ""),
            )
            if tab_hint_price is not None:
                last_tab_hint = tab_hint_price

            card_samples = dom_state.get("cardSamples") or []
            parsed_prices: list[float] = []
            sig_items: list[str] = []
            for cs in card_samples:
                p_val: Optional[float] = None
                for p_txt in cs.get("priceNodes") or []:
                    cand = parse_price_number(p_txt)
                    if cand is not None:
                        p_val = cand
                        break
                if p_val is None and cs.get("aria"):
                    p_val = parse_price_number(cs["aria"])
                if p_val is not None and p_val > 0:
                    parsed_prices.append(p_val)
                sig_items.append(f"{p_val or ''}|{cs.get('lines', '')}")

            dom_min_price = min(parsed_prices) if parsed_prices else None
            last_dom_min = dom_min_price
            curr_sig = tuple(sig_items)

            if not spinner_active and aria_selected and curr_sig and parsed_prices:
                if curr_sig == prev_card_sig:
                    dom_stable_count += 1
                else:
                    dom_stable_count = 1
                prev_card_sig = curr_sig

                if tab_hint_price is not None and tab_hint_price > 0 and dom_min_price is not None and dom_min_price <= tab_hint_price * 1.01:
                    pair = (tab_hint_price, dom_min_price)
                    if pair == prev_hint_dom_pair:
                        dom_hint_stable_count += 1
                    else:
                        dom_hint_stable_count = 1
                    prev_hint_dom_pair = pair
                else:
                    dom_hint_stable_count = 0
                    prev_hint_dom_pair = None
            else:
                dom_stable_count = 0
                prev_card_sig = None
                dom_hint_stable_count = 0
                prev_hint_dom_pair = None

            elapsed_now = time.perf_counter() - t0
            at_deadline = time.perf_counter() >= hard_deadline
            should_exit, status_reason = evaluate_cheapest_wait_state(
                aria_selected=aria_selected,
                spinner_active=spinner_active,
                tab_hint_price=tab_hint_price,
                dom_min_price=dom_min_price,
                no_cheaper_notice=bool(dom_state.get("noCheaper")),
                dom_stable_count=dom_stable_count,
                at_deadline=at_deadline,
                spinner_seen=spinner_seen,
                dom_hint_stable_count=dom_hint_stable_count,
                elapsed_s=elapsed_now,
            )

            if should_exit:
                final_status = status_reason
                break

            page.wait_for_timeout(400)

        elapsed = time.perf_counter() - t0
        rpc_delta = rpc_payload_counter.get("count", 0) - rpc_before
        if final_status == "cheapest_tab_mismatch":
            logger.warning(
                "cheapest_tab_mismatch (tab_hint=$%s vs dom_min=$%s, %.2fs, rpc_payloads=%d)",
                f"{last_tab_hint:,.0f}" if last_tab_hint is not None else "N/A",
                f"{last_dom_min:,.0f}" if last_dom_min is not None else "N/A",
                elapsed,
                rpc_delta,
            )
        else:
            logger.info(
                "Cheapest tab transition: status=%s (%.2fs, tab_hint=%s, dom_min=%s, rpc_payloads=%d)",
                final_status,
                elapsed,
                f"${last_tab_hint:,.0f}" if last_tab_hint is not None else "N/A",
                f"${last_dom_min:,.0f}" if last_dom_min is not None else "N/A",
                rpc_delta,
            )
        return True, final_status, last_tab_hint

    def scrape_slot_in_browser(self, url: str) -> Optional[SlotScrapeResult]:
        """
        Execute Layer 1 (`AF_initDataCallback` `ds:1` + `GetShoppingResults` RPC listener)
        and Layer 2 (Best Tab DOM + Cheapest Tab click & DOM extraction) on `url`.
        If the bound proxy fails or is blocked, switches this pooled session in-place to
        a reusable direct browser so subsequent slots do not relaunch Chrome per slot.
        """
        if not self._context:
            self.start()

        try:
            return self._scrape_slot_once(url)
        except Exception as exc:
            if self.proxy_url is not None and _is_proxy_or_tunnel_error(exc):
                self._switch_to_direct_in_place()
                res = self._scrape_slot_once(url)
                if res:
                    res.extraction_source = "patchright_direct_fallback"
                return res
            raise

    def scrape_slot_direct_fallback(self, url: str) -> Optional[SlotScrapeResult]:
        """
        Direct connection fallback executed on the existing Playwright instance
        when the primary proxy context fails, times out, or returns 0 results.
        """
        if self._pw is None:
            return None
        direct_browser = None
        direct_context = None
        try:
            launch_args = [
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-background-networking",
                "--disable-sync",
                "--mute-audio",
                "--window-size=1920,1080",
            ]
            try:
                direct_browser = self._pw.chromium.launch(
                    channel="chrome",
                    headless=True,
                    args=launch_args,
                )
            except Exception:
                direct_browser = self._pw.chromium.launch(
                    headless=True,
                    args=launch_args,
                )
            direct_context = direct_browser.new_context(
                no_viewport=True,
                locale="es-MX",
                timezone_id="America/Mexico_City",
            )
            direct_context.add_cookies(GOOGLE_CONSENT_COOKIES)
            direct_context.route("**/*", self._handle_route)

            tmp_session = PooledPatchrightSession(proxy_url=None, headless=True)
            tmp_session._pw = self._pw
            tmp_session._browser = direct_browser
            tmp_session._context = direct_context
            res = tmp_session.scrape_slot_in_browser(url)
            if res:
                res.extraction_source = "patchright_direct_fallback"
            return res
        except Exception as exc:
            logger.warning("Direct browser fallback encountered error: %s", sanitize_log_text(exc))
            return None
        finally:
            if direct_context is not None:
                try:
                    direct_context.close()
                except Exception:
                    pass
            if direct_browser is not None:
                try:
                    direct_browser.close()
                except Exception:
                    pass

    def close(self) -> None:
        """Gracefully close browser context, browser instance, and Playwright driver."""
        for resource in (self._context, self._browser):
            if resource is not None:
                try:
                    resource.close()
                except Exception:
                    pass
        if self._pw is not None:
            try:
                self._pw.stop()
            except Exception:
                pass
        self._context = None
        self._browser = None
        self._pw = None

    def __enter__(self) -> "PooledPatchrightSession":
        return self.start()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


# ==============================================================================
# LAYER 3: FAST HTTP SSR FALLBACK (`curl_cffi` TLS Impersonation + `fast-flights`)
# ==============================================================================


def _build_ssr_slot_result(
    html: str,
    url: str,
    expected_curr: str,
    extraction_source: str,
) -> Optional[SlotScrapeResult]:
    """Parse an SSR HTML payload with currency and corridor validation."""
    extraction = parse_ds1_from_html(html, expected_currency=expected_curr)
    if not extraction:
        return None

    expected_origins, expected_destinations = _extract_expected_corridor_iatas(url)
    filtered_top = _filter_offers_for_corridor(
        [extraction.cheapest_offer, *extraction.top_offers],
        expected_origins,
        expected_destinations,
    )
    if not filtered_top:
        return None
    filtered_top.sort(key=lambda o: o.price)
    cheapest = filtered_top[0]
    best_filtered = (
        _filter_offers_for_corridor([extraction.best_offer], expected_origins, expected_destinations)
        if extraction.best_offer
        else []
    )
    best = best_filtered[0] if best_filtered else cheapest

    return SlotScrapeResult(
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
        extraction_source=extraction_source,
        raw_top_offers=[o.to_dict() for o in filtered_top[:3]],
    )


def scrape_via_curl_cffi_ssr(
    url: str,
    proxy_url: Optional[str] = None,
    timeout: int = 20,
) -> Optional[SlotScrapeResult]:
    """
    Layer 3A: Execute a Chrome TLS-impersonated HTTP GET request via `curl_cffi`
    with Google `SOCS` consent cookie and extract the SSR `ds:1` payload directly.
    """
    if curl_requests is None:
        logger.warning("curl_cffi is not available; skipping Layer 3A SSR fallback.")
        return None

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

    try:
        resp = curl_requests.get(
            url,
            headers=headers,
            cookies=cookies,
            proxy=proxy_url,
            impersonate="chrome",
            timeout=timeout,
            allow_redirects=True,
        )
        if resp.status_code != 200 or not resp.text:
            return None

        return _build_ssr_slot_result(
            resp.text, url=url, expected_curr=expected_curr, extraction_source="curl_cffi_ssr"
        )
    except Exception as exc:
        logger.warning("Layer 3A (curl_cffi SSR) failed for %s: %s", url[:80], sanitize_log_text(exc))
        return None


def scrape_via_fast_flights_fallback(
    url: str,
    proxy_url: Optional[str] = None,
) -> Optional[SlotScrapeResult]:
    """
    Layer 3B: Secondary HTTP fallback using `primp` (full N:M URL + locale cookies) and
    `fast-flights` with explicit `currency` and `language` plus ds:1 currency token verification.
    """
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        tfs_b64 = qs.get("tfs", [""])[0]
        curr = (qs.get("curr", [DEFAULT_CURRENCY])[0] or DEFAULT_CURRENCY).strip().upper()
        hl = (qs.get("hl", [DEFAULT_HL])[0] or DEFAULT_HL).strip().lower()
        if not tfs_b64:
            return None

        # 1. Preferred Tier 3B: fetch the exact N:M URL via `primp` (chrome_145 TLS) with SOCS cookie
        try:
            from primp import Client

            client = Client(
                impersonate="chrome_145",
                impersonate_os="macos",
                referer=True,
                proxy=proxy_url,
                cookie_store=True,
            )
            client.set_cookies(
                "https://www.google.com",
                [f"SOCS={GOOGLE_SOCS_COOKIE_VALUE}", "CONSENT=YES+cb.20240514-07-p0.es+FX+999"],
            )
            primp_resp = client.get(url)
            if getattr(primp_resp, "status_code", 0) == 200 and getattr(primp_resp, "text", ""):
                primp_ssr = _build_ssr_slot_result(
                    primp_resp.text,
                    url=url,
                    expected_curr=curr,
                    extraction_source=f"fast_flights_{curr.lower()}",
                )
                if primp_ssr and primp_ssr.cheapest_tab_price > 0:
                    return primp_ssr
        except Exception as primp_exc:
            logger.debug("Tier 3B primp full-URL SSR fetch skipped: %s", sanitize_log_text(primp_exc))

        # 2. Secondary Tier 3B: `fast-flights` query WITH explicit currency & language + token verification
        from fast_flights import FlightQuery, Passengers, create_filter, fetch_flights_html, parse as ff_parse

        decoded = decode_tfs_parameter(tfs_b64)
        origins_list = decoded.get("origins") or []
        destinations_list = decoded.get("destinations") or []
        origin = origins_list[0] if origins_list else (decoded.get("origin") or "").split(",")[0].strip()
        destination = (
            destinations_list[0]
            if destinations_list
            else (decoded.get("destination") or "").split(",")[0].strip()
        )
        outbound_date = decoded.get("outbound_date")
        return_date = decoded.get("return_date")
        trip_type = decoded.get("trip_type", "round-trip")
        max_stops = decoded.get("max_stops")

        if not (origin and destination and outbound_date):
            return None

        flights_list = [
            FlightQuery(
                date=outbound_date,
                from_airport=origin,
                to_airport=destination,
                max_stops=max_stops,
            )
        ]
        if trip_type == "round-trip" and return_date:
            flights_list.append(
                FlightQuery(
                    date=return_date,
                    from_airport=destination,
                    to_airport=origin,
                    max_stops=max_stops,
                )
            )

        q = create_filter(
            flights=flights_list,
            trip="round-trip" if (trip_type == "round-trip" and return_date) else "one-way",
            seat="economy",
            passengers=Passengers(adults=1),
            currency=curr,
            language=hl,
        )
        html = fetch_flights_html(q, proxy=proxy_url)
        if not html:
            return None

        # Verify currency embedded in ds:1 booking token before accepting any price
        detected_curr = extract_ds1_currency_from_html(html)
        if detected_curr and detected_curr != curr:
            logger.warning(
                "Layer 3B (fast-flights) currency mismatch rejected: got %s, expected %s",
                detected_curr,
                curr,
            )
            return None

        ds1_res = _build_ssr_slot_result(
            html, url=url, expected_curr=curr, extraction_source=f"fast_flights_{curr.lower()}"
        )
        if ds1_res and ds1_res.cheapest_tab_price > 0:
            return ds1_res

        if detected_curr != curr:
            return None

        res = ff_parse(html)
        if not res:
            return None

        parsed_offers: list[FlightOffer] = []
        for idx, fl in enumerate(res):
            price_val = getattr(fl, "price", None)
            if price_val is None:
                continue
            try:
                price_float = float(price_val)
            except (ValueError, TypeError):
                continue
            if price_float <= 0:
                continue

            airlines_list = getattr(fl, "airlines", []) or []
            airline_name = ", ".join(airlines_list) if airlines_list else "Aerolínea"

            sub_flights = getattr(fl, "flights", []) or []
            stop_count = max(0, len(sub_flights) - 1)
            stops_str = "Directo" if stop_count == 0 else f"{stop_count} escala(s)"

            dur_mins = sum(getattr(sf, "duration", 0) for sf in sub_flights)
            duration_str = f"{dur_mins // 60} h {dur_mins % 60} min" if dur_mins > 0 else "N/A"

            dep_arr = ""
            if sub_flights:
                first_f = sub_flights[0]
                last_f = sub_flights[-1]
                dep_t = getattr(getattr(first_f, "departure", None), "time", None)
                arr_t = getattr(getattr(last_f, "arrival", None), "time", None)
                if dep_t and arr_t:
                    dep_arr = f"{dep_t[0]:02d}:{dep_t[1]:02d} - {arr_t[0]:02d}:{arr_t[1]:02d}"

            parsed_offers.append(
                FlightOffer(
                    price=round(price_float, 2),
                    airline=airline_name[:60],
                    stops=stops_str,
                    duration=duration_str,
                    dep_arr=dep_arr[:50],
                    origin_airport=origin,
                    destination_airport=destination,
                    is_self_transfer=detect_self_transfer(f"{airline_name} {stops_str}"),
                    tab_source="best" if idx == 0 else "cheapest",
                )
            )

        if not parsed_offers:
            return None

        best_offer = next((o for o in parsed_offers if o.tab_source == "best"), parsed_offers[0])
        cheapest_offer = min(parsed_offers, key=lambda o: o.price)

        return SlotScrapeResult(
            cheapest_tab_price=cheapest_offer.price,
            cheapest_airline=cheapest_offer.airline,
            cheapest_stops=cheapest_offer.stops,
            cheapest_duration=cheapest_offer.duration,
            cheapest_dep_arr=cheapest_offer.dep_arr,
            is_self_transfer=cheapest_offer.is_self_transfer,
            origin_airport=origin,
            destination_airport=destination,
            best_tab_price=best_offer.price,
            best_airline=best_offer.airline,
            best_stops=best_offer.stops,
            best_duration=best_offer.duration,
            best_origin_airport=origin,
            best_destination_airport=destination,
            extraction_source=f"fast_flights_{curr.lower()}",
            raw_top_offers=[o.to_dict() for o in sorted(parsed_offers, key=lambda x: x.price)[:3]],
        )
    except Exception as exc:
        logger.debug("Layer 3B (fast-flights) fallback failed: %s", sanitize_log_text(exc))
        return None


def scrape_date_slot(
    url: str,
    pooled_session: Optional[PooledPatchrightSession] = None,
    proxy_url: Optional[str] = None,
    date_range_label: str = "",
) -> Optional[SlotScrapeResult]:
    """
    High-level orchestrator for scraping a single `date_slot` across all 3 layers:
      1. Pooled Patchright Browser (Layer 1 `ds:1` + RPC & Layer 2 Best/Cheapest DOM Tabs)
      2. If proxy fails or browser returns no offers, retry direct or fall back to
         Layer 3 (`curl_cffi` TLS-impersonated SSR `ds:1` + `fast-flights`).
    """
    start_ts = time.perf_counter()
    slot_tag = f"[{date_range_label}] " if date_range_label else ""

    def _fmt_hint(val: Optional[float]) -> str:
        return f"${val:,.0f}" if val is not None and val > 0 else "N/A"

    # Tier 1 & 2: Pooled Patchright Browser Execution
    session_to_use = pooled_session
    own_session = False
    active_proxy: Optional[str] = session_to_use.proxy_url if session_to_use is not None else proxy_url
    proxy_failed = False

    if session_to_use is None:
        try:
            session_to_use = PooledPatchrightSession(proxy_url=active_proxy, headless=True).start()
            own_session = True
            active_proxy = session_to_use.proxy_url
        except Exception as init_exc:
            logger.debug(
                "%sCould not start standalone browser session on %s: %s",
                slot_tag,
                redact_proxy_for_log(active_proxy),
                sanitize_log_text(init_exc),
            )
            if active_proxy is not None and _is_proxy_or_tunnel_error(init_exc):
                proxy_failed = True
                active_proxy = None
            session_to_use = None

    if session_to_use is not None:
        try:
            try:
                browser_result = session_to_use.scrape_slot_in_browser(url)
                active_proxy = session_to_use.proxy_url
                if browser_result and browser_result.cheapest_tab_price > 0:
                    elapsed = time.perf_counter() - start_ts
                    logger.info(
                        "%sScraped slot via %s (%s | tab_status=%s) in %.2fs -> Cheapest: $%s (%s) | Best: $%s | tab_hint=%s",
                        slot_tag,
                        browser_result.extraction_source,
                        browser_result.cheapest_source,
                        browser_result.cheapest_tab_status or "N/A",
                        elapsed,
                        f"{browser_result.cheapest_tab_price:,.0f}",
                        browser_result.cheapest_airline,
                        f"{(browser_result.best_tab_price or browser_result.cheapest_tab_price):,.0f}",
                        _fmt_hint(browser_result.tab_hint_price),
                    )
                    return browser_result
            except Exception as browser_exc:
                logger.warning(
                    "%sPatchright browser extraction on %s encountered error (%s); trying direct fallback.",
                    slot_tag,
                    redact_proxy_for_log(session_to_use.proxy_url),
                    sanitize_log_text(browser_exc),
                )
                if session_to_use.proxy_url is not None and _is_proxy_or_tunnel_error(browser_exc):
                    proxy_failed = True
                    active_proxy = None

            # Tier 2B: Switch pooled session in-place to direct if proxy yielded 0 results or failed
            if session_to_use.proxy_url is not None:
                try:
                    session_to_use._switch_to_direct_in_place()
                    active_proxy = None
                    proxy_failed = True
                    direct_res = session_to_use._scrape_slot_once(url)
                    if direct_res and direct_res.cheapest_tab_price > 0:
                        direct_res.extraction_source = "patchright_direct_fallback"
                        elapsed = time.perf_counter() - start_ts
                        logger.info(
                            "%sScraped slot via direct browser fallback (%s | tab_status=%s) in %.2fs -> Cheapest: $%s (%s) | Best: $%s | tab_hint=%s",
                            slot_tag,
                            direct_res.cheapest_source,
                            direct_res.cheapest_tab_status or "N/A",
                            elapsed,
                            f"{direct_res.cheapest_tab_price:,.0f}",
                            direct_res.cheapest_airline,
                            f"{(direct_res.best_tab_price or direct_res.cheapest_tab_price):,.0f}",
                            _fmt_hint(direct_res.tab_hint_price),
                        )
                        return direct_res
                except Exception as direct_exc:
                    logger.debug("%sDirect browser fallback error: %s", slot_tag, sanitize_log_text(direct_exc))
        finally:
            if own_session and session_to_use is not None:
                try:
                    session_to_use.close()
                except Exception:
                    pass

    # Tier 3A: curl_cffi TLS-impersonated SSR `ds:1` extraction (respects active channel; never retries a failed proxy)
    tier3_proxy = None if proxy_failed else active_proxy
    ssr_result = scrape_via_curl_cffi_ssr(url, proxy_url=tier3_proxy)
    if ssr_result is None and tier3_proxy is not None:
        ssr_result = scrape_via_curl_cffi_ssr(url, proxy_url=None)
    if ssr_result and ssr_result.cheapest_tab_price > 0:
        elapsed = time.perf_counter() - start_ts
        logger.info(
            "%sScraped slot via Layer 3 curl_cffi SSR in %.2fs -> Cheapest: $%s | Best: $%s | tab_hint=N/A",
            slot_tag,
            elapsed,
            f"{ssr_result.cheapest_tab_price:,.0f}",
            f"{(ssr_result.best_tab_price or ssr_result.cheapest_tab_price):,.0f}",
        )
        return ssr_result

    # Tier 3B: fast-flights / primp fallback (respects active channel; never retries a failed proxy)
    ff_result = scrape_via_fast_flights_fallback(url, proxy_url=tier3_proxy)
    if ff_result is None and tier3_proxy is not None:
        ff_result = scrape_via_fast_flights_fallback(url, proxy_url=None)
    if ff_result and ff_result.cheapest_tab_price > 0:
        elapsed = time.perf_counter() - start_ts
        logger.info(
            "%sScraped slot via Layer 3 fast-flights in %.2fs -> Cheapest: $%s | Best: $%s | tab_hint=N/A",
            slot_tag,
            elapsed,
            f"{ff_result.cheapest_tab_price:,.0f}",
            f"{(ff_result.best_tab_price or ff_result.cheapest_tab_price):,.0f}",
        )
        return ff_result

    logger.error("%sAll 3 extraction layers failed to extract prices for URL: %s", slot_tag, url[:100])
    return None
