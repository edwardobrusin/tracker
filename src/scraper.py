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
    GOOGLE_CONSENT_COOKIES,
    GOOGLE_SOCS_COOKIE_VALUE,
    HEADLESS_MODE,
    NAVIGATION_TIMEOUT_MS,
)
from src.ds1_parser import (
    FlightOffer,
    detect_self_transfer,
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


def _is_proxy_or_tunnel_error(exc: Exception) -> bool:
    """Return True if an exception indicates a proxy/tunnel failure (e.g. FortiGate 403 or unreachable proxy)."""
    msg = str(exc).upper()
    markers = (
        "ERR_PROXY_CONNECTION_FAILED",
        "ERR_TUNNEL_CONNECTION_FAILED",
        "ERR_PROXY_AUTH",
        "ERR_PROXY_TUNNEL_BLOCKED",
        "403",
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
                self.proxy_url or "direct",
            )
        except Exception as exc:
            logger.warning(
                "Chrome channel launch (headless=%s) failed (%s); trying fallback headless=True.",
                self.headless,
                exc,
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
                    exc2,
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
            self.proxy_url,
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

    def _extract_dom_cards(self, page: Any, tab_source: str, max_cards: int = 5) -> list[FlightOffer]:
        """
        Layer 2: Extract flight offers from visible Google Flights DOM cards using both
        `aria-label` semantic parsing and specific card price/airline/stops/duration selectors.
        """
        offers: list[FlightOffer] = []

        # Strictly query Google Flights itinerary list items (`li.pIav2d`) so landing-page
        # promo cards and passenger selector rows (`div[role="listitem"]`) are never parsed.
        card_locators = page.locator("li.pIav2d")
        count = card_locators.count()

        limit = min(count, max_cards * 2)
        for idx in range(limit):
            try:
                card = card_locators.nth(idx)
                aria_label = ""
                # Check aria-label on the card or its inner interactive link/div
                raw_aria = card.get_attribute("aria-label")
                if raw_aria:
                    aria_label = raw_aria
                else:
                    inner_labeled = card.locator("[aria-label]").first
                    if inner_labeled.count() > 0:
                        aria_label = inner_labeled.get_attribute("aria-label") or ""

                inner_text = card.inner_text(timeout=1200)

                # Explicit price element check inside card (.YMlIz, [data-gs])
                price_override: Optional[float] = None
                price_el = card.locator(".YMlIz, span[data-gs]").first
                if price_el.count() > 0:
                    price_text = price_el.inner_text(timeout=800)
                    price_override = parse_price_number(price_text)

                offer = parse_flight_card_text(
                    aria_label=aria_label,
                    inner_text=inner_text,
                    price_override=price_override,
                    tab_source=tab_source,
                )
                if offer and offer.price > 0:
                    # Deduplicate identical offers
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
        page = self._context.new_page()
        rpc_cheapest_offers: list[FlightOffer] = []

        def _on_response(response: Any) -> None:
            try:
                resp_url = response.url
                if "FlightsFrontendService/GetShoppingResults" in resp_url and response.status == 200:
                    body = response.text()
                    parsed_rpc = parse_shopping_rpc_response(body)
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
            ds1_extraction = parse_ds1_from_html(html_content)

            # ------------------------------------------------------------------
            # LAYER 2A: Extract initial "Best" (`Mejores`) Tab cards from DOM
            # ------------------------------------------------------------------
            best_dom_offers = _filter_offers_for_corridor(
                self._extract_dom_cards(page, tab_source="best", max_cards=3),
                expected_origins,
                expected_destinations,
            )

            # ------------------------------------------------------------------
            # LAYER 2B: Click "Cheapest" (`Más baratos` / `Cheapest`) Tab
            # ------------------------------------------------------------------
            cheapest_tab_clicked = False
            try:
                cheapest_tab = page.get_by_role(
                    "tab",
                    name=re.compile(r"Cheapest|Más baratos|Baratos", re.I),
                ).first
                if cheapest_tab.count() > 0:
                    cheapest_tab.click(timeout=4000)
                    cheapest_tab_clicked = True
                else:
                    # Fallback locator for tab headers
                    fallback_tab = page.locator(
                        '[role="tab"]:has-text("Más baratos"), [role="tab"]:has-text("Cheapest")'
                    ).first
                    if fallback_tab.count() > 0:
                        fallback_tab.click(timeout=4000)
                        cheapest_tab_clicked = True
            except Exception as click_exc:
                logger.debug("Cheapest tab click skipped or unavailable: %s", click_exc)

            if cheapest_tab_clicked:
                # Wait 1,500ms for Google Flights list re-ordering / RPC hydration
                page.wait_for_timeout(CHEAPEST_TAB_WAIT_MS)

            cheapest_dom_offers = _filter_offers_for_corridor(
                self._extract_dom_cards(page, tab_source="cheapest", max_cards=3),
                expected_origins,
                expected_destinations,
            )
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
            # MERGE & SELECT BEST + CHEAPEST OFFERS ACROSS LAYER 1 & LAYER 2
            # ------------------------------------------------------------------
            best_offer: Optional[FlightOffer] = None
            if best_dom_offers:
                best_offer = best_dom_offers[0]
            elif ds1_best:
                best_offer = ds1_best

            cheapest_candidates: list[FlightOffer] = []
            if cheapest_dom_offers:
                cheapest_candidates.extend(cheapest_dom_offers)
            if filtered_rpc_offers:
                cheapest_candidates.extend(filtered_rpc_offers)
            if ds1_offers:
                cheapest_candidates.extend(ds1_offers)
            if best_dom_offers:
                cheapest_candidates.extend(best_dom_offers)

            if not cheapest_candidates:
                return None

            cheapest_candidates.sort(key=lambda o: o.price)
            cheapest_offer = cheapest_candidates[0]

            if best_offer is None:
                best_offer = cheapest_offer

            # Determine primary extraction_source tag for telemetry
            if cheapest_dom_offers or best_dom_offers:
                source_tag = "patchright_dom"
            elif filtered_rpc_offers:
                source_tag = "patchright_rpc"
            else:
                source_tag = "patchright_ds1"

            top_3_dicts = [o.to_dict() for o in cheapest_candidates[:3]]

            fallback_orig = next(iter(expected_origins)) if len(expected_origins) == 1 else None
            fallback_dest = next(iter(expected_destinations)) if len(expected_destinations) == 1 else None

            c_orig = cheapest_offer.origin_airport or fallback_orig
            c_dest = cheapest_offer.destination_airport or fallback_dest
            b_orig = (best_offer.origin_airport if best_offer else None) or c_orig
            b_dest = (best_offer.destination_airport if best_offer else None) or c_dest

            return SlotScrapeResult(
                cheapest_tab_price=cheapest_offer.price,
                cheapest_airline=cheapest_offer.airline,
                cheapest_stops=cheapest_offer.stops,
                cheapest_duration=cheapest_offer.duration,
                cheapest_dep_arr=cheapest_offer.dep_arr,
                is_self_transfer=cheapest_offer.is_self_transfer,
                origin_airport=c_orig,
                destination_airport=c_dest,
                best_tab_price=best_offer.price if best_offer else cheapest_offer.price,
                best_airline=best_offer.airline if best_offer else cheapest_offer.airline,
                best_stops=best_offer.stops if best_offer else cheapest_offer.stops,
                best_duration=best_offer.duration if best_offer else cheapest_offer.duration,
                best_origin_airport=b_orig,
                best_destination_airport=b_dest,
                extraction_source=source_tag,
                raw_top_offers=top_3_dicts,
            )
        finally:
            try:
                page.close()
            except Exception:
                pass

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
            logger.warning("Direct browser fallback encountered error: %s", exc)
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

        extraction = parse_ds1_from_html(resp.text)
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
            extraction_source="curl_cffi_ssr",
            raw_top_offers=[o.to_dict() for o in filtered_top[:3]],
        )
    except Exception as exc:
        logger.warning("Layer 3A (curl_cffi SSR) failed for %s: %s", url[:80], exc)
        return None


def scrape_via_fast_flights_fallback(url: str) -> Optional[SlotScrapeResult]:
    """
    Layer 3B: Secondary HTTP fallback using `fast-flights` if `tfs` parameters can be decoded.
    Uses primary airport pair if N:M list is decoded.
    """
    try:
        from fast_flights import FlightQuery, Passengers, create_filter, get_flights

        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        tfs_b64 = qs.get("tfs", [""])[0]
        curr = qs.get("curr", ["MXN"])[0]
        if not tfs_b64:
            return None

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

        if not (origin and destination and outbound_date):
            return None

        flights_list = [
            FlightQuery(date=outbound_date, from_airport=origin, to_airport=destination)
        ]
        if trip_type == "round-trip" and return_date:
            flights_list.append(
                FlightQuery(date=return_date, from_airport=destination, to_airport=origin)
            )

        q = create_filter(
            flights=flights_list,
            trip="round-trip" if (trip_type == "round-trip" and return_date) else "one-way",
            seat="economy",
            passengers=Passengers(adults=1),
        )
        res = get_flights(q)
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
        logger.debug("Layer 3B (fast-flights) fallback failed: %s", exc)
        return None


def scrape_date_slot(
    url: str,
    pooled_session: Optional[PooledPatchrightSession] = None,
    proxy_url: Optional[str] = None,
) -> Optional[SlotScrapeResult]:
    """
    High-level orchestrator for scraping a single `date_slot` across all 3 layers:
      1. Pooled Patchright Browser (Layer 1 `ds:1` + RPC & Layer 2 Best/Cheapest DOM Tabs)
      2. If proxy fails or browser returns no offers, retry direct or fall back to
         Layer 3 (`curl_cffi` TLS-impersonated SSR `ds:1` + `fast-flights`).
    """
    start_ts = time.perf_counter()

    # Tier 1 & 2: Pooled Patchright Browser Execution
    session_to_use = pooled_session
    own_session = False
    if session_to_use is None:
        try:
            session_to_use = PooledPatchrightSession(proxy_url=proxy_url, headless=True).start()
            own_session = True
        except Exception as init_exc:
            logger.debug("Could not start standalone browser session: %s", init_exc)
            session_to_use = None

    if session_to_use is not None:
        try:
            try:
                browser_result = session_to_use.scrape_slot_in_browser(url)
                if browser_result and browser_result.cheapest_tab_price > 0:
                    elapsed = time.perf_counter() - start_ts
                    logger.info(
                        "Scraped slot via %s in %.2fs -> Cheapest: $%.0f (%s) | Best: $%.0f",
                        browser_result.extraction_source,
                        elapsed,
                        browser_result.cheapest_tab_price,
                        browser_result.cheapest_airline,
                        browser_result.best_tab_price or browser_result.cheapest_tab_price,
                    )
                    return browser_result
            except Exception as browser_exc:
                logger.warning(
                    "Patchright browser extraction on %s encountered error (%s); trying direct fallback.",
                    session_to_use.proxy_url or "direct",
                    browser_exc,
                )

            # Tier 2B: Switch pooled session in-place to direct if proxy yielded 0 results or failed
            if session_to_use.proxy_url is not None:
                try:
                    session_to_use._switch_to_direct_in_place()
                    direct_res = session_to_use._scrape_slot_once(url)
                    if direct_res and direct_res.cheapest_tab_price > 0:
                        direct_res.extraction_source = "patchright_direct_fallback"
                        elapsed = time.perf_counter() - start_ts
                        logger.info(
                            "Scraped slot via direct browser fallback in %.2fs -> Cheapest: $%.0f (%s) | Best: $%.0f",
                            elapsed,
                            direct_res.cheapest_tab_price,
                            direct_res.cheapest_airline,
                            direct_res.best_tab_price or direct_res.cheapest_tab_price,
                        )
                        return direct_res
                except Exception as direct_exc:
                    logger.debug("Direct browser fallback error: %s", direct_exc)
        finally:
            if own_session and session_to_use is not None:
                try:
                    session_to_use.close()
                except Exception:
                    pass

    # Tier 3A: curl_cffi TLS-impersonated SSR `ds:1` extraction (with proxy, then direct fallback)
    ssr_result = scrape_via_curl_cffi_ssr(url, proxy_url=proxy_url)
    if ssr_result is None and proxy_url is not None:
        ssr_result = scrape_via_curl_cffi_ssr(url, proxy_url=None)
    if ssr_result and ssr_result.cheapest_tab_price > 0:
        elapsed = time.perf_counter() - start_ts
        logger.info(
            "Scraped slot via Layer 3 curl_cffi SSR in %.2fs -> Cheapest: $%.0f | Best: $%.0f",
            elapsed,
            ssr_result.cheapest_tab_price,
            ssr_result.best_tab_price or ssr_result.cheapest_tab_price,
        )
        return ssr_result

    # Tier 3B: fast-flights fallback
    ff_result = scrape_via_fast_flights_fallback(url)
    if ff_result and ff_result.cheapest_tab_price > 0:
        return ff_result

    logger.error("All 3 extraction layers failed to extract prices for URL: %s", url[:100])
    return None
