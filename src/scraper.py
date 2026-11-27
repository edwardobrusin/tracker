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
import random
import re
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Optional, Sequence
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
    DEFAULT_GL,
    DEFAULT_GL_POOL,
    DEFAULT_HL,
    GOOGLE_CONSENT_COOKIES,
    GOOGLE_SOCS_COOKIE_VALUE,
    HEADLESS_MODE,
    NAVIGATION_TIMEOUT_MS,
    parse_gl_pool,
    redact_proxy_for_log,
    sanitize_log_text,
)
from src.ds1_parser import (
    FlightOffer,
    detect_foreign_currency_in_text,
    detect_self_transfer,
    extract_ds1_currency_from_html,
    parse_ds1_from_html,
    parse_flight_card_text,
    parse_price_number,
    parse_shopping_rpc_response,
)
from src.tfs_builder import CHEAPEST_TFU, decode_tfs_parameter, normalize_flights_url

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
    sorted_by_price: bool = False
    tab_clicks: int = 0
    loaders_time_s: Optional[float] = None
    gl: Optional[str] = None
    winning_gl: Optional[str] = None
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


def parse_tab_hint_price(
    aria_label: str = "",
    text: str = "",
    expected_currency: Optional[str] = None,
) -> Optional[float]:
    """
    Extract the numeric price announced in the Google Flights 'Los más bajos' tab header
    (e.g. `aria-label="19236 pesos mexicanos"` or `"19.236\\xa0MXN"` -> `19236.0`).
    Rejects any hint carrying foreign currency markers when `expected_currency` is set.
    """
    if expected_currency and detect_foreign_currency_in_text(f"{aria_label or ''} {text or ''}", expected_currency):
        return None
    for candidate in (aria_label or "", text or ""):
        clean = str(candidate).strip()
        if not clean:
            continue
        parsed = parse_price_number(clean, expected_currency=expected_currency)
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
    must keep waiting for DOM synchronization, needs explicit Price sort, or timed out / mismatched.
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
        # Tab finished loading and cards settled under default "vuelos destacados" sort -> trigger Precio sort
        if dom_stable_count >= 3:
            return True, "cheapest_needs_price_sort"
        return False, "waiting_dom_sync"

    if dom_stable_count >= 2:
        return True, "cheapest_dom_stabilized"
    if at_deadline:
        return True, "cheapest_timeout"
    return False, "waiting_dom_stability"


def evaluate_post_sort_wait_state(
    spinner_active: bool,
    rpc_delta_sort: int,
    target_hint: Optional[float],
    dom_min_price: Optional[float],
    baseline_dom_min: Optional[float],
    dom_stable_count: int,
    elapsed_since_click_s: float,
    at_deadline: bool = False,
) -> tuple[bool, str]:
    """
    Prevent premature exits on frozen pre-sort DOM reads right after clicking 'Precio':
      - While `spinner_active` is True, keep waiting unless `at_deadline`.
      - Exit immediately once `dom_min_price <= target_hint * 1.01`.
      - If `rpc_delta_sort > 0` and DOM stabilized >= 2 reads, exit (`post_sort_rpc_stable`).
      - If `rpc_delta_sort == 0`, allow early exit only if DOM actually dropped vs `baseline_dom_min`
        and stabilized >= 2 reads (`>= 1.2s`); otherwise keep waiting for the post-sort RPC until deadline.
    """
    if spinner_active:
        return (True, "post_sort_deadline") if at_deadline else (False, "post_sort_spinner")

    if (
        target_hint is not None
        and target_hint > 0
        and dom_min_price is not None
        and dom_min_price > 0
        and dom_min_price <= target_hint * 1.01
    ):
        return True, "post_sort_hint_matched"

    if rpc_delta_sort > 0 and dom_min_price is not None and dom_stable_count >= 2:
        return True, "post_sort_rpc_stable"

    if (
        rpc_delta_sort == 0
        and dom_min_price is not None
        and baseline_dom_min is not None
        and dom_min_price < baseline_dom_min - 1.0
        and dom_stable_count >= 2
        and elapsed_since_click_s >= 1.2
    ):
        return True, "post_sort_client_drop_stable"

    if at_deadline:
        return True, "post_sort_deadline"

    return False, "waiting_post_sort_rpc"


def evaluate_direct_cheapest_state(
    aria_selected: bool,
    active_loaders: int,
    cards_count: int,
    elapsed_s: float,
    dom_min_price: Optional[float] = None,
    prev_dom_min_price: Optional[float] = None,
    confirm_reads: int = 0,
    no_cheaper_notice: bool = False,
    at_deadline: bool = False,
) -> tuple[bool, str]:
    """
    Deterministic readiness check for direct `tfu=EgoIABAAGAAgAigB` landing:
      - Keeps waiting whenever any of the 3 visual loaders is active (`active_loaders > 0`).
      - Requires `aria_selected=True`, `elapsed_s >= 1.5`, and `cards_count > 0`.
      - Once `active_loaders == 0`, requires 1 confirmatory stability read (`confirm_reads >= 1`)
        500 ms later with `active_loaders == 0` and unchanged `dom_min_price`.
    """
    if active_loaders > 0:
        return (True, "direct_cheapest_timeout") if at_deadline else (False, "waiting_loaders")

    if not aria_selected:
        return (True, "direct_cheapest_timeout") if at_deadline else (False, "waiting_tab_selected")

    if no_cheaper_notice and elapsed_s >= 1.5:
        return True, "direct_cheapest_no_cheaper_notice"

    if elapsed_s < 1.5 or cards_count <= 0:
        return (True, "direct_cheapest_timeout") if at_deadline else (False, "waiting_min_settle")

    prices_match = (
        dom_min_price is None and prev_dom_min_price is None
    ) or (
        dom_min_price is not None
        and prev_dom_min_price is not None
        and abs(dom_min_price - prev_dom_min_price) < 1.0
    )

    if confirm_reads >= 1 and prices_match:
        return True, "direct_cheapest_settled"

    if at_deadline:
        return True, "direct_cheapest_settled"

    return False, "waiting_confirm_stability"


def _extract_dom_min_from_samples(
    card_samples: list[dict[str, Any]],
    expected_currency: Optional[str] = None,
) -> Optional[float]:
    """Return minimum positive price parsed from probe `cardSamples` matching `expected_currency`."""
    prices: list[float] = []
    for cs in card_samples or []:
        aria_txt = str(cs.get("aria") or "")
        price_nodes = cs.get("priceNodes") or []
        if expected_currency and detect_foreign_currency_in_text(
            f"{aria_txt} {' '.join(str(x) for x in price_nodes)}",
            expected_currency,
        ):
            continue
        p_val: Optional[float] = None
        for p_txt in price_nodes:
            cand = parse_price_number(p_txt, expected_currency=expected_currency)
            if cand is not None:
                p_val = cand
                break
        if p_val is None and aria_txt:
            p_val = parse_price_number(aria_txt, expected_currency=expected_currency)
        if p_val is not None and p_val > 0:
            prices.append(p_val)
    return min(prices) if prices else None


def partition_sectioned_raw_cards(
    raw_cards: list[dict[str, Any]],
    max_cards: int = 80,
    expected_currency: Optional[str] = None,
) -> tuple[list[FlightOffer], list[FlightOffer], list[FlightOffer]]:
    """
    Parse raw DOM card dicts into `(top_offers, other_offers, all_offers)`,
    separating `Top departing flights` / `Vuelos de ida destacados` (Best)
    from `Other departing flights` / `Otros vuelos de ida` (Cheapest).
    """
    top_offers: list[FlightOffer] = []
    other_offers: list[FlightOffer] = []
    all_offers: list[FlightOffer] = []

    def _append_dedup(bucket: list[FlightOffer], offer: FlightOffer) -> None:
        if not any(
            abs(o.price - offer.price) < 1.0
            and o.airline == offer.airline
            and o.dep_arr == offer.dep_arr
            for o in bucket
        ):
            bucket.append(offer)

    for item in raw_cards or []:
        try:
            section = str(item.get("section") or "other").strip().lower()
            is_top = section == "top"
            tab_src = "best" if is_top else "cheapest"
            aria_label = str(item.get("aria") or "")
            inner_text = str(item.get("innerText") or "")
            price_text = str(item.get("priceText") or "")
            price_override = parse_price_number(price_text) if price_text else None

            offer = parse_flight_card_text(
                aria_label=aria_label,
                inner_text=inner_text,
                price_override=price_override,
                tab_source=tab_src,
                expected_currency=expected_currency,
            )
            if offer and offer.price > 0:
                if is_top:
                    _append_dedup(top_offers, offer)
                else:
                    _append_dedup(other_offers, offer)
                _append_dedup(all_offers, offer)
                if len(all_offers) >= max_cards:
                    break
        except Exception:
            continue

    return top_offers, other_offers, all_offers


def consolidate_slot_offers(
    best_dom_offers: list[FlightOffer],
    cheapest_dom_offers: list[FlightOffer],
    rpc_offers: list[FlightOffer],
    ds1_offers: list[FlightOffer],
    ds1_best: Optional[FlightOffer] = None,
    expected_origins: Optional[set[str]] = None,
    expected_destinations: Optional[set[str]] = None,
    sorted_dom_offers: Optional[list[FlightOffer]] = None,
) -> Optional[dict[str, Any]]:
    """
    Consolidate Best and Cheapest offers across DOM, sorted-by-Price DOM, GetShoppingResults RPC,
    and initial ds:1. Always selects `min(...)` and records which source won
    (`cheapest_dom_sorted_price`, `cheapest_dom`, `cheapest_rpc`, `cheapest_ds1_min`).
    """
    best_offer: Optional[FlightOffer] = None
    if best_dom_offers:
        best_offer = best_dom_offers[0]
    elif ds1_best:
        best_offer = ds1_best

    tagged_candidates: list[tuple[FlightOffer, str]] = []
    for offer in (sorted_dom_offers or []):
        tagged_candidates.append((offer, "cheapest_dom_sorted_price"))
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

    # Sort by price ASC; prefer sorted-by-price DOM -> DOM -> RPC -> ds1_min on exact tie
    source_rank = {
        "cheapest_dom_sorted_price": 0,
        "cheapest_dom": 1,
        "cheapest_rpc": 2,
        "cheapest_ds1_min": 3,
    }
    tagged_candidates.sort(key=lambda pair: (pair[0].price, source_rank.get(pair[1], 4)))
    cheapest_offer, cheapest_source = tagged_candidates[0]

    if best_offer is None:
        best_offer = cheapest_offer

    if cheapest_source in ("cheapest_dom", "cheapest_dom_sorted_price") or best_dom_offers:
        source_tag = "patchright_dom" if cheapest_source in ("cheapest_dom", "cheapest_dom_sorted_price") else (
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
                    const allCards = Array.from(document.querySelectorAll('li.pIav2d'));
                    const visCards = allCards.filter(c => c.offsetParent !== null);
                    const cards = (visCards.length ? visCards : allCards).slice(0, maxItems);
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

    def _extract_sectioned_dom_cards(
        self,
        page: Any,
        max_cards: int = 80,
        expected_currency: Optional[str] = None,
    ) -> tuple[list[FlightOffer], list[FlightOffer], list[FlightOffer]]:
        """
        Extract visible Google Flights DOM cards partitioned into:
          - `top_offers`: cards under `Top departing flights` / `Vuelos de ida destacados` (Best)
          - `other_offers`: cards under `Other departing flights` / `Otros vuelos de ida` (Cheapest)
          - `all_offers`: deduplicated union across both sections up to `max_cards`.
        """
        limit = max(1, max_cards * 2)
        try:
            raw_cards = page.evaluate(
                """(maxItems) => {
                    const allCards = Array.from(document.querySelectorAll('li.pIav2d'));
                    const visCards = allCards.filter(c => c.offsetParent !== null);
                    const cards = (visCards.length ? visCards : allCards).slice(0, maxItems);
                    const otherRe = /otros vuelos|other departing|other flights|vuelos más baratos|cheapest flights/i;
                    const topRe = /vuelos de ida destacados|vuelos destacados|mejores vuelos|top departing|top flights|best flights|mejores opciones/i;
                    return cards.map(card => {
                        let section = 'unknown';
                        let node = card;
                        for (let step = 0; step < 6 && node; step++) {
                            const parent = node.parentElement;
                            if (!parent) break;
                            let sib = node.previousElementSibling;
                            while (sib) {
                                const st = (sib.innerText || '').trim();
                                if (otherRe.test(st)) {
                                    section = 'other';
                                    break;
                                }
                                if (topRe.test(st)) {
                                    section = 'top';
                                    break;
                                }
                                sib = sib.previousElementSibling;
                            }
                            if (section !== 'unknown') break;
                            node = parent;
                        }
                        let aria = card.getAttribute('aria-label') || '';
                        if (!aria) {
                            const innerLabeled = card.querySelector('[aria-label]');
                            if (innerLabeled) aria = innerLabeled.getAttribute('aria-label') || '';
                        }
                        const innerText = card.innerText || '';
                        const priceEl = card.querySelector('.YMlIz, span[data-gs]');
                        const priceText = priceEl ? (priceEl.innerText || '') : '';
                        return { section, aria, innerText, priceText };
                    });
                }""",
                limit,
            )
        except Exception:
            raw_cards = []

        if not isinstance(raw_cards, list):
            return [], [], []
        return partition_sectioned_raw_cards(
            raw_cards,
            max_cards=max_cards,
            expected_currency=expected_currency,
        )

    def _probe_direct_cheapest_state(self, page: Any) -> dict[str, Any]:
        """
        Atomic probe monitoring EXCLUSIVELY the 3 real visual loaders in direct Cheapest view:
          1. `div.DU29of` (`[jscontroller="DFTXbf"]` inside `.bU1Yvc`)
          2. `div[role="progressbar"][jsname="LbNpof"].VfPpkd-JGcpL-P1ekSe` (16x16 spinner)
          3. `div[role="progressbar"][jsname="LbNpof"].VfPpkd-qNpTzb-P1ekSe` (top 990x2 bar)
        Ignores hidden `document.body` loading text to eliminate false positives.
        """
        try:
            state = page.evaluate(
                """() => {
                    const isStyleVisible = (el) => {
                        const cs = window.getComputedStyle(el);
                        return cs.display !== 'none' && cs.visibility !== 'hidden' && parseFloat(cs.opacity || '1') > 0;
                    };

                    const activeSet = new Set();

                    // 1. div.DU29of inside .bU1Yvc ([jscontroller="DFTXbf"])
                    for (const el of document.querySelectorAll('.bU1Yvc div.DU29of, .bU1Yvc [jscontroller="DFTXbf"]')) {
                        if (isStyleVisible(el)) activeSet.add(el);
                    }

                    // 2 & 3. Spinner 16x16 and top progress bar 990x2 (must also have width > 0 && height > 0)
                    for (const el of document.querySelectorAll(
                        'div[role="progressbar"][jsname="LbNpof"].VfPpkd-JGcpL-P1ekSe, div[role="progressbar"][jsname="LbNpof"].VfPpkd-qNpTzb-P1ekSe'
                    )) {
                        if (!isStyleVisible(el)) continue;
                        const r = el.getBoundingClientRect();
                        if (r.width > 0 && r.height > 0) activeSet.add(el);
                    }

                    const tabs = Array.from(document.querySelectorAll('[role="tab"]'));
                    const tabRe = /Los más bajos|Más bajos|Cheapest|Más baratos|Baratos/i;
                    const cTab = tabs.find(t => tabRe.test(t.innerText || ''));
                    const tabFound = !!cTab;
                    const ariaSelected = cTab ? (cTab.getAttribute('aria-selected') === 'true') : false;

                    const bSpan = cTab ? cTab.querySelector('span.bU1Yvc, .bU1Yvc') : null;
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

                    const allCards = Array.from(document.querySelectorAll('li.pIav2d'));
                    const visCards = allCards.filter(c => c.offsetParent !== null);
                    const cards = visCards.length ? visCards : allCards;
                    const cardSamples = cards.map((c, idx) => {
                        const aria = c.getAttribute('aria-label') || (c.querySelector('[aria-label]')?.getAttribute('aria-label') || '');
                        const priceNodes = Array.from(c.querySelectorAll('.YMlIz, span[data-gs]')).map(n => (n.textContent || '').trim()).filter(Boolean);
                        const lines = idx < 6 ? (c.textContent || '').split('\\n').map(s => s.trim()).filter(Boolean).slice(0, 4).join('|') : '';
                        return { aria, priceNodes, lines };
                    });

                    return {
                        tabFound,
                        ariaSelected,
                        activeLoaders: activeSet.size,
                        cardsCount: cards.length,
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
            "activeLoaders": 0,
            "cardsCount": 0,
            "tabHintAria": "",
            "tabHintText": "",
            "noCheaper": False,
            "cardSamples": [],
        }

    def _wait_for_direct_cheapest_settled(
        self,
        page: Any,
        t_goto: float,
        max_wait_s: float = 35.0,
    ) -> tuple[str, Optional[float], int, int, float]:
        """
        Wait deterministically (400 ms poll, 35.0 s hard cap) until `activeLoaders == 0`,
        `elapsed_s >= 1.5`, and `cardsCount > 0`, followed by 1 confirmatory stability
        read 500 ms later with `activeLoaders == 0` and unchanged `dom_min_price`.
        Includes a minimal defensive fallback: if after 5.0 s the Cheapest tab is not
        `aria-selected="true"`, clicks it at most once (`click()` + `el.click()` fallback).
        """
        t_wait_start = time.perf_counter()
        hard_deadline = t_wait_start + max(3.0, max_wait_s)
        tab_pattern = re.compile(r"Los más bajos|Más bajos|Cheapest|Más baratos|Baratos", re.I)

        fallback_clicked = False
        tab_clicks = 0
        confirm_reads = 0
        prev_confirm_min: Optional[float] = None
        last_tab_hint: Optional[float] = None
        last_active_loaders = 0
        final_status = "direct_cheapest_timeout"

        while True:
            probe = self._probe_direct_cheapest_state(page)
            wait_elapsed = time.perf_counter() - t_wait_start
            goto_elapsed = time.perf_counter() - t_goto

            tab_found = bool(probe.get("tabFound"))
            aria_selected = bool(probe.get("ariaSelected"))

            # Defensive fallback only if after 5.0 s Cheapest tab is present but not selected
            if tab_found and not aria_selected and not fallback_clicked and wait_elapsed >= 5.0:
                fallback_clicked = True
                try:
                    c_tab = page.locator('[role="tab"]').filter(has_text=tab_pattern).first
                    if c_tab.count() == 0:
                        c_tab = page.get_by_role("tab", name=tab_pattern).first
                    if c_tab.count() > 0:
                        try:
                            c_tab.click(timeout=3500)
                        except Exception:
                            c_tab.evaluate("el => el.click()")
                        tab_clicks += 1
                        confirm_reads = 0
                        prev_confirm_min = None
                        page.wait_for_timeout(400)
                        continue
                except Exception as click_exc:
                    logger.debug("Defensive cheapest tab fallback click skipped: %s", sanitize_log_text(click_exc))

            active_loaders = int(probe.get("activeLoaders") or 0)
            last_active_loaders = active_loaders
            cards_count = int(probe.get("cardsCount") or 0)

            expected_currency = getattr(self, "_active_expected_currency", None)
            hp = parse_tab_hint_price(
                probe.get("tabHintAria", ""),
                probe.get("tabHintText", ""),
                expected_currency=expected_currency,
            )
            if hp is not None and hp > 0:
                last_tab_hint = hp

            dom_min_price = _extract_dom_min_from_samples(
                probe.get("cardSamples") or [],
                expected_currency=expected_currency,
            )

            # If a route has no tab bar after >=5s, do not block forever on missing tabFound
            effective_aria_selected = aria_selected or (not tab_found and wait_elapsed >= 5.0)
            ready_for_confirm = (
                effective_aria_selected
                and active_loaders == 0
                and goto_elapsed >= 1.5
                and cards_count > 0
            )
            if ready_for_confirm:
                if prev_confirm_min is not None or confirm_reads > 0:
                    same_price = (
                        dom_min_price is None and prev_confirm_min is None
                    ) or (
                        dom_min_price is not None
                        and prev_confirm_min is not None
                        and abs(dom_min_price - prev_confirm_min) < 1.0
                    )
                    if same_price:
                        confirm_reads += 1
                    else:
                        confirm_reads = 0
                        prev_confirm_min = dom_min_price
                else:
                    confirm_reads = 0
                    prev_confirm_min = dom_min_price
            else:
                confirm_reads = 0
                prev_confirm_min = None

            at_deadline = time.perf_counter() >= hard_deadline
            should_exit, status_reason = evaluate_direct_cheapest_state(
                aria_selected=effective_aria_selected,
                active_loaders=active_loaders,
                cards_count=cards_count,
                elapsed_s=goto_elapsed,
                dom_min_price=dom_min_price,
                prev_dom_min_price=prev_confirm_min,
                confirm_reads=confirm_reads,
                no_cheaper_notice=bool(probe.get("noCheaper")),
                at_deadline=at_deadline,
            )
            if should_exit:
                final_status = status_reason
                break

            wait_ms = 500 if status_reason == "waiting_confirm_stability" else 400
            page.wait_for_timeout(wait_ms)

        loaders_time_s = round(time.perf_counter() - t_goto, 2)
        return final_status, last_tab_hint, last_active_loaders, tab_clicks, loaders_time_s

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

    def _detect_page_currency_mismatch(
        self,
        page: Any,
        html_content: str,
        expected_curr: str,
    ) -> Optional[str]:
        """
        Verify that the rendered Google Flights page, final URL `curr=` query param,
        footer/currency picker, visible card `aria-label`/price nodes, `span[data-gs]`,
        and `ds:1` booking token all match `expected_curr`.
        Returns the conflicting currency code if mismatched, else None.
        """
        exp = (expected_curr or DEFAULT_CURRENCY).strip().upper()
        try:
            cur_url = str(getattr(page, "url", "") or "")
            if cur_url and "curr=" in cur_url:
                url_curr = (parse_qs(urlparse(cur_url).query).get("curr", [""])[0] or "").strip().upper()
                if url_curr and url_curr != exp:
                    return url_curr
        except Exception:
            pass

        try:
            dom_probe = page.evaluate(
                """() => {
                    const footerTexts = Array.from(
                        document.querySelectorAll('footer button, button[aria-label*="Moneda"], button[aria-label*="Currency"], [data-currency]')
                    ).map(el => (el.getAttribute('data-currency') || el.getAttribute('aria-label') || el.innerText || '').trim()).filter(Boolean);

                    const cards = Array.from(document.querySelectorAll('li.pIav2d')).slice(0, 5);
                    const cardTexts = [];
                    for (const c of cards) {
                        const aria = c.getAttribute('aria-label') || (c.querySelector('[aria-label]')?.getAttribute('aria-label') || '');
                        if (aria) cardTexts.push(aria);
                        for (const n of c.querySelectorAll('.YMlIz, span[data-gs]')) {
                            const t = (n.textContent || '').trim();
                            if (t) cardTexts.push(t);
                            const gs = n.getAttribute('data-gs') || '';
                            if (gs) cardTexts.push(gs);
                        }
                    }
                    return {
                        footerText: footerTexts.join(' | '),
                        cardText: cardTexts.join(' | '),
                    };
                }"""
            )
            if isinstance(dom_probe, dict):
                for key in ("footerText", "cardText"):
                    txt = str(dom_probe.get(key) or "")
                    if txt:
                        foreign = detect_foreign_currency_in_text(txt, exp)
                        if foreign:
                            return foreign
        except Exception:
            pass

        try:
            ds1_curr = extract_ds1_currency_from_html(html_content)
            if ds1_curr and ds1_curr != exp:
                return ds1_curr
        except Exception:
            pass

        return None

    def _scrape_slot_once(
        self,
        url: str,
        gl: Optional[str] = None,
        _currency_retry: bool = False,
    ) -> Optional[SlotScrapeResult]:
        """
        Navigate directly to `tfu=EgoIABAAGAAgAigB` (`Los más bajos`), wait deterministically
        for `loaders == 0` (+ 500 ms confirmatory read), and extract both Cheapest and Best
        from the same page without switching back and forth between tabs.
        """
        target_url = normalize_flights_url(url, tfu=CHEAPEST_TFU, gl=gl)
        parsed_qs = parse_qs(urlparse(target_url).query)
        expected_origins, expected_destinations = _extract_expected_corridor_iatas(target_url)
        expected_curr = (parsed_qs.get("curr", [DEFAULT_CURRENCY])[0] or DEFAULT_CURRENCY).strip().upper()
        gl_eff = (parsed_qs.get("gl", [DEFAULT_GL])[0] or DEFAULT_GL).strip().upper()

        page = self._context.new_page()
        page.set_default_timeout(15000)
        rpc_cheapest_offers: list[FlightOffer] = []
        rpc_payload_counter = {"count": 0}
        retry_with_explicit_curr = False
        result_out: Optional[SlotScrapeResult] = None

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
            t_goto = time.perf_counter()
            page.goto(target_url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT_MS)
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
            # LAYER 2: Deterministic direct-Cheapest settle (loaders == 0)
            # ------------------------------------------------------------------
            self._active_expected_currency = expected_curr
            cheapest_tab_status, tab_hint_price, active_loaders, tab_clicks, loaders_time_s = (
                self._wait_for_direct_cheapest_settled(page, t_goto=t_goto, max_wait_s=35.0)
            )

            mismatch_curr = self._detect_page_currency_mismatch(page, html_content, expected_curr)
            if mismatch_curr:
                logger.warning(
                    "currency_mismatch: expected=%s, detected=%s, gl=%s",
                    expected_curr,
                    mismatch_curr,
                    gl_eff,
                )
                if not _currency_retry:
                    retry_with_explicit_curr = True
            else:
                top_dom_offers, _other_dom_offers, all_dom_offers = self._extract_sectioned_dom_cards(
                    page,
                    max_cards=80,
                    expected_currency=expected_curr,
                )
                best_dom_offers = _filter_offers_for_corridor(top_dom_offers, expected_origins, expected_destinations)
                cheapest_dom_offers = _filter_offers_for_corridor(all_dom_offers, expected_origins, expected_destinations)
                filtered_rpc_offers = _filter_offers_for_corridor(
                    rpc_cheapest_offers,
                    expected_origins,
                    expected_destinations,
                )
                rpc_best = next((o for o in filtered_rpc_offers if o.tab_source == "best"), None)

                sorted_dom_offers: list[FlightOffer] = []
                sorted_by_price = False

                prelim_min = min(
                    (o.price for o in [*cheapest_dom_offers, *filtered_rpc_offers, *ds1_offers] if o.price > 0),
                    default=None,
                )
                # Safety net ONLY if loaders == 0 finished, tab_hint is valid, and cheapest > tab_hint * 1.02
                if (
                    active_loaders == 0
                    and tab_hint_price is not None
                    and tab_hint_price > 0
                    and (prelim_min is None or prelim_min > tab_hint_price * 1.02)
                ):
                    sorted_applied, sort_hint, _ = self._sort_results_by_price(
                        page=page,
                        tab_hint_price=tab_hint_price,
                        max_wait_s=12.0,
                        rpc_payload_counter=rpc_payload_counter,
                    )
                    if sort_hint is not None and sort_hint > 0:
                        tab_hint_price = min(tab_hint_price, sort_hint) if tab_hint_price else sort_hint
                    if sorted_applied:
                        sorted_by_price = True
                        _, _, post_sort_all = self._extract_sectioned_dom_cards(
                            page,
                            max_cards=80,
                            expected_currency=expected_curr,
                        )
                        sorted_dom_offers = _filter_offers_for_corridor(
                            post_sort_all, expected_origins, expected_destinations
                        )
                        filtered_rpc_offers = _filter_offers_for_corridor(
                            rpc_cheapest_offers,
                            expected_origins,
                            expected_destinations,
                        )
                        cheapest_tab_status = "direct_cheapest_sorted_safety_net"

                # ------------------------------------------------------------------
                # MERGE & SELECT BEST (Top departing flights / ds:1 / RPC) + CHEAPEST
                # ------------------------------------------------------------------
                consolidated = consolidate_slot_offers(
                    best_dom_offers=best_dom_offers,
                    cheapest_dom_offers=cheapest_dom_offers,
                    rpc_offers=filtered_rpc_offers,
                    ds1_offers=ds1_offers,
                    ds1_best=ds1_best or rpc_best,
                    expected_origins=expected_origins,
                    expected_destinations=expected_destinations,
                    sorted_dom_offers=sorted_dom_offers,
                )
                if consolidated:
                    cheapest_offer = consolidated["cheapest_offer"]
                    best_offer = consolidated["best_offer"]

                    result_out = SlotScrapeResult(
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
                        sorted_by_price=sorted_by_price,
                        tab_clicks=tab_clicks,
                        loaders_time_s=loaders_time_s,
                        gl=gl_eff,
                        winning_gl=gl_eff,
                        raw_top_offers=consolidated["top_3_dicts"],
                    )
        finally:
            self._active_expected_currency = None
            try:
                page.close()
            except Exception:
                pass
        if retry_with_explicit_curr:
            explicit_url = normalize_flights_url(url, tfu=CHEAPEST_TFU, gl=gl_eff, curr=expected_curr)
            return self._scrape_slot_once(explicit_url, gl=gl_eff, _currency_retry=True)
        return result_out

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

                    const cards = Array.from(document.querySelectorAll('li.pIav2d'));
                    const cardSamples = cards.map((c, idx) => {
                        const aria = c.getAttribute('aria-label') || (c.querySelector('[aria-label]')?.getAttribute('aria-label') || '');
                        const priceNodes = Array.from(c.querySelectorAll('.YMlIz, span[data-gs]')).map(n => (n.textContent || '').trim()).filter(Boolean);
                        const lines = idx < 6 ? (c.textContent || '').split('\\n').map(s => s.trim()).filter(Boolean).slice(0, 4).join('|') : '';
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

    def _sort_results_by_price(
        self,
        page: Any,
        tab_hint_price: Optional[float] = None,
        max_wait_s: float = 11.0,
        rpc_payload_counter: Optional[dict[str, int]] = None,
    ) -> tuple[bool, Optional[float], Optional[float]]:
        """
        Explicitly switch the VISIBLE Google Flights sort dropdown (`button[jsname="AefGQb"]`)
        to "Precio" / "Price", verify the button label actually changed to
        "Ordenados por precio" / "Sorted by price" (retrying once if needed), and wait
        with a dedicated post-sort budget (`max_wait_s`) for any `GetShoppingResults`
        post-sort RPC and DOM reorder instead of exiting early on frozen DOM reads.
        Never returns `True` unless the visible sort button confirmed Precio/Price.
        """
        t0 = time.perf_counter()
        deadline = t0 + max(1.5, max_wait_s)

        def _find_visible_sort_btn() -> Optional[Any]:
            for sel in (
                'button[jsname="AefGQb"]:visible',
                'button[aria-label*="Ordenados por"]:visible, button[aria-label*="Sorted by"]:visible',
                'button[jsname="AefGQb"]',
                'button[aria-label*="Ordenados por"], button[aria-label*="Sorted by"]',
            ):
                try:
                    cand = page.locator(sel).first
                    if cand.count() > 0:
                        if hasattr(cand, "is_visible"):
                            if not cand.is_visible():
                                continue
                        return cand
                except Exception:
                    continue
            return None

        def _read_sort_btn_label(btn_loc: Any) -> str:
            parts: list[str] = []
            try:
                if hasattr(btn_loc, "inner_text"):
                    txt = str(btn_loc.inner_text() or "").strip()
                    if txt:
                        parts.append(txt)
            except Exception:
                pass
            try:
                if hasattr(btn_loc, "get_attribute"):
                    aria = str(btn_loc.get_attribute("aria-label") or "").strip()
                    if aria:
                        parts.append(aria)
            except Exception:
                pass
            return " | ".join(parts)

        def _is_price_sorted_label(lbl: str) -> bool:
            low = (lbl or "").lower()
            return "precio" in low or "price" in low

        # Wait up to 5.0s for the active tab's visible sort button when a sort button exists in DOM
        sort_btn = _find_visible_sort_btn()
        if sort_btn is None:
            has_dom_sort_btn = False
            try:
                has_dom_sort_btn = (
                    page.locator('button[jsname="AefGQb"], button[aria-label*="Ordenados por"], button[aria-label*="Sorted by"]').count() > 0
                )
            except Exception:
                has_dom_sort_btn = False
            if has_dom_sort_btn:
                btn_wait_end = min(deadline, time.perf_counter() + (5.0 if max_wait_s >= 8.0 else 1.5))
                while sort_btn is None and time.perf_counter() < btn_wait_end:
                    page.wait_for_timeout(250)
                    sort_btn = _find_visible_sort_btn()

        if sort_btn is None:
            logger.warning("Sort by Precio skipped: no visible button[jsname='AefGQb'] found.")
            return False, tab_hint_price, None

        initial_label = _read_sort_btn_label(sort_btn)
        if _is_price_sorted_label(initial_label):
            logger.info("Sort button already shows Precio/Price ('%s').", initial_label)
            return True, tab_hint_price, None

        price_opt_re = re.compile(r"^\s*(Precio|Price)\s*$", re.I)

        def _attempt_select_precio(strategy_num: int) -> bool:
            btn = _find_visible_sort_btn() or sort_btn
            try:
                if strategy_num == 1:
                    btn.click(timeout=1800)
                else:
                    btn.evaluate("el => el.click()")
            except Exception:
                try:
                    btn.evaluate("el => el.click()")
                except Exception:
                    return False

            page.wait_for_timeout(350)

            for opt_sel in (
                '[role="menuitemradio"]:visible, [role="option"]:visible, [role="menuitem"]:visible',
                '[role="menuitemradio"], [role="option"], [role="menuitem"], li',
            ):
                try:
                    opt = page.locator(opt_sel).filter(has_text=price_opt_re).first
                    if opt.count() > 0:
                        if hasattr(opt, "is_visible") and not opt.is_visible() and ":visible" in opt_sel:
                            continue
                        try:
                            opt.click(timeout=1800)
                        except Exception:
                            opt.evaluate("el => el.click()")
                        return True
                except Exception:
                    continue

            try:
                return bool(
                    page.evaluate(
                        """() => {
                            const items = Array.from(document.querySelectorAll('[role="menuitemradio"], [role="option"], [role="menuitem"]'));
                            const visItems = items.filter(el => el.offsetParent !== null);
                            const pool = visItems.length ? visItems : items;
                            const re = /^\\s*(Precio|Price)\\s*$/i;
                            const target = pool.find(el => re.test((el.innerText || el.textContent || '').trim()));
                            if (!target) return false;
                            target.click();
                            return true;
                        }"""
                    )
                )
            except Exception:
                return False

        rpc_before_sort = (rpc_payload_counter or {}).get("count", 0)

        # Capture baseline DOM min before clicking "Precio" so frozen pre-sort DOM cannot fake stability
        baseline_dom_min: Optional[float] = None
        try:
            pre_st = self._probe_cheapest_dom_state(page)
            pre_prices: list[float] = []
            for cs in pre_st.get("cardSamples") or []:
                pv = None
                for pt in cs.get("priceNodes") or []:
                    pv = parse_price_number(pt)
                    if pv is not None:
                        break
                if pv is None and cs.get("aria"):
                    pv = parse_price_number(cs["aria"])
                if pv is not None and pv > 0:
                    pre_prices.append(pv)
            if pre_prices:
                baseline_dom_min = min(pre_prices)
        except Exception:
            pass

        # Attempt 1 + mandatory button-label verification (retry once if unchanged)
        confirmed_sorted = False
        latest_label = initial_label
        t_sort_clicked = time.perf_counter()
        for attempt in (1, 2):
            if time.perf_counter() >= deadline:
                break
            clicked_menu = _attempt_select_precio(attempt)
            if not clicked_menu:
                continue
            t_sort_clicked = time.perf_counter()

            verify_end = min(deadline, time.perf_counter() + 1.4)
            while True:
                page.wait_for_timeout(250)
                vis_btn_now = _find_visible_sort_btn()
                if vis_btn_now is not None:
                    latest_label = _read_sort_btn_label(vis_btn_now)
                    if _is_price_sorted_label(latest_label):
                        confirmed_sorted = True
                        break
                if time.perf_counter() >= verify_end:
                    break

            if confirmed_sorted:
                break

        if not confirmed_sorted:
            logger.warning(
                "Sort by Precio did NOT change visible button label (still '%s'); returning sorted_by_price=False.",
                latest_label or initial_label,
            )
            return False, tab_hint_price, None

        # Button confirmed "Ordenados por precio" / "Sorted by price": give dedicated budget for RPC + DOM
        post_click_deadline = max(deadline, t_sort_clicked + max(1.5, max_wait_s))
        last_hint = tab_hint_price
        last_dom_min: Optional[float] = None
        prev_top_sig: Optional[tuple[Optional[float], str]] = None
        stable_count = 0
        post_wait_status = "post_sort_deadline"

        while True:
            st = self._probe_cheapest_dom_state(page)
            spinner_active = bool(st.get("spinnerActive"))
            hint_now = parse_tab_hint_price(st.get("tabHintAria", ""), st.get("tabHintText", ""))
            if hint_now is not None and hint_now > 0:
                last_hint = min(last_hint, hint_now) if last_hint else hint_now

            parsed_prices: list[float] = []
            top_line = ""
            for idx, cs in enumerate(st.get("cardSamples") or []):
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
                if idx == 0:
                    top_line = f"{p_val or ''}|{cs.get('lines', '')}"

            dom_min_now = min(parsed_prices) if parsed_prices else None
            last_dom_min = dom_min_now

            if not spinner_active and dom_min_now is not None:
                sig_now = (dom_min_now, top_line)
                if sig_now == prev_top_sig:
                    stable_count += 1
                else:
                    stable_count = 1
                prev_top_sig = sig_now
            else:
                stable_count = 0
                prev_top_sig = None

            rpc_delta_sort = (rpc_payload_counter or {}).get("count", 0) - rpc_before_sort
            elapsed_since_click = time.perf_counter() - t_sort_clicked
            at_deadline = time.perf_counter() >= post_click_deadline

            should_exit, post_wait_status = evaluate_post_sort_wait_state(
                spinner_active=spinner_active,
                rpc_delta_sort=rpc_delta_sort,
                target_hint=last_hint,
                dom_min_price=dom_min_now,
                baseline_dom_min=baseline_dom_min,
                dom_stable_count=stable_count,
                elapsed_since_click_s=elapsed_since_click,
                at_deadline=at_deadline,
            )
            if should_exit:
                break

            page.wait_for_timeout(300)

        rpc_delta_sort = (rpc_payload_counter or {}).get("count", 0) - rpc_before_sort
        logger.info(
            "Applied sort by Precio: sorted_by_price=True (label='%s', wait_status=%s, %.2fs, tab_hint=%s, dom_min=%s, rpc_payloads_post_sort=%d)",
            latest_label,
            post_wait_status,
            time.perf_counter() - t0,
            f"${last_hint:,.0f}" if last_hint is not None else "N/A",
            f"${last_dom_min:,.0f}" if last_dom_min is not None else "N/A",
            rpc_delta_sort,
        )
        return True, last_hint, last_dom_min

    def _switch_to_cheapest_tab_deterministic(
        self,
        page: Any,
        rpc_payload_counter: dict[str, int],
        best_dom_offers: list[FlightOffer],
        expected_currency: Optional[str] = None,
        hard_timeout_s: float = 14.5,
    ) -> tuple[bool, str, Optional[float]]:
        """
        Deterministically switch from 'Mejores opciones' to 'Los más bajos' / 'Cheapest':
          1. Try `cheapest_tab.click(timeout=3500)` with immediate DOM `el.click()` fallback.
          2. Poll every 400 ms (hard deadline `hard_timeout_s`, leaving budget for Price sort)
             via single `page.evaluate(...)` reading `ariaSelected`, `.bU1Yvc` spinner, `tab_hint_price`,
             and `dom_min_price` across ALL `li.pIav2d` cards.
          3. Require `spinner_seen` (or >= 2.0 s settle) AND 2 consecutive stable readings
             where `dom_min_price <= tab_hint_price * 1.01` before early exit; exit cleanly to
             trigger `_sort_results_by_price()` if tab settled with `dom_min > tab_hint * 1.01`.
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

        # Primary click (capped at 3500ms) with immediate DOM click() fallback
        clicked_ok = False
        try:
            cheapest_tab.click(timeout=3500)
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
                if bool(dom_state.get("tabFound")):
                    try:
                        reclick_loc = _locate_tab()
                        if reclick_loc.count() > 0:
                            reclick_loc.evaluate("el => el.click()")
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
            if not dom_state.get("tabFound") and elapsed_now >= 3.2:
                final_status = "cheapest_tab_missing"
                break

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

    def scrape_slot_in_browser(
        self,
        url: str,
        gl: Optional[str] = None,
    ) -> Optional[SlotScrapeResult]:
        """
        Execute Layer 1 (`AF_initDataCallback` `ds:1` + `GetShoppingResults` RPC listener)
        and Layer 2 (Direct Cheapest DOM + Top/Best extraction) on `url`.
        If the bound proxy fails or is blocked, switches this pooled session in-place to
        a reusable direct browser so subsequent slots do not relaunch Chrome per slot.
        """
        if not self._context:
            self.start()

        try:
            return self._scrape_slot_once(url, gl=gl)
        except Exception as exc:
            if self.proxy_url is not None and _is_proxy_or_tunnel_error(exc):
                self._switch_to_direct_in_place()
                res = self._scrape_slot_once(url, gl=gl)
                if res:
                    res.extraction_source = "patchright_direct_fallback"
                return res
            raise

    def scrape_slot_direct_fallback(
        self,
        url: str,
        gl: Optional[str] = None,
    ) -> Optional[SlotScrapeResult]:
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

            tmp_session = PooledPatchrightSession(proxy_url=None, headless=HEADLESS_MODE)
            tmp_session._pw = self._pw
            tmp_session._browser = direct_browser
            tmp_session._context = direct_context
            res = tmp_session.scrape_slot_in_browser(url, gl=gl)
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
    gl: Optional[str] = None,
) -> Optional[SlotScrapeResult]:
    """
    High-level orchestrator for scraping a single `date_slot` across all 3 layers:
      1. Pooled Patchright Browser (Direct `tfu=EgoIABAAGAAgAigB` + `loaders == 0` settle + `ds:1`/RPC)
      2. If proxy fails or browser returns no offers, retry direct or fall back to
         Layer 3 (`curl_cffi` TLS-impersonated SSR `ds:1` + `fast-flights`).
    """
    start_ts = time.perf_counter()
    target_url = normalize_flights_url(url, tfu=CHEAPEST_TFU, gl=gl)
    gl_eff = (parse_qs(urlparse(target_url).query).get("gl", [DEFAULT_GL])[0] or DEFAULT_GL).strip().upper()
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
            session_to_use = PooledPatchrightSession(proxy_url=active_proxy, headless=HEADLESS_MODE).start()
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
                browser_result = session_to_use.scrape_slot_in_browser(target_url, gl=gl_eff)
                active_proxy = session_to_use.proxy_url
                if browser_result and browser_result.cheapest_tab_price > 0:
                    elapsed = time.perf_counter() - start_ts
                    loaders_t = browser_result.loaders_time_s if browser_result.loaders_time_s is not None else elapsed
                    logger.info(
                        "%sgl=%s, status=%s, loaders_time_s=%.2f, Cheapest=$%s (%s), Best=$%s, tab_hint=%s, clicks=%d, via=%s",
                        slot_tag,
                        browser_result.gl or gl_eff,
                        browser_result.cheapest_tab_status or "N/A",
                        loaders_t,
                        f"{browser_result.cheapest_tab_price:,.0f}",
                        browser_result.cheapest_airline,
                        f"{(browser_result.best_tab_price or browser_result.cheapest_tab_price):,.0f}",
                        _fmt_hint(browser_result.tab_hint_price),
                        browser_result.tab_clicks,
                        redact_proxy_for_log(active_proxy),
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
                    direct_res = session_to_use._scrape_slot_once(target_url, gl=gl_eff)
                    if direct_res and direct_res.cheapest_tab_price > 0:
                        direct_res.extraction_source = "patchright_direct_fallback"
                        elapsed = time.perf_counter() - start_ts
                        loaders_t = direct_res.loaders_time_s if direct_res.loaders_time_s is not None else elapsed
                        logger.info(
                            "%sgl=%s, status=%s, loaders_time_s=%.2f, Cheapest=$%s (%s), Best=$%s, tab_hint=%s, clicks=%d, via=direct_fallback",
                            slot_tag,
                            direct_res.gl or gl_eff,
                            direct_res.cheapest_tab_status or "N/A",
                            loaders_t,
                            f"{direct_res.cheapest_tab_price:,.0f}",
                            direct_res.cheapest_airline,
                            f"{(direct_res.best_tab_price or direct_res.cheapest_tab_price):,.0f}",
                            _fmt_hint(direct_res.tab_hint_price),
                            direct_res.tab_clicks,
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
    ssr_result = scrape_via_curl_cffi_ssr(target_url, proxy_url=tier3_proxy)
    if ssr_result is None and tier3_proxy is not None:
        ssr_result = scrape_via_curl_cffi_ssr(target_url, proxy_url=None)
    if ssr_result and ssr_result.cheapest_tab_price > 0:
        elapsed = time.perf_counter() - start_ts
        logger.info(
            "%sgl=%s, status=tier3_curl_cffi, loaders_time_s=%.2f, Cheapest=$%s, Best=$%s, tab_hint=N/A",
            slot_tag,
            gl_eff,
            elapsed,
            f"{ssr_result.cheapest_tab_price:,.0f}",
            f"{(ssr_result.best_tab_price or ssr_result.cheapest_tab_price):,.0f}",
        )
        return ssr_result

    # Tier 3B: fast-flights / primp fallback (respects active channel; never retries a failed proxy)
    ff_result = scrape_via_fast_flights_fallback(target_url, proxy_url=tier3_proxy)
    if ff_result is None and tier3_proxy is not None:
        ff_result = scrape_via_fast_flights_fallback(target_url, proxy_url=None)
    if ff_result and ff_result.cheapest_tab_price > 0:
        elapsed = time.perf_counter() - start_ts
        logger.info(
            "%sgl=%s, status=tier3_fast_flights, loaders_time_s=%.2f, Cheapest=$%s, Best=$%s, tab_hint=N/A",
            slot_tag,
            gl_eff,
            elapsed,
            f"{ff_result.cheapest_tab_price:,.0f}",
            f"{(ff_result.best_tab_price or ff_result.cheapest_tab_price):,.0f}",
        )
        return ff_result

    logger.error("%sAll 3 extraction layers failed to extract prices for URL: %s", slot_tag, url[:100])
    return None


_BASE_SCRAPE_DATE_SLOT = scrape_date_slot


def _effective_result_price(res: Optional[SlotScrapeResult]) -> Optional[float]:
    """Return positive `cheapest_tab_price or best_tab_price` for Multi-GL comparison."""
    if res is None:
        return None
    if res.cheapest_tab_price is not None and res.cheapest_tab_price > 0:
        return float(res.cheapest_tab_price)
    if res.best_tab_price is not None and res.best_tab_price > 0:
        return float(res.best_tab_price)
    return None


def scrape_date_slot_multi_gl(
    url: str,
    gl_pool: Optional[Sequence[str] | str] = None,
    pooled_session: Optional[PooledPatchrightSession] = None,
    proxy_url: Optional[str] = None,
    date_range_label: str = "",
    inter_gl_delay: tuple[float, float] = (1.0, 1.8),
) -> Optional[SlotScrapeResult]:
    """
    Compete a `date_slot` across all markets in `gl_pool` (deduplicated, in order,
    default `("MX", "IT", "DE")`) in RAM, reusing a single `PooledPatchrightSession`.

    Selection rules:
      - Evaluates `effective_price = res.cheapest_tab_price or res.best_tab_price` (> 0).
      - On exact price tie, favors the first market in `gl_pool` order (e.g. MX wins ties).
      - Tolerates individual `gl` failures as long as at least one `gl` returns a valid offer.
      - Returns ONLY the winning `SlotScrapeResult` with `winning_gl` populated and logs
        one compact comparison line.
    """
    start_ts = time.perf_counter()
    markets = parse_gl_pool(gl_pool, fallback=DEFAULT_GL_POOL)
    slot_tag = f"[{date_range_label}] " if date_range_label else ""

    session_to_use = pooled_session
    own_session = False
    active_proxy = session_to_use.proxy_url if session_to_use is not None else proxy_url

    if session_to_use is None and scrape_date_slot is _BASE_SCRAPE_DATE_SLOT:
        try:
            session_to_use = PooledPatchrightSession(proxy_url=active_proxy, headless=HEADLESS_MODE).start()
            own_session = True
            active_proxy = session_to_use.proxy_url
        except Exception as init_exc:
            logger.debug(
                "%sMulti-GL could not start pooled browser session on %s: %s",
                slot_tag,
                redact_proxy_for_log(active_proxy),
                sanitize_log_text(init_exc),
            )
            session_to_use = None

    candidates: list[tuple[int, str, float, SlotScrapeResult]] = []
    summary_parts: list[str] = []

    try:
        for idx, gl_code in enumerate(markets):
            res: Optional[SlotScrapeResult] = None
            try:
                res = scrape_date_slot(
                    url=url,
                    pooled_session=session_to_use,
                    proxy_url=active_proxy,
                    date_range_label=date_range_label,
                    gl=gl_code,
                )
                if session_to_use is not None:
                    active_proxy = session_to_use.proxy_url
            except Exception as exc:
                logger.warning(
                    "%sMulti-GL gl=%s failed (%s); continuing pool.",
                    slot_tag,
                    gl_code,
                    sanitize_log_text(exc),
                )

            eff_price = _effective_result_price(res)
            if res is not None and eff_price is not None and eff_price > 0:
                candidates.append((idx, gl_code, eff_price, res))
                summary_parts.append(f"{gl_code}=${eff_price:,.0f}")
            else:
                summary_parts.append(f"{gl_code}=ERR")

            if idx < len(markets) - 1 and inter_gl_delay:
                lo, hi = inter_gl_delay
                if hi > 0:
                    time.sleep(random.uniform(max(0.0, lo), max(lo, hi)))
    finally:
        if own_session and session_to_use is not None:
            try:
                session_to_use.close()
            except Exception:
                pass

    # Cross-market anti-currency-leak safety net: discard anomalous low outliers (< 0.30 * median of others)
    while len(candidates) >= 2:
        min_item = min(candidates, key=lambda item: (item[2], item[0]))
        other_prices = sorted(item[2] for item in candidates if item is not min_item)
        n_other = len(other_prices)
        mid = n_other // 2
        others_median = (
            other_prices[mid]
            if n_other % 2 == 1
            else (other_prices[mid - 1] + other_prices[mid]) / 2.0
        )
        if others_median > 0 and min_item[2] < 0.30 * others_median:
            logger.warning(
                "%smulti_gl_currency_outlier_rejected: gl=%s price=$%s < 0.30 * median_others($%s); discarding candidate.",
                slot_tag,
                min_item[1],
                f"{min_item[2]:,.0f}",
                f"{others_median:,.0f}",
            )
            candidates.remove(min_item)
            summary_parts = [
                f"{min_item[1]}=REJECTED_OUTLIER" if p.startswith(f"{min_item[1]}=") else p
                for p in summary_parts
            ]
        else:
            break

    elapsed = time.perf_counter() - start_ts
    if not candidates:
        logger.warning(
            "%smulti_gl: %s -> no valid offers in %.1fs",
            slot_tag,
            " | ".join(summary_parts),
            elapsed,
        )
        return None

    _, winner_gl, winner_price, winner_res = min(candidates, key=lambda item: (item[2], item[0]))
    winner_res.winning_gl = winner_gl
    winner_res.gl = winner_gl
    best_val = (
        float(winner_res.best_tab_price)
        if (winner_res.best_tab_price is not None and winner_res.best_tab_price > 0)
        else winner_price
    )
    logger.info(
        "%smulti_gl: %s -> winner=%s ($%s, Best=$%s) in %.1fs",
        slot_tag,
        " | ".join(summary_parts),
        winner_gl,
        f"{winner_price:,.0f}",
        f"{best_val:,.0f}",
        elapsed,
    )
    return winner_res
