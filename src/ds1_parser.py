"""
Layer 1 (`AF_initDataCallback` `ds:1` & `GetShoppingResults` RPC) and
Semantic ARIA/DOM Flight Offer Parser for FlightPulse Matrix v4.0.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

from src.config import SELF_TRANSFER_KEYWORDS

logger = logging.getLogger("flightpulse.ds1_parser")


@dataclass
class FlightOffer:
    """Normalized flight offer extracted from ds:1, GetShoppingResults RPC, or DOM/ARIA."""

    price: float
    airline: str = "Aerolínea"
    stops: str = "Directo"
    duration: str = "N/A"
    dep_arr: str = ""
    origin_airport: Optional[str] = None
    destination_airport: Optional[str] = None
    is_self_transfer: bool = False
    tab_source: str = "best"  # "best" or "cheapest"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class DualTabExtraction:
    """Combined Best Tab + Cheapest Tab extraction result."""

    cheapest_offer: FlightOffer
    best_offer: Optional[FlightOffer] = None
    top_offers: list[FlightOffer] = field(default_factory=list)
    extraction_source: str = "patchright_ds1"


# ==============================================================================
# SELF-TRANSFER & FORMATTING HELPERS
# ==============================================================================


def detect_self_transfer(text: str) -> bool:
    """
    Detect self-transfer / separate tickets warnings in Spanish or English.
    """
    if not text:
        return False
    lowered = text.lower()
    return any(keyword in lowered for keyword in SELF_TRANSFER_KEYWORDS)


def format_minutes_to_duration(total_minutes: int | float) -> str:
    """Convert total duration in minutes (e.g. 645) into '10 h 45 min'."""
    mins = int(round(float(total_minutes)))
    if mins <= 0:
        return "N/A"
    hours, rem = divmod(mins, 60)
    if hours > 0 and rem > 0:
        return f"{hours} h {rem} min"
    if hours > 0:
        return f"{hours} h"
    return f"{rem} min"


def format_stops_label(stop_count: int) -> str:
    """Format numeric stop count into Spanish label."""
    if stop_count <= 0:
        return "Directo"
    if stop_count == 1:
        return "1 escala"
    return f"{stop_count} escalas"


def _safe_get(obj: Any, *indices: int) -> Any:
    """Safely traverse nested lists by integer indices without raising IndexError/TypeError."""
    curr = obj
    for idx in indices:
        if isinstance(curr, list) and -len(curr) <= idx < len(curr):
            curr = curr[idx]
        else:
            return None
    return curr


# ==============================================================================
# BALANCED JSON EXTRACTOR FOR AF_initDataCallback({key: 'ds:1', ...})
# ==============================================================================


def extract_ds1_json_from_html(html: str) -> Optional[list[Any]]:
    """
    Locate `AF_initDataCallback({key: 'ds:1'` in Google Flights SSR HTML and
    extract the `data: [...]` JSON array using balanced bracket scanning.
    """
    if not html:
        return None

    # Match key: 'ds:1' or key:"ds:1"
    pattern = re.compile(r"AF_initDataCallback\s*\(\s*\{[^}]*?key\s*:\s*['\"]ds:1['\"]", re.DOTALL)
    match = pattern.search(html)
    if not match:
        return None

    search_start = match.end()
    data_marker = re.search(r"data\s*:\s*\[", html[search_start : search_start + 1500])
    if not data_marker:
        return None

    bracket_start = search_start + data_marker.end() - 1
    depth = 0
    in_string = False
    escape = False
    quote_char = ""

    n = len(html)
    for pos in range(bracket_start, n):
        ch = html[pos]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == quote_char:
                in_string = False
        else:
            if ch in ('"', "'"):
                in_string = True
                quote_char = ch
            elif ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
                if depth == 0:
                    raw_json = html[bracket_start : pos + 1]
                    try:
                        parsed = json.loads(raw_json)
                        if isinstance(parsed, list):
                            return parsed
                    except json.JSONDecodeError:
                        return None
                    break
    return None


# ==============================================================================
# STRUCTURAL ITINERARY PARSER FOR GOOGLE FLIGHTS ds:1 & RPC PAYLOADS
# ==============================================================================


def _extract_price_from_itinerary_node(node: list[Any]) -> Optional[float]:
    """
    Extract ticket price from a Google Flights itinerary entry inside `ds:1` or RPC.
    In Google Flights `ds:1`, an outer itinerary entry is typically `[itinerary_details, price_info, ...]`:
      - `node[1][0][1]` -> integer price in local currency (e.g., 14520)
      - `node[1][0][0]` or `node[0][22]` -> fallback price locations
    """
    # Primary location: node[1][0][1]
    candidates = [
        _safe_get(node, 1, 0, 1),
        _safe_get(node, 1, 1),
        _safe_get(node, 1, 0, 0),
        _safe_get(node, 22, 0, 1),
    ]
    for cand in candidates:
        if isinstance(cand, (int, float)) and 10 < cand < 2_000_000:
            return float(cand)

    # Secondary scan inside node[1] for a `[currency_or_null, amount]` pair
    price_block = _safe_get(node, 1)
    if isinstance(price_block, list):
        serialized = json.dumps(price_block)
        nums = [
            float(m)
            for m in re.findall(r"\b(\d{3,7})\b", serialized)
            if 50 <= float(m) <= 1_500_000
        ]
        if nums:
            return nums[0]

    return None


def _format_time_tuple(t_list: Any) -> str:
    """Format `[hour, minute]` or `[hour]` from Google Flights array into `HH:MM`."""
    if not isinstance(t_list, list) or not t_list:
        return ""
    h = int(t_list[0]) if len(t_list) >= 1 and isinstance(t_list[0], (int, float)) else 0
    m = int(t_list[1]) if len(t_list) >= 2 and isinstance(t_list[1], (int, float)) else 0
    return f"{h:02d}:{m:02d}"


def _parse_single_itinerary_entry(entry: Any, tab_source: str = "best") -> Optional[FlightOffer]:
    """
    Parse a single Google Flights itinerary array entry into a `FlightOffer`.
    """
    if not isinstance(entry, list) or len(entry) < 2:
        return None

    price = _extract_price_from_itinerary_node(entry)
    if price is None or price <= 0:
        return None

    core = entry[0] if isinstance(entry[0], list) else entry

    # Extract airline(s): typically core[1] is `["Iberia"]` or core[0] is airline code
    airline_names: list[str] = []
    raw_airlines = _safe_get(core, 1)
    if isinstance(raw_airlines, list):
        for item in raw_airlines:
            if isinstance(item, str) and item.strip():
                airline_names.append(item.strip())

    # Extract flight legs: typically core[2] is a list of leg arrays
    legs = _safe_get(core, 2)
    if not isinstance(legs, list):
        legs = []

    if not airline_names and legs:
        for leg in legs:
            leg_airline = _safe_get(leg, 22, 3) or _safe_get(leg, 22, 0)
            if isinstance(leg_airline, str) and leg_airline.strip() and leg_airline not in airline_names:
                airline_names.append(leg_airline.strip())

    airline_str = ", ".join(airline_names[:2]) if airline_names else "Aerolínea"

    # Extract total duration in minutes: typically core[9] is integer minutes
    raw_duration = _safe_get(core, 9)
    if not isinstance(raw_duration, (int, float)) or raw_duration <= 0:
        # Sum leg durations if core[9] is unavailable
        leg_mins = [
            _safe_get(leg, 11)
            for leg in legs
            if isinstance(_safe_get(leg, 11), (int, float))
        ]
        raw_duration = sum(leg_mins) if leg_mins else 0
    duration_str = format_minutes_to_duration(raw_duration) if raw_duration else "N/A"

    # Stops count: len(legs) - 1
    stop_count = max(0, len(legs) - 1) if legs else 0
    stops_str = format_stops_label(stop_count)

    # Departure & arrival time string and winning IATA airport codes from first and last leg
    dep_arr_str = ""
    dep_iata_clean: Optional[str] = None
    arr_iata_clean: Optional[str] = None
    if legs:
        first_leg = legs[0]
        last_leg = legs[-1]
        dep_time = _format_time_tuple(_safe_get(first_leg, 8))
        arr_time = _format_time_tuple(_safe_get(last_leg, 10))
        raw_dep_iata = _safe_get(first_leg, 3) or _safe_get(core, 3) or ""
        raw_arr_iata = _safe_get(last_leg, 6) or _safe_get(core, 6) or ""
        if isinstance(raw_dep_iata, str) and re.match(r"^[A-Z]{3}$", raw_dep_iata.strip().upper()):
            dep_iata_clean = raw_dep_iata.strip().upper()
        if isinstance(raw_arr_iata, str) and re.match(r"^[A-Z]{3}$", raw_arr_iata.strip().upper()):
            arr_iata_clean = raw_arr_iata.strip().upper()
        if dep_time and arr_time:
            route_suffix = (
                f" ({dep_iata_clean}-{arr_iata_clean})"
                if dep_iata_clean and arr_iata_clean
                else ""
            )
            dep_arr_str = f"{dep_time} - {arr_time}{route_suffix}"

    # Check self-transfer indicators in entry JSON dump
    serialized_entry = json.dumps(entry, ensure_ascii=False)
    is_self_transfer = detect_self_transfer(serialized_entry)

    return FlightOffer(
        price=round(float(price), 2),
        airline=airline_str,
        stops=stops_str,
        duration=duration_str,
        dep_arr=dep_arr_str,
        origin_airport=dep_iata_clean,
        destination_airport=arr_iata_clean,
        is_self_transfer=is_self_transfer,
        tab_source=tab_source,
    )


def parse_ds1_data_array(data: list[Any], default_tab: str = "best") -> list[FlightOffer]:
    """
    Extract all FlightOffer items from a parsed `ds:1` or `GetShoppingResults` JSON array.
    Checks standard Google Flights indices (`data[2][0]` for Best flights and `data[3][0]`
    for Other/Cheapest flights) and falls back to recursive structural walking.
    """
    offers: list[FlightOffer] = []

    # 1. Standard Top / "Best" flights bucket: data[2][0]
    best_bucket = _safe_get(data, 2, 0)
    if isinstance(best_bucket, list):
        for item in best_bucket:
            offer = _parse_single_itinerary_entry(item, tab_source="best")
            if offer:
                offers.append(offer)

    # 2. Standard "Other" / All flights bucket: data[3][0]
    other_bucket = _safe_get(data, 3, 0)
    if isinstance(other_bucket, list):
        for item in other_bucket:
            offer = _parse_single_itinerary_entry(item, tab_source="cheapest")
            if offer:
                offers.append(offer)

    # 3. Structural fallback walk if standard buckets were empty or relocated
    if not offers:
        def _walk(node: Any, depth: int = 0) -> None:
            if depth > 7 or not isinstance(node, list):
                return
            offer = _parse_single_itinerary_entry(node, tab_source=default_tab)
            if offer and offer.airline != "Aerolínea":
                offers.append(offer)
                return
            for child in node:
                if isinstance(child, list):
                    _walk(child, depth + 1)

        _walk(data)

    return offers


def parse_ds1_from_html(html: str) -> Optional[DualTabExtraction]:
    """
    Parse Google Flights SSR HTML `AF_initDataCallback({key: 'ds:1'})` payload
    into a `DualTabExtraction` containing both the Best Tab offer and the Cheapest offer.
    """
    ds1_array = extract_ds1_json_from_html(html)
    if not ds1_array:
        return None

    offers = parse_ds1_data_array(ds1_array)
    if not offers:
        return None

    best_candidates = [o for o in offers if o.tab_source == "best"]
    best_offer = best_candidates[0] if best_candidates else offers[0]

    sorted_by_price = sorted(offers, key=lambda o: o.price)
    cheapest_offer = sorted_by_price[0]

    return DualTabExtraction(
        cheapest_offer=cheapest_offer,
        best_offer=best_offer,
        top_offers=sorted_by_price[:5],
        extraction_source="patchright_ds1",
    )


def parse_shopping_rpc_response(raw_body: str) -> list[FlightOffer]:
    """
    Parse a `FlightsFrontendService/GetShoppingResults` XHR response body captured
    via `page.on("response", ...)` when clicking the "Cheapest" (`Más baratos`) tab.
    """
    if not raw_body:
        return []

    cleaned = raw_body.strip()
    if cleaned.startswith(")]}'"):
        cleaned = cleaned[4:].strip()

    offers: list[FlightOffer] = []
    # batchexecute streaming responses can have length prefixes between lines
    for line in cleaned.splitlines():
        line_str = line.strip()
        if not line_str or not line_str.startswith("["):
            continue
        try:
            outer = json.loads(line_str)
        except json.JSONDecodeError:
            continue

        if not isinstance(outer, list):
            continue

        for entry in outer:
            if isinstance(entry, list) and len(entry) >= 3 and entry[0] == "wrb.fr":
                inner_json_str = entry[2]
                if isinstance(inner_json_str, str):
                    try:
                        inner_data = json.loads(inner_json_str)
                        if isinstance(inner_data, list):
                            offers.extend(parse_ds1_data_array(inner_data, default_tab="cheapest"))
                    except json.JSONDecodeError:
                        continue

    return offers


# ==============================================================================
# LAYER 2: SEMANTIC ARIA-LABEL & DOM FLIGHT CARD PARSER
# ==============================================================================


PRICE_PATTERNS = (
    # Explicit currency prefix ($14,520, MX$14,520, Desde 14,520)
    re.compile(
        r"(?:MX\$|US\$|\$|EUR|€|Desde\s+)\s*(\d{1,3}(?:[.,\s\u00a0]\d{3})+|\d{3,7})(?!\s*(?:kg|CO2|g\b|árboles|h\b|hr|min|m\b|escalas))",
        re.IGNORECASE,
    ),
    # Explicit currency suffix (14,520 MXN, 31.295 MXN, 14520 pesos)
    re.compile(
        r"(?<!\d)(\d{1,3}(?:[.,\s\u00a0]\d{3})+|\d{3,7})\s*(?:MXN|USD|EUR|pesos|dólares)",
        re.IGNORECASE,
    ),
    # Formatted thousands with comma or dot (31.295, 14,520) not followed by non-price units
    re.compile(
        r"(?<!\d)(\d{1,3}(?:[.,]\d{3})+)(?!\s*(?:kg|CO2|g\b|árboles|h\b|hr|min|m\b|escalas|stops))",
        re.IGNORECASE,
    ),
)

DURATION_PATTERN = re.compile(
    r"(\d{1,2}\s*(?:h|hr|hrs|horas?)\s*(?:\d{1,2}\s*(?:m|min|minutos?))?|\d{1,2}\s*(?:m|min|minutos?))",
    re.IGNORECASE,
)

STOPS_PATTERN = re.compile(
    r"(Directo|Sin escalas|Nonstop|\d+\s*escalas?|\d+\s*stops?)",
    re.IGNORECASE,
)

TIME_RANGE_PATTERN = re.compile(
    r"(\d{1,2}:\d{2}\s*(?:a\.?\s*m\.?|p\.?\s*m\.?|AM|PM)?\s*[-–—a]\s*\d{1,2}:\d{2}\s*(?:a\.?\s*m\.?|p\.?\s*m\.?|AM|PM)?(?:\+\d)?)",
    re.IGNORECASE,
)

AIRLINE_ARIA_PATTERNS = (
    # Spanish Google Flights aria-label: "Vuelo con Iberia." or "con Aeroméxico."
    re.compile(r"(?:Vuelo\s+con|Operado\s+por|con)\s+([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s,&/-]{2,40}?)(?:\.|,|;|\s+Sale|\s+Directo|\s+\d+\s+escala)", re.IGNORECASE),
    # English Google Flights aria-label: "Flight with Iberia." or "with American Airlines."
    re.compile(r"(?:Flight\s+with|with)\s+([A-Za-z0-9\s,&/-]{2,40}?)(?:\.|,|;|\s+Leaves|\s+Nonstop|\s+\d+\s+stop)", re.IGNORECASE),
)

NON_AIRPORT_THREE_LETTER_TOKENS: set[str] = {
    "MXN", "USD", "EUR", "CAD", "COP", "GBP", "JPY", "BRL", "CLP", "ARS", "PEN",
    "HRS", "MIN", "SEG", " NON", "DIR", "CON", "POR", "DEL", "LAS", "LOS", "UNA",
    "DOS", "MAS", "MÁS", "VER", "IDA", "MES", "DIA", "DÍA", "AÑO", "UTC", "GMT",
}

AIRPORT_NAME_FALLBACK_KEYWORDS: tuple[tuple[str, str], ...] = (
    ("felipe ángeles", "NLU"),
    ("felipe angeles", "NLU"),
    ("santa lucía", "NLU"),
    ("benito juárez", "MEX"),
    ("benito juarez", "MEX"),
    ("toluca", "TLC"),
    ("narita", "NRT"),
    ("haneda", "HND"),
    ("barajas", "MAD"),
    ("el prat", "BCN"),
    ("charles de gaulle", "CDG"),
    ("orly", "ORY"),
    ("heathrow", "LHR"),
    ("gatwick", "LGW"),
    ("stansted", "STN"),
    ("luton", "LTN"),
    ("john f. kennedy", "JFK"),
    ("newark", "EWR"),
    ("laguardia", "LGA"),
    ("fiumicino", "FCO"),
    ("ciampino", "CIA"),
    ("malpensa", "MXP"),
    ("linate", "LIN"),
    ("incheon", "ICN"),
    ("gimpo", "GMP"),
    ("ezeiza", "EZE"),
    ("aeroparque", "AEP"),
    ("guarulhos", "GRU"),
    ("congonhas", "CGH"),
    ("viracopos", "VCP"),
)

ROUTE_HYPHEN_RE = re.compile(r"\b([A-Z]{3})\s*[-–—➔→]\s*([A-Z]{3})\b")


def extract_route_iatas_from_card(
    aria_label: str,
    inner_text: str,
    dep_arr: str = "",
) -> tuple[Optional[str], Optional[str]]:
    """
    Extract the specific departure and arrival IATA airport codes (`origin_airport`, `destination_airport`)
    from a Google Flights DOM card (`inner_text` and `aria-label`).
    """
    # 1. Explicit hyphenated pair in inner_text or dep_arr (e.g. "NLU–NRT", "MEX - HND")
    for source_text in (inner_text or "", dep_arr or "", aria_label or ""):
        for match in ROUTE_HYPHEN_RE.finditer(source_text):
            c1, c2 = match.group(1).upper(), match.group(2).upper()
            if c1 not in NON_AIRPORT_THREE_LETTER_TOKENS and c2 not in NON_AIRPORT_THREE_LETTER_TOKENS:
                return c1, c2

    # 2. Standalone 3-letter IATA lines in inner_text (when Google Flights renders multi-airport highlight spans)
    if inner_text:
        lines = [ln.strip() for ln in inner_text.splitlines() if ln.strip()]
        standalone_iatas: list[str] = []
        for ln in lines:
            if re.match(r"^[A-Z]{3}$", ln) and ln not in NON_AIRPORT_THREE_LETTER_TOKENS:
                standalone_iatas.append(ln)
        if len(standalone_iatas) >= 2:
            return standalone_iatas[0], standalone_iatas[-1]

    # 3. Fallback: Airport full names in Spanish/English aria-label (e.g. "Sale del Aeropuerto... Felipe Ángeles y llega al... Narita")
    if aria_label:
        lower_aria = aria_label.lower()
        found_by_pos: list[tuple[int, str]] = []
        for keyword, iata_code in AIRPORT_NAME_FALLBACK_KEYWORDS:
            pos = lower_aria.find(keyword)
            if pos != -1:
                found_by_pos.append((pos, iata_code))
        found_by_pos.sort(key=lambda x: x[0])
        dedup_iatas: list[str] = []
        for _, code in found_by_pos:
            if code not in dedup_iatas:
                dedup_iatas.append(code)
        if len(dedup_iatas) >= 2:
            return dedup_iatas[0], dedup_iatas[1]

    return None, None


def parse_price_number(raw_price_text: str) -> Optional[float]:
    """
    Parse a price string such as `"$14,520 MXN"`, `"14.520"`, or `"Desde 14,520 pesos mexicanos"`
    into a clean float (`14520.0`).
    """
    if not raw_price_text:
        return None

    for pattern in PRICE_PATTERNS:
        for match in pattern.finditer(raw_price_text):
            num_str = match.group(1).strip()
            # Remove thousands separators (comma, dot when followed by 3 digits, or thin space)
            normalized = re.sub(r"[,\s\u00a0]", "", num_str)
            if re.match(r"^\d{1,3}(\.\d{3})+$", normalized):
                normalized = normalized.replace(".", "")
            try:
                val = float(normalized)
                # Filter out flight numbers or hours/years that aren't airfare amounts
                if 150 <= val <= 1_500_000:
                    return round(val, 2)
            except ValueError:
                continue
    return None


def parse_flight_card_text(
    aria_label: str,
    inner_text: str,
    price_override: Optional[float] = None,
    tab_source: str = "best",
) -> Optional[FlightOffer]:
    """
    Parse a Google Flights DOM flight card (`li.pIav2d` or `[role="listitem"]`)
    combining its semantic `aria-label` and visible `inner_text`.
    """
    combined = f"{aria_label or ''} | {inner_text or ''}".strip()
    if not combined or combined == "|":
        return None

    price = price_override or parse_price_number(aria_label) or parse_price_number(inner_text)
    if price is None or price <= 0:
        return None

    # Stops
    stops_match = STOPS_PATTERN.search(combined)
    if stops_match:
        raw_stops = stops_match.group(1).strip()
        if raw_stops.lower() in ("sin escalas", "nonstop", "directo"):
            stops = "Directo"
        else:
            stops = raw_stops
    else:
        stops = "Directo"

    # Duration
    dur_match = DURATION_PATTERN.search(combined)
    duration = dur_match.group(1).strip() if dur_match else "N/A"

    # Departure -> Arrival time
    time_match = TIME_RANGE_PATTERN.search(combined)
    dep_arr = time_match.group(1).strip() if time_match else ""

    # Airline extraction
    airline = ""
    for pat in AIRLINE_ARIA_PATTERNS:
        m = pat.search(aria_label or "")
        if m:
            candidate = m.group(1).strip(" .,;-")
            if candidate and not any(k in candidate.lower() for k in ("escala", "stop", "pesos", "mxn")):
                airline = candidate
                break

    if not airline and inner_text:
        # Look for airline line in visible inner_text (typically the line right after departure/arrival times)
        lines = [ln.strip() for ln in inner_text.splitlines() if ln.strip()]
        for ln in lines:
            if len(ln) < 2:
                continue
            # Strip codeshare / operator suffixes e.g. "Iberia, FinnairOperado por ..."
            cleaned_airline = re.split(r"(?:Operado por|Operated by)\b", ln, flags=re.I)[0].strip(" .,;-")
            if len(cleaned_airline) < 2 or len(cleaned_airline) > 65:
                continue
            if re.search(
                r"\d{1,2}:\d{2}|\$|MXN|USD|EUR|Directo|escala|stop|\d+\s*h\b|CO2|emisiones|árboles|ida y vuelta|round trip",
                cleaned_airline,
                re.I,
            ):
                continue
            if re.match(r"^[A-Z]{3}\s*[-–—➔→]\s*[A-Z]{3}$", cleaned_airline):
                continue
            # Skip comma-separated lists of 3-letter layover airport codes (e.g. "MAD, HEL" or "DFW, HEL, RVN")
            if re.match(r"^[A-Z]{3}(?:\s*,\s*[A-Z]{3})+$", cleaned_airline):
                continue
            airline = cleaned_airline
            break

    if not airline:
        airline = "Aerolínea"

    orig_iata, dest_iata = extract_route_iatas_from_card(
        aria_label=aria_label,
        inner_text=inner_text,
        dep_arr=dep_arr,
    )
    if orig_iata and dest_iata and dep_arr and f"({orig_iata}-{dest_iata})" not in dep_arr:
        dep_arr = f"{dep_arr} ({orig_iata}-{dest_iata})"

    is_self_transfer = detect_self_transfer(combined)

    return FlightOffer(
        price=price,
        airline=airline[:60],
        stops=stops[:30],
        duration=duration[:30],
        dep_arr=dep_arr[:50],
        origin_airport=orig_iata,
        destination_airport=dest_iata,
        is_self_transfer=is_self_transfer,
        tab_source=tab_source,
    )
