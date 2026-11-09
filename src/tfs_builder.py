"""
Protobuf `?tfs=` Multi-Date URL Generator & Decoder for FlightPulse Matrix v4.0.

Supports:
  - Mode A (Batch URL Paste): Normalizes Google Flights URLs, locks locale/currency
    (`&curr=MXN&gl=MX&hl=es`), and decodes embedded `?tfs=` Protobuf bytes to extract
    origin, destination, outbound_date, and return_date automatically.
  - Mode B (Filter Profile + Multi-Date Matrix Generator): Deterministic pure-Python
    Protobuf wire encoder (+ `fast-flights` helper integration) supporting Origin,
    Destination, Outbound/Return Dates, Trip Type, Cabin Class, Adults, Max Stops,
    Allowed Airlines (IATA), Outbound/Return Departure & Arrival Hour Windows (0-23),
    and Carry-on Bags.
"""

from __future__ import annotations

import base64
import logging
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Optional, Sequence
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from src.config import DEFAULT_CURRENCY, DEFAULT_GL, DEFAULT_HL, MAX_SLOTS_PER_DESTINATION

__all__ = [
    "FilterProfile",
    "METRO_IATA_EXPANSION_MAP",
    "parse_iata_list",
    "normalize_iata_codes",
    "format_corridor_display",
    "encode_tfs_protobuf",
    "build_google_flights_url",
    "decode_tfs_parameter",
    "normalize_google_flights_url",
    "format_date_slot_label",
    "parse_pasted_urls",
    "generate_date_matrix",
]

logger = logging.getLogger("flightpulse.tfs_builder")

# Deterministic expansion of Metropolitan Area IATA codes into physical airport IATAs
METRO_IATA_EXPANSION_MAP: dict[str, tuple[str, ...]] = {
    "TYO": ("NRT", "HND"),
    "LON": ("LHR", "LGW", "STN", "LTN", "LCY"),
    "NYC": ("JFK", "EWR", "LGA"),
    "PAR": ("CDG", "ORY"),
    "ROM": ("FCO", "CIA"),
    "MIL": ("MXP", "LIN", "BGY"),
    "CHI": ("ORD", "MDW"),
    "WAS": ("IAD", "DCA", "BWI"),
    "BUE": ("EZE", "AEP"),
    "SAO": ("GRU", "CGH", "VCP"),
    "RIO": ("GIG", "SDU"),
    "SEL": ("ICN", "GMP"),
    "YTO": ("YYZ", "YTZ"),
    "BJS": ("PEK", "PKX"),
    "OSA": ("KIX", "ITM"),
}

IATA_TOKEN_RE = re.compile(r"^[A-Z]{3}$")


def parse_iata_list(
    raw_value: str | Sequence[str],
    expand_metro: bool = True,
    max_airports: int = 7,
) -> list[str]:
    """
    Parse, validate, deduplicate, and optionally expand metro codes from a comma/space/slash
    delimited string or sequence of IATA codes.
    Raises ValueError if no valid 3-letter IATA codes are provided or if any token is invalid.
    """
    if isinstance(raw_value, str):
        raw_tokens = [tok.strip().upper() for tok in re.split(r"[,;/\s]+", raw_value) if tok.strip()]
    elif isinstance(raw_value, Sequence):
        raw_tokens = []
        for item in raw_value:
            for tok in re.split(r"[,;/\s]+", str(item)):
                if tok.strip():
                    raw_tokens.append(tok.strip().upper())
    else:
        raw_tokens = []

    if not raw_tokens:
        raise ValueError("Se requiere al menos un código IATA de 3 letras (ej. MEX o MEX, NLU).")

    resolved: list[str] = []
    for token in raw_tokens:
        if not IATA_TOKEN_RE.match(token):
            raise ValueError(
                f"Código IATA inválido '{token}'. Cada aeropuerto debe tener exactamente 3 letras (ej. MEX, NLU, NRT)."
            )
        expanded = METRO_IATA_EXPANSION_MAP.get(token, (token,)) if expand_metro else (token,)
        for code in expanded:
            if code not in resolved:
                resolved.append(code)

    return resolved[:max_airports]


def normalize_iata_codes(
    raw_value: str | Sequence[str],
    expand_metro: bool = True,
    max_airports: int = 7,
) -> str:
    """
    Return the canonical comma-delimited string representation (without spaces)
    for storage in `destinations.origin` and `destinations.destination`, e.g. 'MEX,NLU'.
    """
    return ",".join(parse_iata_list(raw_value, expand_metro=expand_metro, max_airports=max_airports))


def format_corridor_display(
    origin: str | Sequence[str],
    destination: str | Sequence[str],
    arrow: str = "➔",
) -> str:
    """
    Format an N:M airport corridor for UI and Telegram headers:
      Single: 'MEX ➔ MAD'
      Multi:  '[MEX, NLU] ➔ [NRT, HND]'
    """
    try:
        orig_list = parse_iata_list(origin, expand_metro=False)
    except ValueError:
        orig_list = [str(origin).strip().upper() or "MEX"]

    try:
        dest_list = parse_iata_list(destination, expand_metro=False)
    except ValueError:
        dest_list = [str(destination).strip().upper() or "MAD"]

    is_multi = len(orig_list) > 1 or len(dest_list) > 1
    if is_multi:
        return f"[{', '.join(orig_list)}] {arrow} [{', '.join(dest_list)}]"
    return f"{orig_list[0]} {arrow} {dest_list[0]}"

SPANISH_WEEKDAYS = ("Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom")
SPANISH_MONTHS = (
    "",
    "Ene",
    "Feb",
    "Mar",
    "Abr",
    "May",
    "Jun",
    "Jul",
    "Ago",
    "Sep",
    "Oct",
    "Nov",
    "Dic",
)

CABIN_MAP: dict[str, int] = {
    "economy": 1,
    "premium-economy": 2,
    "premium_economy": 2,
    "business": 3,
    "first": 4,
}

TRIP_TYPE_MAP: dict[str, int] = {
    "round-trip": 1,
    "round_trip": 1,
    "roundtrip": 1,
    "one-way": 2,
    "one_way": 2,
    "oneway": 2,
}


@dataclass
class FilterProfile:
    """
    Represents the filter configuration stored in `destinations.filter_config` JSONB
    and encoded into Google Flights `?tfs=` Protobuf parameters.
    """

    max_stops: Optional[int] = None  # None = Any, 0 = Nonstop, 1 = Max 1 stop, 2 = Max 2 stops
    airlines: list[str] = field(default_factory=list)  # e.g., ["IB", "AM"]
    dep_start_hour: int = 0  # 0..23
    dep_end_hour: int = 23  # 0..23 (23 means no upper bound / end of day)
    arr_start_hour: int = 0  # 0..23
    arr_end_hour: int = 23  # 0..23
    ret_dep_start_hour: int = 0  # 0..23
    ret_dep_end_hour: int = 23  # 0..23
    ret_arr_start_hour: int = 0  # 0..23
    ret_arr_end_hour: int = 23  # 0..23
    carry_on_bags: int = 0  # 0, 1, or 2
    seat: str = "economy"  # economy, premium-economy, business, first
    adults: int = 1  # 1..9
    hide_self_transfer: bool = False

    @classmethod
    def from_dict(cls, data: Optional[dict[str, Any]]) -> "FilterProfile":
        if not data:
            return cls()
        raw_stops = data.get("max_stops")
        max_stops: Optional[int] = None
        if raw_stops is not None and str(raw_stops).strip() not in ("", "any", "None", "-1"):
            try:
                val = int(raw_stops)
                if val in (0, 1, 2):
                    max_stops = val
            except (ValueError, TypeError):
                max_stops = None

        raw_airlines = data.get("airlines") or []
        if isinstance(raw_airlines, str):
            airlines = [
                a.strip().upper()
                for a in raw_airlines.split(",")
                if a.strip() and len(a.strip()) >= 2
            ]
        else:
            airlines = [
                str(a).strip().upper()
                for a in raw_airlines
                if str(a).strip() and len(str(a).strip()) >= 2
            ]

        return cls(
            max_stops=max_stops,
            airlines=airlines,
            dep_start_hour=int(data.get("dep_start_hour", 0)),
            dep_end_hour=int(data.get("dep_end_hour", 23)),
            arr_start_hour=int(data.get("arr_start_hour", 0)),
            arr_end_hour=int(data.get("arr_end_hour", 23)),
            ret_dep_start_hour=int(data.get("ret_dep_start_hour", data.get("dep_start_hour", 0))),
            ret_dep_end_hour=int(data.get("ret_dep_end_hour", data.get("dep_end_hour", 23))),
            ret_arr_start_hour=int(data.get("ret_arr_start_hour", data.get("arr_start_hour", 0))),
            ret_arr_end_hour=int(data.get("ret_arr_end_hour", data.get("arr_end_hour", 23))),
            carry_on_bags=max(0, int(data.get("carry_on_bags", 0))),
            seat=str(data.get("seat", "economy")).strip().lower(),
            adults=max(1, min(9, int(data.get("adults", 1)))),
            hide_self_transfer=bool(data.get("hide_self_transfer", False)),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ==============================================================================
# PURE-PYTHON PROTOBUF WIRE ENCODER & DECODER
# ==============================================================================


def _encode_varint(value: int) -> bytes:
    """Encode an unsigned integer into Protobuf base-128 varint format."""
    if value < 0:
        raise ValueError(f"Varint cannot be negative: {value}")
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            break
    return bytes(out)


def _encode_tag(field_number: int, wire_type: int) -> bytes:
    """Encode a Protobuf field tag (field_number << 3 | wire_type)."""
    return _encode_varint((field_number << 3) | wire_type)


def _encode_varint_field(field_number: int, value: int) -> bytes:
    """Encode a varint field (wire_type = 0)."""
    return _encode_tag(field_number, 0) + _encode_varint(value)


def _encode_bytes_field(field_number: int, payload: bytes) -> bytes:
    """Encode a length-delimited bytes/submessage field (wire_type = 2)."""
    return _encode_tag(field_number, 2) + _encode_varint(len(payload)) + payload


def _encode_string_field(field_number: int, text: str) -> bytes:
    """Encode a UTF-8 string field (wire_type = 2)."""
    return _encode_bytes_field(field_number, text.encode("utf-8"))


def _encode_airport_message(iata_code: str) -> bytes:
    """
    Encode a Google Flights Airport message:
      field 1 (varint) = 1 (IATA Airport entity)
      field 2 (string) = "MEX"
    """
    clean_iata = iata_code.strip().upper()
    return _encode_varint_field(1, 1) + _encode_string_field(2, clean_iata)


def _encode_flight_slice_message(
    origin: str | Sequence[str],
    destination: str | Sequence[str],
    flight_date: str,
    max_stops: Optional[int] = None,
    airlines: Optional[Sequence[str]] = None,
    dep_start_hour: int = 0,
    dep_end_hour: int = 23,
    arr_start_hour: int = 0,
    arr_end_hour: int = 23,
) -> bytes:
    """
    Encode a single FlightSlice (outbound or return leg) message supporting N:M airports:
      field 2 (string): YYYY-MM-DD
      field 5 (varint, optional): max_stops (0=Nonstop, 1=Max 1 stop, 2=Max 2 stops)
      field 6 (string, repeated): allowed IATA airline codes
      field 8 (varint, optional): earliest departure hour (0..23)
      field 9 (varint, optional): latest departure hour (1..24)
      field 10 (varint, optional): earliest arrival hour (0..23)
      field 11 (varint, optional): latest arrival hour (1..24)
      field 13 (message, repeated): origin Airport(s) (tag 0x6a repeated per origin IATA)
      field 14 (message, repeated): destination Airport(s) (tag 0x72 repeated per destination IATA)
    """
    origin_list = parse_iata_list(origin, expand_metro=True)
    destination_list = parse_iata_list(destination, expand_metro=True)

    buf = bytearray()
    buf.extend(_encode_string_field(2, flight_date.strip()))

    if max_stops is not None and max_stops in (0, 1, 2):
        buf.extend(_encode_varint_field(5, int(max_stops)))

    if airlines:
        for code in airlines:
            cleaned = str(code).strip().upper()
            if cleaned:
                buf.extend(_encode_string_field(6, cleaned))

    # Departure hour window (only encode when constrained away from full day 0..23)
    if 0 < dep_start_hour <= 23 or 0 <= dep_end_hour < 23:
        buf.extend(_encode_varint_field(8, max(0, min(23, int(dep_start_hour)))))
        upper_dep = int(dep_end_hour) + 1 if int(dep_end_hour) < 23 else 24
        buf.extend(_encode_varint_field(9, max(1, min(24, upper_dep))))

    # Arrival hour window (only encode when constrained away from full day 0..23)
    if 0 < arr_start_hour <= 23 or 0 <= arr_end_hour < 23:
        buf.extend(_encode_varint_field(10, max(0, min(23, int(arr_start_hour)))))
        upper_arr = int(arr_end_hour) + 1 if int(arr_end_hour) < 23 else 24
        buf.extend(_encode_varint_field(11, max(1, min(24, upper_arr))))

    # Emit repeated field 13 (0x6a) for each origin airport
    for orig_iata in origin_list:
        buf.extend(_encode_bytes_field(13, _encode_airport_message(orig_iata)))

    # Emit repeated field 14 (0x72) for each destination airport
    for dest_iata in destination_list:
        buf.extend(_encode_bytes_field(14, _encode_airport_message(dest_iata)))

    return bytes(buf)


def encode_tfs_protobuf(
    origin: str | Sequence[str],
    destination: str | Sequence[str],
    outbound_date: str,
    return_date: Optional[str] = None,
    trip_type: str = "round-trip",
    filter_profile: Optional[FilterProfile | dict[str, Any]] = None,
) -> str:
    """
    Deterministically encode a complete Google Flights `?tfs=` URL-safe Base64 parameter
    supporting both 1:1 and N:M multi-airport corridors.
    Bypasses `fast-flights` automatically when multiple origins/destinations or advanced
    slice filters are active so repeated field 13 and field 14 messages are preserved.
    """
    profile = (
        filter_profile
        if isinstance(filter_profile, FilterProfile)
        else FilterProfile.from_dict(filter_profile)
    )

    origin_list = parse_iata_list(origin, expand_metro=True)
    destination_list = parse_iata_list(destination, expand_metro=True)
    is_multi_airport = len(origin_list) > 1 or len(destination_list) > 1

    is_one_way = trip_type.strip().lower() in ("one-way", "one_way", "oneway") or not return_date
    trip_enum = 2 if is_one_way else 1
    seat_enum = CABIN_MAP.get(profile.seat.lower(), 1)
    adults_count = max(1, min(9, int(profile.adults)))

    has_advanced_filters = bool(
        is_multi_airport
        or profile.airlines
        or profile.carry_on_bags > 0
        or profile.dep_start_hour > 0
        or profile.dep_end_hour < 23
        or profile.arr_start_hour > 0
        or profile.arr_end_hour < 23
        or profile.ret_dep_start_hour > 0
        or profile.ret_dep_end_hour < 23
        or profile.ret_arr_start_hour > 0
        or profile.ret_arr_end_hour < 23
    )

    # Attempt fast-flights TFSData helper only when 1:1 single airport and no advanced filters
    if not has_advanced_filters:
        try:
            from fast_flights import FlightQuery, Passengers, create_filter

            ff_flights = [
                FlightQuery(
                    date=outbound_date.strip(),
                    from_airport=origin_list[0],
                    to_airport=destination_list[0],
                    max_stops=profile.max_stops,
                )
            ]
            if not is_one_way and return_date:
                ff_flights.append(
                    FlightQuery(
                        date=return_date.strip(),
                        from_airport=destination_list[0],
                        to_airport=origin_list[0],
                        max_stops=profile.max_stops,
                    )
                )
            ff_filter = create_filter(
                flights=ff_flights,
                trip="one-way" if is_one_way else "round-trip",
                seat=profile.seat if profile.seat in ("economy", "premium-economy", "business", "first") else "economy",
                passengers=Passengers(adults=adults_count),
                max_stops=profile.max_stops,
            )
            b64_str = ff_filter.to_str().rstrip("=")
            if b64_str:
                return b64_str
        except Exception:
            pass

    root = bytearray()
    # Field 1: version = 28
    root.extend(_encode_varint_field(1, 28))
    # Field 2: trip type (1 = round-trip, 2 = one-way)
    root.extend(_encode_varint_field(2, trip_enum))

    # Field 3 (slice 1): Outbound leg (origins -> destinations)
    outbound_slice = _encode_flight_slice_message(
        origin=origin_list,
        destination=destination_list,
        flight_date=outbound_date,
        max_stops=profile.max_stops,
        airlines=profile.airlines,
        dep_start_hour=profile.dep_start_hour,
        dep_end_hour=profile.dep_end_hour,
        arr_start_hour=profile.arr_start_hour,
        arr_end_hour=profile.arr_end_hour,
    )
    root.extend(_encode_bytes_field(3, outbound_slice))

    # Field 3 (slice 2): Return leg (destinations -> origins)
    if not is_one_way and return_date:
        return_slice = _encode_flight_slice_message(
            origin=destination_list,
            destination=origin_list,
            flight_date=return_date,
            max_stops=profile.max_stops,
            airlines=profile.airlines,
            dep_start_hour=profile.ret_dep_start_hour,
            dep_end_hour=profile.ret_dep_end_hour,
            arr_start_hour=profile.ret_arr_start_hour,
            arr_end_hour=profile.ret_arr_end_hour,
        )
        root.extend(_encode_bytes_field(3, return_slice))

    # Field 8: Passengers (1 = Adult, repeated `adults` times)
    for _ in range(adults_count):
        root.extend(_encode_varint_field(8, 1))

    # Field 9: Cabin class / Seat
    root.extend(_encode_varint_field(9, seat_enum))

    # Field 14: Carry-on bags filter (submessage field 1 = carry_on_bags)
    if profile.carry_on_bags > 0:
        bags_msg = _encode_varint_field(1, min(2, int(profile.carry_on_bags)))
        root.extend(_encode_bytes_field(14, bags_msg))

    # Field 19: Standard Google Flights search view flag = 1
    root.extend(_encode_varint_field(19, 1))

    encoded = base64.urlsafe_b64encode(bytes(root)).decode("ascii").rstrip("=")
    return encoded


def build_google_flights_url(
    origin: str | Sequence[str],
    destination: str | Sequence[str],
    outbound_date: str,
    return_date: Optional[str] = None,
    trip_type: str = "round-trip",
    filter_profile: Optional[FilterProfile | dict[str, Any]] = None,
    curr: str = DEFAULT_CURRENCY,
    gl: str = DEFAULT_GL,
    hl: str = DEFAULT_HL,
) -> str:
    """
    Build a complete, locale-locked Google Flights search URL with an encoded `?tfs=` parameter.
    """
    tfs_token = encode_tfs_protobuf(
        origin=origin,
        destination=destination,
        outbound_date=outbound_date,
        return_date=return_date,
        trip_type=trip_type,
        filter_profile=filter_profile,
    )
    query_params = {
        "tfs": tfs_token,
        "curr": (curr or DEFAULT_CURRENCY).strip().upper(),
        "gl": (gl or DEFAULT_GL).strip().upper(),
        "hl": (hl or DEFAULT_HL).strip().lower(),
        "tfu": "EgQIABABIgA",
    }
    return f"https://www.google.com/travel/flights/search?{urlencode(query_params)}"


# ==============================================================================
# PROTOBUF WIRE DECODER & MODE A (BATCH URL PASTE NORMALIZER)
# ==============================================================================


def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    """Read a base-128 varint from `data` starting at `offset`. Returns (value, new_offset)."""
    result = 0
    shift = 0
    pos = offset
    while pos < len(data):
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result, pos
        shift += 7
        if shift >= 64:
            raise ValueError("Malformed Protobuf varint")
    raise ValueError("Unexpected EOF while reading varint")


def _parse_protobuf_fields(data: bytes) -> list[tuple[int, int, Any]]:
    """
    Parse raw Protobuf bytes into a list of (field_number, wire_type, value) tuples.
    """
    fields: list[tuple[int, int, Any]] = []
    pos = 0
    n = len(data)
    while pos < n:
        try:
            tag, pos = _read_varint(data, pos)
        except ValueError:
            break
        field_num = tag >> 3
        wire_type = tag & 0x07
        if field_num <= 0:
            break

        if wire_type == 0:  # Varint
            val, pos = _read_varint(data, pos)
            fields.append((field_num, wire_type, val))
        elif wire_type == 1:  # 64-bit
            if pos + 8 > n:
                break
            fields.append((field_num, wire_type, data[pos : pos + 8]))
            pos += 8
        elif wire_type == 2:  # Length-delimited
            length, pos = _read_varint(data, pos)
            if length < 0 or pos + length > n:
                break
            fields.append((field_num, wire_type, data[pos : pos + length]))
            pos += length
        elif wire_type == 5:  # 32-bit
            if pos + 4 > n:
                break
            fields.append((field_num, wire_type, data[pos : pos + 4]))
            pos += 4
        else:
            break
    return fields


def decode_tfs_parameter(tfs_b64: str) -> dict[str, Any]:
    """
    Decode a Google Flights `?tfs=` Base64 Protobuf string (or full URL) to extract legs
    (dates, multiple origins, multiple destinations, max_stops, airlines) for Mode A pasted URLs.
    """
    result: dict[str, Any] = {
        "origin": None,
        "destination": None,
        "origins": [],
        "destinations": [],
        "outbound_date": None,
        "return_date": None,
        "trip_type": "round-trip",
        "max_stops": None,
        "airlines": [],
    }
    if not tfs_b64:
        return result

    cleaned = tfs_b64.strip()
    if "tfs=" in cleaned:
        tfs_match = re.search(r"[?&]tfs=([^&#]+)", cleaned)
        if tfs_match:
            cleaned = tfs_match.group(1)

    padding = (4 - len(cleaned) % 4) % 4
    padded = cleaned + ("=" * padding)

    try:
        raw_bytes = base64.urlsafe_b64decode(padded)
    except Exception:
        try:
            raw_bytes = base64.b64decode(padded)
        except Exception:
            return result

    slices: list[dict[str, Any]] = []
    top_fields = _parse_protobuf_fields(raw_bytes)
    for field_num, wire_type, val in top_fields:
        if field_num == 2 and wire_type == 0:
            result["trip_type"] = "one-way" if val == 2 else "round-trip"
        elif field_num == 3 and wire_type == 2 and isinstance(val, bytes):
            slice_info: dict[str, Any] = {
                "date": None,
                "origins": [],
                "destinations": [],
                "max_stops": None,
                "airlines": [],
            }
            sub_fields = _parse_protobuf_fields(val)
            for s_num, s_wire, s_val in sub_fields:
                if s_num == 2 and s_wire == 2 and isinstance(s_val, bytes):
                    try:
                        text = s_val.decode("utf-8", errors="ignore").strip()
                        if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
                            slice_info["date"] = text
                    except Exception:
                        pass
                elif s_num == 5 and s_wire == 0:
                    slice_info["max_stops"] = int(s_val)
                elif s_num == 6 and s_wire == 2 and isinstance(s_val, bytes):
                    code = s_val.decode("utf-8", errors="ignore").strip().upper()
                    if 2 <= len(code) <= 3:
                        slice_info["airlines"].append(code)
                elif s_num in (13, 14) and s_wire == 2 and isinstance(s_val, bytes):
                    ap_fields = _parse_protobuf_fields(s_val)
                    for ap_num, ap_wire, ap_val in ap_fields:
                        if ap_num == 2 and ap_wire == 2 and isinstance(ap_val, bytes):
                            code = ap_val.decode("utf-8", errors="ignore").strip().upper()
                            if IATA_TOKEN_RE.match(code):
                                if s_num == 13 and code not in slice_info["origins"]:
                                    slice_info["origins"].append(code)
                                elif s_num == 14 and code not in slice_info["destinations"]:
                                    slice_info["destinations"].append(code)
            slices.append(slice_info)

    if not slices or not slices[0].get("date"):
        date_matches = re.findall(rb"(20\d{2}-\d{2}-\d{2})", raw_bytes)
        decoded_dates = [m.decode("ascii") for m in date_matches]
        if decoded_dates:
            result["outbound_date"] = decoded_dates[0]
            if len(decoded_dates) > 1:
                result["return_date"] = decoded_dates[1]
            else:
                result["trip_type"] = "one-way"
    else:
        first = slices[0]
        origins_list = first.get("origins") or []
        destinations_list = first.get("destinations") or []
        result["outbound_date"] = first.get("date")
        result["origins"] = origins_list
        result["destinations"] = destinations_list
        result["origin"] = ",".join(origins_list) if origins_list else None
        result["destination"] = ",".join(destinations_list) if destinations_list else None
        result["max_stops"] = first.get("max_stops")
        result["airlines"] = first.get("airlines") or []
        if len(slices) >= 2 and slices[1].get("date"):
            result["return_date"] = slices[1].get("date")
            result["trip_type"] = "round-trip"
        else:
            result["trip_type"] = "one-way"

    return result


def normalize_google_flights_url(
    url: str,
    curr: str = DEFAULT_CURRENCY,
    gl: str = DEFAULT_GL,
    hl: str = DEFAULT_HL,
) -> str:
    """
    Normalize any Google Flights URL and lock `&curr=MXN&gl=MX&hl=es` (or custom locale)
    so extracted prices and ARIA labels are always in a deterministic currency and language.
    Cleans leading/trailing brackets, markdown formatting, quotes and spaces.
    """
    if not url or not url.strip():
        raise ValueError("URL cannot be empty")

    raw = url.strip()

    # Extract clean URL if line is wrapped in Markdown, brackets, quotes or bullet points
    match = re.search(r"https?://[^\s)\]\"\'`>]+", raw)
    if match:
        raw = match.group(0).rstrip(".,;)]\"\'`>")
    else:
        match_no_proto = re.search(
            r"(?:www\.)?google\.[a-z.]+/travel/flights[^\s)\]\"\'`>]*",
            raw,
            re.IGNORECASE,
        )
        if match_no_proto:
            raw = "https://" + match_no_proto.group(0).rstrip(".,;)]\"\'`>")
        else:
            cleaned = re.sub(r"^[\[\(\<\"\'`\*\-\d\.\s]+", "", raw).rstrip(".,;)]\"\'`>")
            if not cleaned.startswith(("http://", "https://")):
                raw = "https://" + cleaned
            else:
                raw = cleaned

    try:
        parsed = urlparse(raw)
    except ValueError as exc:
        logger.warning("urlparse failed for raw='%s': %s", raw[:80], exc)
        raise

    qs = parse_qs(parsed.query, keep_blank_values=True)

    flat_params: dict[str, str] = {}
    for k, v_list in qs.items():
        if v_list:
            flat_params[k] = v_list[0]

    flat_params["curr"] = (curr or DEFAULT_CURRENCY).strip().upper()
    flat_params["gl"] = (gl or DEFAULT_GL).strip().upper()
    flat_params["hl"] = (hl or DEFAULT_HL).strip().lower()

    path = parsed.path or "/travel/flights/search"
    if not path.startswith("/travel/flights"):
        path = "/travel/flights/search"

    normalized = urlunparse(
        (
            "https",
            "www.google.com",
            path,
            "",
            urlencode(flat_params),
            "",
        )
    )
    return normalized


def format_date_slot_label(outbound_date: str, return_date: Optional[str] = None) -> str:
    """
    Format a human-friendly date slot label in Spanish, e.g.:
      "Vie 06 Nov - Dom 15 Nov (9d)" or "Vie 06 Nov (Solo ida)"
    """
    try:
        out_dt = datetime.strptime(outbound_date.strip(), "%Y-%m-%d").date()
        out_str = f"{SPANISH_WEEKDAYS[out_dt.weekday()]} {out_dt.day:02d} {SPANISH_MONTHS[out_dt.month]}"
        if return_date and return_date.strip():
            ret_dt = datetime.strptime(return_date.strip(), "%Y-%m-%d").date()
            ret_str = f"{SPANISH_WEEKDAYS[ret_dt.weekday()]} {ret_dt.day:02d} {SPANISH_MONTHS[ret_dt.month]}"
            days = (ret_dt - out_dt).days
            return f"{out_str} - {ret_str} ({days}d)"
        return f"{out_str} (Solo ida)"
    except Exception:
        if return_date:
            return f"{outbound_date} -> {return_date}"
        return outbound_date


def parse_pasted_urls(
    raw_text: str,
    curr: str = DEFAULT_CURRENCY,
    gl: str = DEFAULT_GL,
    hl: str = DEFAULT_HL,
    fallback_origin: str = "MEX",
    fallback_destination: str = "MAD",
    max_slots: int = MAX_SLOTS_PER_DESTINATION,
) -> list[dict[str, Any]]:
    """
    Mode A (Batch URL Paste):
    Parse 1 to 10 pasted Google Flights URLs (separated by newlines), normalize
    `&curr=MXN&gl=MX&hl=es`, and extract `(outbound_date, return_date, label)`
    from each URL's `?tfs=` parameter. Robust to surrounding markdown, brackets and notes.
    """
    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    parsed_slots: list[dict[str, Any]] = []

    for idx, line in enumerate(lines):
        if len(parsed_slots) >= max_slots:
            break
        if "google." not in line and "tfs=" not in line and "flights" not in line:
            continue

        try:
            normalized_url = normalize_google_flights_url(line, curr=curr, gl=gl, hl=hl)
            qs = parse_qs(urlparse(normalized_url).query)
            tfs_val = qs.get("tfs", [""])[0]
            decoded = decode_tfs_parameter(tfs_val) if tfs_val else {}

            outbound_date = decoded.get("outbound_date") or (
                date.today() + timedelta(days=14 + idx * 7)
            ).strftime("%Y-%m-%d")
            return_date = decoded.get("return_date")
            label = format_date_slot_label(outbound_date, return_date)

            parsed_slots.append(
                {
                    "label": label,
                    "outbound_date": outbound_date,
                    "return_date": return_date,
                    "google_flights_url": normalized_url,
                    "origin": decoded.get("origin") or fallback_origin,
                    "destination": decoded.get("destination") or fallback_destination,
                    "trip_type": decoded.get("trip_type", "round-trip"),
                }
            )
        except Exception as exc:
            logger.warning("Skipping invalid pasted flight line '%s': %s", line[:80], exc)
            continue

    return parsed_slots

    return parsed_slots


# ==============================================================================
# MODE B: MULTI-DATE MATRIX GENERATOR (UP TO 10 DATE SLOTS)
# ==============================================================================


def generate_date_matrix(
    start_date: date | str,
    end_date: date | str,
    stay_days_list: Sequence[int] = (7,),
    weekdays_only: Optional[Sequence[int]] = None,
    max_slots: int = MAX_SLOTS_PER_DESTINATION,
    origin: str = "MEX",
    destination: str = "MAD",
    trip_type: str = "round-trip",
    filter_profile: Optional[FilterProfile | dict[str, Any]] = None,
    curr: str = DEFAULT_CURRENCY,
    gl: str = DEFAULT_GL,
    hl: str = DEFAULT_HL,
) -> list[dict[str, Any]]:
    """
    Generate up to `max_slots` (default 10) optimal date combinations across the
    `[start_date, end_date]` window matching `stay_days_list` and `weekdays_only`
    (where Monday=0 ... Sunday=6).

    If more than `max_slots` candidate combinations exist in the window, evenly samples
    across the date range so the entire travel window is covered without clustering only
    at the start of the window.
    """
    if isinstance(start_date, str):
        start_dt = datetime.strptime(start_date.strip(), "%Y-%m-%d").date()
    else:
        start_dt = start_date

    if isinstance(end_date, str):
        end_dt = datetime.strptime(end_date.strip(), "%Y-%m-%d").date()
    else:
        end_dt = end_date

    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt

    is_one_way = trip_type.strip().lower() in ("one-way", "one_way", "oneway")
    stays = sorted({max(1, int(d)) for d in stay_days_list}) if stay_days_list else [7]
    allowed_weekdays = (
        {int(w) for w in weekdays_only if 0 <= int(w) <= 6}
        if weekdays_only is not None and len(weekdays_only) > 0
        else {0, 1, 2, 3, 4, 5, 6}
    )

    candidates: list[tuple[date, Optional[date]]] = []
    cursor = start_dt
    while cursor <= end_dt:
        if cursor.weekday() in allowed_weekdays:
            if is_one_way:
                candidates.append((cursor, None))
            else:
                for stay in stays:
                    ret_dt = cursor + timedelta(days=stay)
                    if ret_dt <= end_dt or len(candidates) < max_slots:
                        candidates.append((cursor, ret_dt))
        cursor += timedelta(days=1)

    # Deduplicate while preserving chronological order
    seen: set[tuple[str, Optional[str]]] = set()
    unique_candidates: list[tuple[date, Optional[date]]] = []
    for out_d, ret_d in candidates:
        key = (out_d.isoformat(), ret_d.isoformat() if ret_d else None)
        if key not in seen:
            seen.add(key)
            unique_candidates.append((out_d, ret_d))

    # Evenly sample across the window if there are more candidates than `max_slots`
    limit = max(1, min(int(max_slots), MAX_SLOTS_PER_DESTINATION))
    if len(unique_candidates) > limit:
        selected: list[tuple[date, Optional[date]]] = []
        total = len(unique_candidates)
        for i in range(limit):
            idx = int(round(i * (total - 1) / (limit - 1))) if limit > 1 else 0
            selected.append(unique_candidates[idx])
    else:
        selected = unique_candidates

    matrix_slots: list[dict[str, Any]] = []
    for out_d, ret_d in selected:
        out_str = out_d.strftime("%Y-%m-%d")
        ret_str = ret_d.strftime("%Y-%m-%d") if ret_d else None
        url = build_google_flights_url(
            origin=origin,
            destination=destination,
            outbound_date=out_str,
            return_date=ret_str,
            trip_type="one-way" if is_one_way else "round-trip",
            filter_profile=filter_profile,
            curr=curr,
            gl=gl,
            hl=hl,
        )
        label = format_date_slot_label(out_str, ret_str)
        matrix_slots.append(
            {
                "label": label,
                "outbound_date": out_str,
                "return_date": ret_str,
                "google_flights_url": url,
            }
        )

    return matrix_slots
