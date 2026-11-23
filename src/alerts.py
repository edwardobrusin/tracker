"""
Consolidated Destination Digest Alert Engine (`src/alerts.py`) for FlightPulse Matrix v4.0.

Features:
  - Strict 330-day window filtering: excludes `FUTURE_RELEASE` slots before evaluating
    triggers or rendering Telegram messages.
  - Evaluates only 2 high-signal triggers using `effective_price = cheapest_price or best_price`:
      1. TARGET_BREACH (Meta): immediate alert (bypasses cooldown) on first breach or
         when crossing down from above `target_price`; while remaining under target,
         re-alerts only on >=3% extra drop (respecting `cooldown_hours`) or >=5% extra
         drop (bypassing cooldown).
      2. ALL_TIME_LOW (Bajo Histórico): requires prior DB history (`historical_min_price`
         not None), uses `reference_low = last_atl_alerted_price or historical_min_price`,
         fires on >=2% drop (`effective_price <= reference_low * 0.98`), and always
         bypasses `cooldown_hours`.
  - Sends ONE compact HTML Telegram message per destination (`parse_mode="HTML"`).
"""

from __future__ import annotations

import datetime
import html
import logging
import re
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    import requests
else:
    try:
        import requests
    except ImportError:
        requests = None

from src.config import (
    DEFAULT_CURRENCY,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    get_flight_window_status,
)
from src.db import (
    get_destination,
    list_date_slots,
    list_destinations,
    record_alert_and_update_slots,
)
from src.tfs_builder import format_date_slot_label

logger = logging.getLogger("flightpulse.alerts")


def _normalize_stops_compact(stops_raw: Any) -> str:
    """Normalize stop label into 'Directo', '1 escala', or 'N escalas'."""
    text = str(stops_raw or "Directo").strip()
    lower = text.lower()
    if lower in ("", "directo", "sin escalas", "nonstop", "0 escalas", "0 stops"):
        return "Directo"
    m = re.search(r"(\d+)", lower)
    if m:
        count = int(m.group(1))
        if count <= 0:
            return "Directo"
        if count == 1:
            return "1 escala"
        return f"{count} escalas"
    return text


def _format_compact_slot_date(slot: dict[str, Any]) -> str:
    """Return compact date label like 'Vie 27 Ago - Dom 12 Sep' without trailing '(16d)'."""
    raw_lbl = str(slot.get("label") or "").strip()
    if not raw_lbl:
        out_d = str(slot.get("outbound_date") or "").strip()
        ret_d = str(slot.get("return_date") or "").strip() or None
        raw_lbl = format_date_slot_label(out_d, ret_d) if out_d else "Fecha"
    return re.sub(r"\s*\(\d+d\)$", "", raw_lbl).strip()


def _format_trigger_label(trigger_type: str) -> str:
    """Map internal trigger_type string to Spanish compact label."""
    parts = {p.strip() for p in str(trigger_type or "").split("+") if p.strip()}
    has_meta = "TARGET_BREACH" in parts
    has_low = "ALL_TIME_LOW" in parts
    if has_meta and has_low:
        return "Meta / Bajo Histórico"
    if has_meta:
        return "Meta"
    if has_low:
        return "Bajo Histórico"
    if "TEST" in str(trigger_type or ""):
        return "Meta / Bajo Histórico"
    return "Alerta"


def _is_slot_future_release(slot_meta: dict[str, Any]) -> bool:
    """Return True if a slot is outside the 330-day advance booking window."""
    if slot_meta.get("is_future_release"):
        return True
    win_info = slot_meta.get("window_info") or get_flight_window_status(
        slot_meta.get("outbound_date"),
        slot_meta.get("return_date"),
    )
    return win_info.get("status") == "FUTURE_RELEASE"


def _effective_price_from_values(cheapest_val: Any, best_val: Any) -> Optional[float]:
    """Return positive `cheapest_price or best_price` as float, else None."""
    if cheapest_val is not None:
        try:
            c_f = float(cheapest_val)
            if c_f > 0:
                return c_f
        except (TypeError, ValueError):
            pass
    if best_val is not None:
        try:
            b_f = float(best_val)
            if b_f > 0:
                return b_f
        except (TypeError, ValueError):
            pass
    return None


def evaluate_slot_alert_triggers(
    slot_meta: dict[str, Any],
    scrape_result: dict[str, Any],
    snapshot_stats: dict[str, Any],
) -> Optional[dict[str, Any]]:
    """
    Evaluate whether a newly scraped `date_slot` breaches Meta (`TARGET_BREACH`)
    or Bajo Histórico (`ALL_TIME_LOW`), excluding `FUTURE_RELEASE` (>330d) slots.
    """
    if _is_slot_future_release(slot_meta):
        logger.debug("Skipping alert evaluation on FUTURE_RELEASE slot id=%s.", slot_meta.get("id"))
        return None

    cheapest_raw = scrape_result.get("cheapest_tab_price")
    best_raw = scrape_result.get("best_tab_price")
    cheapest_price = float(cheapest_raw) if (cheapest_raw is not None and float(cheapest_raw) > 0) else None
    best_price = float(best_raw) if (best_raw is not None and float(best_raw) > 0) else None

    effective_price = _effective_price_from_values(cheapest_price, best_price)
    if effective_price is None:
        return None

    target_price = float(slot_meta.get("target_price") or 0.0)
    cooldown_hours = int(slot_meta.get("cooldown_hours") or 12)
    filter_cfg = slot_meta.get("filter_config") or {}

    is_self_transfer = bool(scrape_result.get("is_self_transfer", False))
    if filter_cfg.get("hide_self_transfer") and is_self_transfer:
        logger.info(
            "Skipping alert on slot id=%s because hide_self_transfer=True and itinerary is self-transfer.",
            slot_meta.get("id"),
        )
        return None

    historical_min = snapshot_stats.get("historical_min_price")
    last_alerted_at = snapshot_stats.get("last_alerted_at") or slot_meta.get("last_alerted_at")
    last_alerted_price = (
        snapshot_stats.get("last_alerted_price")
        if snapshot_stats.get("last_alerted_price") is not None
        else slot_meta.get("last_alerted_price")
    )
    prev_effective = (
        snapshot_stats.get("prev_effective_price")
        if snapshot_stats.get("prev_effective_price") is not None
        else slot_meta.get("prev_effective_price")
    )
    last_atl_alerted_price = (
        snapshot_stats.get("last_atl_alerted_price")
        if snapshot_stats.get("last_atl_alerted_price") is not None
        else slot_meta.get("last_atl_alerted_price")
    )

    elapsed_hours = float("inf")
    if last_alerted_at is not None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if last_alerted_at.tzinfo is None:
            last_alerted_at = last_alerted_at.replace(tzinfo=datetime.timezone.utc)
        elapsed_hours = (now_utc - last_alerted_at).total_seconds() / 3600.0

    triggers: list[str] = []

    # 1. TARGET_BREACH (Meta)
    if target_price > 0 and effective_price <= target_price:
        never_alerted = last_alerted_at is None or last_alerted_price is None
        crossed_below = prev_effective is not None and float(prev_effective) > target_price
        if never_alerted or crossed_below:
            triggers.append("TARGET_BREACH")
        elif float(last_alerted_price) > 0:
            last_p = float(last_alerted_price)
            if effective_price <= last_p * 0.95:
                triggers.append("TARGET_BREACH")
            elif effective_price <= last_p * 0.97 and elapsed_hours >= cooldown_hours:
                triggers.append("TARGET_BREACH")

    # 2. ALL_TIME_LOW (Bajo Histórico — always bypasses cooldown_hours)
    if historical_min is not None and float(historical_min) > 0:
        reference_low = (
            float(last_atl_alerted_price)
            if (last_atl_alerted_price is not None and float(last_atl_alerted_price) > 0)
            else float(historical_min)
        )
        if effective_price <= reference_low * 0.98:
            triggers.append("ALL_TIME_LOW")

    if not triggers:
        return None

    return {
        "date_slot_id": slot_meta.get("id"),
        "label": slot_meta.get("label") or f"{slot_meta.get('outbound_date')} -> {slot_meta.get('return_date')}",
        "outbound_date": slot_meta.get("outbound_date"),
        "return_date": slot_meta.get("return_date"),
        "google_flights_url": slot_meta.get("google_flights_url"),
        "cheapest_tab_price": cheapest_price,
        "cheapest_airline": scrape_result.get("cheapest_airline") or "Aerolínea",
        "cheapest_stops": scrape_result.get("cheapest_stops") or "Directo",
        "cheapest_duration": scrape_result.get("cheapest_duration") or "N/A",
        "cheapest_dep_arr": scrape_result.get("cheapest_dep_arr") or "",
        "origin_airport": scrape_result.get("origin_airport"),
        "destination_airport": scrape_result.get("destination_airport"),
        "is_self_transfer": is_self_transfer,
        "best_tab_price": best_price,
        "best_airline": scrape_result.get("best_airline") or scrape_result.get("cheapest_airline") or "Aerolínea",
        "best_stops": scrape_result.get("best_stops") or scrape_result.get("cheapest_stops") or "Directo",
        "best_duration": scrape_result.get("best_duration") or scrape_result.get("cheapest_duration") or "N/A",
        "best_origin_airport": scrape_result.get("best_origin_airport") or scrape_result.get("origin_airport"),
        "best_destination_airport": scrape_result.get("best_destination_airport") or scrape_result.get("destination_airport"),
        "effective_price": effective_price,
        "trigger_type": "+".join(triggers),
        "historical_min_price": historical_min,
    }


def format_consolidated_destination_digest(
    destination_meta: dict[str, Any],
    triggered_slots: list[dict[str, Any]],
    is_test: bool = False,
) -> str:
    """
    Format ONE compact HTML Telegram message (`parse_mode="HTML"`) for a destination,
    strictly excluding any `FUTURE_RELEASE` (>330d) slots.
    """
    active_slots = [s for s in triggered_slots if not _is_slot_future_release(s)]
    if not active_slots:
        return ""

    sorted_slots = sorted(
        active_slots,
        key=lambda x: _effective_price_from_values(x.get("cheapest_tab_price"), x.get("best_tab_price")) or 9_999_999.0,
    )

    raw_dest_name = str(destination_meta.get("name") or "Ruta Monitoreada").strip()
    if is_test and not raw_dest_name.startswith("[PRUEBA]"):
        raw_dest_name = f"[PRUEBA] {raw_dest_name}"
    dest_name_html = html.escape(raw_dest_name, quote=True)

    raw_origin = str(destination_meta.get("origin") or "MEX")
    raw_dest = str(destination_meta.get("destination") or destination_meta.get("destination_code") or "MAD")
    default_single_orig = raw_origin.split(",")[0].strip().upper() if "," not in raw_origin else ""
    default_single_dest = raw_dest.split(",")[0].strip().upper() if "," not in raw_dest else ""
    currency = html.escape(str(destination_meta.get("currency") or DEFAULT_CURRENCY).strip().upper(), quote=True)

    slot_trigger_labels = [_format_trigger_label(str(s.get("trigger_type") or "")) for s in sorted_slots]
    all_trigger_parts: set[str] = set()
    for s in sorted_slots:
        for p in str(s.get("trigger_type") or "").split("+"):
            if p.strip():
                all_trigger_parts.add(p.strip())

    if "TARGET_BREACH" in all_trigger_parts and "ALL_TIME_LOW" in all_trigger_parts:
        header_trigger = "Meta / Bajo Histórico"
    elif "TARGET_BREACH" in all_trigger_parts:
        header_trigger = "Meta"
    elif "ALL_TIME_LOW" in all_trigger_parts:
        header_trigger = "Bajo Histórico"
    elif is_test:
        header_trigger = "Meta / Bajo Histórico"
    else:
        header_trigger = slot_trigger_labels[0]

    mixed_triggers = len(set(slot_trigger_labels)) > 1
    lines: list[str] = [
        f"<b>{dest_name_html}</b> | {html.escape(header_trigger, quote=True)}"
    ]

    for idx, slot in enumerate(sorted_slots, start=1):
        date_text = html.escape(_format_compact_slot_date(slot), quote=True)
        slot_suffix = f" · {html.escape(slot_trigger_labels[idx - 1], quote=True)}" if mixed_triggers else ""
        lines.append(f"{idx}. {date_text}{slot_suffix}")

        c_price = slot.get("cheapest_tab_price")
        b_price = slot.get("best_tab_price")

        c_orig = html.escape(str(slot.get("origin_airport") or default_single_orig).strip().upper(), quote=True)
        c_dest = html.escape(str(slot.get("destination_airport") or default_single_dest).strip().upper(), quote=True)

        if c_price is not None and float(c_price) > 0:
            c_stops = html.escape(_normalize_stops_compact(slot.get("cheapest_stops")), quote=True)
            c_dur = html.escape(str(slot.get("cheapest_duration") or "N/A").strip(), quote=True)
            c_route = f"{c_orig} -&gt; {c_dest}, " if (c_orig and c_dest) else ""
            self_mark = " ⚠️" if slot.get("is_self_transfer") else ""
            lines.append(f"Barato: ${float(c_price):,.0f} {currency} ({c_route}{c_stops}{self_mark}, {c_dur})")

        if b_price is not None and float(b_price) > 0:
            b_orig = html.escape(str(slot.get("best_origin_airport") or c_orig).strip().upper(), quote=True)
            b_dest = html.escape(str(slot.get("best_destination_airport") or c_dest).strip().upper(), quote=True)
            b_stops = html.escape(_normalize_stops_compact(slot.get("best_stops")), quote=True)
            b_dur = html.escape(str(slot.get("best_duration") or "N/A").strip(), quote=True)
            b_route = f"{b_orig} -&gt; {b_dest}, " if (b_orig and b_dest) else ""
            lines.append(f"Mejor: ${float(b_price):,.0f} {currency} ({b_route}{b_stops}, {b_dur})")

        raw_url = str(slot.get("google_flights_url") or "https://www.google.com/travel/flights").strip()
        escaped_url = html.escape(raw_url, quote=True)
        lines.append(f'<a href="{escaped_url}"><b>[Reservar]</b></a>')

        if idx < len(sorted_slots):
            lines.append("")

    return "\n".join(lines)


def send_telegram_message(html_text: str) -> tuple[bool, str]:
    """
    Dispatch an HTML-formatted message (`parse_mode="HTML"`) via the Telegram Bot HTTP API.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        msg = "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not configured."
        logger.warning(msg)
        return False, msg

    if requests is None:
        msg = "The 'requests' module is not installed."
        logger.warning(msg)
        return False, msg

    if not html_text.strip():
        return False, "Empty Telegram message body (filtered out)."

    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": html_text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        resp = requests.post(api_url, json=payload, timeout=15)
        if resp.status_code == 200:
            logger.info("Successfully delivered Consolidated Telegram Digest Alert.")
            return True, "OK"

        logger.warning(
            "Telegram HTML send returned HTTP %d (%s); retrying plain text fallback.",
            resp.status_code,
            resp.text[:150],
        )
        plain_text = html.unescape(re.sub(r"<[^>]+>", "", html_text))
        payload_plain = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": plain_text,
            "disable_web_page_preview": True,
        }
        resp_fallback = requests.post(api_url, json=payload_plain, timeout=15)
        if resp_fallback.status_code == 200:
            return True, "OK (Plain-text fallback)"
        return False, f"HTTP {resp_fallback.status_code}: {resp_fallback.text[:200]}"
    except Exception as exc:
        logger.error("Telegram API request failed: %s", exc)
        return False, str(exc)


def dispatch_destination_digest(
    destination_meta: dict[str, Any],
    triggered_slots: list[dict[str, Any]],
) -> bool:
    """
    Build and send ONE compact HTML Telegram message for all triggered active slots
    of a destination, then persist `alert_logs` and update `last_alerted_at` / `last_alerted_price`.
    """
    eligible_slots = [s for s in triggered_slots if not _is_slot_future_release(s)]
    if not eligible_slots:
        return False

    dest_id = int(destination_meta["id"])
    message_html = format_consolidated_destination_digest(
        destination_meta=destination_meta,
        triggered_slots=eligible_slots,
        is_test=False,
    )
    if not message_html:
        return False

    sent_ok, status_detail = send_telegram_message(message_html)
    if sent_ok:
        record_alert_and_update_slots(
            destination_id=dest_id,
            triggered_slots=eligible_slots,
            message_preview=message_html,
        )
        logger.info(
            "Recorded %d slot alerts for destination_id=%d (%s)",
            len(eligible_slots),
            dest_id,
            destination_meta.get("name"),
        )
        return True

    logger.error(
        "Failed to send Telegram digest for destination_id=%d: %s",
        dest_id,
        status_detail,
    )
    return False


def send_test_digest_alert(destination_id: Optional[int] = None) -> tuple[bool, str, str]:
    """
    Send a test compact HTML card to Telegram from the Streamlit Control Center (Tab 3).
    Reuses the exact production template with `[PRUEBA] ` prepended, using up to 2 active
    non-FUTURE_RELEASE slots with real prices (or 1 synthetic `[PRUEBA]` slot if none exist).
    """
    dest_meta: Optional[dict[str, Any]] = None
    slots: list[dict[str, Any]] = []

    try:
        if destination_id is not None:
            dest_meta = get_destination(destination_id)
            if dest_meta:
                slots = list_date_slots(destination_id=destination_id, active_only=True)
        else:
            all_dests = list_destinations(active_only=True) or list_destinations(active_only=False)
            if all_dests:
                dest_meta = all_dests[0]
                slots = list_date_slots(destination_id=int(dest_meta["id"]), active_only=True)
    except Exception as exc:
        logger.debug("Database lookup for test alert skipped: %s", exc)

    if not dest_meta:
        dest_meta = {
            "id": 0,
            "name": "CDMX -> Madrid (Demo)",
            "origin": "MEX",
            "destination": "MAD",
            "currency": "MXN",
            "target_price": 15500.0,
        }

    triggered_preview: list[dict[str, Any]] = []
    for s in slots:
        if _is_slot_future_release(s):
            continue
        c_price = s.get("latest_cheapest_price")
        b_price = s.get("latest_best_price")
        if _effective_price_from_values(c_price, b_price) is None:
            continue

        triggered_preview.append(
            {
                "date_slot_id": s.get("id"),
                "label": s.get("label") or format_date_slot_label(str(s.get("outbound_date", "")), s.get("return_date")),
                "outbound_date": s.get("outbound_date"),
                "return_date": s.get("return_date"),
                "google_flights_url": s.get("google_flights_url") or "https://www.google.com/travel/flights",
                "cheapest_tab_price": c_price,
                "cheapest_stops": s.get("latest_stops") or "Directo",
                "cheapest_duration": s.get("latest_duration") or "N/A",
                "origin_airport": s.get("latest_origin_airport"),
                "destination_airport": s.get("latest_destination_airport"),
                "is_self_transfer": bool(s.get("latest_is_self_transfer", False)),
                "best_tab_price": b_price,
                "best_stops": s.get("latest_stops") or "Directo",
                "best_duration": s.get("latest_duration") or "N/A",
                "best_origin_airport": s.get("latest_best_origin_airport") or s.get("latest_origin_airport"),
                "best_destination_airport": s.get("latest_best_destination_airport") or s.get("latest_destination_airport"),
                "trigger_type": "TARGET_BREACH+ALL_TIME_LOW",
            }
        )
        if len(triggered_preview) >= 2:
            break

    if not triggered_preview:
        triggered_preview = [
            {
                "date_slot_id": None,
                "label": "Vie 27 Ago - Dom 12 Sep",
                "outbound_date": "2027-08-27",
                "return_date": "2027-09-12",
                "google_flights_url": "https://www.google.com/travel/flights?curr=MXN&gl=MX&hl=es",
                "cheapest_tab_price": 17993.0,
                "cheapest_stops": "Directo",
                "cheapest_duration": "10 h 35 min",
                "origin_airport": str(dest_meta.get("origin") or "MEX").split(",")[0].strip().upper(),
                "destination_airport": str(dest_meta.get("destination") or "MAD").split(",")[0].strip().upper(),
                "is_self_transfer": False,
                "best_tab_price": 17993.0,
                "best_stops": "Directo",
                "best_duration": "10 h 35 min",
                "best_origin_airport": str(dest_meta.get("origin") or "MEX").split(",")[0].strip().upper(),
                "best_destination_airport": str(dest_meta.get("destination") or "MAD").split(",")[0].strip().upper(),
                "trigger_type": "TEST_DIGEST",
            }
        ]

    html_card = format_consolidated_destination_digest(
        destination_meta=dest_meta,
        triggered_slots=triggered_preview,
        is_test=True,
    )
    ok, detail = send_telegram_message(html_card)
    return ok, detail, html_card
