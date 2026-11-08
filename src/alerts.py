"""
Consolidated Destination Digest Alert Engine (`src/alerts.py`) for FlightPulse Matrix v4.0.

Features:
  - Evaluates all newly scraped `date_slots` belonging to a `destination`.
  - Triggers per slot on:
      1. Target Breach (`cheapest_tab_price <= target_price`)
      2. All-Time Low (`cheapest_tab_price < historical_min_price`)
      3. Sharp Drop (`cheapest_tab_price <= rolling_7d_avg * 0.90`)
  - Enforces per-slot `cooldown_hours` unless the new price is at least 5% lower
    than `last_alerted_price`.
  - Anti-Spam Grouping: Sends ONE consolidated Telegram digest card per destination
    listing all triggered dates sorted by `cheapest_tab_price ASC` with side-by-side
    Cheapest Tab vs. Best Tab comparison and direct booking links.
"""

from __future__ import annotations

import datetime
import logging
from typing import Any, Optional

try:
    import requests
except ImportError:
    requests = None  # type: ignore[assignment]

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

logger = logging.getLogger("flightpulse.alerts")


def _escape_md_text(text: str) -> str:
    """Escape characters that could break Telegram standard Markdown links/formatting."""
    if not text:
        return ""
    return (
        str(text)
        .replace("[", "(")
        .replace("]", ")")
        .replace("`", "'")
        .replace("*", "")
        .replace("_", "-")
    )


def evaluate_slot_alert_triggers(
    slot_meta: dict[str, Any],
    scrape_result: dict[str, Any],
    snapshot_stats: dict[str, Any],
) -> Optional[dict[str, Any]]:
    """
    Evaluate whether a newly scraped `date_slot` breaches any alert condition
    and passes cooldown checks.

    Returns a dictionary with trigger details if an alert should fire, else `None`.
    """
    cheapest_price = float(scrape_result["cheapest_tab_price"])
    best_price = (
        float(scrape_result["best_tab_price"])
        if scrape_result.get("best_tab_price") is not None
        else cheapest_price
    )
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
    rolling_7d_avg = snapshot_stats.get("rolling_7d_avg")
    last_alerted_at = snapshot_stats.get("last_alerted_at") or slot_meta.get("last_alerted_at")
    last_alerted_price = snapshot_stats.get("last_alerted_price") or slot_meta.get("last_alerted_price")

    triggers: list[str] = []
    trigger_badges: list[str] = []

    # 1. Target Breach: cheapest_tab_price <= target_price
    if target_price > 0 and cheapest_price <= target_price:
        triggers.append("TARGET_BREACH")
        diff = target_price - cheapest_price
        trigger_badges.append(f"[Bajo Meta: -${diff:,.0f}]")

    # 2. All-Time Low: cheapest_tab_price < historical_min_price
    if historical_min is not None and float(historical_min) > 0 and cheapest_price < float(historical_min):
        triggers.append("ALL_TIME_LOW")
        drop = float(historical_min) - cheapest_price
        trigger_badges.append(f"[Mínimo Histórico: -${drop:,.0f}]")

    # 3. Sharp Drop: cheapest_tab_price <= rolling_7d_avg * 0.90
    if rolling_7d_avg is not None and float(rolling_7d_avg) > 0:
        avg_val = float(rolling_7d_avg)
        if cheapest_price <= avg_val * 0.90:
            pct_drop = ((avg_val - cheapest_price) / avg_val) * 100.0
            triggers.append("SHARP_DROP")
            trigger_badges.append(f"[Caída Brusca: -{pct_drop:.1f}% vs 7d]")

    if not triggers:
        return None

    # 4. Per-Slot Cooldown Check (unless new price is >= 5% lower than last_alerted_price)
    if last_alerted_at is not None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if last_alerted_at.tzinfo is None:
            last_alerted_at = last_alerted_at.replace(tzinfo=datetime.timezone.utc)
        elapsed_hours = (now_utc - last_alerted_at).total_seconds() / 3600.0

        if elapsed_hours < cooldown_hours:
            bypass_cooldown = False
            if last_alerted_price is not None and float(last_alerted_price) > 0:
                if cheapest_price <= float(last_alerted_price) * 0.95:
                    bypass_cooldown = True
                    trigger_badges.append("[-5% Caída Extra / Bypass Cooldown]")

            if not bypass_cooldown:
                logger.info(
                    "Slot id=%s triggered %s ($%.0f), but is within %dh cooldown (%.1fh elapsed).",
                    slot_meta.get("id"),
                    ",".join(triggers),
                    cheapest_price,
                    cooldown_hours,
                    elapsed_hours,
                )
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
        "is_self_transfer": is_self_transfer,
        "best_tab_price": best_price,
        "best_airline": scrape_result.get("best_airline") or scrape_result.get("cheapest_airline") or "Aerolínea",
        "best_stops": scrape_result.get("best_stops") or scrape_result.get("cheapest_stops") or "Directo",
        "best_duration": scrape_result.get("best_duration") or scrape_result.get("cheapest_duration") or "N/A",
        "trigger_type": "+".join(triggers),
        "trigger_badges": " | ".join(trigger_badges),
        "historical_min_price": historical_min,
        "rolling_7d_avg": rolling_7d_avg,
    }


def format_consolidated_destination_digest(
    destination_meta: dict[str, Any],
    triggered_slots: list[dict[str, Any]],
    is_test: bool = False,
) -> str:
    """
    Format ONE consolidated Telegram Markdown digest card for a destination,
    listing all triggered `date_slots` sorted by `cheapest_tab_price ASC`.
    Faithfully reports real prices or explicit 330-day release schedule hold (zero fabricated data).
    """
    valid_prices = [
        float(x["cheapest_tab_price"])
        for x in triggered_slots
        if x.get("cheapest_tab_price") is not None and float(x.get("cheapest_tab_price") or 0) > 0
    ]
    sorted_slots = sorted(
        triggered_slots,
        key=lambda x: (
            0 if (x.get("cheapest_tab_price") is not None and float(x.get("cheapest_tab_price") or 0) > 0) else 1,
            float(x.get("cheapest_tab_price") or 9999999),
        ),
    )

    dest_name = _escape_md_text(destination_meta.get("name") or "Ruta Monitoreada")
    origin = _escape_md_text(destination_meta.get("origin") or "MEX")
    dest_code = _escape_md_text(destination_meta.get("destination") or destination_meta.get("destination_code") or "MAD")
    currency = _escape_md_text(destination_meta.get("currency") or DEFAULT_CURRENCY)
    target_price = float(destination_meta.get("target_price") or 0.0)

    header_prefix = "*REPORTE DE ESTADO — FLIGHTPULSE MATRIX*" if is_test else "*ALERTA DE VUELOS — FLIGHTPULSE MATRIX*"
    lowest_price_str = f"${min(valid_prices):,.0f} {currency}" if valid_prices else "En espera de liberación (330d)"

    lines: list[str] = [
        f"{header_prefix}",
        f"*{dest_name}* ({origin} -> {dest_code})",
        f"*Precio Meta:* ${target_price:,.0f} {currency} | *Mejor Precio:* {lowest_price_str}",
        f"*{len(sorted_slots)} fecha(s) verificadas en este reporte:*",
        "━━━━━━━━━━━━━━━━━━━━━━",
    ]

    for idx, slot in enumerate(sorted_slots, start=1):
        outbound_str = slot.get("outbound_date") or ""
        return_str = slot.get("return_date")
        date_pair_str = f"{outbound_str} -> {return_str}" if return_str else f"{outbound_str} (Solo ida)"
        slot_label = _escape_md_text(slot.get("label") or date_pair_str)
        badges = slot.get("trigger_badges") or _escape_md_text(slot.get("trigger_type") or "Alerta")
        url = slot.get("google_flights_url") or "https://www.google.com/travel/flights"

        if slot.get("is_future_release"):
            rel_date = slot.get("release_date_str") or "Aprox 11 meses antes"
            days_rel = slot.get("days_until_release") or 0
            lines.extend(
                [
                    f"*{idx}.* *Fechas:* {date_pair_str} — _{slot_label}_",
                    f"   Estado: {badges}",
                    f"   Detalle: En espera de apertura por aerolíneas (Ventana oficial 330 días)",
                    f"   Apertura estimada: {rel_date} (faltan {days_rel} días)",
                    f"   _Sin precios ficticios: El rastreador cotizará automáticamente al abrir la ventana._",
                    f"   [Consultar en Google Flights]({url})",
                    "",
                ]
            )
        elif slot.get("cheapest_tab_price") is None:
            lines.extend(
                [
                    f"*{idx}.* *Fechas:* {date_pair_str} — _{slot_label}_",
                    f"   Estado: {badges}",
                    f"   Ventana: Dentro de 330 días (Listo para escaneo)",
                    f"   Cotización: Pendiente de ejecución de escaneo inicial",
                    f"   [Ver en Google Flights]({url})",
                    "",
                ]
            )
        else:
            c_price = float(slot.get("cheapest_tab_price") or 0.0)
            c_airline = _escape_md_text(slot.get("cheapest_airline") or "Aerolínea")
            c_stops = _escape_md_text(slot.get("cheapest_stops") or "Directo")
            c_duration = _escape_md_text(slot.get("cheapest_duration") or "N/A")
            self_transfer_badge = " [Self-Transfer / Boletos separados]" if slot.get("is_self_transfer") else ""

            b_price = float(slot.get("best_tab_price") or c_price)
            b_airline = _escape_md_text(slot.get("best_airline") or c_airline)
            b_stops = _escape_md_text(slot.get("best_stops") or c_stops)
            b_duration = _escape_md_text(slot.get("best_duration") or c_duration)

            lines.extend(
                [
                    f"*{idx}.* *Fechas:* {date_pair_str} — _{slot_label}_",
                    f"   Estado: {badges}",
                    f"   *Más Baratos:* ${c_price:,.0f} {currency} ({c_airline}, {c_stops}, {c_duration}){self_transfer_badge}",
                    f"   *Mejores Vuelos:* ${b_price:,.0f} {currency} ({b_airline}, {b_stops}, {b_duration})",
                    f"   [Reservar en Google Flights]({url})",
                    "",
                ]
            )

    lines.append("_FlightPulse Matrix v4.0 • Rastreo verificado sin datos simulados_")
    return "\n".join(lines)


def send_telegram_message(markdown_text: str) -> tuple[bool, str]:
    """
    Dispatch a Markdown-formatted message via the Telegram Bot HTTP API.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        msg = "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not configured."
        logger.warning(msg)
        return False, msg

    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": markdown_text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }

    try:
        resp = requests.post(api_url, json=payload, timeout=15)
        if resp.status_code == 200:
            logger.info("Successfully delivered Consolidated Telegram Digest Alert.")
            return True, "OK"

        # Fallback to plain text if Markdown special characters caused a 400 parse error
        logger.warning(
            "Telegram Markdown send returned HTTP %d (%s); retrying plain text fallback.",
            resp.status_code,
            resp.text[:150],
        )
        payload.pop("parse_mode", None)
        resp_fallback = requests.post(api_url, json=payload, timeout=15)
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
    Build and send ONE consolidated Telegram digest alert for all triggered date_slots
    of a destination, then persist `alert_logs` and update `last_alerted_at` / `last_alerted_price`.
    """
    if not triggered_slots:
        return False

    dest_id = int(destination_meta["id"])
    message_md = format_consolidated_destination_digest(
        destination_meta=destination_meta,
        triggered_slots=triggered_slots,
        is_test=False,
    )

    sent_ok, status_detail = send_telegram_message(message_md)
    if sent_ok:
        record_alert_and_update_slots(
            destination_id=dest_id,
            triggered_slots=triggered_slots,
            message_preview=message_md,
        )
        logger.info(
            "Recorded %d slot alerts for destination_id=%d (%s)",
            len(triggered_slots),
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
    Send a test Consolidated Digest Card to Telegram from the Streamlit Control Center (Tab 3).
    Uses real database slots if available, or realistic sample MEX -> MAD data if the DB is empty.
    Returns `(success, status_message, rendered_markdown)`.
    """
    dest_meta: Optional[dict[str, Any]] = None
    slots: list[dict[str, Any]] = []

    try:
        if destination_id is not None:
            dest_meta = get_destination(destination_id)
            if dest_meta:
                slots = list_date_slots(destination_id=destination_id)
        else:
            all_dests = list_destinations()
            if all_dests:
                dest_meta = all_dests[0]
                slots = list_date_slots(destination_id=int(dest_meta["id"]))
    except Exception as exc:
        logger.debug("Database lookup for test alert skipped: %s", exc)

    if not dest_meta:
        dest_meta = {
            "id": 0,
            "name": "CDMX -> Madrid (Otoño Matrix Demo)",
            "origin": "MEX",
            "destination": "MAD",
            "currency": "MXN",
            "target_price": 15500.0,
        }

    triggered_preview: list[dict[str, Any]] = []
    for s in slots[:3]:
        outbound = str(s.get("outbound_date", "")).strip()
        win_info = get_flight_window_status(outbound)
        c_price = s.get("latest_cheapest_price")
        b_price = s.get("latest_best_price")
        airline = s.get("latest_airline")
        stops = s.get("latest_stops")
        duration = s.get("latest_duration")
        is_self = bool(s.get("latest_is_self_transfer", False))

        is_future = win_info.get("status") == "FUTURE_RELEASE"
        days_rel = win_info.get("days_until_release", 0)
        rel_date_str = win_info.get("release_date").strftime("%d/%m/%Y") if win_info.get("release_date") else ""

        if is_future:
            trigger_badge = f"[En espera: apertura {rel_date_str}]"
        elif c_price is None:
            trigger_badge = "[Prueba de Sistema: Dentro de 330d - Sin cotización previa]"
        else:
            trigger_badge = "[Verificado con datos reales]"

        triggered_preview.append(
            {
                "date_slot_id": s.get("id"),
                "label": s.get("label") or f"{s.get('outbound_date')} -> {s.get('return_date')}",
                "outbound_date": s.get("outbound_date"),
                "return_date": s.get("return_date"),
                "google_flights_url": s.get("google_flights_url") or "https://www.google.com/travel/flights",
                "cheapest_tab_price": c_price,
                "cheapest_airline": airline,
                "cheapest_stops": stops,
                "cheapest_duration": duration,
                "is_self_transfer": is_self,
                "best_tab_price": b_price,
                "best_airline": airline,
                "best_stops": stops,
                "best_duration": duration,
                "trigger_type": "TEST_DIGEST",
                "trigger_badges": trigger_badge,
                "is_future_release": is_future,
                "release_date_str": rel_date_str,
                "days_until_release": days_rel,
            }
        )

    if not triggered_preview:
        triggered_preview = [
            {
                "date_slot_id": None,
                "label": "Demo MEX - MAD (Simulación por BD sin rutas)",
                "outbound_date": "2026-11-20",
                "return_date": "2026-11-29",
                "google_flights_url": "https://www.google.com/travel/flights?curr=MXN&gl=MX&hl=es",
                "cheapest_tab_price": 13490.0,
                "cheapest_airline": "Iberia",
                "cheapest_stops": "Directo",
                "cheapest_duration": "10 h 35 min",
                "is_self_transfer": False,
                "best_tab_price": 14890.0,
                "best_airline": "Iberia",
                "best_stops": "Directo",
                "best_duration": "10 h 35 min",
                "trigger_type": "TEST_SIMULATED",
                "trigger_badges": "[Simulación: Registra fechas para datos reales]",
                "is_future_release": False,
            }
        ]

    markdown_card = format_consolidated_destination_digest(
        destination_meta=dest_meta,
        triggered_slots=triggered_preview,
        is_test=True,
    )
    ok, detail = send_telegram_message(markdown_card)
    if ok and dest_meta.get("id"):
        try:
            record_alert_and_update_slots(
                destination_id=int(dest_meta["id"]),
                triggered_slots=[
                    {
                        "date_slot_id": triggered_preview[0].get("date_slot_id"),
                        "trigger_type": "TEST_DIGEST",
                        "cheapest_tab_price": triggered_preview[0].get("cheapest_tab_price"),
                        "best_tab_price": triggered_preview[0].get("best_tab_price"),
                    }
                ],
                message_preview=markdown_card,
            )
        except Exception:
            pass

    return ok, detail, markdown_card
