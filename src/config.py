"""
Centralized configuration, environment variable loader, and stealth constants
for FlightPulse Matrix v4.0.
"""

from __future__ import annotations

import logging
import os
import socket
import sys
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    env_path = Path(".env")
    if env_path.is_file():
        for _line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            _stripped = _line.strip()
            if _stripped and not _stripped.startswith("#") and "=" in _stripped:
                _k, _v = _stripped.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip().strip("'\""))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("flightpulse.config")


def _get_setting(key: str, default: str = "") -> str:
    """
    Retrieve configuration setting from environment variables first,
    then fall back to Streamlit secrets if available.
    """
    env_val = os.getenv(key)
    if env_val is not None and env_val.strip() != "":
        return env_val.strip()

    if "streamlit" in sys.modules:
        try:
            import streamlit as st

            if hasattr(st, "secrets") and key in st.secrets:
                val = st.secrets[key]
                if val is not None:
                    return str(val).strip()
        except Exception:
            pass

    return default


# Database & External Services
DATABASE_URL: str = _get_setting("DATABASE_URL", "")
TELEGRAM_BOT_TOKEN: str = _get_setting("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = _get_setting("TELEGRAM_CHAT_ID", "")
APP_PASSWORD: str = _get_setting("APP_PASSWORD", "flightpulse-matrix-2026")

# Matrix 10x10 Capacity Limits & Sharded Dispatcher Settings
MAX_DESTINATIONS: int = int(_get_setting("MAX_DESTINATIONS", "10"))
MAX_SLOTS_PER_DESTINATION: int = int(_get_setting("MAX_SLOTS_PER_DESTINATION", "10"))
MAX_TOTAL_SLOTS: int = MAX_DESTINATIONS * MAX_SLOTS_PER_DESTINATION
MAX_SLOTS_PER_RUN: int = int(_get_setting("MAX_SLOTS_PER_RUN", "20"))

# Google Flights Schedule Advance Booking Window (airline industry 330-337 day release)
GOOGLE_FLIGHTS_MAX_ADVANCE_DAYS: int = int(_get_setting("GOOGLE_FLIGHTS_MAX_ADVANCE_DAYS", "330"))

# Default Locale Settings
DEFAULT_CURRENCY: str = _get_setting("DEFAULT_CURRENCY", "MXN")
DEFAULT_GL: str = _get_setting("DEFAULT_GL", "MX")
DEFAULT_HL: str = _get_setting("DEFAULT_HL", "es")

# Proxy Configuration
SOCKS5_PROXY: str = _get_setting("SOCKS5_PROXY", "")
RAW_PROXY_LIST: str = _get_setting("PROXY_LIST", "")

# Browser & Stealth Settings
HEADLESS_MODE: bool = _get_setting("HEADLESS_MODE", "true").lower() in ("true", "1", "yes")
NAVIGATION_TIMEOUT_MS: int = int(_get_setting("NAVIGATION_TIMEOUT_MS", "35000"))
CHEAPEST_TAB_WAIT_MS: int = int(_get_setting("CHEAPEST_TAB_WAIT_MS", "1500"))

# Google SOCS Consent Bypass Cookie (Pre-accepted consent token for es/MX/global)
GOOGLE_SOCS_COOKIE_VALUE: str = "CAISHAgBEhJnd3NfMjAyNDA1MTQtMF9SQzIaAmVzIAEaBgiA_LyaBg"

GOOGLE_CONSENT_COOKIES: list[dict[str, str]] = [
    {
        "name": "SOCS",
        "value": GOOGLE_SOCS_COOKIE_VALUE,
        "domain": ".google.com",
        "path": "/",
    },
    {
        "name": "CONSENT",
        "value": "YES+cb.20240514-07-p0.es+FX+999",
        "domain": ".google.com",
        "path": "/",
    },
]

# Bandwidth & Telemetry Blocking
BLOCKED_RESOURCE_TYPES: set[str] = {"image", "media", "font"}

BLOCKED_TELEMETRY_DOMAINS: tuple[str, ...] = (
    "google-analytics.com",
    "googletagmanager.com",
    "doubleclick.net",
    "googlesyndication.com",
    "googleadservices.com",
    "play.google.com/log",
    "www.google.com/log",
    "adservice.google.com",
    "apis.google.com",
    "ogs.google.com",
    "ssl.gstatic.com/gb/images",
    "fonts.gstatic.com",
    "fonts.googleapis.com",
    "youtube.com",
    "ytimg.com",
)

# Self-Transfer / Separate Tickets Detection Keywords (Spanish & English)
SELF_TRANSFER_KEYWORDS: tuple[str, ...] = (
    "transferencia por cuenta propia",
    "self-transfer",
    "self transfer",
    "boletos separados",
    "separate tickets",
    "cambio de aeropuerto",
    "billetes separados",
    "auto-transferencia",
)

# Common IATA Airlines for UI Helpers
COMMON_AIRLINES: dict[str, str] = {
    "AM": "Aeroméxico (AM)",
    "IB": "Iberia (IB)",
    "Y4": "Volaris (Y4)",
    "VB": "Viva Aerobus (VB)",
    "AA": "American Airlines (AA)",
    "UA": "United Airlines (UA)",
    "DL": "Delta Air Lines (DL)",
    "AF": "Air France (AF)",
    "LH": "Lufthansa (LH)",
    "BA": "British Airways (BA)",
    "KL": "KLM (KL)",
    "TK": "Turkish Airlines (TK)",
    "AV": "Avianca (AV)",
    "CM": "Copa Airlines (CM)",
    "LA": "LATAM Airlines (LA)",
    "AC": "Air Canada (AC)",
    "UX": "Air Europa (UX)",
    "EK": "Emirates (EK)",
    "QR": "Qatar Airways (QR)",
    "NH": "ANA (NH)",
    "JL": "Japan Airlines (JL)",
}


def is_proxy_reachable(proxy_url: str, timeout: float = 1.5) -> bool:
    """
    Verify if a proxy endpoint's host:port TCP socket is accepting connections.
    Particularly useful for checking if local Cloudflare WARP (127.0.0.1:40000) is active.
    """
    if not proxy_url:
        return False
    try:
        parsed = urlparse(proxy_url)
        host = parsed.hostname
        port = parsed.port
        if not host or not port:
            return False
        # Only perform strict pre-check on localhost/loopback proxies so remote proxies
        # with firewall rules aren't prematurely discarded unless localhost is down.
        if host in ("127.0.0.1", "localhost", "::1"):
            with socket.create_connection((host, port), timeout=timeout):
                return True
        return True
    except OSError:
        return False


def get_proxy_pool() -> list[Optional[str]]:
    """
    Build an ordered list of available proxy servers from PROXY_LIST and SOCKS5_PROXY.
    Always appends `None` (direct connection) or ensures `None` is available as a fallback.
    """
    candidates: list[str] = []

    if RAW_PROXY_LIST:
        for item in RAW_PROXY_LIST.replace("\n", ",").split(","):
            cleaned = item.strip()
            if not cleaned:
                continue
            # Auto-convert raw "ip:port:user:pass" or "ip:port" export format
            if not cleaned.startswith(("http://", "https://", "socks5://", "socks4://")):
                parts = cleaned.split(":")
                if len(parts) == 4:
                    ip, port, user, pwd = parts
                    cleaned = f"http://{user}:{pwd}@{ip}:{port}"
                elif len(parts) == 2:
                    ip, port = parts
                    cleaned = f"http://{ip}:{port}"

            if cleaned and cleaned not in candidates:
                candidates.append(cleaned)

    if SOCKS5_PROXY and SOCKS5_PROXY not in candidates:
        candidates.append(SOCKS5_PROXY)

    verified_proxies: list[Optional[str]] = []
    for proxy in candidates:
        if is_proxy_reachable(proxy):
            verified_proxies.append(proxy)
        else:
            logger.warning("Skipping unreachable local proxy: %s", proxy)

    if not verified_proxies:
        return [None]

    return verified_proxies


def test_proxy_connection(proxy_url: Optional[str] = None, timeout: float = 6.0) -> dict[str, Any]:
    """
    Test connectivity, latency, public egress IP, and Google Flights accessibility through a proxy.
    Returns:
      {
        "proxy_url": str,
        "is_direct": bool,
        "success": bool,
        "ip": str,
        "latency_ms": int,
        "google_flights_status": int | None,
        "error": str | None,
      }
    """
    import time

    start_t = time.perf_counter()
    res: dict[str, Any] = {
        "proxy_url": proxy_url or "Conexión Directa",
        "is_direct": proxy_url is None,
        "success": False,
        "ip": "Desconocido",
        "latency_ms": 0,
        "google_flights_status": None,
        "error": None,
    }

    try:
        try:
            from curl_cffi import requests as curl_req

            ip_resp = curl_req.get("https://api.ipify.org?format=json", proxy=proxy_url, timeout=timeout)
            if ip_resp.status_code == 200:
                res["ip"] = ip_resp.json().get("ip", "Desconocido")

            gf_resp = curl_req.get(
                "https://www.google.com/travel/flights?curr=MXN&gl=MX&hl=es",
                proxy=proxy_url,
                impersonate="chrome",
                timeout=timeout,
            )
            res["google_flights_status"] = gf_resp.status_code
            res["latency_ms"] = int((time.perf_counter() - start_t) * 1000)
            res["success"] = gf_resp.status_code == 200
        except ImportError:
            import requests as std_req

            proxies_dict = {"http": proxy_url, "https": proxy_url} if proxy_url else None
            ip_resp = std_req.get("https://api.ipify.org?format=json", proxies=proxies_dict, timeout=timeout)
            if ip_resp.status_code == 200:
                res["ip"] = ip_resp.json().get("ip", "Desconocido")
            res["latency_ms"] = int((time.perf_counter() - start_t) * 1000)
            res["success"] = True
    except Exception as exc:
        res["error"] = str(exc)
        res["latency_ms"] = int((time.perf_counter() - start_t) * 1000)

    return res


def get_flight_window_status(outbound_date_val: Any, return_date_val: Any = None) -> dict[str, Any]:
    """
    Evaluate whether a flight (and its return date for round trips) is within the Google Flights
    330-day advance schedule release window.

    Returns a dict with:
      - status: 'ACTIVE_WINDOW' | 'FUTURE_RELEASE' | 'EXPIRED' | 'UNKNOWN'
      - is_searchable: bool
      - days_until_flight: int
      - days_until_release: int
      - release_date: date | None
      - label: str
      - badge: str
    """
    from datetime import date, datetime, timedelta

    if not outbound_date_val:
        return {
            "status": "UNKNOWN",
            "is_searchable": False,
            "days_until_flight": 0,
            "days_until_release": 0,
            "release_date": None,
            "label": "Fecha sin definir",
            "badge": "Desconocido",
        }

    try:
        def _parse_d(v: Any) -> Optional[date]:
            if not v:
                return None
            if isinstance(v, (date, datetime)):
                return v if isinstance(v, date) else v.date()
            clean = str(v).strip()[:10]
            return datetime.strptime(clean, "%Y-%m-%d").date()

        out_d = _parse_d(outbound_date_val)
        ret_d = _parse_d(return_date_val)
        if out_d is None:
            return {
                "status": "UNKNOWN",
                "is_searchable": False,
                "days_until_flight": 0,
                "days_until_release": 0,
                "release_date": None,
                "label": "Fecha inválida",
                "badge": "Desconocido",
            }

        effective_d = max(out_d, ret_d) if ret_d else out_d

        today = date.today()
        days_until_flight = (out_d - today).days
        days_until_effective = (effective_d - today).days

        if days_until_flight < 0:
            return {
                "status": "EXPIRED",
                "is_searchable": False,
                "days_until_flight": days_until_flight,
                "days_until_release": 0,
                "release_date": None,
                "label": "Fecha pasada",
                "badge": "Expirada",
            }
        elif days_until_effective > GOOGLE_FLIGHTS_MAX_ADVANCE_DAYS:
            days_until_release = days_until_effective - GOOGLE_FLIGHTS_MAX_ADVANCE_DAYS
            release_date = today + timedelta(days=days_until_release)
            rel_str = release_date.strftime("%d/%m/%Y")
            return {
                "status": "FUTURE_RELEASE",
                "is_searchable": False,
                "days_until_flight": days_until_flight,
                "days_until_release": days_until_release,
                "release_date": release_date,
                "label": f"En espera de liberación (Apertura: {rel_str} — faltan {days_until_release}d)",
                "badge": f"En espera (libera {rel_str})",
            }
        else:
            return {
                "status": "ACTIVE_WINDOW",
                "is_searchable": True,
                "days_until_flight": days_until_flight,
                "days_until_release": 0,
                "release_date": today,
                "label": f"En ventana activa ({days_until_flight}d para el vuelo)",
                "badge": "En ventana activa",
            }
    except Exception:
        return {
            "status": "UNKNOWN",
            "is_searchable": True,
            "days_until_flight": 0,
            "days_until_release": 0,
            "release_date": None,
            "label": "Fecha no verificable",
            "badge": "Desconocido",
        }
