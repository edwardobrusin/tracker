"""
Centralized configuration, environment variable loader, and stealth constants
for FlightPulse Matrix v5.0.
"""

from __future__ import annotations

import base64
import collections
import concurrent.futures
import hashlib
import logging
import os
import random
import re
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Any, Optional, Sequence
from urllib.parse import unquote, urlparse

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

# Matrix 50x10 Capacity Limits & Sharded Dispatcher Settings
MAX_DESTINATIONS: int = int(_get_setting("MAX_DESTINATIONS", "50"))
MAX_SLOTS_PER_DESTINATION: int = int(_get_setting("MAX_SLOTS_PER_DESTINATION", "10"))
MAX_TOTAL_SLOTS: int = MAX_DESTINATIONS * MAX_SLOTS_PER_DESTINATION
MAX_SLOTS_PER_RUN: int = int(_get_setting("MAX_SLOTS_PER_RUN", "500"))
DEFAULT_CHECK_INTERVAL_MINUTES: int = int(_get_setting("DEFAULT_CHECK_INTERVAL_MINUTES", "60"))
DUE_GRACE_MINUTES: int = int(_get_setting("DUE_GRACE_MINUTES", "5"))

# Google Flights Schedule Advance Booking Window (airline industry 330-337 day release)
GOOGLE_FLIGHTS_MAX_ADVANCE_DAYS: int = int(_get_setting("GOOGLE_FLIGHTS_MAX_ADVANCE_DAYS", "330"))

# Default Locale & Multi-GL Competition Settings
DEFAULT_CURRENCY: str = _get_setting("DEFAULT_CURRENCY", "MXN")
DEFAULT_GL: str = _get_setting("DEFAULT_GL", "MX")
DEFAULT_HL: str = _get_setting("DEFAULT_HL", "es")
DEFAULT_GL_POOL: tuple[str, ...] = ("MX", "IT", "DE")
SUPPORTED_GL_MARKETS: tuple[str, ...] = ("MX", "IT", "DE", "ES", "US")


def parse_gl_pool(
    raw_pool: Any = None,
    fallback: Sequence[str] = DEFAULT_GL_POOL,
) -> list[str]:
    """
    Parse and deduplicate a Multi-GL pool (string like 'MX,IT,DE' or sequence)
    preserving pool priority order. Falls back to `DEFAULT_GL_POOL` when empty.
    """
    tokens: list[str] = []
    if isinstance(raw_pool, str):
        tokens = [t.strip().upper() for t in re.split(r"[,;\s]+", raw_pool) if t.strip()]
    elif isinstance(raw_pool, (list, tuple, set)):
        for item in raw_pool:
            for t in re.split(r"[,;\s]+", str(item)):
                if t.strip():
                    tokens.append(t.strip().upper())

    deduped: list[str] = []
    for tok in tokens:
        if re.match(r"^[A-Z]{2}$", tok) and tok not in deduped:
            deduped.append(tok)

    if not deduped:
        return [str(code).strip().upper() for code in fallback if str(code).strip()]
    return deduped

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


_PROXY_TUNNEL_CACHE_TTL_SECONDS: float = 300.0
_PROXY_TUNNEL_CACHE: dict[str, tuple[float, bool, Optional[str]]] = {}
_PROXY_TUNNEL_LOCK = threading.Lock()


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


def verify_proxy_tunnel(proxy_url: str, timeout: float = 2.0) -> tuple[bool, Optional[str]]:
    """
    Verify whether an HTTP/HTTPS proxy tunnel can establish a CONNECT handshake
    to Google Flights (`www.google.com:443`) without being blocked by a local firewall
    (such as FortiGate Proxy.HTTP 403) or rejecting credentials.
    Uses a 300s thread-safe cache per proxy URL.
    """
    if not proxy_url:
        return True, None

    now = time.monotonic()
    with _PROXY_TUNNEL_LOCK:
        cached = _PROXY_TUNNEL_CACHE.get(proxy_url)
        if cached and (now - cached[0]) < _PROXY_TUNNEL_CACHE_TTL_SECONDS:
            return cached[1], cached[2]

    ok = False
    reason: Optional[str] = None
    try:
        parsed = urlparse(proxy_url)
        scheme = (parsed.scheme or "http").lower()
        host = parsed.hostname
        port = parsed.port
        if not host or not port:
            return False, "URL de proxy inválida"

        if scheme.startswith("socks"):
            with socket.create_connection((host, port), timeout=timeout):
                ok, reason = True, None
        else:
            with socket.create_connection((host, port), timeout=timeout) as sock:
                sock.settimeout(timeout)
                headers = [
                    "CONNECT www.google.com:443 HTTP/1.1",
                    "Host: www.google.com:443",
                ]
                if parsed.username is not None:
                    user = unquote(parsed.username)
                    pwd = unquote(parsed.password or "")
                    token = base64.b64encode(f"{user}:{pwd}".encode("utf-8")).decode("ascii")
                    headers.append(f"Proxy-Authorization: Basic {token}")
                req_bytes = ("\r\n".join(headers) + "\r\n\r\n").encode("ascii")
                sock.sendall(req_bytes)
                raw_resp = sock.recv(2048).decode("utf-8", errors="replace")

            first_line = raw_resp.splitlines()[0] if raw_resp else ""
            lower_resp = raw_resp.lower()
            if " 200 " in first_line or first_line.endswith(" 200"):
                ok, reason = True, None
            elif "fortigate" in lower_resp or "proxy.http" in lower_resp or "403" in first_line:
                ok, reason = False, "Firewall local bloqueó Proxy.HTTP (FortiGate 403)"
            elif "402" in first_line:
                ok, reason = False, "Cuota de proxy agotada (HTTP 402)"
            elif "407" in first_line:
                ok, reason = False, "Credenciales de proxy rechazadas (HTTP 407)"
            else:
                ok, reason = False, f"Túnel rechazado ({first_line.strip() or 'sin respuesta'})"
    except OSError as exc:
        ok, reason = False, str(exc)

    with _PROXY_TUNNEL_LOCK:
        _PROXY_TUNNEL_CACHE[proxy_url] = (now, ok, reason)

    return ok, reason


def parse_proxy_list_raw(raw_list: str) -> list[str]:
    """Parse a comma/newline-separated proxy list into normalized proxy URLs."""
    candidates: list[str] = []
    if not raw_list:
        return candidates
    for item in raw_list.replace("\n", ",").split(","):
        cleaned = item.strip()
        if not cleaned:
            continue
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
    return candidates


def get_proxy_account(proxy_url: Optional[str]) -> str:
    """Return a non-secret account/channel identifier for a proxy URL."""
    if proxy_url is None:
        return "direct"
    try:
        parsed = urlparse(proxy_url)
        host = parsed.hostname or ""
        port = parsed.port
        if parsed.scheme.startswith("socks") and host in ("127.0.0.1", "localhost", "::1") and port == 40000:
            return "warp"
        if parsed.username:
            return unquote(parsed.username)
        return f"{host}:{port}" if port else (host or "proxy")
    except Exception:
        return "proxy"


def get_proxy_tier(proxy_url: Optional[str]) -> str:
    """Classify a network channel into tier1_warp, tier2_direct, or tier3_proxy."""
    if proxy_url is None:
        return "tier2_direct"
    if get_proxy_account(proxy_url) == "warp":
        return "tier1_warp"
    return "tier3_proxy"


def redact_account_for_log(account: str) -> str:
    """Mask proxy credential username so public logs never expose full account identifiers."""
    clean = str(account or "").strip()
    if not clean or clean in ("direct", "warp", "none"):
        return clean or "direct"
    if ":" in clean and "@" not in clean:
        # host:port fallback without username
        short_h = hashlib.sha256(clean.encode("utf-8")).hexdigest()[:6]
        return f"proxy#{short_h}"
    suffix = clean[-3:] if len(clean) > 3 else "***"
    return f"acct_****{suffix}"


def redact_proxy_for_log(proxy_url: Optional[str]) -> str:
    """
    Return a safe, secret-free identifier for a proxy URL suitable for public logs.
    Never exposes passwords, full usernames, or proxy IPs/ports.
    """
    if not proxy_url:
        return "direct"
    acct = get_proxy_account(proxy_url)
    if acct == "warp":
        return "warp"
    short_hash = hashlib.sha256(proxy_url.encode("utf-8")).hexdigest()[:6]
    masked_acct = redact_account_for_log(acct)
    return f"webshare:{masked_acct}#{short_hash}"


_PROXY_URL_CREDS_RE = re.compile(
    r"(https?|socks5h?|socks4)://[^\s:@/\"']+:[^\s@/\"']+@[^\s\"',;)\]]+",
    re.IGNORECASE,
)
_BARE_HOST_PORT_USER_RE = re.compile(
    r"\b\d{1,3}(?:\.\d{1,3}){3}:\d{2,5}(?::[^\s,;]+){0,2}\b"
)


def sanitize_log_text(text: Any) -> str:
    """
    Scrub proxy URLs with credentials (`scheme://user:pass@ip:port`), bare proxy IPs/ports,
    and known PROXY_LIST credentials from exception/log strings before printing.
    """
    raw = str(text or "")
    if not raw:
        return ""
    cleaned = _PROXY_URL_CREDS_RE.sub(r"\1://[REDACTED_PROXY]", raw)
    cleaned = _BARE_HOST_PORT_USER_RE.sub("[REDACTED_ENDPOINT]", cleaned)

    raw_list = _get_setting("PROXY_LIST", RAW_PROXY_LIST)
    if raw_list:
        for p in parse_proxy_list_raw(raw_list):
            if p in cleaned:
                cleaned = cleaned.replace(p, redact_proxy_for_log(p))
            try:
                parsed = urlparse(p)
                if parsed.password and parsed.password in cleaned:
                    cleaned = cleaned.replace(parsed.password, "***")
                if parsed.username and parsed.username in cleaned:
                    cleaned = cleaned.replace(parsed.username, redact_account_for_log(parsed.username))
                if parsed.hostname and parsed.hostname in cleaned:
                    cleaned = cleaned.replace(parsed.hostname, "[REDACTED_IP]")
            except Exception:
                pass
    return cleaned


def interleave_proxies_by_account(
    proxies: list[str],
    random_offset: bool = True,
    rng: Optional[random.Random] = None,
) -> list[str]:
    """
    Group proxies by credential username, interleave across accounts in round-robin
    (A_0, B_0, A_1, B_1, ...), and optionally apply a random starting offset.
    """
    if len(proxies) <= 1:
        return list(proxies)

    buckets: "collections.OrderedDict[str, list[str]]" = collections.OrderedDict()
    for p in proxies:
        acct = get_proxy_account(p)
        buckets.setdefault(acct, []).append(p)

    interleaved: list[str] = []
    account_lists = list(buckets.values())
    max_len = max(len(lst) for lst in account_lists)
    for idx in range(max_len):
        for lst in account_lists:
            if idx < len(lst):
                interleaved.append(lst[idx])

    if random_offset and len(interleaved) > 1:
        r = rng or random
        offset = r.randrange(len(interleaved))
        interleaved = interleaved[offset:] + interleaved[:offset]

    return interleaved


def get_proxy_pool(
    verify_tunnel: bool = False,
    random_offset: bool = True,
    include_direct: bool = True,
) -> list[Optional[str]]:
    """
    Build an ordered pool prioritizing zero-cost channels first:
      - Nivel 1: Cloudflare WARP (`SOCKS5_PROXY`)
      - Nivel 2: Direct (`None`)
      - Nivel 3: `PROXY_LIST` (Webshare), grouped by account, interleaved round-robin
                 with a random initial rotation offset per run.
    """
    raw_list = _get_setting("PROXY_LIST", RAW_PROXY_LIST)
    socks5_proxy = _get_setting("SOCKS5_PROXY", SOCKS5_PROXY)
    proxy_candidates = parse_proxy_list_raw(raw_list)

    pool: list[Optional[str]] = []

    # Nivel 1: Cloudflare WARP (SOCKS5_PROXY)
    if socks5_proxy:
        if verify_tunnel:
            warp_ok, warp_reason = verify_proxy_tunnel(socks5_proxy)
            if warp_ok:
                pool.append(socks5_proxy)
            elif warp_reason:
                logger.info(
                    "WARP SOCKS5_PROXY unavailable (%s); continuing to Nivel 2 Direct.",
                    sanitize_log_text(warp_reason),
                )
        elif is_proxy_reachable(socks5_proxy):
            pool.append(socks5_proxy)
        else:
            logger.debug("Skipping unreachable local WARP proxy: %s", redact_proxy_for_log(socks5_proxy))

    # Nivel 2: Direct connection
    if include_direct:
        pool.append(None)

    # Nivel 3: PROXY_LIST (interleaved by account with random rotation offset)
    verified_tier3: list[str] = []
    if not verify_tunnel:
        for proxy in proxy_candidates:
            if proxy == socks5_proxy:
                continue
            if is_proxy_reachable(proxy):
                verified_tier3.append(proxy)
            else:
                logger.warning("Skipping unreachable local proxy: %s", redact_proxy_for_log(proxy))
    elif proxy_candidates:
        tier3_targets = [p for p in proxy_candidates if p != socks5_proxy]
        if tier3_targets:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, len(tier3_targets))) as executor:
                results = list(executor.map(verify_proxy_tunnel, tier3_targets))
            blocked_reasons: set[str] = set()
            for proxy, (ok, reason) in zip(tier3_targets, results):
                if ok:
                    verified_tier3.append(proxy)
                elif reason:
                    blocked_reasons.add(sanitize_log_text(reason))
            if not verified_tier3 and blocked_reasons:
                logger.warning(
                    "None of the %d configured Tier-3 proxies could establish a tunnel (%s).",
                    len(tier3_targets),
                    "; ".join(sorted(blocked_reasons)[:2]),
                )

    pool.extend(interleave_proxies_by_account(verified_tier3, random_offset=random_offset))

    if not pool:
        return [None]
    return pool


def test_proxy_connection(proxy_url: Optional[str] = None, timeout: float = 5.0) -> dict[str, Any]:
    """
    Test connectivity, latency, public egress IP, and Google Flights accessibility through a proxy.
    Detects local FortiGate Proxy.HTTP 403 blocks with a human-readable diagnostic message.
    """
    start_t = time.perf_counter()
    res: dict[str, Any] = {
        "proxy_url": redact_proxy_for_log(proxy_url) if proxy_url else "Conexión Directa",
        "is_direct": proxy_url is None,
        "success": False,
        "ip": "Desconocido",
        "latency_ms": 0,
        "google_flights_status": None,
        "error": None,
    }

    if proxy_url:
        tunnel_ok, tunnel_reason = verify_proxy_tunnel(proxy_url, timeout=min(3.0, timeout))
        if not tunnel_ok and tunnel_reason and ("FortiGate" in tunnel_reason or "403" in tunnel_reason):
            res["latency_ms"] = int((time.perf_counter() - start_t) * 1000)
            res["error"] = "Bloqueado por firewall local (FortiGate Proxy.HTTP) — disponible en GitHub Actions"
            return res

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
        err_str = str(exc)
        if "CONNECT tunnel failed, response 403" in err_str or "FortiGate" in err_str:
            res["error"] = "Bloqueado por firewall local (FortiGate Proxy.HTTP) — disponible en GitHub Actions"
        else:
            res["error"] = sanitize_log_text(err_str)
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
