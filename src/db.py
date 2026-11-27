"""
Neon Serverless PostgreSQL Data Access Layer (psycopg v3) for FlightPulse Matrix v5.0.
Implements the 4-table hierarchical schema:
  destinations -> date_slots -> price_snapshots + alert_logs
Supports Multi-GL competition (`gl_pool`, `winning_gl`, `last_winning_gl`, `all_time_low_gl`),
transactional advisory locks for 6-shard concurrency, and RAM-buffered batch read/flush.
"""

from __future__ import annotations

import copy
import datetime
import logging
import sys
import threading
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional, Sequence, cast

if sys.version_info >= (3, 11):
    from typing import LiteralString
else:
    try:
        from typing_extensions import LiteralString
    except ImportError:
        LiteralString = str  # type: ignore[assignment,misc]

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb
else:
    try:
        import psycopg
        from psycopg.rows import dict_row
        from psycopg.types.json import Jsonb
    except ImportError:
        psycopg = None
        dict_row = None

        class Jsonb:  # type: ignore[no-redef]
            def __init__(self, val: Any) -> None:
                self.val = val

from src.config import (
    DATABASE_URL,
    DEFAULT_CHECK_INTERVAL_MINUTES,
    DEFAULT_CURRENCY,
    DEFAULT_GL,
    DEFAULT_GL_POOL,
    DEFAULT_HL,
    DUE_GRACE_MINUTES,
    MAX_DESTINATIONS,
    MAX_SLOTS_PER_DESTINATION,
    MAX_SLOTS_PER_RUN,
    MAX_TOTAL_SLOTS,
    get_flight_window_status,
    parse_gl_pool,
)
from src.tfs_builder import (
    FilterProfile,
    build_google_flights_url,
    normalize_iata_codes,
    parse_iata_list,
)

logger = logging.getLogger("flightpulse.db")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS destinations (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    origin VARCHAR(64) NOT NULL,
    destination VARCHAR(64) NOT NULL,
    origin_airports TEXT[] NOT NULL DEFAULT '{}',
    destination_airports TEXT[] NOT NULL DEFAULT '{}',
    trip_type VARCHAR(20) DEFAULT 'round-trip',
    currency VARCHAR(10) DEFAULT 'MXN',
    gl VARCHAR(10) DEFAULT 'MX',
    gl_pool VARCHAR(64) NOT NULL DEFAULT 'MX,IT,DE',
    hl VARCHAR(10) DEFAULT 'es',
    target_price NUMERIC(12, 2) NOT NULL,
    check_interval_minutes INTEGER DEFAULT 60,
    filter_config JSONB DEFAULT '{}',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS date_slots (
    id SERIAL PRIMARY KEY,
    destination_id INTEGER REFERENCES destinations(id) ON DELETE CASCADE,
    label TEXT,
    outbound_date VARCHAR(15) NOT NULL,
    return_date VARCHAR(15),
    google_flights_url TEXT NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    last_checked_at TIMESTAMPTZ,
    last_alerted_at TIMESTAMPTZ,
    last_alerted_price NUMERIC(12, 2),
    latest_cheapest_price NUMERIC(12, 2),
    latest_best_price NUMERIC(12, 2),
    latest_airline TEXT,
    latest_stops TEXT,
    latest_duration TEXT,
    latest_origin_airport VARCHAR(10),
    latest_destination_airport VARCHAR(10),
    latest_best_origin_airport VARCHAR(10),
    latest_best_destination_airport VARCHAR(10),
    latest_is_self_transfer BOOLEAN DEFAULT FALSE,
    last_winning_gl VARCHAR(8) NOT NULL DEFAULT 'MX',
    all_time_low_price NUMERIC(12, 2),
    all_time_low_gl VARCHAR(8),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS price_snapshots (
    id SERIAL PRIMARY KEY,
    date_slot_id INTEGER REFERENCES date_slots(id) ON DELETE CASCADE,
    destination_id INTEGER REFERENCES destinations(id) ON DELETE CASCADE,
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    cheapest_tab_price NUMERIC(12, 2) NOT NULL,
    best_tab_price NUMERIC(12, 2),
    cheapest_airline TEXT,
    cheapest_stops TEXT,
    cheapest_duration TEXT,
    cheapest_dep_arr TEXT,
    origin_airport VARCHAR(10),
    destination_airport VARCHAR(10),
    best_airline TEXT,
    best_stops TEXT,
    best_duration TEXT,
    best_origin_airport VARCHAR(10),
    best_destination_airport VARCHAR(10),
    is_self_transfer BOOLEAN DEFAULT FALSE,
    winning_gl VARCHAR(8) NOT NULL DEFAULT 'MX',
    extraction_source VARCHAR(30) DEFAULT 'patchright_dom',
    raw_top_offers JSONB DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS alert_logs (
    id SERIAL PRIMARY KEY,
    destination_id INTEGER REFERENCES destinations(id) ON DELETE CASCADE,
    date_slot_id INTEGER REFERENCES date_slots(id) ON DELETE SET NULL,
    sent_at TIMESTAMPTZ DEFAULT NOW(),
    trigger_type TEXT NOT NULL,
    cheapest_price NUMERIC(12, 2),
    best_price NUMERIC(12, 2),
    message_preview TEXT
);

CREATE INDEX IF NOT EXISTS idx_date_slots_destination_id
    ON date_slots(destination_id);

CREATE INDEX IF NOT EXISTS idx_date_slots_due_queue
    ON date_slots(is_active, last_checked_at ASC NULLS FIRST);

CREATE INDEX IF NOT EXISTS idx_price_snapshots_slot_time
    ON price_snapshots(date_slot_id, scraped_at DESC);

CREATE INDEX IF NOT EXISTS idx_price_snapshots_dest_time
    ON price_snapshots(destination_id, scraped_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_logs_dest_sent
    ON alert_logs(destination_id, sent_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_destinations_unique_name
    ON destinations (LOWER(TRIM(name)));
"""

MIGRATION_SQL = """
ALTER TABLE IF EXISTS destinations
    ALTER COLUMN origin TYPE VARCHAR(64),
    ALTER COLUMN destination TYPE VARCHAR(64),
    ADD COLUMN IF NOT EXISTS origin_airports TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS destination_airports TEXT[] NOT NULL DEFAULT '{}';

UPDATE destinations
SET origin_airports = string_to_array(regexp_replace(upper(origin), '\\s+', '', 'g'), ','),
    destination_airports = string_to_array(regexp_replace(upper(destination), '\\s+', '', 'g'), ',')
WHERE cardinality(origin_airports) = 0 OR cardinality(destination_airports) = 0;

ALTER TABLE IF EXISTS date_slots
    ADD COLUMN IF NOT EXISTS latest_origin_airport VARCHAR(10),
    ADD COLUMN IF NOT EXISTS latest_destination_airport VARCHAR(10),
    ADD COLUMN IF NOT EXISTS latest_best_origin_airport VARCHAR(10),
    ADD COLUMN IF NOT EXISTS latest_best_destination_airport VARCHAR(10);

ALTER TABLE IF EXISTS price_snapshots
    ADD COLUMN IF NOT EXISTS origin_airport VARCHAR(10),
    ADD COLUMN IF NOT EXISTS destination_airport VARCHAR(10),
    ADD COLUMN IF NOT EXISTS best_origin_airport VARCHAR(10),
    ADD COLUMN IF NOT EXISTS best_destination_airport VARCHAR(10);

CREATE UNIQUE INDEX IF NOT EXISTS idx_destinations_unique_name
    ON destinations (LOWER(TRIM(name)));

ALTER TABLE IF EXISTS destinations
    DROP COLUMN IF EXISTS cooldown_hours;

ALTER TABLE IF EXISTS destinations
    ADD COLUMN IF NOT EXISTS gl_pool VARCHAR(64) NOT NULL DEFAULT 'MX,IT,DE';

ALTER TABLE IF EXISTS destinations
    ALTER COLUMN check_interval_minutes SET DEFAULT 60;

ALTER TABLE IF EXISTS price_snapshots
    ADD COLUMN IF NOT EXISTS winning_gl VARCHAR(8) NOT NULL DEFAULT 'MX';

ALTER TABLE IF EXISTS date_slots
    ADD COLUMN IF NOT EXISTS last_winning_gl VARCHAR(8) NOT NULL DEFAULT 'MX',
    ADD COLUMN IF NOT EXISTS all_time_low_gl VARCHAR(8);

UPDATE destinations
SET gl_pool = 'MX,IT,DE'
WHERE gl_pool IS NULL OR TRIM(gl_pool) = '';

UPDATE destinations
SET check_interval_minutes = 60
WHERE check_interval_minutes > 60;

UPDATE date_slots ds
SET last_winning_gl = COALESCE(NULLIF(ds.last_winning_gl, ''), d.gl, 'MX')
FROM destinations d
WHERE ds.destination_id = d.id
  AND (ds.last_winning_gl IS NULL OR ds.last_winning_gl = '');
"""


def _to_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, Decimal):
        return float(val)
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


_CACHE_TTL_SECONDS: float = 30.0
_IDLE_RECONNECT_SECONDS: float = 45.0
_READ_CACHE: dict[tuple[Any, ...], tuple[float, Any]] = {}
_CACHE_LOCK = threading.RLock()
_INIT_LOCK = threading.Lock()
_db_initialized: bool = False

_THREAD_LOCAL = threading.local()
_STATS_LOCK = threading.Lock()
_DB_STATS: dict[str, int] = {
    "total_connects": 0,
    "total_queries": 0,
}


def connection_stats() -> dict[str, int]:
    """Return cumulative DB TCP/TLS connection and SQL query execution counters."""
    with _STATS_LOCK:
        return dict(_DB_STATS)


def reset_connection_stats() -> None:
    """Reset DB connection and SQL query counters (used by performance benchmarks)."""
    with _STATS_LOCK:
        _DB_STATS["total_connects"] = 0
        _DB_STATS["total_queries"] = 0


def clear_db_cache() -> None:
    """Invalidate all cached read queries after any write mutation."""
    with _CACHE_LOCK:
        _READ_CACHE.clear()


def _cache_get(key: tuple[Any, ...]) -> tuple[bool, Any]:
    now = time.monotonic()
    with _CACHE_LOCK:
        entry = _READ_CACHE.get(key)
        if entry is None:
            return False, None
        ts, payload = entry
        if (now - ts) >= _CACHE_TTL_SECONDS:
            _READ_CACHE.pop(key, None)
            return False, None
        return True, copy.deepcopy(payload)


def _cache_set(key: tuple[Any, ...], payload: Any) -> None:
    with _CACHE_LOCK:
        _READ_CACHE[key] = (time.monotonic(), copy.deepcopy(payload))


class _TrackedCursor:
    """Lightweight cursor proxy that increments `total_queries` on every SQL execution."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def execute(self, query: Any, params: Any = None, *args: Any, **kwargs: Any) -> Any:
        with _STATS_LOCK:
            _DB_STATS["total_queries"] += 1
        if params is not None:
            return self._inner.execute(query, params, *args, **kwargs)
        return self._inner.execute(query, *args, **kwargs)

    def executemany(self, query: Any, params_seq: Any, *args: Any, **kwargs: Any) -> Any:
        with _STATS_LOCK:
            _DB_STATS["total_queries"] += 1
        return self._inner.executemany(query, params_seq, *args, **kwargs)

    def __enter__(self) -> "_TrackedCursor":
        self._inner.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any:
        return self._inner.__exit__(exc_type, exc_val, exc_tb)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _ReusableConnectionContext:
    """
    Context wrapper around a persistent thread-local `psycopg` connection.
    Commits or rolls back on context exit while keeping the TCP+TLS socket open for reuse.
    """

    def __init__(self, raw_conn: Any) -> None:
        self._raw = raw_conn

    def cursor(self, *args: Any, **kwargs: Any) -> _TrackedCursor:
        return _TrackedCursor(self._raw.cursor(*args, **kwargs))

    def commit(self) -> None:
        self._raw.commit()

    def rollback(self) -> None:
        self._raw.rollback()

    def close(self) -> None:
        # Keep the persistent socket alive across calls; commit any open read txn safely.
        try:
            if not self._raw.closed and self._raw.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:  # type: ignore[union-attr]
                self._raw.commit()
        except Exception:
            _discard_thread_connection()

    def __enter__(self) -> "_ReusableConnectionContext":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            if exc_type is not None:
                try:
                    self._raw.rollback()
                except Exception:
                    _discard_thread_connection()
                if getattr(self._raw, "broken", False) or getattr(self._raw, "closed", False):
                    _discard_thread_connection()
            else:
                try:
                    if self._raw.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:  # type: ignore[union-attr]
                        self._raw.commit()
                except Exception:
                    _discard_thread_connection()
                    raise
        finally:
            _THREAD_LOCAL.last_used_at = time.monotonic()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._raw, name)


def _discard_thread_connection() -> None:
    raw_conn = getattr(_THREAD_LOCAL, "conn", None)
    _THREAD_LOCAL.conn = None
    _THREAD_LOCAL.last_used_at = 0.0
    if raw_conn is not None:
        try:
            raw_conn.close()
        except Exception:
            pass


def _open_raw_connection(retries: int = 3, delay_seconds: float = 1.5) -> Any:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            conn: psycopg.Connection[Any] = cast(Any, psycopg).connect(
                DATABASE_URL,
                row_factory=dict_row,
                connect_timeout=15,
                autocommit=False,
            )
            with _STATS_LOCK:
                _DB_STATS["total_connects"] += 1
            return conn
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Database connection attempt %d/%d failed: %s",
                attempt,
                retries,
                exc,
            )
            if attempt < retries:
                time.sleep(delay_seconds * attempt)

    raise RuntimeError(f"Could not connect to Neon PostgreSQL after {retries} attempts: {last_error}")


def get_connection(retries: int = 3, delay_seconds: float = 1.5) -> _ReusableConnectionContext:
    """
    Obtain a persistent, thread-local `psycopg` v3 connection to Neon Serverless PostgreSQL.
    Reuses the open TCP+TLS socket across calls and transparently reconnects if closed or idle.
    """
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is not configured. Set DATABASE_URL in your .env file or Streamlit secrets."
        )

    if psycopg is None or dict_row is None:
        raise RuntimeError(
            "psycopg is not installed. Install psycopg[binary] to connect to Neon PostgreSQL."
        )

    raw_conn = getattr(_THREAD_LOCAL, "conn", None)
    last_used = float(getattr(_THREAD_LOCAL, "last_used_at", 0.0) or 0.0)

    if raw_conn is not None:
        is_bad = bool(getattr(raw_conn, "closed", False) or getattr(raw_conn, "broken", False))
        if not is_bad:
            try:
                if raw_conn.info.transaction_status == psycopg.pq.TransactionStatus.INERROR:  # type: ignore[union-attr]
                    raw_conn.rollback()
                if (time.monotonic() - last_used) > _IDLE_RECONNECT_SECONDS:
                    raw_conn.execute("SELECT 1;")
                    raw_conn.commit()
            except Exception:
                is_bad = True
        if is_bad:
            _discard_thread_connection()
            raw_conn = None

    if raw_conn is None:
        raw_conn = _open_raw_connection(retries=retries, delay_seconds=delay_seconds)
        _THREAD_LOCAL.conn = raw_conn
        _THREAD_LOCAL.last_used_at = time.monotonic()

    return _ReusableConnectionContext(raw_conn)


def close_db_connection() -> None:
    """
    Explicitly close and clear the persistent thread-local `psycopg` connection
    so Neon Serverless compute can enter sleep during the RAM-buffered scraping phase.
    """
    _discard_thread_connection()


def _is_transient_neon_error(exc: BaseException) -> bool:
    """Return True if `exc` indicates a transient Neon disconnect, SSL close, or closed socket."""
    if psycopg is not None:
        transient_types = tuple(
            t
            for t in (
                getattr(psycopg, "OperationalError", None),
                getattr(psycopg, "InterfaceError", None),
            )
            if t is not None
        )
        if transient_types and isinstance(exc, transient_types):
            return True
    msg = str(exc).lower()
    markers = (
        "ssl connection has been closed unexpectedly",
        "consuming input failed",
        "connection is closed",
        "server closed the connection unexpectedly",
        "eof detected",
        "broken pipe",
        "connection reset",
        "timeout expired",
        "could not connect to neon postgresql",
    )
    return any(m in msg for m in markers)


def _execute_with_neon_retry(
    op_name: str,
    fn: Any,
    max_attempts: int = 4,
    base_delay_s: float = 2.0,
) -> Any:
    """
    Execute a Neon DB operation with exponential backoff (`2s, 4s, 8s`) on transient
    `psycopg.OperationalError`, `psycopg.InterfaceError`, or SSL/socket closures.
    Calls `close_db_connection()` before sleeping so retries open a clean socket.
    """
    attempts = max(1, int(max_attempts))
    last_exc: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            if not _is_transient_neon_error(exc) or attempt >= attempts:
                raise
            last_exc = exc
            delay_s = float(base_delay_s) * (2 ** (attempt - 1))
            logger.warning(
                "[NEON_RETRY] %s failed on attempt %d/%d (%s); closing connection and retrying in %.1fs...",
                op_name,
                attempt,
                attempts,
                exc,
                delay_s,
            )
            close_db_connection()
            time.sleep(delay_s)
    if last_exc is not None:
        raise last_exc


def is_db_connected() -> bool:
    """Return True if the current thread holds an open `psycopg` connection."""
    raw_conn = getattr(_THREAD_LOCAL, "conn", None)
    if raw_conn is None:
        return False
    return not bool(getattr(raw_conn, "closed", True))


def init_db(force: bool = False) -> None:
    """
    Auto-initialize the 4 tables, execute idempotent v5.0 schema migrations
    under a PostgreSQL transactional advisory lock (`pg_advisory_xact_lock(742918341)`),
    and verify indexes in Neon PostgreSQL once per process.
    """
    global _db_initialized
    if _db_initialized and not force:
        return
    with _INIT_LOCK:
        if _db_initialized and not force:
            return
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(742918341);")
                cur.execute(cast(LiteralString, SCHEMA_SQL))
                cur.execute(cast(LiteralString, MIGRATION_SQL))
            conn.commit()
        _db_initialized = True
        logger.info("Database schema initialized and migrated for FlightPulse Matrix v5.0.")


# ==============================================================================
# DESTINATION CRUD OPERATIONS
# ==============================================================================


def list_destinations(active_only: bool = False) -> list[dict[str, Any]]:
    """
    Fetch all destinations with aggregated slot metrics (cached for 30s, invalidated on writes).
    """
    cache_key = ("list_destinations",)
    hit, cached = _cache_get(cache_key)
    if hit:
        all_items: list[dict[str, Any]] = cached
    else:
        query = """
            SELECT
                d.*,
                COUNT(ds.id) AS total_slots,
                COUNT(ds.id) FILTER (WHERE ds.is_active = TRUE) AS active_slots,
                MIN(ds.latest_cheapest_price) FILTER (WHERE ds.is_active = TRUE) AS min_cheapest_price,
                MIN(ds.latest_best_price) FILTER (WHERE ds.is_active = TRUE) AS min_best_price,
                MIN(ds.all_time_low_price) FILTER (WHERE ds.is_active = TRUE) AS min_all_time_low,
                COUNT(ds.id) FILTER (
                    WHERE ds.is_active = TRUE
                      AND ds.latest_cheapest_price IS NOT NULL
                      AND ds.latest_cheapest_price <= d.target_price
                ) AS slots_below_target,
                MAX(ds.last_checked_at) AS last_checked_at
            FROM destinations d
            LEFT JOIN date_slots ds ON ds.destination_id = d.id
            GROUP BY d.id
            ORDER BY d.is_active DESC, d.id ASC;
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(cast(LiteralString, query))
                rows = cur.fetchall()

        all_items = []
        for row in rows:
            item = dict(row)
            item["target_price"] = _to_float(item.get("target_price")) or 0.0
            item["min_cheapest_price"] = _to_float(item.get("min_cheapest_price"))
            item["min_best_price"] = _to_float(item.get("min_best_price"))
            item["min_all_time_low"] = _to_float(item.get("min_all_time_low"))
            item["filter_config"] = item.get("filter_config") or {}
            pool_list = parse_gl_pool(item.get("gl_pool") or item.get("gl"))
            item["gl_pool"] = ",".join(pool_list)
            item["gl"] = str(item.get("gl") or pool_list[0]).strip().upper()
            all_items.append(item)
        _cache_set(cache_key, all_items)

    if active_only:
        return [d for d in all_items if d.get("is_active")]
    return all_items


def get_destination(destination_id: int) -> Optional[dict[str, Any]]:
    """
    Fetch a single destination by ID.
    """
    query = """
        SELECT
            d.*,
            COUNT(ds.id) AS total_slots,
            COUNT(ds.id) FILTER (WHERE ds.is_active = TRUE) AS active_slots
        FROM destinations d
        LEFT JOIN date_slots ds ON ds.destination_id = d.id
        WHERE d.id = %s
        GROUP BY d.id;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (destination_id,))
            row = cur.fetchone()
    if not row:
        return None
    item = dict(row)
    item["target_price"] = _to_float(item.get("target_price")) or 0.0
    item["filter_config"] = item.get("filter_config") or {}
    pool_list = parse_gl_pool(item.get("gl_pool") or item.get("gl"))
    item["gl_pool"] = ",".join(pool_list)
    item["gl"] = str(item.get("gl") or pool_list[0]).strip().upper()
    return item


def _validate_unique_destination_name(
    cur: Any,
    raw_name: str,
    exclude_id: Optional[int] = None,
) -> str:
    """
    Validate that a destination name is non-empty and does not collide
    (case-insensitive, trimmed) with an existing destination.
    """
    clean_name = raw_name.strip()
    if not clean_name:
        raise ValueError("El nombre del destino no puede estar vacío.")

    if exclude_id is None:
        cur.execute(
            "SELECT id, name FROM destinations WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s));",
            (clean_name,),
        )
    else:
        cur.execute(
            "SELECT id, name FROM destinations WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s)) AND id != %s;",
            (clean_name, exclude_id),
        )
    existing = cur.fetchone()
    if existing:
        if exclude_id is None:
            raise ValueError(
                f"Ya existe un destino llamado '{existing['name']}'. "
                "Selecciónalo en la lista para editarlo en lugar de crear uno duplicado."
            )
        raise ValueError(
            f"Ya existe otro destino llamado '{existing['name']}'. Usa un nombre único."
        )
    return clean_name


def create_destination(
    name: str,
    origin: str | list[str],
    destination: str | list[str],
    target_price: float,
    trip_type: str = "round-trip",
    currency: str = DEFAULT_CURRENCY,
    gl: str = DEFAULT_GL,
    hl: str = DEFAULT_HL,
    check_interval_minutes: int = DEFAULT_CHECK_INTERVAL_MINUTES,
    gl_pool: Optional[str | Sequence[str]] = None,
    filter_config: Optional[dict[str, Any]] = None,
    is_active: bool = True,
) -> int:
    """
    Create a new N:M multi-airport destination corridor & filter profile.
    Validates unique destination name, normalizes IATA lists (expanding metro codes
    like TYO -> NRT,HND), and enforces the active matrix limit (`MAX_DESTINATIONS`) when is_active=True.
    """
    origin_airports = parse_iata_list(origin, expand_metro=True)
    destination_airports = parse_iata_list(destination, expand_metro=True)
    norm_origin = ",".join(origin_airports)
    norm_dest = ",".join(destination_airports)

    if gl_pool is not None:
        pool_list = parse_gl_pool(gl_pool)
        clean_gl = pool_list[0]
    else:
        pool_list = parse_gl_pool(DEFAULT_GL_POOL)
        clean_gl = (gl or pool_list[0]).strip().upper()
    pool_str = ",".join(pool_list)

    with get_connection() as conn:
        with conn.cursor() as cur:
            clean_name = _validate_unique_destination_name(cur, name)

            if is_active:
                cur.execute("SELECT COUNT(*) AS cnt FROM destinations WHERE is_active = TRUE;")
                cnt_row = cur.fetchone()
                active_count = int(cnt_row["cnt"]) if cnt_row else 0
                if active_count >= MAX_DESTINATIONS:
                    raise ValueError(
                        f"Maximum active destinations limit reached ({MAX_DESTINATIONS}). "
                        "Pause or delete an existing destination before adding a new active one."
                    )

            cur.execute(
                """
                INSERT INTO destinations (
                    name, origin, destination, origin_airports, destination_airports,
                    trip_type, currency, gl, gl_pool, hl,
                    target_price, check_interval_minutes,
                    filter_config, is_active
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """,
                (
                    clean_name,
                    norm_origin,
                    norm_dest,
                    origin_airports,
                    destination_airports,
                    trip_type.strip().lower(),
                    currency.strip().upper(),
                    clean_gl,
                    pool_str,
                    hl.strip().lower(),
                    Decimal(str(round(target_price, 2))),
                    int(check_interval_minutes),
                    Jsonb(filter_config or {}),
                    bool(is_active),
                ),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to create destination: no row returned.")
            new_id = int(row["id"])
        conn.commit()
    clear_db_cache()
    logger.info("Created destination id=%d (%s: %s -> %s, gl_pool=%s)", new_id, clean_name, norm_origin, norm_dest, pool_str)
    return new_id


def update_destination(
    destination_id: int,
    name: str,
    origin: str | list[str],
    destination: str | list[str],
    target_price: float,
    trip_type: str = "round-trip",
    currency: str = DEFAULT_CURRENCY,
    gl: str = DEFAULT_GL,
    hl: str = DEFAULT_HL,
    check_interval_minutes: int = DEFAULT_CHECK_INTERVAL_MINUTES,
    gl_pool: Optional[str | Sequence[str]] = None,
    filter_config: Optional[dict[str, Any]] = None,
    is_active: bool = True,
) -> None:
    """
    Update an existing destination's metadata, N:M airport lists, Multi-GL pool,
    and filter profile, and automatically regenerate `google_flights_url` on all child `date_slots`.
    """
    origin_airports = parse_iata_list(origin, expand_metro=True)
    destination_airports = parse_iata_list(destination, expand_metro=True)
    norm_origin = ",".join(origin_airports)
    norm_dest = ",".join(destination_airports)
    clean_trip = trip_type.strip().lower()
    clean_curr = currency.strip().upper()
    if gl_pool is not None:
        pool_list = parse_gl_pool(gl_pool)
        clean_gl = pool_list[0]
    else:
        pool_list = parse_gl_pool(DEFAULT_GL_POOL)
        clean_gl = (gl or pool_list[0]).strip().upper()
    pool_str = ",".join(pool_list)
    clean_hl = hl.strip().lower()
    profile_obj = FilterProfile.from_dict(filter_config or {})

    with get_connection() as conn:
        with conn.cursor() as cur:
            clean_name = _validate_unique_destination_name(cur, name, exclude_id=destination_id)

            if is_active:
                cur.execute(
                    "SELECT COUNT(*) AS cnt FROM destinations WHERE is_active = TRUE AND id != %s;",
                    (destination_id,),
                )
                cnt_row = cur.fetchone()
                if cnt_row and int(cnt_row["cnt"]) >= MAX_DESTINATIONS:
                    raise ValueError(
                        f"Cannot activate destination: maximum of {MAX_DESTINATIONS} active destinations reached."
                    )

            cur.execute(
                """
                UPDATE destinations
                SET name = %s,
                    origin = %s,
                    destination = %s,
                    origin_airports = %s,
                    destination_airports = %s,
                    trip_type = %s,
                    currency = %s,
                    gl = %s,
                    gl_pool = %s,
                    hl = %s,
                    target_price = %s,
                    check_interval_minutes = %s,
                    filter_config = %s,
                    is_active = %s
                WHERE id = %s;
                """,
                (
                    clean_name,
                    norm_origin,
                    norm_dest,
                    origin_airports,
                    destination_airports,
                    clean_trip,
                    clean_curr,
                    clean_gl,
                    pool_str,
                    clean_hl,
                    Decimal(str(round(target_price, 2))),
                    int(check_interval_minutes),
                    Jsonb(filter_config or {}),
                    bool(is_active),
                    destination_id,
                ),
            )

            # Synchronize all existing date_slots URLs in a single batched executemany
            cur.execute(
                "SELECT id, outbound_date, return_date FROM date_slots WHERE destination_id = %s;",
                (destination_id,),
            )
            slot_rows = cur.fetchall()
            url_updates: list[tuple[str, int]] = []
            for s_row in slot_rows:
                rebuilt_url = build_google_flights_url(
                    origin=origin_airports,
                    destination=destination_airports,
                    outbound_date=str(s_row["outbound_date"]),
                    return_date=s_row.get("return_date"),
                    trip_type=clean_trip,
                    filter_profile=profile_obj,
                    curr=clean_curr,
                    gl=clean_gl,
                    hl=clean_hl,
                )
                url_updates.append((rebuilt_url, int(s_row["id"])))
            if url_updates:
                cur.executemany(
                    "UPDATE date_slots SET google_flights_url = %s WHERE id = %s;",
                    url_updates,
                )
        conn.commit()
    clear_db_cache()


def set_destination_active(destination_id: int, is_active: bool) -> None:
    """
    Toggle `is_active` status for a destination.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            if is_active:
                cur.execute(
                    "SELECT COUNT(*) AS cnt FROM destinations WHERE is_active = TRUE AND id != %s;",
                    (destination_id,),
                )
                cnt_row = cur.fetchone()
                if cnt_row and int(cnt_row["cnt"]) >= MAX_DESTINATIONS:
                    raise ValueError(
                        f"Cannot activate destination: maximum of {MAX_DESTINATIONS} active destinations reached."
                    )
            cur.execute(
                "UPDATE destinations SET is_active = %s WHERE id = %s;",
                (bool(is_active), destination_id),
            )
        conn.commit()
    clear_db_cache()


def delete_destination(destination_id: int) -> None:
    """
    Delete a destination and cascade-delete its date_slots and price_snapshots.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM destinations WHERE id = %s;", (destination_id,))
        conn.commit()
    clear_db_cache()
    logger.info("Deleted destination id=%d", destination_id)


# ==============================================================================
# DATE SLOTS CRUD & SHARDED DISPATCHER QUEUE
# ==============================================================================


def list_date_slots(
    destination_id: Optional[int] = None,
    active_only: bool = False,
) -> list[dict[str, Any]]:
    """
    List date_slots ordered by outbound_date ASC, joined with parent destination info.
    Caches the full slot list for 30s and filters by destination/active status in memory
    so switching destinations in the dashboard performs 0 SQL queries.
    """
    cache_key = ("list_date_slots_all",)
    hit, cached = _cache_get(cache_key)
    if hit:
        all_slots: list[dict[str, Any]] = cached
    else:
        query = """
            SELECT
                ds.*,
                d.name AS destination_name,
                d.origin AS origin,
                d.destination AS destination_code,
                d.trip_type AS trip_type,
                d.currency AS currency,
                d.gl AS gl,
                d.gl_pool AS gl_pool,
                d.hl AS hl,
                d.target_price AS target_price,
                d.check_interval_minutes AS check_interval_minutes,
                d.filter_config AS filter_config,
                d.is_active AS destination_is_active
            FROM date_slots ds
            JOIN destinations d ON d.id = ds.destination_id
            ORDER BY ds.destination_id ASC, ds.outbound_date ASC, ds.id ASC;
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(cast(LiteralString, query))
                rows = cur.fetchall()

        all_slots = []
        for row in rows:
            item = dict(row)
            item["latest_cheapest_price"] = _to_float(item.get("latest_cheapest_price"))
            item["latest_best_price"] = _to_float(item.get("latest_best_price"))
            item["all_time_low_price"] = _to_float(item.get("all_time_low_price"))
            item["last_alerted_price"] = _to_float(item.get("last_alerted_price"))
            item["target_price"] = _to_float(item.get("target_price")) or 0.0
            item["filter_config"] = item.get("filter_config") or {}
            pool_list = parse_gl_pool(item.get("gl_pool") or item.get("gl"))
            item["gl_pool"] = ",".join(pool_list)
            item["last_winning_gl"] = str(item.get("last_winning_gl") or item.get("gl") or pool_list[0]).strip().upper()
            item["window_info"] = get_flight_window_status(item.get("outbound_date"), item.get("return_date"))
            all_slots.append(item)
        _cache_set(cache_key, all_slots)

    filtered = all_slots
    if destination_id is not None:
        filtered = [s for s in filtered if int(s["destination_id"]) == int(destination_id)]
    if active_only:
        filtered = [s for s in filtered if bool(s.get("is_active"))]
    return filtered


def get_date_slot(slot_id: int) -> Optional[dict[str, Any]]:
    """
    Retrieve a single date_slot joined with its parent destination configuration.
    """
    query = """
        SELECT
            ds.*,
            d.name AS destination_name,
            d.origin AS origin,
            d.destination AS destination_code,
            d.trip_type AS trip_type,
            d.currency AS currency,
            d.gl AS gl,
            d.gl_pool AS gl_pool,
            d.hl AS hl,
            d.target_price AS target_price,
            d.check_interval_minutes AS check_interval_minutes,
            d.filter_config AS filter_config,
            d.is_active AS destination_is_active
        FROM date_slots ds
        JOIN destinations d ON d.id = ds.destination_id
        WHERE ds.id = %s;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (slot_id,))
            row = cur.fetchone()
    if not row:
        return None
    item = dict(row)
    item["latest_cheapest_price"] = _to_float(item.get("latest_cheapest_price"))
    item["latest_best_price"] = _to_float(item.get("latest_best_price"))
    item["all_time_low_price"] = _to_float(item.get("all_time_low_price"))
    item["last_alerted_price"] = _to_float(item.get("last_alerted_price"))
    item["target_price"] = _to_float(item.get("target_price")) or 0.0
    item["filter_config"] = item.get("filter_config") or {}
    pool_list = parse_gl_pool(item.get("gl_pool") or item.get("gl"))
    item["gl_pool"] = ",".join(pool_list)
    item["last_winning_gl"] = str(item.get("last_winning_gl") or item.get("gl") or pool_list[0]).strip().upper()
    return item


def create_date_slot(
    destination_id: int,
    outbound_date: str,
    return_date: Optional[str],
    google_flights_url: str,
    label: Optional[str] = None,
    is_active: bool = True,
) -> int:
    """
    Create a single date_slot under a destination, enforcing the per-destination
    and global MAX_TOTAL_SLOTS active slot capacity limits.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            if is_active:
                cur.execute(
                    "SELECT COUNT(*) AS cnt FROM date_slots WHERE destination_id = %s AND is_active = TRUE;",
                    (destination_id,),
                )
                dest_cnt_row = cur.fetchone()
                dest_cnt = int(dest_cnt_row["cnt"]) if dest_cnt_row else 0
                if dest_cnt >= MAX_SLOTS_PER_DESTINATION:
                    raise ValueError(
                        f"Destination already has {MAX_SLOTS_PER_DESTINATION} active date slots (max {MAX_SLOTS_PER_DESTINATION})."
                    )

                cur.execute("SELECT COUNT(*) AS cnt FROM date_slots WHERE is_active = TRUE;")
                total_cnt_row = cur.fetchone()
                total_cnt = int(total_cnt_row["cnt"]) if total_cnt_row else 0
                if total_cnt >= MAX_TOTAL_SLOTS:
                    raise ValueError(
                        f"Matrix capacity limit reached ({MAX_TOTAL_SLOTS} total active date slots)."
                    )

            cur.execute(
                """
                INSERT INTO date_slots (
                    destination_id, label, outbound_date, return_date, google_flights_url, is_active
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id;
                """,
                (
                    destination_id,
                    label,
                    outbound_date.strip(),
                    return_date.strip() if return_date else None,
                    google_flights_url.strip(),
                    bool(is_active),
                ),
            )
            slot_row = cur.fetchone()
            if not slot_row:
                raise RuntimeError("Failed to insert date_slot: no row returned.")
            slot_id = int(slot_row["id"])
        conn.commit()
    clear_db_cache()
    return slot_id


def bulk_create_date_slots(
    destination_id: int,
    slots: list[dict[str, Any]],
    replace_existing: bool = False,
) -> list[int]:
    """
    Batch-insert up to 10 date_slots for a destination in a single transaction
    using one multi-row INSERT statement, skipping duplicate `(outbound_date, return_date)` pairs.
    If `replace_existing=True`, removes existing slots for this destination before inserting.
    """
    if not slots and not replace_existing:
        return []

    created_ids: list[int] = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            if replace_existing:
                cur.execute("DELETE FROM date_slots WHERE destination_id = %s;", (destination_id,))
                existing_pairs: set[tuple[str, Optional[str]]] = set()
            else:
                cur.execute(
                    "SELECT outbound_date, return_date FROM date_slots WHERE destination_id = %s;",
                    (destination_id,),
                )
                existing_pairs = {
                    (
                        str(r["outbound_date"]).strip(),
                        str(r["return_date"]).strip() if r.get("return_date") else None,
                    )
                    for r in cur.fetchall()
                }

            deduped_slots: list[dict[str, Any]] = []
            seen_pairs = set(existing_pairs)
            for slot in slots:
                out_d = str(slot["outbound_date"]).strip()
                ret_d = str(slot["return_date"]).strip() if slot.get("return_date") else None
                pair_key = (out_d, ret_d)
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)
                deduped_slots.append(
                    {
                        "label": slot.get("label"),
                        "outbound_date": out_d,
                        "return_date": ret_d,
                        "google_flights_url": str(slot["google_flights_url"]).strip(),
                        "is_active": bool(slot.get("is_active", True)),
                    }
                )

            if not deduped_slots:
                conn.commit()
                clear_db_cache()
                return []

            cur.execute(
                "SELECT COUNT(*) AS cnt FROM date_slots WHERE destination_id = %s AND is_active = TRUE;",
                (destination_id,),
            )
            existing_row = cur.fetchone()
            existing_active = int(existing_row["cnt"]) if existing_row else 0
            available_for_dest = max(0, MAX_SLOTS_PER_DESTINATION - existing_active)

            cur.execute("SELECT COUNT(*) AS cnt FROM date_slots WHERE is_active = TRUE;")
            total_row = cur.fetchone()
            total_active = int(total_row["cnt"]) if total_row else 0
            available_total = max(0, MAX_TOTAL_SLOTS - total_active)

            max_can_add = min(available_for_dest, available_total, len(deduped_slots))
            if max_can_add <= 0:
                raise ValueError(
                    f"Cannot add date slots: destination has {existing_active}/{MAX_SLOTS_PER_DESTINATION} active slots "
                    f"(total active across matrix: {total_active}/{MAX_TOTAL_SLOTS})."
                )

            to_insert = deduped_slots[:max_can_add]
            placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s)"] * len(to_insert))
            flat_params: list[Any] = []
            for slot in to_insert:
                flat_params.extend(
                    [
                        destination_id,
                        slot["label"],
                        slot["outbound_date"],
                        slot["return_date"],
                        slot["google_flights_url"],
                        slot["is_active"],
                    ]
                )
            insert_sql = f"""
                INSERT INTO date_slots (
                    destination_id, label, outbound_date, return_date, google_flights_url, is_active
                )
                VALUES {placeholders}
                RETURNING id;
            """
            cur.execute(cast(LiteralString, insert_sql), tuple(flat_params))
            created_ids = [int(r["id"]) for r in cur.fetchall()]
        conn.commit()

    clear_db_cache()
    logger.info(
        "Bulk-created %d date slots for destination_id=%d (replace_existing=%s)",
        len(created_ids),
        destination_id,
        replace_existing,
    )
    return created_ids


def set_date_slot_active(slot_id: int, is_active: bool) -> None:
    """
    Toggle `is_active` for a single date_slot with capacity verification.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            if is_active:
                cur.execute("SELECT destination_id FROM date_slots WHERE id = %s;", (slot_id,))
                row = cur.fetchone()
                if not row:
                    return
                dest_id = int(row["destination_id"])
                cur.execute(
                    "SELECT COUNT(*) AS cnt FROM date_slots WHERE destination_id = %s AND is_active = TRUE AND id != %s;",
                    (dest_id, slot_id),
                )
                slot_cnt_row = cur.fetchone()
                if slot_cnt_row and int(slot_cnt_row["cnt"]) >= MAX_SLOTS_PER_DESTINATION:
                    raise ValueError(
                        f"Maximum of {MAX_SLOTS_PER_DESTINATION} active date slots per destination reached."
                    )
            cur.execute(
                "UPDATE date_slots SET is_active = %s WHERE id = %s;",
                (bool(is_active), slot_id),
            )
        conn.commit()
    clear_db_cache()


def delete_date_slot(slot_id: int) -> None:
    """
    Delete a single date_slot by ID.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM date_slots WHERE id = %s;", (slot_id,))
        conn.commit()
    clear_db_cache()


def partition_destinations_into_shards(
    destinations: Sequence[dict[str, Any]],
    total_shards: int = 6,
) -> list[list[dict[str, Any]]]:
    """
    Deterministic greedy bin-packing of active destinations across `total_shards` buckets.
    Sorts destinations by `(-active_slot_count, destination_id)` and always assigns the
    next destination to the shard with the lowest accumulated slot load (tie-break: lowest shard index).
    """
    if total_shards < 1:
        raise ValueError(f"total_shards must be >= 1, got {total_shards}")

    def _dest_id(d: dict[str, Any]) -> int:
        raw = d.get("id") if d.get("id") is not None else d.get("destination_id")
        return int(raw or 0)

    def _slot_count(d: dict[str, Any]) -> int:
        for k in ("active_slot_count", "active_in_window_slots", "active_slots", "slot_count"):
            if d.get(k) is not None:
                return max(0, int(d[k]))
        return 0

    ordered = sorted(
        destinations,
        key=lambda d: (-_slot_count(d), _dest_id(d)),
    )
    buckets: list[list[dict[str, Any]]] = [[] for _ in range(total_shards)]
    loads = [0] * total_shards

    for dest in ordered:
        best_shard = min(range(total_shards), key=lambda idx: (loads[idx], idx))
        buckets[best_shard].append(dict(dest))
        loads[best_shard] += _slot_count(dest)

    return buckets


def assign_destinations_to_shard(
    destinations: Sequence[dict[str, Any]],
    shard_index: int,
    total_shards: int = 6,
) -> list[dict[str, Any]]:
    """
    Return the deterministic, 100% disjoint subset of destinations assigned to `shard_index`.
    """
    if total_shards < 1:
        raise ValueError(f"total_shards must be >= 1, got {total_shards}")
    if shard_index < 0 or shard_index >= total_shards:
        raise ValueError(f"shard_index ({shard_index}) must be in [0, {total_shards - 1}]")
    return partition_destinations_into_shards(destinations, total_shards=total_shards)[shard_index]


def fetch_due_date_slots(
    max_slots: int = MAX_SLOTS_PER_RUN,
    force_all: bool = False,
    destination_id: Optional[int] = None,
    slot_id: Optional[int] = None,
    due_grace_minutes: int = DUE_GRACE_MINUTES,
) -> list[dict[str, Any]]:
    """
    Sharded Anti-Ban Queue Dispatcher query:
    Selects up to `max_slots` due `date_slots` where `is_active = TRUE` and
    `(last_checked_at IS NULL OR last_checked_at <= NOW() - ((check_interval_minutes - due_grace_minutes) * interval '1 minute'))`,
    ordered by `last_checked_at ASC NULLS FIRST`.
    """
    conditions: list[str] = []
    params: list[Any] = []

    if slot_id is not None:
        conditions.append("ds.id = %s")
        params.append(slot_id)
    else:
        conditions.append("ds.is_active = TRUE")
        conditions.append("d.is_active = TRUE")
        # Enforce Google Flights 330-day airline schedule release window (both outbound & return)
        conditions.append("ds.outbound_date::date <= (CURRENT_DATE + INTERVAL '330 days')")
        conditions.append("ds.outbound_date::date >= CURRENT_DATE")
        conditions.append("(ds.return_date IS NULL OR ds.return_date::date <= (CURRENT_DATE + INTERVAL '330 days'))")
        if destination_id is not None:
            conditions.append("ds.destination_id = %s")
            params.append(destination_id)
        if not force_all:
            conditions.append(
                "(ds.last_checked_at IS NULL OR ds.last_checked_at <= NOW() - (GREATEST(0, d.check_interval_minutes - %s) * INTERVAL '1 minute'))"
            )
            params.append(int(due_grace_minutes))

    where_sql = "WHERE " + " AND ".join(conditions)
    params.append(int(max_slots))

    query = f"""
        SELECT
            ds.*,
            d.name AS destination_name,
            d.origin AS origin,
            d.destination AS destination_code,
            d.trip_type AS trip_type,
            d.currency AS currency,
            d.gl AS gl,
            d.gl_pool AS gl_pool,
            d.hl AS hl,
            d.target_price AS target_price,
            d.check_interval_minutes AS check_interval_minutes,
            d.filter_config AS filter_config
        FROM date_slots ds
        JOIN destinations d ON d.id = ds.destination_id
        {where_sql}
        ORDER BY ds.last_checked_at ASC NULLS FIRST, ds.id ASC
        LIMIT %s;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(cast(LiteralString, query), tuple(params))
            rows = cur.fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["latest_cheapest_price"] = _to_float(item.get("latest_cheapest_price"))
        item["latest_best_price"] = _to_float(item.get("latest_best_price"))
        item["all_time_low_price"] = _to_float(item.get("all_time_low_price"))
        item["last_alerted_price"] = _to_float(item.get("last_alerted_price"))
        item["target_price"] = _to_float(item.get("target_price")) or 0.0
        item["filter_config"] = item.get("filter_config") or {}
        pool_list = parse_gl_pool(item.get("gl_pool") or item.get("gl"))
        item["gl_pool"] = ",".join(pool_list)
        item["last_winning_gl"] = str(item.get("last_winning_gl") or item.get("gl") or pool_list[0]).strip().upper()
        item["window_info"] = get_flight_window_status(item.get("outbound_date"), item.get("return_date"))
        results.append(item)
    return results


def fetch_shard_due_work_package(
    shard_index: int = 0,
    total_shards: int = 1,
    force_all: bool = False,
    max_slots: int = MAX_SLOTS_PER_RUN,
    due_grace_minutes: int = DUE_GRACE_MINUTES,
    destination_id: Optional[int] = None,
    slot_id: Optional[int] = None,
) -> dict[str, Any]:
    """
    Phase 1 single-round-trip read for RAM-buffered sharded execution:
      1. Reads all active destinations with their active (<=330d) slot counts.
      2. Deterministically assigns this shard's disjoint subset via `assign_destinations_to_shard`.
      3. Fetches due `date_slots` for those destinations along with all historical baselines
         (`historical_min_price`, `prev_effective_price`, `last_alerted_price`, `last_atl_alerted_price`).
    """
    if total_shards < 1:
        raise ValueError(f"total_shards must be >= 1, got {total_shards}")
    if shard_index < 0 or shard_index >= total_shards:
        raise ValueError(f"shard_index ({shard_index}) must be in [0, {total_shards - 1}]")

    def _op() -> dict[str, Any]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                if slot_id is not None:
                    cur.execute(
                        """
                        SELECT
                            d.*,
                            COUNT(ds.id) FILTER (
                                WHERE ds.is_active = TRUE
                                  AND ds.outbound_date::date >= CURRENT_DATE
                                  AND ds.outbound_date::date <= (CURRENT_DATE + INTERVAL '330 days')
                                  AND (ds.return_date IS NULL OR ds.return_date::date <= (CURRENT_DATE + INTERVAL '330 days'))
                            ) AS active_slot_count
                        FROM destinations d
                        JOIN date_slots ds ON ds.destination_id = d.id
                        WHERE ds.id = %s
                        GROUP BY d.id;
                        """,
                        (slot_id,),
                    )
                    all_dest_rows = [dict(r) for r in cur.fetchall()]
                elif destination_id is not None:
                    cur.execute(
                        """
                        SELECT
                            d.*,
                            COUNT(ds.id) FILTER (
                                WHERE ds.is_active = TRUE
                                  AND ds.outbound_date::date >= CURRENT_DATE
                                  AND ds.outbound_date::date <= (CURRENT_DATE + INTERVAL '330 days')
                                  AND (ds.return_date IS NULL OR ds.return_date::date <= (CURRENT_DATE + INTERVAL '330 days'))
                            ) AS active_slot_count
                        FROM destinations d
                        LEFT JOIN date_slots ds ON ds.destination_id = d.id
                        WHERE d.id = %s
                        GROUP BY d.id;
                        """,
                        (destination_id,),
                    )
                    all_dest_rows = [dict(r) for r in cur.fetchall()]
                else:
                    cur.execute(
                        """
                        SELECT
                            d.*,
                            COUNT(ds.id) FILTER (
                                WHERE ds.is_active = TRUE
                                  AND ds.outbound_date::date >= CURRENT_DATE
                                  AND ds.outbound_date::date <= (CURRENT_DATE + INTERVAL '330 days')
                                  AND (ds.return_date IS NULL OR ds.return_date::date <= (CURRENT_DATE + INTERVAL '330 days'))
                            ) AS active_slot_count
                        FROM destinations d
                        LEFT JOIN date_slots ds ON ds.destination_id = d.id
                        WHERE d.is_active = TRUE
                        GROUP BY d.id
                        ORDER BY d.id ASC;
                        """
                    )
                    all_dest_rows = [dict(r) for r in cur.fetchall()]

                for d_item in all_dest_rows:
                    d_item["target_price"] = _to_float(d_item.get("target_price")) or 0.0
                    d_item["filter_config"] = d_item.get("filter_config") or {}
                    pool_list = parse_gl_pool(d_item.get("gl_pool") or d_item.get("gl"))
                    d_item["gl_pool"] = ",".join(pool_list)
                    d_item["gl"] = str(d_item.get("gl") or pool_list[0]).strip().upper()
                    d_item["active_slot_count"] = int(d_item.get("active_slot_count") or 0)

                if slot_id is not None or destination_id is not None:
                    assigned_dests = all_dest_rows
                else:
                    assigned_dests = assign_destinations_to_shard(
                        all_dest_rows,
                        shard_index=shard_index,
                        total_shards=total_shards,
                    )

                if not assigned_dests or int(max_slots) <= 0:
                    return {
                        "shard_index": shard_index,
                        "total_shards": total_shards,
                        "all_active_destinations": all_dest_rows,
                        "assigned_destinations": assigned_dests,
                        "due_slots": [],
                    }

                assigned_ids = [int(d["id"]) for d in assigned_dests]
                conditions: list[str] = ["ds.destination_id = ANY(%s)"]
                params: list[Any] = [assigned_ids]

                if slot_id is not None:
                    conditions.append("ds.id = %s")
                    params.append(slot_id)
                else:
                    conditions.append("ds.is_active = TRUE")
                    if destination_id is None:
                        conditions.append("d.is_active = TRUE")
                    conditions.append("ds.outbound_date::date <= (CURRENT_DATE + INTERVAL '330 days')")
                    conditions.append("ds.outbound_date::date >= CURRENT_DATE")
                    conditions.append("(ds.return_date IS NULL OR ds.return_date::date <= (CURRENT_DATE + INTERVAL '330 days'))")
                    if not force_all:
                        conditions.append(
                            "(ds.last_checked_at IS NULL OR ds.last_checked_at <= NOW() - (GREATEST(0, d.check_interval_minutes - %s) * INTERVAL '1 minute'))"
                        )
                        params.append(int(due_grace_minutes))

                where_sql = "WHERE " + " AND ".join(conditions)
                params.append(int(max_slots))

                due_query = f"""
                    SELECT
                        ds.*,
                        d.name AS destination_name,
                        d.origin AS origin,
                        d.destination AS destination_code,
                        d.trip_type AS trip_type,
                        d.currency AS currency,
                        d.gl AS gl,
                        d.gl_pool AS gl_pool,
                        d.hl AS hl,
                        d.target_price AS target_price,
                        d.check_interval_minutes AS check_interval_minutes,
                        d.filter_config AS filter_config,
                        (
                            SELECT MIN(ps.cheapest_tab_price)
                            FROM price_snapshots ps
                            WHERE ps.date_slot_id = ds.id
                        ) AS snap_min_price,
                        (
                            SELECT COALESCE(al.cheapest_price, al.best_price)
                            FROM alert_logs al
                            WHERE al.date_slot_id = ds.id
                              AND POSITION('ALL_TIME_LOW' IN al.trigger_type) > 0
                            ORDER BY al.sent_at DESC
                            LIMIT 1
                        ) AS last_atl_alerted_price
                    FROM date_slots ds
                    JOIN destinations d ON d.id = ds.destination_id
                    {where_sql}
                    ORDER BY ds.last_checked_at ASC NULLS FIRST, ds.id ASC
                    LIMIT %s;
                """
                cur.execute(cast(LiteralString, due_query), tuple(params))
                slot_rows = cur.fetchall()

        due_slots: list[dict[str, Any]] = []
        for row in slot_rows:
            item = dict(row)
            prev_cheapest = _to_float(item.get("latest_cheapest_price"))
            prev_best = _to_float(item.get("latest_best_price"))
            slot_atl = _to_float(item.get("all_time_low_price"))
            snap_min = _to_float(item.get("snap_min_price"))
            if slot_atl is not None and snap_min is not None:
                historical_min_price: Optional[float] = min(slot_atl, snap_min)
            else:
                historical_min_price = slot_atl if slot_atl is not None else snap_min

            pool_list = parse_gl_pool(item.get("gl_pool") or item.get("gl"))
            item["latest_cheapest_price"] = prev_cheapest
            item["latest_best_price"] = prev_best
            item["all_time_low_price"] = slot_atl
            item["last_alerted_price"] = _to_float(item.get("last_alerted_price"))
            item["target_price"] = _to_float(item.get("target_price")) or 0.0
            item["filter_config"] = item.get("filter_config") or {}
            item["gl_pool"] = ",".join(pool_list)
            item["last_winning_gl"] = str(item.get("last_winning_gl") or item.get("gl") or pool_list[0]).strip().upper()
            item["historical_min_price"] = historical_min_price
            item["prev_effective_price"] = prev_cheapest if prev_cheapest is not None else prev_best
            item["last_atl_alerted_price"] = _to_float(item.get("last_atl_alerted_price"))
            item["window_info"] = get_flight_window_status(item.get("outbound_date"), item.get("return_date"))
            due_slots.append(item)

        return {
            "shard_index": shard_index,
            "total_shards": total_shards,
            "all_active_destinations": all_dest_rows,
            "assigned_destinations": assigned_dests,
            "due_slots": due_slots,
        }

    return cast(dict[str, Any], _execute_with_neon_retry("fetch_shard_due_work_package", _op))


# ==============================================================================
# PRICE SNAPSHOTS & HISTORICAL METRICS
# ==============================================================================


def record_price_snapshot(
    date_slot_id: int,
    destination_id: int,
    cheapest_tab_price: float,
    best_tab_price: Optional[float] = None,
    cheapest_airline: Optional[str] = None,
    cheapest_stops: Optional[str] = None,
    cheapest_duration: Optional[str] = None,
    cheapest_dep_arr: Optional[str] = None,
    origin_airport: Optional[str] = None,
    destination_airport: Optional[str] = None,
    best_airline: Optional[str] = None,
    best_stops: Optional[str] = None,
    best_duration: Optional[str] = None,
    best_origin_airport: Optional[str] = None,
    best_destination_airport: Optional[str] = None,
    is_self_transfer: bool = False,
    winning_gl: str = DEFAULT_GL,
    extraction_source: str = "patchright_dom",
    raw_top_offers: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    """
    Persist a new dual-tab price snapshot including winning N:M airports and `winning_gl`
    and atomically update the parent `date_slots` cached columns (`last_winning_gl`, `all_time_low_gl`).
    """
    cheapest_dec = Decimal(str(round(cheapest_tab_price, 2)))
    best_dec = Decimal(str(round(best_tab_price, 2))) if best_tab_price is not None else None
    clean_orig = origin_airport.strip().upper()[:10] if origin_airport else None
    clean_dest = destination_airport.strip().upper()[:10] if destination_airport else None
    clean_best_orig = (best_origin_airport or origin_airport or "").strip().upper()[:10] or None
    clean_best_dest = (best_destination_airport or destination_airport or "").strip().upper()[:10] or None
    clean_winning_gl = (winning_gl or DEFAULT_GL).strip().upper()[:8]

    with get_connection() as conn:
        with conn.cursor() as cur:
            # 1. Fetch prior historical baseline for this date_slot BEFORE inserting current snapshot
            cur.execute(
                """
                SELECT
                    ds.all_time_low_price AS slot_all_time_low,
                    ds.all_time_low_gl AS slot_all_time_low_gl,
                    ds.last_alerted_at,
                    ds.last_alerted_price,
                    ds.latest_cheapest_price AS prev_cheapest_price,
                    ds.latest_best_price AS prev_best_price,
                    (
                        SELECT COALESCE(al.cheapest_price, al.best_price)
                        FROM alert_logs al
                        WHERE al.date_slot_id = %s
                          AND POSITION('ALL_TIME_LOW' IN al.trigger_type) > 0
                        ORDER BY al.sent_at DESC
                        LIMIT 1
                    ) AS last_atl_alerted_price,
                    (
                        SELECT MIN(ps.cheapest_tab_price)
                        FROM price_snapshots ps
                        WHERE ps.date_slot_id = %s
                    ) AS snap_min_price,
                    (
                        SELECT AVG(ps.cheapest_tab_price)
                        FROM price_snapshots ps
                        WHERE ps.date_slot_id = %s
                          AND ps.scraped_at >= NOW() - INTERVAL '7 days'
                    ) AS rolling_7d_avg,
                    (
                        SELECT COUNT(*)
                        FROM price_snapshots ps
                        WHERE ps.date_slot_id = %s
                          AND ps.scraped_at >= NOW() - INTERVAL '7 days'
                    ) AS rolling_7d_count
                FROM date_slots ds
                WHERE ds.id = %s;
                """,
                (date_slot_id, date_slot_id, date_slot_id, date_slot_id, date_slot_id),
            )
            stats_row_raw = cur.fetchone()
            stats_row = dict(stats_row_raw) if stats_row_raw else {}

            slot_atl = _to_float(stats_row.get("slot_all_time_low"))
            snap_min = _to_float(stats_row.get("snap_min_price"))
            if slot_atl is not None and snap_min is not None:
                historical_min_price: Optional[float] = min(slot_atl, snap_min)
            else:
                historical_min_price = slot_atl if slot_atl is not None else snap_min

            rolling_7d_avg = _to_float(stats_row.get("rolling_7d_avg"))
            rolling_7d_count = int(stats_row.get("rolling_7d_count") or 0)

            # 2. Insert snapshot into price_snapshots
            cur.execute(
                """
                INSERT INTO price_snapshots (
                    date_slot_id, destination_id, cheapest_tab_price, best_tab_price,
                    cheapest_airline, cheapest_stops, cheapest_duration, cheapest_dep_arr,
                    origin_airport, destination_airport,
                    best_airline, best_stops, best_duration,
                    best_origin_airport, best_destination_airport,
                    is_self_transfer, winning_gl, extraction_source, raw_top_offers
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, scraped_at;
                """,
                (
                    date_slot_id,
                    destination_id,
                    cheapest_dec,
                    best_dec,
                    cheapest_airline,
                    cheapest_stops,
                    cheapest_duration,
                    cheapest_dep_arr,
                    clean_orig,
                    clean_dest,
                    best_airline,
                    best_stops,
                    best_duration,
                    clean_best_orig,
                    clean_best_dest,
                    bool(is_self_transfer),
                    clean_winning_gl,
                    extraction_source[:30],
                    Jsonb(raw_top_offers or []),
                ),
            )
            snap_inserted = cur.fetchone()
            if not snap_inserted:
                raise RuntimeError("Failed to record price snapshot: no row returned.")
            snapshot_id = int(snap_inserted["id"])
            scraped_at = snap_inserted["scraped_at"]

            # 3. Compute updated all-time low & all_time_low_gl and update date_slots
            if historical_min_price is None or float(cheapest_dec) <= historical_min_price:
                new_all_time_low = float(cheapest_dec)
                new_all_time_low_gl = clean_winning_gl
            else:
                new_all_time_low = historical_min_price
                new_all_time_low_gl = (
                    str(stats_row.get("slot_all_time_low_gl") or "").strip().upper()[:8] or None
                )

            cur.execute(
                """
                UPDATE date_slots
                SET last_checked_at = NOW(),
                    latest_cheapest_price = %s,
                    latest_best_price = %s,
                    latest_airline = %s,
                    latest_stops = %s,
                    latest_duration = %s,
                    latest_origin_airport = %s,
                    latest_destination_airport = %s,
                    latest_best_origin_airport = %s,
                    latest_best_destination_airport = %s,
                    latest_is_self_transfer = %s,
                    last_winning_gl = %s,
                    all_time_low_price = %s,
                    all_time_low_gl = %s
                WHERE id = %s;
                """,
                (
                    cheapest_dec,
                    best_dec,
                    cheapest_airline,
                    cheapest_stops,
                    cheapest_duration,
                    clean_orig,
                    clean_dest,
                    clean_best_orig,
                    clean_best_dest,
                    bool(is_self_transfer),
                    clean_winning_gl,
                    Decimal(str(round(new_all_time_low, 2))),
                    new_all_time_low_gl,
                    date_slot_id,
                ),
            )
        conn.commit()
    clear_db_cache()

    prev_cheapest = _to_float(stats_row.get("prev_cheapest_price"))
    prev_best = _to_float(stats_row.get("prev_best_price"))
    prev_effective_price = prev_cheapest if prev_cheapest is not None else prev_best

    return {
        "snapshot_id": snapshot_id,
        "scraped_at": scraped_at,
        "winning_gl": clean_winning_gl,
        "historical_min_price": historical_min_price,
        "new_all_time_low": new_all_time_low,
        "new_all_time_low_gl": new_all_time_low_gl,
        "rolling_7d_avg": rolling_7d_avg,
        "rolling_7d_count": rolling_7d_count,
        "last_alerted_at": stats_row.get("last_alerted_at"),
        "last_alerted_price": _to_float(stats_row.get("last_alerted_price")),
        "prev_effective_price": prev_effective_price,
        "last_atl_alerted_price": _to_float(stats_row.get("last_atl_alerted_price")),
    }


def flush_shard_results_batch(
    ram_results: Sequence[dict[str, Any]],
    batch_started_at: datetime.datetime,
    failed_slot_ids: Optional[Sequence[int]] = None,
) -> dict[str, int]:
    """
    Phase 3 atomic batch flush for RAM-buffered scraping:
      - Inserts all `price_snapshots` (preserving each slot's real `scraped_at` and `winning_gl`)
      - Updates `date_slots` (`last_checked_at = batch_started_at`, `latest_*`, `last_winning_gl`,
        `all_time_low_price`, `all_time_low_gl`)
      - Updates `last_checked_at = batch_started_at` on any failed slots so they do not starve the queue.
    Executes in a single PostgreSQL transaction.
    """
    if not ram_results and not failed_slot_ids:
        return {"snapshots_inserted": 0, "slots_updated": 0, "failed_touched": 0}

    if batch_started_at.tzinfo is None:
        batch_ts = batch_started_at.replace(tzinfo=datetime.timezone.utc)
    else:
        batch_ts = batch_started_at

    snap_params: list[tuple[Any, ...]] = []
    slot_params: list[tuple[Any, ...]] = []

    for item in ram_results:
        slot_id = int(item["date_slot_id"])
        dest_id = int(item["destination_id"])
        cheapest_val = float(item["cheapest_tab_price"])
        best_val = item.get("best_tab_price")
        cheapest_dec = Decimal(str(round(cheapest_val, 2)))
        best_dec = Decimal(str(round(float(best_val), 2))) if best_val is not None else None

        clean_orig = str(item.get("origin_airport") or "").strip().upper()[:10] or None
        clean_dest = str(item.get("destination_airport") or "").strip().upper()[:10] or None
        clean_best_orig = (
            str(item.get("best_origin_airport") or clean_orig or "").strip().upper()[:10] or None
        )
        clean_best_dest = (
            str(item.get("best_destination_airport") or clean_dest or "").strip().upper()[:10] or None
        )
        clean_winning_gl = str(item.get("winning_gl") or DEFAULT_GL).strip().upper()[:8]
        scraped_at = item.get("scraped_at") or datetime.datetime.now(datetime.timezone.utc)
        if isinstance(scraped_at, datetime.datetime) and scraped_at.tzinfo is None:
            scraped_at = scraped_at.replace(tzinfo=datetime.timezone.utc)

        snap_params.append(
            (
                slot_id,
                dest_id,
                scraped_at,
                cheapest_dec,
                best_dec,
                item.get("cheapest_airline"),
                item.get("cheapest_stops"),
                item.get("cheapest_duration"),
                item.get("cheapest_dep_arr"),
                clean_orig,
                clean_dest,
                item.get("best_airline"),
                item.get("best_stops"),
                item.get("best_duration"),
                clean_best_orig,
                clean_best_dest,
                bool(item.get("is_self_transfer", False)),
                clean_winning_gl,
                str(item.get("extraction_source") or "patchright_dom")[:30],
                Jsonb(item.get("raw_top_offers") or []),
                slot_id,
                scraped_at,
            )
        )

        slot_params.append(
            (
                batch_ts,
                cheapest_dec,
                best_dec,
                item.get("cheapest_airline"),
                item.get("cheapest_stops"),
                item.get("cheapest_duration"),
                clean_orig,
                clean_dest,
                clean_best_orig,
                clean_best_dest,
                bool(item.get("is_self_transfer", False)),
                clean_winning_gl,
                cheapest_dec,
                cheapest_dec,
                cheapest_dec,
                clean_winning_gl,
                slot_id,
            )
        )

    clean_failed_ids = [int(sid) for sid in (failed_slot_ids or [])]

    def _op() -> dict[str, int]:
        inserted_count = 0
        with get_connection() as conn:
            with conn.cursor() as cur:
                if snap_params:
                    cur.executemany(
                        """
                        INSERT INTO price_snapshots (
                            date_slot_id, destination_id, scraped_at,
                            cheapest_tab_price, best_tab_price,
                            cheapest_airline, cheapest_stops, cheapest_duration, cheapest_dep_arr,
                            origin_airport, destination_airport,
                            best_airline, best_stops, best_duration,
                            best_origin_airport, best_destination_airport,
                            is_self_transfer, winning_gl, extraction_source, raw_top_offers
                        )
                        SELECT
                            %s::integer, %s::integer, %s::timestamptz,
                            %s::numeric, %s::numeric,
                            %s::text, %s::text, %s::text, %s::text,
                            %s::varchar, %s::varchar,
                            %s::text, %s::text, %s::text,
                            %s::varchar, %s::varchar,
                            %s::boolean, %s::varchar, %s::varchar, %s::jsonb
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM price_snapshots ps
                            WHERE ps.date_slot_id = %s::integer
                              AND ps.scraped_at = %s::timestamptz
                        );
                        """,
                        snap_params,
                    )
                    inserted_count = len(snap_params)
                if slot_params:
                    cur.executemany(
                        """
                        UPDATE date_slots
                        SET last_checked_at = %s,
                            latest_cheapest_price = %s,
                            latest_best_price = %s,
                            latest_airline = %s,
                            latest_stops = %s,
                            latest_duration = %s,
                            latest_origin_airport = %s,
                            latest_destination_airport = %s,
                            latest_best_origin_airport = %s,
                            latest_best_destination_airport = %s,
                            latest_is_self_transfer = %s,
                            last_winning_gl = %s,
                            all_time_low_price = CASE
                                WHEN all_time_low_price IS NULL THEN %s
                                ELSE LEAST(all_time_low_price, %s)
                            END,
                            all_time_low_gl = CASE
                                WHEN all_time_low_price IS NULL OR %s <= all_time_low_price THEN %s
                                ELSE all_time_low_gl
                            END
                        WHERE id = %s;
                        """,
                        slot_params,
                    )
                if clean_failed_ids:
                    cur.execute(
                        "UPDATE date_slots SET last_checked_at = %s WHERE id = ANY(%s);",
                        (batch_ts, clean_failed_ids),
                    )
            conn.commit()
        clear_db_cache()
        return {
            "snapshots_inserted": inserted_count,
            "slots_updated": len(slot_params),
            "failed_touched": len(clean_failed_ids),
        }

    return cast(dict[str, int], _execute_with_neon_retry("flush_shard_results_batch", _op))


def touch_slot_checked(date_slot_id: int) -> None:
    """
    Update `last_checked_at = NOW()` when a slot was attempted so a single failing
    date slot does not starve the rest of the queue on subsequent runs.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE date_slots SET last_checked_at = NOW() WHERE id = %s;",
                (date_slot_id,),
            )
        conn.commit()
    clear_db_cache()


def get_snapshots_for_destination(destination_id: int, days: int = 30) -> list[dict[str, Any]]:
    """
    Fetch historical price snapshots for all date slots belonging to a destination
    for the Plotly multi-date evolution chart and dual-line Cheapest vs. Best chart.
    Cached for 30s per `(destination_id, days)`.
    """
    cache_key = ("get_snapshots", int(destination_id), int(days))
    hit, cached = _cache_get(cache_key)
    if hit:
        return cached

    query = """
        SELECT
            ps.*,
            ds.label AS slot_label,
            ds.outbound_date,
            ds.return_date
        FROM price_snapshots ps
        JOIN date_slots ds ON ds.id = ps.date_slot_id
        WHERE ps.destination_id = %s
          AND ps.scraped_at >= NOW() - (%s * INTERVAL '1 day')
        ORDER BY ps.scraped_at ASC;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (destination_id, int(days)))
            rows = cur.fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["cheapest_tab_price"] = _to_float(item.get("cheapest_tab_price"))
        item["best_tab_price"] = _to_float(item.get("best_tab_price"))
        results.append(item)
    _cache_set(cache_key, results)
    return results


# ==============================================================================
# ALERTS LOGGING & DASHBOARD KPI SUMMARY
# ==============================================================================


def record_alert_and_update_slots(
    destination_id: int,
    triggered_slots: list[dict[str, Any]],
    message_preview: str,
) -> list[int]:
    """
    Record an alert_logs entry for each triggered slot in a destination digest
    and update `last_alerted_at = NOW(), last_alerted_price = cheapest_tab_price`
    on each corresponding `date_slots` row.
    """
    log_ids: list[int] = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            for item in triggered_slots:
                slot_id = item.get("date_slot_id")
                trigger_type = str(item.get("trigger_type", "TARGET_BREACH"))
                cheapest_price = item.get("cheapest_tab_price")
                best_price = item.get("best_tab_price")

                cheapest_dec = (
                    Decimal(str(round(float(cheapest_price), 2)))
                    if cheapest_price is not None
                    else None
                )
                best_dec = (
                    Decimal(str(round(float(best_price), 2)))
                    if best_price is not None
                    else None
                )

                cur.execute(
                    """
                    INSERT INTO alert_logs (
                        destination_id, date_slot_id, trigger_type,
                        cheapest_price, best_price, message_preview
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (
                        destination_id,
                        slot_id,
                        trigger_type,
                        cheapest_dec,
                        best_dec,
                        message_preview[:1500],
                    ),
                )
                ins_alert = cur.fetchone()
                if ins_alert:
                    log_ids.append(int(ins_alert["id"]))

                effective_dec = cheapest_dec if cheapest_dec is not None else best_dec
                if slot_id is not None and effective_dec is not None:
                    cur.execute(
                        """
                        UPDATE date_slots
                        SET last_alerted_at = NOW(),
                            last_alerted_price = %s
                        WHERE id = %s;
                        """,
                        (effective_dec, int(slot_id)),
                    )
        conn.commit()
    clear_db_cache()
    return log_ids


def record_alerts_batch(
    alert_batches: Sequence[dict[str, Any]],
) -> list[int]:
    """
    Phase 3 batch persistence of `alert_logs` and `date_slots` alert state across
    all destinations in a shard in a single PostgreSQL transaction.
    Each item in `alert_batches` has: `destination_id`, `triggered_slots`, `message_preview`.
    """
    if not alert_batches:
        return []

    log_params: list[tuple[Any, ...]] = []
    slot_updates: list[tuple[Any, int]] = []

    for batch in alert_batches:
        dest_id = int(batch["destination_id"])
        message_preview = str(batch.get("message_preview") or "")[:1500]
        for item in batch.get("triggered_slots") or []:
            slot_id = item.get("date_slot_id")
            trigger_type = str(item.get("trigger_type", "TARGET_BREACH"))
            cheapest_price = item.get("cheapest_tab_price")
            best_price = item.get("best_tab_price")
            cheapest_dec = (
                Decimal(str(round(float(cheapest_price), 2)))
                if cheapest_price is not None
                else None
            )
            best_dec = (
                Decimal(str(round(float(best_price), 2)))
                if best_price is not None
                else None
            )
            log_params.append(
                (dest_id, slot_id, trigger_type, cheapest_dec, best_dec, message_preview)
            )
            effective_dec = cheapest_dec if cheapest_dec is not None else best_dec
            if slot_id is not None and effective_dec is not None:
                slot_updates.append((effective_dec, int(slot_id)))

    if not log_params and not slot_updates:
        return []

    def _op() -> list[int]:
        log_ids: list[int] = []
        with get_connection() as conn:
            with conn.cursor() as cur:
                for lp in log_params:
                    cur.execute(
                        """
                        INSERT INTO alert_logs (
                            destination_id, date_slot_id, trigger_type,
                            cheapest_price, best_price, message_preview
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING id;
                        """,
                        lp,
                    )
                    row = cur.fetchone()
                    if row:
                        log_ids.append(int(row["id"]))
                if slot_updates:
                    cur.executemany(
                        """
                        UPDATE date_slots
                        SET last_alerted_at = NOW(),
                            last_alerted_price = %s
                        WHERE id = %s;
                        """,
                        slot_updates,
                    )
            conn.commit()
        clear_db_cache()
        return log_ids

    return cast(list[int], _execute_with_neon_retry("record_alerts_batch", _op))


def get_recent_alert_logs(limit: int = 50) -> list[dict[str, Any]]:
    """
    Retrieve recent alert logs joined with destination and date_slot labels (cached for 30s).
    """
    cache_key = ("get_recent_alert_logs", int(limit))
    hit, cached = _cache_get(cache_key)
    if hit:
        return cached

    query = """
        SELECT
            al.*,
            d.name AS destination_name,
            d.origin,
            d.destination AS destination_code,
            d.currency,
            ds.label AS slot_label,
            ds.outbound_date,
            ds.return_date
        FROM alert_logs al
        LEFT JOIN destinations d ON d.id = al.destination_id
        LEFT JOIN date_slots ds ON ds.id = al.date_slot_id
        ORDER BY al.sent_at DESC
        LIMIT %s;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (int(limit),))
            rows = cur.fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["cheapest_price"] = _to_float(item.get("cheapest_price"))
        item["best_price"] = _to_float(item.get("best_price"))
        results.append(item)
    _cache_set(cache_key, results)
    return results


def get_dashboard_summary() -> dict[str, Any]:
    """
    Compute executive Top Metrics Ribbon KPIs and extraction source health stats
    in a single SQL CTE round-trip (cached for 30s, invalidated on any write).
    """
    cache_key = ("get_dashboard_summary",)
    hit, cached = _cache_get(cache_key)
    if hit:
        return cached

    cte_query = """
        WITH dest_counts AS (
            SELECT
                COUNT(*) FILTER (WHERE is_active = TRUE) AS active_destinations,
                COUNT(*) AS total_destinations
            FROM destinations
        ),
        slot_counts AS (
            SELECT
                COUNT(*) AS total_date_slots,
                COUNT(*) FILTER (WHERE ds.is_active = TRUE AND d.is_active = TRUE) AS active_date_slots,
                COUNT(*) FILTER (
                    WHERE ds.is_active = TRUE AND d.is_active = TRUE
                      AND ds.outbound_date::date <= CURRENT_DATE + INTERVAL '330 days'
                      AND ds.outbound_date::date >= CURRENT_DATE
                      AND (ds.return_date IS NULL OR ds.return_date::date <= CURRENT_DATE + INTERVAL '330 days')
                ) AS slots_in_window,
                COUNT(*) FILTER (
                    WHERE ds.is_active = TRUE AND d.is_active = TRUE
                      AND (
                          ds.outbound_date::date > CURRENT_DATE + INTERVAL '330 days'
                          OR ds.return_date::date > CURRENT_DATE + INTERVAL '330 days'
                      )
                ) AS slots_pending_release,
                COUNT(*) FILTER (
                    WHERE ds.is_active = TRUE
                      AND d.is_active = TRUE
                      AND ds.latest_cheapest_price IS NOT NULL
                      AND ds.latest_cheapest_price <= d.target_price
                ) AS slots_below_target
            FROM date_slots ds
            JOIN destinations d ON d.id = ds.destination_id
        ),
        best_deal AS (
            SELECT json_build_object(
                'slot_id', ds.id,
                'label', ds.label,
                'outbound_date', ds.outbound_date,
                'return_date', ds.return_date,
                'latest_cheapest_price', ds.latest_cheapest_price,
                'latest_best_price', ds.latest_best_price,
                'latest_airline', ds.latest_airline,
                'latest_origin_airport', ds.latest_origin_airport,
                'latest_destination_airport', ds.latest_destination_airport,
                'destination_name', d.name,
                'origin', d.origin,
                'destination_code', d.destination,
                'currency', d.currency,
                'target_price', d.target_price
            ) AS deal_json
            FROM date_slots ds
            JOIN destinations d ON d.id = ds.destination_id
            WHERE ds.is_active = TRUE
              AND d.is_active = TRUE
              AND ds.latest_cheapest_price IS NOT NULL
            ORDER BY (ds.latest_cheapest_price - d.target_price) ASC, ds.latest_cheapest_price ASC
            LIMIT 1
        ),
        sources AS (
            SELECT COALESCE(
                json_agg(src_row ORDER BY src_row.count DESC),
                '[]'::json
            ) AS sources_json
            FROM (
                SELECT
                    extraction_source,
                    COUNT(*) AS count,
                    MAX(scraped_at) AS last_seen
                FROM price_snapshots
                WHERE scraped_at >= NOW() - INTERVAL '7 days'
                GROUP BY extraction_source
            ) src_row
        )
        SELECT
            dc.active_destinations,
            dc.total_destinations,
            sc.active_date_slots,
            sc.slots_in_window,
            sc.slots_pending_release,
            sc.total_date_slots,
            sc.slots_below_target,
            (SELECT deal_json FROM best_deal) AS best_deal_json,
            (SELECT sources_json FROM sources) AS sources_json
        FROM dest_counts dc, slot_counts sc;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(cte_query)
            row = cur.fetchone()
            counts = dict(row) if row else {}

    best_deal_raw = counts.get("best_deal_json")
    best_deal_dict: Optional[dict[str, Any]] = None
    if isinstance(best_deal_raw, dict):
        best_deal_dict = dict(best_deal_raw)
        best_deal_dict["latest_cheapest_price"] = _to_float(best_deal_dict.get("latest_cheapest_price"))
        best_deal_dict["latest_best_price"] = _to_float(best_deal_dict.get("latest_best_price"))
        best_deal_dict["target_price"] = _to_float(best_deal_dict.get("target_price"))

    raw_sources = counts.get("sources_json") or []
    sources_list = [dict(r) for r in raw_sources] if isinstance(raw_sources, list) else []

    summary = {
        "active_destinations": int(counts.get("active_destinations") or 0),
        "total_destinations": int(counts.get("total_destinations") or 0),
        "active_date_slots": int(counts.get("active_date_slots") or 0),
        "slots_in_window": int(counts.get("slots_in_window") or 0),
        "slots_pending_release": int(counts.get("slots_pending_release") or 0),
        "total_date_slots": int(counts.get("total_date_slots") or 0),
        "slots_below_target": int(counts.get("slots_below_target") or 0),
        "best_deal": best_deal_dict,
        "extraction_sources": sources_list,
    }
    _cache_set(cache_key, summary)
    return summary
