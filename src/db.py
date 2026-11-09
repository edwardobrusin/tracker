"""
Neon Serverless PostgreSQL Data Access Layer (psycopg v3) for FlightPulse Matrix v4.0.
Implements the 4-table hierarchical schema:
  destinations -> date_slots -> price_snapshots + alert_logs
"""

from __future__ import annotations

import logging
import sys
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional, cast

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
    DEFAULT_CURRENCY,
    DEFAULT_GL,
    DEFAULT_HL,
    MAX_DESTINATIONS,
    MAX_SLOTS_PER_DESTINATION,
    MAX_SLOTS_PER_RUN,
    MAX_TOTAL_SLOTS,
    get_flight_window_status,
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
    hl VARCHAR(10) DEFAULT 'es',
    target_price NUMERIC(12, 2) NOT NULL,
    check_interval_minutes INTEGER DEFAULT 60,
    cooldown_hours INTEGER DEFAULT 12,
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
    all_time_low_price NUMERIC(12, 2),
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


def get_connection(retries: int = 3, delay_seconds: float = 1.5) -> psycopg.Connection[Any]:
    """
    Create a psycopg v3 connection to Neon Serverless PostgreSQL with retry
    logic to handle serverless compute cold-starts gracefully.
    """
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is not configured. Set DATABASE_URL in your .env file or Streamlit secrets."
        )

    if psycopg is None or dict_row is None:
        raise RuntimeError(
            "psycopg is not installed. Install psycopg[binary] to connect to Neon PostgreSQL."
        )

    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            conn: psycopg.Connection[Any] = cast(Any, psycopg).connect(
                DATABASE_URL,
                row_factory=dict_row,
                connect_timeout=15,
                autocommit=False,
            )
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


def init_db() -> None:
    """
    Auto-initialize the 4 tables, execute idempotent N:M multi-airport migrations,
    and verify indexes in Neon PostgreSQL.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(cast(LiteralString, SCHEMA_SQL))
            cur.execute(cast(LiteralString, MIGRATION_SQL))
        conn.commit()
    logger.info("Database schema initialized and migrated for N:M Multi-Airport support.")


# ==============================================================================
# DESTINATION CRUD OPERATIONS
# ==============================================================================


def list_destinations(active_only: bool = False) -> list[dict[str, Any]]:
    """
    Fetch all destinations with aggregated slot metrics (active slot count,
    lowest cheapest/best prices across slots, slots below target price).
    """
    where_clause = "WHERE d.is_active = TRUE" if active_only else ""
    query = f"""
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
        {where_clause}
        GROUP BY d.id
        ORDER BY d.is_active DESC, d.id ASC;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(cast(LiteralString, query))
            rows = cur.fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["target_price"] = _to_float(item.get("target_price")) or 0.0
        item["min_cheapest_price"] = _to_float(item.get("min_cheapest_price"))
        item["min_best_price"] = _to_float(item.get("min_best_price"))
        item["min_all_time_low"] = _to_float(item.get("min_all_time_low"))
        item["filter_config"] = item.get("filter_config") or {}
        results.append(item)
    return results


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
    return item


def create_destination(
    name: str,
    origin: str | list[str],
    destination: str | list[str],
    target_price: float,
    trip_type: str = "round-trip",
    currency: str = DEFAULT_CURRENCY,
    gl: str = DEFAULT_GL,
    hl: str = DEFAULT_HL,
    check_interval_minutes: int = 60,
    cooldown_hours: int = 12,
    filter_config: Optional[dict[str, Any]] = None,
    is_active: bool = True,
) -> int:
    """
    Create a new N:M multi-airport destination corridor & filter profile.
    Validates and normalizes IATA lists (expanding metro codes like TYO -> NRT,HND)
    and enforces the 10-destination active matrix limit when is_active=True.
    """
    origin_airports = parse_iata_list(origin, expand_metro=True)
    destination_airports = parse_iata_list(destination, expand_metro=True)
    norm_origin = ",".join(origin_airports)
    norm_dest = ",".join(destination_airports)

    with get_connection() as conn:
        with conn.cursor() as cur:
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
                    trip_type, currency, gl, hl,
                    target_price, check_interval_minutes, cooldown_hours,
                    filter_config, is_active
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """,
                (
                    name.strip(),
                    norm_origin,
                    norm_dest,
                    origin_airports,
                    destination_airports,
                    trip_type.strip().lower(),
                    currency.strip().upper(),
                    gl.strip().upper(),
                    hl.strip().lower(),
                    Decimal(str(round(target_price, 2))),
                    int(check_interval_minutes),
                    int(cooldown_hours),
                    Jsonb(filter_config or {}),
                    bool(is_active),
                ),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Failed to create destination: no row returned.")
            new_id = int(row["id"])
        conn.commit()
    logger.info("Created destination id=%d (%s: %s -> %s)", new_id, name, norm_origin, norm_dest)
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
    check_interval_minutes: int = 60,
    cooldown_hours: int = 12,
    filter_config: Optional[dict[str, Any]] = None,
    is_active: bool = True,
) -> None:
    """
    Update an existing destination's metadata, N:M airport lists, and filter profile,
    and automatically regenerate `google_flights_url` on all child `date_slots`.
    """
    origin_airports = parse_iata_list(origin, expand_metro=True)
    destination_airports = parse_iata_list(destination, expand_metro=True)
    norm_origin = ",".join(origin_airports)
    norm_dest = ",".join(destination_airports)
    clean_trip = trip_type.strip().lower()
    clean_curr = currency.strip().upper()
    clean_gl = gl.strip().upper()
    clean_hl = hl.strip().lower()
    profile_obj = FilterProfile.from_dict(filter_config or {})

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
                    hl = %s,
                    target_price = %s,
                    check_interval_minutes = %s,
                    cooldown_hours = %s,
                    filter_config = %s,
                    is_active = %s
                WHERE id = %s;
                """,
                (
                    name.strip(),
                    norm_origin,
                    norm_dest,
                    origin_airports,
                    destination_airports,
                    clean_trip,
                    clean_curr,
                    clean_gl,
                    clean_hl,
                    Decimal(str(round(target_price, 2))),
                    int(check_interval_minutes),
                    int(cooldown_hours),
                    Jsonb(filter_config or {}),
                    bool(is_active),
                    destination_id,
                ),
            )

            # Synchronize all existing date_slots URLs with the updated N:M corridor and filters
            cur.execute(
                "SELECT id, outbound_date, return_date FROM date_slots WHERE destination_id = %s;",
                (destination_id,),
            )
            slot_rows = cur.fetchall()
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
                cur.execute(
                    "UPDATE date_slots SET google_flights_url = %s WHERE id = %s;",
                    (rebuilt_url, int(s_row["id"])),
                )
        conn.commit()


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


def delete_destination(destination_id: int) -> None:
    """
    Delete a destination and cascade-delete its date_slots and price_snapshots.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM destinations WHERE id = %s;", (destination_id,))
        conn.commit()
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
    """
    conditions: list[str] = []
    params: list[Any] = []

    if destination_id is not None:
        conditions.append("ds.destination_id = %s")
        params.append(destination_id)
    if active_only:
        conditions.append("ds.is_active = TRUE")

    where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"""
        SELECT
            ds.*,
            d.name AS destination_name,
            d.origin AS origin,
            d.destination AS destination_code,
            d.trip_type AS trip_type,
            d.currency AS currency,
            d.gl AS gl,
            d.hl AS hl,
            d.target_price AS target_price,
            d.check_interval_minutes AS check_interval_minutes,
            d.cooldown_hours AS cooldown_hours,
            d.filter_config AS filter_config,
            d.is_active AS destination_is_active
        FROM date_slots ds
        JOIN destinations d ON d.id = ds.destination_id
        {where_sql}
        ORDER BY ds.destination_id ASC, ds.outbound_date ASC, ds.id ASC;
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
        item["window_info"] = get_flight_window_status(item.get("outbound_date"), item.get("return_date"))
        results.append(item)
    return results


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
            d.hl AS hl,
            d.target_price AS target_price,
            d.check_interval_minutes AS check_interval_minutes,
            d.cooldown_hours AS cooldown_hours,
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
    Create a single date_slot under a destination, enforcing the 10-slot per
    destination and 100-total active slot capacity limits.
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
    return slot_id


def bulk_create_date_slots(
    destination_id: int,
    slots: list[dict[str, Any]],
    replace_existing: bool = False,
) -> list[int]:
    """
    Batch-insert up to 10 date_slots for a destination in a single transaction.
    If `replace_existing=True`, removes existing slots for this destination before inserting.
    """
    created_ids: list[int] = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            if replace_existing:
                cur.execute("DELETE FROM date_slots WHERE destination_id = %s;", (destination_id,))

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

            max_can_add = min(available_for_dest, available_total, len(slots))
            if max_can_add <= 0 and slots:
                raise ValueError(
                    f"Cannot add date slots: destination has {existing_active}/{MAX_SLOTS_PER_DESTINATION} active slots "
                    f"(total active across matrix: {total_active}/{MAX_TOTAL_SLOTS})."
                )

            for idx, slot in enumerate(slots[:max_can_add]):
                outbound_date = str(slot["outbound_date"]).strip()
                return_date = str(slot["return_date"]).strip() if slot.get("return_date") else None
                url = str(slot["google_flights_url"]).strip()
                label = slot.get("label")
                is_active = bool(slot.get("is_active", True))

                cur.execute(
                    """
                    INSERT INTO date_slots (
                        destination_id, label, outbound_date, return_date, google_flights_url, is_active
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (destination_id, label, outbound_date, return_date, url, is_active),
                )
                ins_row = cur.fetchone()
                if ins_row:
                    created_ids.append(int(ins_row["id"]))
        conn.commit()

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


def delete_date_slot(slot_id: int) -> None:
    """
    Delete a single date_slot by ID.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM date_slots WHERE id = %s;", (slot_id,))
        conn.commit()


def fetch_due_date_slots(
    max_slots: int = MAX_SLOTS_PER_RUN,
    force_all: bool = False,
    destination_id: Optional[int] = None,
    slot_id: Optional[int] = None,
) -> list[dict[str, Any]]:
    """
    Sharded Anti-Ban Queue Dispatcher query:
    Selects up to `max_slots` due `date_slots` where `is_active = TRUE` and
    `(last_checked_at IS NULL OR EXTRACT(EPOCH FROM (NOW() - last_checked_at))/60 >= check_interval_minutes)`,
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
                "(ds.last_checked_at IS NULL OR EXTRACT(EPOCH FROM (NOW() - ds.last_checked_at)) / 60.0 >= d.check_interval_minutes)"
            )

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
            d.hl AS hl,
            d.target_price AS target_price,
            d.check_interval_minutes AS check_interval_minutes,
            d.cooldown_hours AS cooldown_hours,
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
        item["window_info"] = get_flight_window_status(item.get("outbound_date"), item.get("return_date"))
        results.append(item)
    return results


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
    extraction_source: str = "patchright_dom",
    raw_top_offers: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    """
    Persist a new dual-tab price snapshot including winning N:M airports
    (`origin_airport`, `destination_airport`, `best_origin_airport`, `best_destination_airport`)
    and atomically update the parent `date_slots` cached columns.
    """
    cheapest_dec = Decimal(str(round(cheapest_tab_price, 2)))
    best_dec = Decimal(str(round(best_tab_price, 2))) if best_tab_price is not None else None
    clean_orig = origin_airport.strip().upper()[:10] if origin_airport else None
    clean_dest = destination_airport.strip().upper()[:10] if destination_airport else None
    clean_best_orig = (best_origin_airport or origin_airport or "").strip().upper()[:10] or None
    clean_best_dest = (best_destination_airport or destination_airport or "").strip().upper()[:10] or None

    with get_connection() as conn:
        with conn.cursor() as cur:
            # 1. Fetch prior historical baseline for this date_slot BEFORE inserting current snapshot
            cur.execute(
                """
                SELECT
                    ds.all_time_low_price AS slot_all_time_low,
                    ds.last_alerted_at,
                    ds.last_alerted_price,
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
                (date_slot_id, date_slot_id, date_slot_id, date_slot_id),
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
                    is_self_transfer, extraction_source, raw_top_offers
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    extraction_source[:30],
                    Jsonb(raw_top_offers or []),
                ),
            )
            snap_inserted = cur.fetchone()
            if not snap_inserted:
                raise RuntimeError("Failed to record price snapshot: no row returned.")
            snapshot_id = int(snap_inserted["id"])
            scraped_at = snap_inserted["scraped_at"]

            # 3. Compute updated all-time low and update date_slots
            new_all_time_low = (
                min(historical_min_price, float(cheapest_dec))
                if historical_min_price is not None
                else float(cheapest_dec)
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
                    all_time_low_price = %s
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
                    Decimal(str(round(new_all_time_low, 2))),
                    date_slot_id,
                ),
            )
        conn.commit()

    return {
        "snapshot_id": snapshot_id,
        "scraped_at": scraped_at,
        "historical_min_price": historical_min_price,
        "new_all_time_low": new_all_time_low,
        "rolling_7d_avg": rolling_7d_avg,
        "rolling_7d_count": rolling_7d_count,
        "last_alerted_at": stats_row.get("last_alerted_at"),
        "last_alerted_price": _to_float(stats_row.get("last_alerted_price")),
    }


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


def get_snapshots_for_destination(destination_id: int, days: int = 30) -> list[dict[str, Any]]:
    """
    Fetch historical price snapshots for all date slots belonging to a destination
    for the Plotly multi-date evolution chart and dual-line Cheapest vs. Best chart.
    """
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

                if slot_id is not None and cheapest_dec is not None:
                    cur.execute(
                        """
                        UPDATE date_slots
                        SET last_alerted_at = NOW(),
                            last_alerted_price = %s
                        WHERE id = %s;
                        """,
                        (cheapest_dec, int(slot_id)),
                    )
        conn.commit()
    return log_ids


def get_recent_alert_logs(limit: int = 50) -> list[dict[str, Any]]:
    """
    Retrieve recent alert logs joined with destination and date_slot labels.
    """
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
    return results


def get_dashboard_summary() -> dict[str, Any]:
    """
    Compute executive Top Metrics Ribbon KPIs and extraction source health stats.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM destinations WHERE is_active = TRUE) AS active_destinations,
                    (SELECT COUNT(*) FROM destinations) AS total_destinations,
                    (SELECT COUNT(*) FROM date_slots ds JOIN destinations d ON d.id = ds.destination_id WHERE ds.is_active = TRUE AND d.is_active = TRUE) AS active_date_slots,
                    (
                        SELECT COUNT(*)
                        FROM date_slots ds
                        JOIN destinations d ON d.id = ds.destination_id
                        WHERE ds.is_active = TRUE AND d.is_active = TRUE
                          AND ds.outbound_date::date <= CURRENT_DATE + INTERVAL '330 days'
                          AND ds.outbound_date::date >= CURRENT_DATE
                          AND (ds.return_date IS NULL OR ds.return_date::date <= CURRENT_DATE + INTERVAL '330 days')
                    ) AS slots_in_window,
                    (
                        SELECT COUNT(*)
                        FROM date_slots ds
                        JOIN destinations d ON d.id = ds.destination_id
                        WHERE ds.is_active = TRUE AND d.is_active = TRUE
                          AND (
                              ds.outbound_date::date > CURRENT_DATE + INTERVAL '330 days'
                              OR ds.return_date::date > CURRENT_DATE + INTERVAL '330 days'
                          )
                    ) AS slots_pending_release,
                    (SELECT COUNT(*) FROM date_slots) AS total_date_slots,
                    (
                        SELECT COUNT(*)
                        FROM date_slots ds
                        JOIN destinations d ON d.id = ds.destination_id
                        WHERE ds.is_active = TRUE
                          AND d.is_active = TRUE
                          AND ds.latest_cheapest_price IS NOT NULL
                          AND ds.latest_cheapest_price <= d.target_price
                    ) AS slots_below_target;
                """
            )
            counts_row = cur.fetchone()
            counts = dict(counts_row) if counts_row else {}

            # Best deal across all active dates
            cur.execute(
                """
                SELECT
                    ds.id AS slot_id,
                    ds.label,
                    ds.outbound_date,
                    ds.return_date,
                    ds.latest_cheapest_price,
                    ds.latest_best_price,
                    ds.latest_airline,
                    ds.latest_origin_airport,
                    ds.latest_destination_airport,
                    d.name AS destination_name,
                    d.origin,
                    d.destination AS destination_code,
                    d.currency,
                    d.target_price
                FROM date_slots ds
                JOIN destinations d ON d.id = ds.destination_id
                WHERE ds.is_active = TRUE
                  AND d.is_active = TRUE
                  AND ds.latest_cheapest_price IS NOT NULL
                ORDER BY (ds.latest_cheapest_price - d.target_price) ASC, ds.latest_cheapest_price ASC
                LIMIT 1;
                """
            )
            best_deal = cur.fetchone()

            # Extraction source distribution (last 7 days)
            cur.execute(
                """
                SELECT
                    extraction_source,
                    COUNT(*) AS count,
                    MAX(scraped_at) AS last_seen
                FROM price_snapshots
                WHERE scraped_at >= NOW() - INTERVAL '7 days'
                GROUP BY extraction_source
                ORDER BY count DESC;
                """
            )
            source_rows = cur.fetchall()

    best_deal_dict: Optional[dict[str, Any]] = None
    if best_deal:
        best_deal_dict = dict(best_deal)
        best_deal_dict["latest_cheapest_price"] = _to_float(best_deal_dict.get("latest_cheapest_price"))
        best_deal_dict["latest_best_price"] = _to_float(best_deal_dict.get("latest_best_price"))
        best_deal_dict["target_price"] = _to_float(best_deal_dict.get("target_price"))

    return {
        "active_destinations": int(counts.get("active_destinations") or 0),
        "total_destinations": int(counts.get("total_destinations") or 0),
        "active_date_slots": int(counts.get("active_date_slots") or 0),
        "slots_in_window": int(counts.get("slots_in_window") or 0),
        "slots_pending_release": int(counts.get("slots_pending_release") or 0),
        "total_date_slots": int(counts.get("total_date_slots") or 0),
        "slots_below_target": int(counts.get("slots_below_target") or 0),
        "best_deal": best_deal_dict,
        "extraction_sources": [dict(r) for r in source_rows],
    }
