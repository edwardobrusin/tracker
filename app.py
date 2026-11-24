"""
FlightPulse Matrix v4.0 — Executive Streamlit Control Center (`app.py`).

Redesigned according to `skills/frontend-design`, `skills/ui-styling`, and `skills/ui-ux-pro-max`.
Features:
  - 330-Day Advance Booking Window Intelligence (airline release cycle tracking)
  - Single-Calendar Range Picker in Tab 2 (fluid 1-click start / 2-click return)
  - Honest status & verified Telegram alerts (zero fabricated prices)
  - Executive Aviation Radar Dark Theme with high contrast & accessible typography
  - Tab 1: 10-Date Matrix Comparison Table + Plotly Multi-Date & Dual-Tab Evolution Charts
  - Tab 2: 2-Step Flight Corridor & Filter Profile + Options A, B (Interactive Range), C
  - Tab 3: Live Scanner, Extraction Health & Telegram Tester
"""

from __future__ import annotations

import concurrent.futures
import datetime
import hmac
from typing import Any, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.alerts import send_test_digest_alert
from src.config import (
    APP_PASSWORD,
    COMMON_AIRLINES,
    DEFAULT_CHECK_INTERVAL_MINUTES,
    DEFAULT_CURRENCY,
    DEFAULT_GL,
    DEFAULT_HL,
    GOOGLE_FLIGHTS_MAX_ADVANCE_DAYS,
    MAX_DESTINATIONS,
    MAX_SLOTS_PER_DESTINATION,
    MAX_TOTAL_SLOTS,
    get_flight_window_status,
    get_proxy_pool,
    sanitize_log_text,
    test_proxy_connection,
)
from src.db import (
    bulk_create_date_slots,
    clear_db_cache,
    create_destination,
    delete_date_slot,
    delete_destination,
    get_dashboard_summary,
    get_recent_alert_logs,
    get_snapshots_for_destination,
    init_db,
    list_date_slots,
    list_destinations,
    set_date_slot_active,
    set_destination_active,
    update_destination,
)
from src.runner import run_sharded_dispatcher
from src.tfs_builder import (
    FilterProfile,
    build_google_flights_url,
    format_corridor_display,
    format_date_slot_label,
    generate_date_matrix,
    normalize_iata_codes,
    parse_iata_list,
    parse_pasted_urls,
)

st.set_page_config(
    page_title=f"FlightPulse Matrix v4.0 | {MAX_DESTINATIONS}×{MAX_SLOTS_PER_DESTINATION} Stealth Flight Tracker",
    page_icon="✈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Executive Dark-Mode Cockpit CSS Styling
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    code, pre, .mono-data {
        font-family: 'JetBrains Mono', monospace !important;
    }

    .block-container {
        padding-top: 1.2rem;
        padding-bottom: 2.5rem;
    }

    /* Executive KPI Cards */
    div[data-testid="stMetric"] {
        background: linear-gradient(145deg, #0F172A 0%, #1E293B 100%);
        border: 1px solid #334155;
        border-radius: 14px;
        padding: 16px 20px;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.4);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    div[data-testid="stMetric"]:hover {
        border-color: #38BDF8;
        transform: translateY(-2px);
    }
    div[data-testid="stMetricLabel"] > label {
        color: #94A3B8 !important;
        font-weight: 600;
        font-size: 0.84rem;
        letter-spacing: 0.02em;
    }
    div[data-testid="stMetricValue"] {
        color: #F8FAFC !important;
        font-weight: 700;
        font-size: 1.75rem !important;
    }

    /* Status Pills & Badges */
    .status-pill-emerald {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 9999px;
        font-size: 0.76rem;
        font-weight: 600;
        background-color: rgba(16, 185, 129, 0.15);
        color: #34D399;
        border: 1px solid rgba(16, 185, 129, 0.35);
    }
    .status-pill-sky {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 9999px;
        font-size: 0.76rem;
        font-weight: 600;
        background-color: rgba(56, 189, 248, 0.15);
        color: #38BDF8;
        border: 1px solid rgba(56, 189, 248, 0.35);
    }
    .status-pill-violet {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 9999px;
        font-size: 0.76rem;
        font-weight: 600;
        background-color: rgba(139, 92, 246, 0.15);
        color: #A78BFA;
        border: 1px solid rgba(139, 92, 246, 0.35);
    }
    .status-pill-amber {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 9999px;
        font-size: 0.76rem;
        font-weight: 600;
        background-color: rgba(245, 158, 11, 0.15);
        color: #FBBF24;
        border: 1px solid rgba(245, 158, 11, 0.35);
    }
    .status-pill-slate {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 9999px;
        font-size: 0.76rem;
        font-weight: 600;
        background-color: rgba(100, 116, 139, 0.15);
        color: #94A3B8;
        border: 1px solid rgba(100, 116, 139, 0.35);
    }

    /* Corridor Header Banner */
    .corridor-banner {
        background: linear-gradient(135deg, #0F172A 0%, #1E293B 100%);
        border: 1px solid #334155;
        border-radius: 14px;
        padding: 18px 24px;
        margin-bottom: 20px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        flex-wrap: wrap;
        gap: 14px;
    }
    .corridor-codes {
        font-size: 1.5rem;
        font-weight: 700;
        color: #F8FAFC;
        display: flex;
        align-items: center;
        gap: 10px;
    }
    .corridor-iata {
        background: #1E293B;
        border: 1px solid #475569;
        padding: 2px 10px;
        border-radius: 8px;
        color: #38BDF8;
        font-family: 'JetBrains Mono', monospace;
    }

    /* Form container styling */
    .stForm {
        background: #0F172A;
        border: 1px solid #334155;
        border-radius: 14px;
        padding: 22px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def _format_relative_time(dt_val: Any) -> str:
    """Format a timezone-aware timestamp into a human-friendly relative time string."""
    if not dt_val:
        return "Pendiente"
    if isinstance(dt_val, str):
        try:
            dt_val = datetime.datetime.fromisoformat(dt_val)
        except Exception:
            return dt_val
    now = datetime.datetime.now(datetime.timezone.utc)
    if getattr(dt_val, "tzinfo", None) is None:
        dt_val = dt_val.replace(tzinfo=datetime.timezone.utc)
    diff_sec = max(0, (now - dt_val).total_seconds())
    mins = int(diff_sec // 60)
    if mins < 1:
        return "Hace instantes"
    if mins < 60:
        return f"Hace {mins}m"
    hours = mins // 60
    rem_mins = mins % 60
    if hours < 24:
        return f"Hace {hours}h {rem_mins}m"
    days = hours // 24
    return f"Hace {days}d"


def check_password_gate() -> bool:
    """Protect the Streamlit Control Center with `APP_PASSWORD`."""
    if st.session_state.get("authenticated", False):
        return True

    col_left, col_center, col_right = st.columns([1, 1.4, 1])
    with col_center:
        st.markdown("")
        st.markdown("## FlightPulse Matrix v4.0")
        st.caption(
            f"Self-Hosted {MAX_DESTINATIONS}×{MAX_SLOTS_PER_DESTINATION} Google Flights Dual-Tab Stealth Engine (`Best` vs. `Cheapest`)"
        )
        with st.form("login_form", clear_on_submit=False):
            pwd_input = st.text_input(
                "Contraseña de Acceso (`APP_PASSWORD`)",
                type="password",
                placeholder="Ingresa tu contraseña...",
            )
            submitted = st.form_submit_button("Desbloquear Control Center", width="stretch")
            if submitted:
                if hmac.compare_digest(pwd_input.strip(), APP_PASSWORD.strip()):
                    st.session_state["authenticated"] = True
                    st.rerun()
                else:
                    st.error("Contraseña incorrecta. Verifica `APP_PASSWORD` en tu `.env` o Secrets.")
    return False


def ensure_database_ready() -> bool:
    """Initialize Neon PostgreSQL tables once per session and report connection health."""
    if not st.session_state.get("db_initialized", False):
        try:
            init_db()
            st.session_state["db_initialized"] = True
        except Exception as exc:
            st.error(
                f"**Error de conexión a Neon PostgreSQL (`DATABASE_URL`):** `{exc}`\n\n"
                "Configura `DATABASE_URL` en tu archivo `.env` o en Streamlit Cloud Secrets."
            )
            return False
    return True


# ==============================================================================
# TOP METRICS RIBBON (WITH 330-DAY WINDOW METRICS)
# ==============================================================================


def render_top_metrics_ribbon(summary: dict[str, Any]) -> None:
    """Render the 4 executive KPI cards across the top of the dashboard."""
    active_dests = summary.get("active_destinations", 0)
    active_slots = summary.get("active_date_slots", 0)
    slots_in_win = summary.get("slots_in_window", active_slots)
    slots_pending = summary.get("slots_pending_release", 0)
    below_target = summary.get("slots_below_target", 0)
    best_deal = summary.get("best_deal")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric(
            label="Corredores Activos",
            value=f"{active_dests} / {MAX_DESTINATIONS}",
            delta=f"{MAX_DESTINATIONS - active_dests} disponibles",
            delta_color="normal",
        )
    with c2:
        slots_delta_str = (
            f"{slots_in_win} en ventana • {slots_pending} en espera 330d"
            if slots_pending > 0
            else f"{MAX_TOTAL_SLOTS - active_slots} slots libres"
        )
        st.metric(
            label="Fechas Rastreadas",
            value=f"{active_slots} / {MAX_TOTAL_SLOTS}",
            delta=slots_delta_str,
            delta_color="normal" if slots_in_win > 0 else "off",
        )
    with c3:
        if best_deal and best_deal.get("latest_cheapest_price") is not None:
            price = float(best_deal["latest_cheapest_price"])
            curr = best_deal.get("currency", DEFAULT_CURRENCY)
            win_orig = best_deal.get("latest_origin_airport") or best_deal.get("origin")
            win_dest = best_deal.get("latest_destination_airport") or best_deal.get("destination_code")
            route = f"{win_orig} ➔ {win_dest}"
            label_str = best_deal.get("label") or best_deal.get("outbound_date")
            st.metric(
                label="Mejor Tarifa Global Hoy",
                value=f"${price:,.0f} {curr}",
                delta=f"{route} • {label_str}",
                delta_color="normal",
            )
        else:
            st.metric(
                label="Mejor Tarifa Global Hoy",
                value="En monitoreo",
                delta="Sin cotizaciones activas aún",
                delta_color="off",
            )
    with c4:
        st.metric(
            label="Tarifas Bajo Meta",
            value=f"{below_target}",
            delta="Oportunidades activas" if below_target > 0 else "Monitoreando caídas",
            delta_color="normal" if below_target > 0 else "off",
        )


# ==============================================================================
# TAB 1: MATRIX HEATMAP & DESTINATION EXPLORER
# ==============================================================================


def _rerun_fragment_safe() -> None:
    """Rerun only the enclosing `@st.fragment` when active, falling back to full rerun."""
    try:
        st.rerun(scope="fragment")
    except Exception:
        st.rerun()


@st.fragment
def render_tab1_matrix_explorer() -> None:
    """Render Tab 1: Corridor Banner, 10-Date Comparison Table, Heatmap, and Plotly Charts."""
    destinations = list_destinations(active_only=False)
    if not destinations:
        st.info(
            "Aún no tienes destinos configurados. Ve a **Destinos y Fechas** "
            "para crear tu primera ruta y sus fechas."
        )
        return

    dest_options: list[tuple[Optional[int], str]] = []
    for d in destinations:
        corridor_lbl = format_corridor_display(d["origin"], d["destination"])
        dest_options.append(
            (
                int(d["id"]),
                f"[{'Activo' if d.get('is_active') else 'Pausado'}] {d['name']} ({corridor_lbl}) — "
                f"{d.get('active_slots', 0)}/{MAX_SLOTS_PER_DESTINATION} fechas | Meta: ${float(d['target_price']):,.0f} {d['currency']}",
            )
        )
    dest_options.append((None, "Ver Todos los Destinos (Vista Global de la Matriz)"))

    col_sel, col_quick_btn = st.columns([3, 1])
    with col_sel:
        selected_option = st.selectbox(
            "Seleccionar Destino Monitoreado",
            options=dest_options,
            format_func=lambda x: str(x[1]) if isinstance(x, tuple) else str(x),
            key="tab1_dest_select",
        )
    selected_dest_id: Optional[int] = selected_option[0]
    selected_dest = (
        next((d for d in destinations if int(d["id"]) == selected_dest_id), None)
        if selected_dest_id is not None
        else None
    )

    # --------------------------------------------------------------------------
    # CORRIDOR HEADER HERO BANNER (MULTI-AIRPORT BADGE PILLS)
    # --------------------------------------------------------------------------
    if selected_dest:
        f_cfg = FilterProfile.from_dict(selected_dest.get("filter_config"))
        stops_txt = (
            "Directo"
            if f_cfg.max_stops == 0
            else f"Máx {f_cfg.max_stops} escala(s)"
            if f_cfg.max_stops in (1, 2)
            else "Cualquier escala"
        )
        airlines_txt = ", ".join(f_cfg.airlines) if f_cfg.airlines else "Todas las aerolíneas"
        bags_txt = f"{f_cfg.carry_on_bags} maleta(s)" if f_cfg.carry_on_bags > 0 else "Sin filtro equipaje"
        hours_txt = f"{f_cfg.dep_start_hour:02d}:00–{f_cfg.dep_end_hour:02d}:59"

        orig_badges_html = " ".join(
            f'<span class="corridor-iata">{code.strip()}</span>'
            for code in str(selected_dest["origin"]).split(",")
            if code.strip()
        )
        dest_badges_html = " ".join(
            f'<span class="corridor-iata">{code.strip()}</span>'
            for code in str(selected_dest["destination"]).split(",")
            if code.strip()
        )

        st.markdown(
            f"""
            <div class="corridor-banner">
                <div>
                    <div class="corridor-codes" style="flex-wrap: wrap;">
                        {orig_badges_html}
                        <span style="color: #64748B;">➔</span>
                        {dest_badges_html}
                        <span style="font-size: 1.15rem; color: #E2E8F0; margin-left: 8px;">{selected_dest['name']}</span>
                    </div>
                    <div style="margin-top: 8px; font-size: 0.88rem; color: #94A3B8;">
                        <strong>Meta:</strong> <span style="color: #34D399; font-weight: 600;">${float(selected_dest['target_price']):,.0f} {selected_dest['currency']}</span>
                        &nbsp;•&nbsp; <strong>Escaneo:</strong> Cada {selected_dest.get('check_interval_minutes', DEFAULT_CHECK_INTERVAL_MINUTES)}m
                        &nbsp;•&nbsp; <strong>Cooldown:</strong> {selected_dest.get('cooldown_hours', 12)}h
                    </div>
                </div>
                <div>
                    <span class="status-pill-sky">{stops_txt}</span>
                    <span class="status-pill-sky">{bags_txt}</span>
                    <span class="status-pill-sky">Salida: {hours_txt}</span>
                    <span class="status-pill-sky">{airlines_txt}</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    slots = list_date_slots(destination_id=selected_dest_id, active_only=False)
    if not slots:
        st.warning("Este destino aún no tiene fechas registradas. Agrégalas en la Pestaña 2.")
        if selected_dest:
            _render_destination_management_drawer(selected_dest, slots)
        return

    # --------------------------------------------------------------------------
    # 10-DATE COMPARISON MATRIX TABLE (WITH WINNING ROUTE & 330-DAY VERIFICATION)
    # --------------------------------------------------------------------------
    st.markdown("### Comparativa de Fechas (Más Baratos vs. Mejores Vuelos)")

    matrix_rows: list[dict[str, Any]] = []
    for s in slots:
        out_d = s.get("outbound_date") or ""
        ret_d = s.get("return_date")
        date_pair_label = s.get("label") or (f"{out_d} ➔ {ret_d}" if ret_d else f"{out_d} (Solo ida)")
        corridor_fallback = format_corridor_display(s.get("origin") or "MEX", s.get("destination_code") or "MAD")
        if selected_dest_id is None:
            date_pair_label = f"{corridor_fallback} • {date_pair_label}"

        c_price = s.get("latest_cheapest_price")
        b_price = s.get("latest_best_price")
        atl_price = s.get("all_time_low_price")
        target_p = float(s.get("target_price") or 0.0)
        curr = s.get("currency") or DEFAULT_CURRENCY

        win_orig = s.get("latest_origin_airport")
        win_dest = s.get("latest_destination_airport")
        if win_orig and win_dest:
            winning_route_str = f"{win_orig} ➔ {win_dest}"
        elif "," not in str(s.get("origin") or "") and "," not in str(s.get("destination_code") or ""):
            winning_route_str = f"{s.get('origin')} ➔ {s.get('destination_code')}"
        else:
            winning_route_str = corridor_fallback

        win_info = s.get("window_info") or get_flight_window_status(out_d, ret_d)
        is_future = win_info.get("status") == "FUTURE_RELEASE"

        # Format price and spread
        rel_date_obj = win_info.get("release_date")
        if is_future:
            c_price_fmt = f"Libera el {rel_date_obj.strftime('%d/%m/%Y')}" if rel_date_obj else "Fuera de ventana"
            b_price_fmt = "—"
            spread_fmt = "—"
            atl_fmt = "—"
            target_diff_fmt = f"Faltan {win_info.get('days_until_release', 0)}d para ventana 330d"
            airline_stops_str = "En espera de publicación por aerolíneas"
            dur_str = "—"
        elif c_price is not None:
            c_price_float = float(c_price)
            b_price_float = float(b_price) if b_price is not None else None
            c_price_fmt = f"${c_price_float:,.0f} {curr}"
            b_price_fmt = f"${b_price_float:,.0f} {curr}" if b_price_float is not None else "—"
            spread = (b_price_float - c_price_float) if b_price_float is not None else None
            spread_fmt = f"+${spread:,.0f} {curr}" if spread is not None and spread > 0 else f"$0 {curr}" if spread is not None else "—"
            atl_fmt = f"${float(atl_price):,.0f} {curr}" if atl_price is not None else "—"
            diff = c_price_float - target_p
            target_diff_fmt = f"-${abs(diff):,.0f} (Bajo Meta)" if diff <= 0 else f"+${diff:,.0f} sobre meta"

            airline = s.get("latest_airline") or "Aerolínea"
            stops = s.get("latest_stops") or "Directo"
            self_trf = " [Self-Transfer]" if s.get("latest_is_self_transfer") else ""
            airline_stops_str = f"{airline} ({stops}){self_trf}"
            dur_str = s.get("latest_duration") or "—"
        else:
            c_price_fmt = "Pendiente de escaneo"
            b_price_fmt = "—"
            spread_fmt = "—"
            atl_fmt = "—"
            target_diff_fmt = "Listo para escanear"
            airline_stops_str = "Dentro de ventana (Sin escaneo aún)"
            dur_str = "—"

        matrix_rows.append(
            {
                "ID": int(s["id"]),
                "Estado": "Activo" if s.get("is_active") else "Pausado",
                "Fecha / Tramo": date_pair_label,
                "Ruta Ganadora": winning_route_str,
                "Ventana (330d)": win_info.get("badge", "Dentro de ventana"),
                "Más Baratos": c_price_fmt,
                "Mejores Vuelos": b_price_fmt,
                "Margen": spread_fmt,
                "Aerolínea & Escalas": airline_stops_str,
                "Duración": dur_str,
                "Mín. Histórico": atl_fmt,
                "Vs Meta": target_diff_fmt,
                "Último Chequeo": _format_relative_time(s.get("last_checked_at")),
                "Google Flights": s.get("google_flights_url"),
                "_raw_cheapest": c_price,
                "_raw_best": b_price,
                "_is_future": is_future,
            }
        )

    df_matrix = pd.DataFrame(matrix_rows)
    display_cols = [
        "Fecha / Tramo",
        "Ruta Ganadora",
        "Ventana (330d)",
        "Más Baratos",
        "Mejores Vuelos",
        "Margen",
        "Aerolínea & Escalas",
        "Duración",
        "Mín. Histórico",
        "Vs Meta",
        "Último Chequeo",
        "Google Flights",
    ]

    st.dataframe(
        df_matrix[display_cols],
        width="stretch",
        hide_index=True,
        column_config={
            "Fecha / Tramo": st.column_config.TextColumn("Fecha / Tramo", width="medium"),
            "Ruta Ganadora": st.column_config.TextColumn("Ruta Ganadora", width="small"),
            "Ventana (330d)": st.column_config.TextColumn("Ventana (330d)", width="medium"),
            "Más Baratos": st.column_config.TextColumn("Más Baratos", width="small"),
            "Mejores Vuelos": st.column_config.TextColumn("Mejores Vuelos", width="small"),
            "Margen": st.column_config.TextColumn("Margen", width="small"),
            "Aerolínea & Escalas": st.column_config.TextColumn("Aerolínea & Escalas", width="medium"),
            "Duración": st.column_config.TextColumn("Duración", width="small"),
            "Mín. Histórico": st.column_config.TextColumn("Mín. Histórico", width="small"),
            "Vs Meta": st.column_config.TextColumn("Vs Meta", width="medium"),
            "Último Chequeo": st.column_config.TextColumn("Último Chequeo", width="small"),
            "Google Flights": st.column_config.LinkColumn(
                "Google Flights",
                help="Abrir búsqueda consolidada N:M con filtros Protobuf ?tfs= en Google Flights",
                display_text="Ver en Google Flights ↗",
                width="small",
            ),
        },
    )

    # --------------------------------------------------------------------------
    # COMPARATIVE BAR CHART (ONLY REAL PRICED SLOTS)
    # --------------------------------------------------------------------------
    priced_rows = [r for r in matrix_rows if r["_raw_cheapest"] is not None]
    if priced_rows:
        fig_bar = go.Figure()
        fig_bar.add_trace(
            go.Bar(
                name="Más Baratos (Cheapest Tab)",
                x=[r["Fecha / Tramo"] for r in priced_rows],
                y=[r["_raw_cheapest"] for r in priced_rows],
                marker_color="#38BDF8",
                text=[f"${r['_raw_cheapest']:,.0f}" for r in priced_rows],
                textposition="auto",
            )
        )
        fig_bar.add_trace(
            go.Bar(
                name="Mejores Vuelos (Best Tab)",
                x=[r["Fecha / Tramo"] for r in priced_rows],
                y=[r["_raw_best"] or r["_raw_cheapest"] for r in priced_rows],
                marker_color="#818CF8",
                text=[f"${(r['_raw_best'] or r['_raw_cheapest']):,.0f}" for r in priced_rows],
                textposition="auto",
            )
        )
        if selected_dest and float(selected_dest.get("target_price") or 0) > 0:
            t_price = float(selected_dest["target_price"])
            fig_bar.add_hline(
                y=t_price,
                line_dash="dash",
                line_color="#10B981",
                annotation_text=f"Meta: ${t_price:,.0f}",
                annotation_position="top right",
            )
        fig_bar.update_layout(
            title="Comparativa Tarifaria Actual: Más Baratos vs. Mejores Vuelos",
            barmode="group",
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(15,23,42,0.6)",
            height=370,
            margin=dict(l=20, r=20, t=45, b=40),
            yaxis_title="Precio (MXN)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_bar, width="stretch")
    else:
        st.info("Las fechas seleccionadas aún no cuentan con cotizaciones históricas registradas.")

    # --------------------------------------------------------------------------
    # INTERACTIVE PLOTLY EVOLUTION CHARTS
    # --------------------------------------------------------------------------
    st.markdown("---")
    st.markdown("### Evolución Histórica de Precios")

    target_dest_for_chart = selected_dest_id if selected_dest_id is not None else int(destinations[0]["id"])
    dest_for_chart_meta = next((d for d in destinations if int(d["id"]) == target_dest_for_chart), destinations[0])

    col_mode, col_days = st.columns([2.2, 1])
    with col_mode:
        chart_mode = st.radio(
            "Modo de Visualización",
            options=[
                "Evolución Multi-Fecha (Todas las fechas en un solo gráfico)",
                "Inspección Dual-Tab (Más Baratos vs. Mejores para una fecha)",
            ],
            horizontal=True,
            key="tab1_chart_mode",
        )
    with col_days:
        lookback_days = st.selectbox(
            "Ventana Histórica",
            options=[7, 14, 30, 60, 90],
            index=2,
            format_func=lambda d: f"Últimos {d} días",
            key="tab1_lookback_days",
        )

    snapshots = get_snapshots_for_destination(destination_id=target_dest_for_chart, days=lookback_days)
    if not snapshots:
        st.info(
            "Aún no hay snapshots históricos registrados para este destino. "
            "Usa la Pestaña 3 (**Tab 3 — Escáner en Vivo**) para ejecutar un escaneo en vivo cuando las fechas estén en ventana."
        )
    else:
        df_snap = pd.DataFrame(snapshots)
        df_snap["slot_display"] = df_snap.apply(
            lambda r: r.get("slot_label") or f"{r.get('outbound_date')} ➔ {r.get('return_date')}",
            axis=1,
        )

        if chart_mode.startswith("Evolución Multi-Fecha"):
            fig_multi = px.line(
                df_snap,
                x="scraped_at",
                y="cheapest_tab_price",
                color="slot_display",
                markers=True,
                title=f"Evolución de Tarifas Más Baratas — {dest_for_chart_meta['name']}",
                labels={
                    "scraped_at": "Fecha de Escaneo",
                    "cheapest_tab_price": f"Precio ({dest_for_chart_meta['currency']})",
                    "slot_display": "Combinación de Fechas",
                },
                template="plotly_dark",
            )
            if float(dest_for_chart_meta.get("target_price") or 0) > 0:
                t_val = float(dest_for_chart_meta["target_price"])
                fig_multi.add_hline(
                    y=t_val,
                    line_dash="dash",
                    line_color="#10B981",
                    annotation_text=f"Meta (${t_val:,.0f})",
                )
            fig_multi.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(15,23,42,0.6)",
                height=420,
                hovermode="x unified",
            )
            st.plotly_chart(fig_multi, width="stretch")
        else:
            slot_choices = [
                (int(s["id"]), s.get("label") or f"{s.get('outbound_date')} ➔ {s.get('return_date')}")
                for s in slots
                if int(s["destination_id"]) == target_dest_for_chart
            ]
            chosen_slot_tuple = st.selectbox(
                "Seleccionar Combinación de Fechas",
                options=slot_choices,
                format_func=lambda x: str(x[1]),
                key="tab1_dual_slot_picker",
            )
            chosen_slot_id = chosen_slot_tuple[0]
            df_slot = df_snap[df_snap["date_slot_id"] == chosen_slot_id].copy()

            if df_slot.empty:
                st.warning("Esta combinación de fechas aún no tiene snapshots registrados.")
            else:
                fig_dual = go.Figure()
                fig_dual.add_trace(
                    go.Scatter(
                        x=df_slot["scraped_at"],
                        y=df_slot["cheapest_tab_price"],
                        mode="lines+markers",
                        name="Más Baratos",
                        line=dict(color="#38BDF8", width=3),
                    )
                )
                fig_dual.add_trace(
                    go.Scatter(
                        x=df_slot["scraped_at"],
                        y=df_slot["best_tab_price"],
                        mode="lines+markers",
                        name="Mejores Vuelos",
                        line=dict(color="#F59E0B", width=2.5, dash="dot"),
                    )
                )
                if float(dest_for_chart_meta.get("target_price") or 0) > 0:
                    t_val = float(dest_for_chart_meta["target_price"])
                    fig_dual.add_hline(
                        y=t_val,
                        line_dash="dash",
                        line_color="#10B981",
                        annotation_text=f"Meta (${t_val:,.0f})",
                    )
                fig_dual.update_layout(
                    title=f"Comparativa Dual-Tab: {chosen_slot_tuple[1]}",
                    xaxis_title="Fecha de Escaneo",
                    yaxis_title=f"Precio ({dest_for_chart_meta['currency']})",
                    template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(15,23,42,0.6)",
                    height=420,
                    hovermode="x unified",
                )
                st.plotly_chart(fig_dual, width="stretch")

    # --------------------------------------------------------------------------
    # DESTINATION & SLOTS MANAGEMENT DRAWER
    # --------------------------------------------------------------------------
    _render_destination_management_drawer(selected_dest, slots)


def _render_destination_management_drawer(
    selected_dest: Optional[dict[str, Any]],
    slots: list[dict[str, Any]],
) -> None:
    """Render destination pause/activate/delete and individual slot management controls."""
    with st.expander("Administrar Destino y Fechas (Pausar / Activar / Eliminar)"):
        if selected_dest:
            d_col1, d_col2, d_col3 = st.columns([2, 1, 1])
            with d_col1:
                st.markdown(
                    f"**Destino:** `{selected_dest['name']}` (`{selected_dest['origin']} ➔ {selected_dest['destination']}`)"
                )
            with d_col2:
                new_state = not bool(selected_dest.get("is_active", True))
                btn_lbl = "Reactivar Destino" if new_state else "Pausar Destino"
                if st.button(btn_lbl, key=f"toggle_dest_{selected_dest['id']}", width="stretch"):
                    try:
                        set_destination_active(int(selected_dest["id"]), new_state)
                        st.success("Estado del destino actualizado.")
                        st.rerun()
                    except Exception as exc:
                        st.error(str(exc))
            with d_col3:
                if st.button(
                    "Eliminar Destino Completo",
                    key=f"del_dest_{selected_dest['id']}",
                    type="secondary",
                    width="stretch",
                ):
                    delete_destination(int(selected_dest["id"]))
                    st.success("Destino y sus slots eliminados.")
                    st.rerun()

        if slots:
            st.markdown("##### Fechas Individuales de este Destino")
            for s in slots:
                sc1, sc2, sc3, sc4 = st.columns([3, 1.5, 1, 1])
                with sc1:
                    win = s.get("window_info") or get_flight_window_status(s.get("outbound_date"), s.get("return_date"))
                    st.write(
                        f"[{'Activo' if s.get('is_active') else 'Pausado'}] **{s.get('label') or s.get('outbound_date')}** "
                        f"({win['badge']})"
                    )
                with sc2:
                    p_val = s.get("latest_cheapest_price")
                    st.write(f"${p_val:,.0f}" if p_val else "Sin escaneo")
                with sc3:
                    toggle_slot_state = not bool(s.get("is_active", True))
                    if st.button(
                        "Activar" if toggle_slot_state else "Pausar",
                        key=f"tog_slot_{s['id']}",
                        width="stretch",
                    ):
                        try:
                            set_date_slot_active(int(s["id"]), toggle_slot_state)
                            st.rerun()
                        except Exception as exc:
                            st.error(str(exc))
                with sc4:
                    if st.button("Borrar", key=f"del_slot_{s['id']}", width="stretch"):
                        delete_date_slot(int(s["id"]))
                        st.rerun()


# ==============================================================================
# TAB 2: CREATE / MANAGE DESTINATION & 10-DATE GENERATOR
# ==============================================================================


def _build_staged_slot(
    out_date: datetime.date,
    ret_date: Optional[datetime.date],
    origin: str,
    dest: str,
    trip_type: str,
    profile: FilterProfile,
    curr: str,
    gl: str,
    hl: str,
) -> dict[str, Any]:
    """Build a staging-tray date slot dictionary with Google Flights URL and 330-day window status."""
    out_str = out_date.strftime("%Y-%m-%d")
    ret_str = ret_date.strftime("%Y-%m-%d") if ret_date else None
    lbl = format_date_slot_label(out_str, ret_str)
    win_status = get_flight_window_status(out_str, ret_str)
    slot_url = build_google_flights_url(
        origin=origin,
        destination=dest,
        outbound_date=out_str,
        return_date=ret_str,
        trip_type=trip_type,
        filter_profile=profile,
        curr=curr,
        gl=gl,
        hl=hl,
    )
    return {
        "label": lbl,
        "outbound_date": out_str,
        "return_date": ret_str,
        "google_flights_url": slot_url,
        "window_info": win_status,
    }


@st.fragment
def render_tab2_destination_and_generator() -> None:
    """
    Render Tab 2:
      1. Destination & Filter Profile (Create / Edit / Delete, duplicate-protected)
      2. Travel Dates (Unified destination context, saved dates, Calendar Range, Auto-Generator, Paste URLs)
    """
    destinations = list_destinations(active_only=False)
    st.markdown("### 1. Destino y Filtros")

    mode_options: list[tuple[Optional[int], str]] = [
        (None, "+ Nuevo destino")
    ]
    for d in destinations:
        mode_options.append(
            (
                int(d["id"]),
                f"Editar: {d['name']} ({d['origin']} ➔ {d['destination']})",
            )
        )

    sel_col, del_col = st.columns([4, 1])
    with sel_col:
        selected_mode = st.selectbox(
            "Destino",
            options=mode_options,
            format_func=lambda x: str(x[1]) if isinstance(x, tuple) else str(x),
            key="tab2_dest_mode",
        )
    editing_dest_id: Optional[int] = selected_mode[0]
    existing_dest = (
        next((d for d in destinations if int(d["id"]) == editing_dest_id), None)
        if editing_dest_id is not None
        else None
    )
    existing_cfg = FilterProfile.from_dict(existing_dest.get("filter_config") if existing_dest else {})

    with del_col:
        if existing_dest is not None:
            st.markdown("<div style='height: 28px;'></div>", unsafe_allow_html=True)
            with st.popover("Eliminar destino", width="stretch"):
                st.warning(
                    f"¿Eliminar **{existing_dest['name']}** y todas sus fechas e historial?"
                )
                if st.button(
                    "Sí, eliminar",
                    key=f"tab2_confirm_del_{editing_dest_id}",
                    type="primary",
                    width="stretch",
                ):
                    delete_destination(int(editing_dest_id))
                    if st.session_state.get("last_created_dest_id") == editing_dest_id:
                        st.session_state.pop("last_created_dest_id", None)
                    st.session_state["staged_slots_list"] = []
                    st.session_state.pop("staged_dest_id", None)
                    st.success(f"Destino **{existing_dest['name']}** eliminado.")
                    st.rerun()

    key_suffix = str(editing_dest_id) if editing_dest_id is not None else "new"
    has_custom_filters = bool(
        existing_dest
        and (
            existing_cfg.max_stops is not None
            or existing_cfg.airlines
            or existing_cfg.carry_on_bags > 0
            or existing_cfg.adults != 1
            or existing_cfg.hide_self_transfer
            or existing_cfg.dep_start_hour != 0
            or existing_cfg.dep_end_hour != 23
            or existing_cfg.arr_start_hour != 0
            or existing_cfg.arr_end_hour != 23
        )
    )

    with st.form(f"destination_profile_form_{key_suffix}", clear_on_submit=False):
        r1c1, r1c2, r1c3, r1c4 = st.columns([1.8, 1.1, 1.1, 1.0])
        with r1c1:
            name_val = st.text_input(
                "Nombre",
                value=existing_dest["name"] if existing_dest else "",
                placeholder="Ej. Tokio (Otoño)",
                key=f"dest_name_{key_suffix}",
            )
        with r1c2:
            origin_val = st.text_input(
                "Orígenes",
                value=existing_dest["origin"] if existing_dest else "MEX, NLU",
                max_chars=64,
                placeholder="MEX, NLU",
                help="Códigos IATA separados por coma (ej. MEX, NLU).",
                key=f"dest_origin_{key_suffix}",
            ).upper()
        with r1c3:
            dest_val = st.text_input(
                "Destinos",
                value=existing_dest["destination"] if existing_dest else "NRT, HND",
                max_chars=64,
                placeholder="NRT, HND o TYO",
                help="Aeropuertos o códigos de ciudad (TYO, LON, NYC, PAR).",
                key=f"dest_code_{key_suffix}",
            ).upper()
        with r1c4:
            trip_type_val = st.selectbox(
                "Viaje",
                options=["round-trip", "one-way"],
                index=0 if (not existing_dest or existing_dest.get("trip_type") == "round-trip") else 1,
                format_func=lambda t: "Redondo" if t == "round-trip" else "Solo ida",
                key=f"dest_trip_{key_suffix}",
            )

        r2c1, r2c2, r2c3, r2c4, r2c5, r2c6 = st.columns([1.05, 0.8, 0.85, 0.95, 0.95, 1.0])
        with r2c1:
            target_price_val = st.number_input(
                "Precio meta",
                min_value=100.0,
                max_value=500000.0,
                value=float(existing_dest["target_price"]) if existing_dest else 16000.0,
                step=250.0,
                key=f"dest_target_{key_suffix}",
            )
        with r2c2:
            currencies = ["MXN", "USD", "EUR", "CAD", "COP"]
            curr_val = existing_dest.get("currency", "MXN") if existing_dest else "MXN"
            currency_val = st.selectbox(
                "Moneda",
                options=currencies,
                index=currencies.index(curr_val) if curr_val in currencies else 0,
                key=f"dest_curr_{key_suffix}",
            )
        with r2c3:
            gl_options = ["MX", "IT", "ES", "DE", "US"]
            curr_gl = str(existing_dest.get("gl") or DEFAULT_GL).strip().upper() if existing_dest else DEFAULT_GL
            if curr_gl not in gl_options:
                gl_options.append(curr_gl)
            gl_val = st.selectbox(
                "Mercado / POS (gl)",
                options=gl_options,
                index=gl_options.index(curr_gl) if curr_gl in gl_options else 0,
                help="Mercado Google Flights (gl): usa IT para hubs europeos o MX por defecto.",
                key=f"dest_gl_{key_suffix}",
            )
        with r2c4:
            cabins = ["economy", "premium-economy", "business", "first"]
            seat_val = st.selectbox(
                "Cabina",
                options=cabins,
                index=cabins.index(existing_cfg.seat) if existing_cfg.seat in cabins else 0,
                format_func=lambda s: {
                    "economy": "Turista",
                    "premium-economy": "Premium",
                    "business": "Ejecutiva",
                    "first": "Primera",
                }[s],
                key=f"dest_seat_{key_suffix}",
            )
        with r2c5:
            interval_choices = [60, 180, 240, 360]
            curr_interval = (
                int(existing_dest.get("check_interval_minutes", DEFAULT_CHECK_INTERVAL_MINUTES))
                if existing_dest
                else DEFAULT_CHECK_INTERVAL_MINUTES
            )
            interval_idx = (
                interval_choices.index(curr_interval)
                if curr_interval in interval_choices
                else interval_choices.index(240)
            )
            check_interval_val = st.selectbox(
                "Escaneo",
                options=interval_choices,
                index=interval_idx,
                format_func=lambda m: f"Cada {m // 60}h",
                key=f"dest_interval_{key_suffix}",
            )
        with r2c6:
            cooldown_choices = [6, 12, 24]
            curr_cd = int(existing_dest.get("cooldown_hours", 12)) if existing_dest else 12
            cooldown_val = st.selectbox(
                "Cooldown",
                options=cooldown_choices,
                index=cooldown_choices.index(curr_cd) if curr_cd in cooldown_choices else 1,
                format_func=lambda h: f"{h} horas",
                help="Espera mínima entre re-alertas por Meta cuando cae >=3% (Bajo Histórico y caídas >=5% siempre alertan inmediato).",
                key=f"dest_cooldown_{key_suffix}",
            )

        with st.expander("Filtros opcionales", expanded=has_custom_filters):
            f1, f2, f3, f4 = st.columns([1.1, 2.0, 1.0, 0.9])
            with f1:
                stops_options = [None, 0, 1, 2]
                stops_idx = stops_options.index(existing_cfg.max_stops) if existing_cfg.max_stops in stops_options else 0
                max_stops_val = st.selectbox(
                    "Escalas",
                    options=stops_options,
                    index=stops_idx,
                    format_func=lambda s: {
                        None: "Cualquiera",
                        0: "Directo",
                        1: "Máx. 1 escala",
                        2: "Máx. 2 escalas",
                    }[s],
                    key=f"dest_stops_{key_suffix}",
                )
            with f2:
                default_airlines = [a for a in existing_cfg.airlines if a in COMMON_AIRLINES]
                selected_airlines = st.multiselect(
                    "Aerolíneas",
                    options=list(COMMON_AIRLINES.keys()),
                    default=default_airlines,
                    placeholder="Todas",
                    format_func=lambda k: str(COMMON_AIRLINES.get(str(k), str(k))),
                    key=f"dest_airlines_{key_suffix}",
                )
            with f3:
                carry_on_val = st.selectbox(
                    "Equipaje de mano",
                    options=[0, 1, 2],
                    index=min(2, max(0, existing_cfg.carry_on_bags)),
                    format_func=lambda b: "Sin filtro" if b == 0 else f"{b} maleta(s)",
                    key=f"dest_bags_{key_suffix}",
                )
            with f4:
                adults_val = st.number_input(
                    "Pasajeros",
                    min_value=1,
                    max_value=9,
                    value=existing_cfg.adults,
                    step=1,
                    key=f"dest_adults_{key_suffix}",
                )

            h1, h2, h3 = st.columns([1.4, 1.4, 1.2])
            with h1:
                dep_window = st.slider(
                    "Hora de salida",
                    min_value=0,
                    max_value=23,
                    value=(existing_cfg.dep_start_hour, existing_cfg.dep_end_hour),
                    key=f"dest_dep_{key_suffix}",
                )
            with h2:
                arr_window = st.slider(
                    "Hora de llegada",
                    min_value=0,
                    max_value=23,
                    value=(existing_cfg.arr_start_hour, existing_cfg.arr_end_hour),
                    key=f"dest_arr_{key_suffix}",
                )
            with h3:
                custom_iata_str = st.text_input(
                    "Otras aerolíneas (IATA)",
                    value=",".join([a for a in existing_cfg.airlines if a not in COMMON_AIRLINES]),
                    placeholder="Ej. NH, JL",
                    key=f"dest_extra_airlines_{key_suffix}",
                )
                hide_self_transfer_val = st.checkbox(
                    "Evitar self-transfer",
                    value=existing_cfg.hide_self_transfer,
                    help="Ocultar vuelos con cambio de boleto por cuenta propia.",
                    key=f"dest_self_trf_{key_suffix}",
                )

        submit_btn_text = "Guardar cambios" if editing_dest_id is not None else "Crear destino"
        submitted_dest = st.form_submit_button(submit_btn_text, type="primary", width="stretch")

        if submitted_dest:
            clean_name = name_val.strip()
            if not clean_name:
                st.error("Ingresa un nombre para el destino.")
            else:
                duplicate_match = next(
                    (
                        d
                        for d in destinations
                        if d["name"].strip().lower() == clean_name.lower()
                        and (editing_dest_id is None or int(d["id"]) != editing_dest_id)
                    ),
                    None,
                )
                if duplicate_match is not None:
                    if editing_dest_id is None:
                        st.error(
                            f"Ya existe un destino llamado **{duplicate_match['name']}**. "
                            "Selecciónalo arriba para editarlo; no se permiten destinos duplicados."
                        )
                    else:
                        st.error(
                            f"Ya existe otro destino llamado **{duplicate_match['name']}**. "
                            "Elige un nombre diferente."
                        )
                else:
                    merged_airlines = list(selected_airlines)
                    for extra in custom_iata_str.split(","):
                        code = extra.strip().upper()
                        if code and code not in merged_airlines:
                            merged_airlines.append(code)

                    profile_obj = FilterProfile(
                        max_stops=max_stops_val,
                        airlines=merged_airlines,
                        dep_start_hour=dep_window[0],
                        dep_end_hour=dep_window[1],
                        arr_start_hour=arr_window[0],
                        arr_end_hour=arr_window[1],
                        ret_dep_start_hour=dep_window[0],
                        ret_dep_end_hour=dep_window[1],
                        ret_arr_start_hour=arr_window[0],
                        ret_arr_end_hour=arr_window[1],
                        carry_on_bags=carry_on_val,
                        seat=seat_val,
                        adults=int(adults_val),
                        hide_self_transfer=hide_self_transfer_val,
                    )

                    try:
                        norm_orig = normalize_iata_codes(origin_val, expand_metro=True)
                        norm_dest = normalize_iata_codes(dest_val, expand_metro=True)
                        corridor_disp = format_corridor_display(norm_orig, norm_dest)

                        if editing_dest_id is None:
                            new_id = create_destination(
                                name=clean_name,
                                origin=norm_orig,
                                destination=norm_dest,
                                target_price=float(target_price_val),
                                trip_type=trip_type_val,
                                currency=currency_val,
                                gl=gl_val,
                                hl=DEFAULT_HL,
                                check_interval_minutes=int(check_interval_val),
                                cooldown_hours=int(cooldown_val),
                                filter_config=profile_obj.to_dict(),
                                is_active=True,
                            )
                            st.session_state["last_created_dest_id"] = new_id
                            st.success(
                                f"Destino **{clean_name}** (`{corridor_disp}`) creado. "
                                "Ahora agrega sus fechas abajo."
                            )
                            st.rerun()
                        else:
                            update_destination(
                                destination_id=editing_dest_id,
                                name=clean_name,
                                origin=norm_orig,
                                destination=norm_dest,
                                target_price=float(target_price_val),
                                trip_type=trip_type_val,
                                currency=currency_val,
                                gl=gl_val,
                                hl=DEFAULT_HL,
                                check_interval_minutes=int(check_interval_val),
                                cooldown_hours=int(cooldown_val),
                                filter_config=profile_obj.to_dict(),
                                is_active=bool(existing_dest.get("is_active", True)) if existing_dest else True,
                            )
                            st.success(f"Destino **{clean_name}** (`{corridor_disp}`) actualizado.")
                            st.rerun()
                    except Exception as exc:
                        st.error(f"No se pudo guardar el destino: {exc}")

    # --------------------------------------------------------------------------
    # STEP 2: TRAVEL DATES (UP TO 10 COMBINATIONS)
    # --------------------------------------------------------------------------
    st.markdown("---")
    st.markdown("### 2. Fechas de Viaje")

    if not destinations:
        st.info("Crea al menos un destino arriba para agregar fechas.")
        return

    if existing_dest is not None:
        target_dest = existing_dest
        st.caption(
            f"**Destino activo:** {target_dest['name']} ({target_dest['origin']} ➔ {target_dest['destination']}) — "
            f"{target_dest.get('active_slots', 0)}/{MAX_SLOTS_PER_DESTINATION} fechas"
        )
    else:
        default_dest_idx = 0
        last_id = st.session_state.get("last_created_dest_id")
        if last_id is not None:
            for i, d in enumerate(destinations):
                if int(d["id"]) == int(last_id):
                    default_dest_idx = i
                    break

        target_dest = st.selectbox(
            "Destino",
            options=destinations,
            index=default_dest_idx,
            format_func=lambda d: (
                f"{d['name']} ({d['origin']} ➔ {d['destination']}) — "
                f"{d.get('active_slots', 0)}/{MAX_SLOTS_PER_DESTINATION} fechas"
            ),
            key="step2_target_dest",
        )

    dest_id = int(target_dest["id"])
    dest_origin = str(target_dest["origin"])
    dest_code = str(target_dest["destination"])
    dest_trip = str(target_dest.get("trip_type", "round-trip"))
    dest_curr = str(target_dest.get("currency", DEFAULT_CURRENCY))
    dest_gl = str(target_dest.get("gl", DEFAULT_GL))
    dest_hl = str(target_dest.get("hl", DEFAULT_HL))
    dest_profile = FilterProfile.from_dict(target_dest.get("filter_config"))

    if st.session_state.get("staged_dest_id") != dest_id:
        st.session_state["staged_slots_list"] = []
        st.session_state["staged_dest_id"] = dest_id

    saved_slots = list_date_slots(destination_id=dest_id, active_only=False)
    saved_pairs: set[tuple[str, Optional[str]]] = {
        (
            str(s["outbound_date"]).strip(),
            str(s["return_date"]).strip() if s.get("return_date") else None,
        )
        for s in saved_slots
    }

    if saved_slots:
        with st.expander(f"Fechas guardadas ({len(saved_slots)}/{MAX_SLOTS_PER_DESTINATION})", expanded=False):
            for sv in saved_slots:
                svc1, svc2, svc3 = st.columns([2.6, 1.8, 0.8])
                with svc1:
                    st.markdown(f"`{sv.get('label') or sv.get('outbound_date')}`")
                with svc2:
                    sv_win = sv.get("window_info") or get_flight_window_status(sv.get("outbound_date"), sv.get("return_date"))
                    sv_price = sv.get("latest_cheapest_price")
                    price_txt = f" • ${sv_price:,.0f} {dest_curr}" if sv_price is not None else ""
                    st.caption(f"{sv_win['badge']}{price_txt}")
                with svc3:
                    if st.button("Borrar", key=f"tab2_del_saved_{sv['id']}", width="stretch"):
                        delete_date_slot(int(sv["id"]))
                        st.rerun()

    today = datetime.date.today()
    max_searchable_date = today + datetime.timedelta(days=GOOGLE_FLIGHTS_MAX_ADVANCE_DAYS)
    st.caption(
        f"Google Flights muestra vuelos hasta el **{max_searchable_date.strftime('%d/%m/%Y')}** "
        "(fechas posteriores quedan en espera automática)."
    )

    replace_mode = st.checkbox(
        "Reemplazar fechas actuales",
        value=False,
        key="step2_replace_existing",
    )

    opt_b_tab, opt_a_tab, opt_c_tab = st.tabs(
        [
            "Calendario",
            "Generador automático",
            "Pegar URLs",
        ]
    )

    # --------------------------------------------------------------------------
    # CALENDAR RANGE PICKER WITH STAGING TRAY
    # --------------------------------------------------------------------------
    with opt_b_tab:
        st.caption("Selecciona ida y vuelta en el calendario y agrégalas a la lista.")

        if "staged_slots_list" not in st.session_state:
            st.session_state["staged_slots_list"] = []

        staged_slots: list[dict[str, Any]] = st.session_state["staged_slots_list"]

        default_cal_start = today + datetime.timedelta(days=21)
        default_cal_end = default_cal_start + datetime.timedelta(days=9)

        cal_c1, cal_c2 = st.columns([1.4, 1.6])

        with cal_c1:
            if dest_trip == "one-way":
                single_cal_val = st.date_input(
                    "Fecha de salida",
                    value=default_cal_start,
                    min_value=today,
                    key="opb_single_date",
                )
                range_start = single_cal_val
                range_end = None
            else:
                date_range_picker = st.date_input(
                    "Fechas (ida y vuelta)",
                    value=(default_cal_start, default_cal_end),
                    min_value=today,
                    key="opb_range_picker",
                )
                if isinstance(date_range_picker, (tuple, list)) and len(date_range_picker) == 2:
                    range_start, range_end = date_range_picker[0], date_range_picker[1]
                elif isinstance(date_range_picker, (tuple, list)) and len(date_range_picker) == 1:
                    range_start = date_range_picker[0]
                    range_end = None
                else:
                    range_start = date_range_picker
                    range_end = None

        with cal_c2:
            if range_start:
                out_str = range_start.strftime("%Y-%m-%d")
                ret_str = range_end.strftime("%Y-%m-%d") if range_end else None
                lbl = format_date_slot_label(out_str, ret_str)
                win_status = get_flight_window_status(out_str, ret_str)

                st.markdown(f"**Selección:** `{lbl}`")
                if win_status["status"] == "ACTIVE_WINDOW":
                    st.success(f"Disponible para rastreo inmediato ({win_status['days_until_flight']} días)")
                elif win_status["status"] == "FUTURE_RELEASE":
                    st.info(
                        f"Abre el **{win_status['release_date'].strftime('%d/%m/%Y')}** "
                        f"(en {win_status['days_until_release']} días). Se activará automáticamente."
                    )
                else:
                    st.warning("Fecha no disponible")

                can_add = len(staged_slots) < MAX_SLOTS_PER_DESTINATION
                add_btn_txt = f"Agregar ({len(staged_slots)}/{MAX_SLOTS_PER_DESTINATION})" if can_add else "Lista llena (10)"
                staged_pairs = {(s["outbound_date"], s.get("return_date")) for s in staged_slots}
                blocked_pairs = staged_pairs if replace_mode else (staged_pairs | saved_pairs)

                b_add, b_shift7, b_4wks = st.columns([1.15, 0.9, 1.05])
                with b_add:
                    if st.button(add_btn_txt, disabled=not can_add, key="btn_add_staged", type="primary", width="stretch"):
                        if (out_str, ret_str) in staged_pairs:
                            st.warning("Esta fecha ya está en la lista.")
                        elif not replace_mode and (out_str, ret_str) in saved_pairs:
                            st.warning("Esta fecha ya está guardada en el destino.")
                        else:
                            staged_slots.append(
                                _build_staged_slot(
                                    range_start,
                                    range_end,
                                    dest_origin,
                                    dest_code,
                                    dest_trip,
                                    dest_profile,
                                    dest_curr,
                                    dest_gl,
                                    dest_hl,
                                )
                            )
                            _rerun_fragment_safe()
                with b_shift7:
                    if st.button("+7 días", disabled=not can_add, key="btn_shift_7", width="stretch"):
                        if staged_slots:
                            last_slot = staged_slots[-1]
                            base_out = datetime.date.fromisoformat(str(last_slot["outbound_date"]))
                            base_ret = (
                                datetime.date.fromisoformat(str(last_slot["return_date"]))
                                if last_slot.get("return_date")
                                else None
                            )
                        else:
                            base_out = range_start
                            base_ret = range_end

                        shift_out = base_out + datetime.timedelta(days=7)
                        shift_ret = (base_ret + datetime.timedelta(days=7)) if base_ret else None
                        pair_key = (
                            shift_out.strftime("%Y-%m-%d"),
                            shift_ret.strftime("%Y-%m-%d") if shift_ret else None,
                        )
                        if pair_key in blocked_pairs:
                            st.warning("La fecha +7 días ya está agregada o guardada.")
                        else:
                            staged_slots.append(
                                _build_staged_slot(
                                    shift_out,
                                    shift_ret,
                                    dest_origin,
                                    dest_code,
                                    dest_trip,
                                    dest_profile,
                                    dest_curr,
                                    dest_gl,
                                    dest_hl,
                                )
                            )
                            _rerun_fragment_safe()
                with b_4wks:
                    if st.button("+4 semanas", disabled=not can_add, key="btn_add_4_weeks", width="stretch"):
                        if staged_slots:
                            last_slot = staged_slots[-1]
                            cursor_out = datetime.date.fromisoformat(str(last_slot["outbound_date"])) + datetime.timedelta(days=7)
                            cursor_ret = (
                                (datetime.date.fromisoformat(str(last_slot["return_date"])) + datetime.timedelta(days=7))
                                if last_slot.get("return_date")
                                else None
                            )
                        else:
                            cursor_out = range_start
                            cursor_ret = range_end

                        added_count = 0
                        steps = 0
                        while added_count < 4 and len(staged_slots) < MAX_SLOTS_PER_DESTINATION and steps < 12:
                            pair_key = (
                                cursor_out.strftime("%Y-%m-%d"),
                                cursor_ret.strftime("%Y-%m-%d") if cursor_ret else None,
                            )
                            if pair_key not in blocked_pairs:
                                staged_slots.append(
                                    _build_staged_slot(
                                        cursor_out,
                                        cursor_ret,
                                        dest_origin,
                                        dest_code,
                                        dest_trip,
                                        dest_profile,
                                        dest_curr,
                                        dest_gl,
                                        dest_hl,
                                    )
                                )
                                blocked_pairs.add(pair_key)
                                added_count += 1
                            cursor_out = cursor_out + datetime.timedelta(days=7)
                            cursor_ret = (cursor_ret + datetime.timedelta(days=7)) if cursor_ret else None
                            steps += 1
                        if added_count > 0:
                            _rerun_fragment_safe()
                        else:
                            st.warning("Esas semanas ya están agregadas o guardadas.")

        # Visual Tray of Staged Slots
        st.markdown("---")
        tray_h1, tray_h2 = st.columns([3, 1])
        with tray_h1:
            st.markdown(f"##### Fechas por guardar ({len(staged_slots)}/{MAX_SLOTS_PER_DESTINATION})")
        with tray_h2:
            if staged_slots and st.button("Limpiar", key="btn_clear_staged", width="stretch"):
                st.session_state["staged_slots_list"] = []
                _rerun_fragment_safe()

        if not staged_slots:
            st.info("Lista vacía. Selecciona fechas arriba y presiona **Agregar**.")
        else:
            for s_idx, slot in enumerate(staged_slots):
                sc1, sc2, sc3 = st.columns([2.5, 2, 0.5])
                with sc1:
                    st.markdown(f"**{s_idx + 1}.** `{slot['label']}`")
                with sc2:
                    win = slot.get("window_info") or get_flight_window_status(slot.get("outbound_date"), slot.get("return_date"))
                    if win["status"] == "ACTIVE_WINDOW":
                        st.markdown("<span class='status-pill-emerald'>Activa</span>", unsafe_allow_html=True)
                    elif win["status"] == "FUTURE_RELEASE":
                        st.markdown(f"<span class='status-pill-violet'>Abre {win['release_date'].strftime('%d/%m/%Y')}</span>", unsafe_allow_html=True)
                    else:
                        st.markdown("<span class='status-pill-slate'>Expirada</span>", unsafe_allow_html=True)
                with sc3:
                    if st.button("✕", key=f"del_staged_{s_idx}", help="Quitar"):
                        staged_slots.pop(s_idx)
                        _rerun_fragment_safe()

            st.markdown("")
            if st.button(
                f"Guardar {len(staged_slots)} fecha(s) en {target_dest['name']}",
                key="save_staged_slots_btn",
                type="primary",
                width="stretch",
            ):
                try:
                    inserted = bulk_create_date_slots(
                        destination_id=dest_id,
                        slots=staged_slots,
                        replace_existing=replace_mode,
                    )
                    st.session_state["staged_slots_list"] = []
                    st.success(f"Se guardaron {len(inserted)} fecha(s) en **{target_dest['name']}**.")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))

    # --------------------------------------------------------------------------
    # AUTOMATIC 10-DATE WINDOW GENERATOR
    # --------------------------------------------------------------------------
    with opt_a_tab:
        st.caption("Genera hasta 10 combinaciones según rango, duración y días de salida.")
        default_start = today + datetime.timedelta(days=14)
        default_end = today + datetime.timedelta(days=60)

        a1, a2, a3, a4 = st.columns([1.2, 1.2, 1.4, 1])
        with a1:
            win_start = st.date_input("Desde", value=default_start, key="opta_start")
        with a2:
            win_end = st.date_input("Hasta", value=default_end, key="opta_end")
        with a3:
            stay_range = st.slider(
                "Duración (días)",
                min_value=2,
                max_value=30,
                value=(7, 10),
                key="opta_stay",
            )
        with a4:
            max_gen_slots = st.number_input(
                "Máx. fechas",
                min_value=1,
                max_value=MAX_SLOTS_PER_DESTINATION,
                value=MAX_SLOTS_PER_DESTINATION,
                step=1,
                key="opta_max",
            )

        weekday_names = [
            (0, "Lun"),
            (1, "Mar"),
            (2, "Mié"),
            (3, "Jue"),
            (4, "Vie"),
            (5, "Sáb"),
            (6, "Dom"),
        ]
        selected_weekdays = st.multiselect(
            "Días de salida",
            options=[w[0] for w in weekday_names],
            default=[3, 4, 5],
            format_func=lambda idx: dict(weekday_names)[idx],
            key="opta_weekdays",
        )

        stay_list = list(range(stay_range[0], stay_range[1] + 1))
        generated_preview = generate_date_matrix(
            start_date=win_start,
            end_date=win_end,
            stay_days_list=stay_list,
            weekdays_only=selected_weekdays,
            max_slots=int(max_gen_slots),
            origin=dest_origin,
            destination=dest_code,
            trip_type=dest_trip,
            filter_profile=dest_profile,
            curr=dest_curr,
            gl=dest_gl,
            hl=dest_hl,
        )

        if generated_preview:
            st.markdown(f"**Vista previa ({len(generated_preview)})**")
            preview_display = []
            for g in generated_preview:
                w_info = get_flight_window_status(g["outbound_date"], g.get("return_date"))
                preview_display.append({
                    "Etiqueta": g["label"],
                    "Ida": g["outbound_date"],
                    "Vuelta": g.get("return_date") or "—",
                    "Ventana": w_info["badge"],
                    "Google Flights": g["google_flights_url"],
                })
            st.dataframe(
                pd.DataFrame(preview_display),
                width="stretch",
                hide_index=True,
            )
            if st.button(
                f"Guardar {len(generated_preview)} fecha(s) en {target_dest['name']}",
                key="save_opta_btn",
                type="primary",
                width="stretch",
            ):
                try:
                    inserted = bulk_create_date_slots(
                        destination_id=dest_id,
                        slots=generated_preview,
                        replace_existing=replace_mode,
                    )
                    st.success(f"Se guardaron {len(inserted)} fecha(s) en **{target_dest['name']}**.")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))
        else:
            st.warning("No se encontraron combinaciones para esos días.")

    # --------------------------------------------------------------------------
    # PASTE GOOGLE FLIGHTS URLS
    # --------------------------------------------------------------------------
    with opt_c_tab:
        st.caption("Pega de 1 a 10 enlaces de Google Flights (uno por línea).")
        raw_urls_input = st.text_area(
            "Enlaces de Google Flights",
            height=150,
            placeholder="https://www.google.com/travel/flights/search?tfs=...",
            key="optc_textarea",
        )

        if raw_urls_input.strip():
            try:
                parsed_url_slots = parse_pasted_urls(
                    raw_text=raw_urls_input,
                    curr=dest_curr,
                    gl=dest_gl,
                    hl=dest_hl,
                    fallback_origin=dest_origin,
                    fallback_destination=dest_code,
                    max_slots=MAX_SLOTS_PER_DESTINATION,
                )
            except Exception as parse_err:
                st.error(f"Error al leer los enlaces: {parse_err}")
                parsed_url_slots = []

            if parsed_url_slots:
                st.markdown(f"**Fechas detectadas ({len(parsed_url_slots)})**")
                display_pasted = []
                for p in parsed_url_slots:
                    w_info = get_flight_window_status(p["outbound_date"], p.get("return_date"))
                    display_pasted.append({
                        "Etiqueta": p["label"],
                        "Ida": p["outbound_date"],
                        "Vuelta": p.get("return_date") or "—",
                        "Ventana": w_info["badge"],
                        "Google Flights": p["google_flights_url"],
                    })
                st.dataframe(
                    pd.DataFrame(display_pasted),
                    width="stretch",
                    hide_index=True,
                )
                if st.button(
                    f"Guardar {len(parsed_url_slots)} enlace(s) en {target_dest['name']}",
                    key="save_optc_btn",
                    type="primary",
                    width="stretch",
                ):
                    try:
                        inserted = bulk_create_date_slots(
                            destination_id=dest_id,
                            slots=parsed_url_slots,
                            replace_existing=replace_mode,
                        )
                        st.success(f"Se guardaron {len(inserted)} enlace(s) en **{target_dest['name']}**.")
                        st.rerun()
                    except Exception as exc:
                        st.error(str(exc))
            else:
                st.warning("No se detectaron enlaces válidos de Google Flights.")


# ==============================================================================
# TAB 3: LIVE SCANNER, LOGS & TELEGRAM TESTER
# ==============================================================================


@st.fragment
def render_tab3_scanner_and_logs() -> None:
    """
    Render Tab 3:
      - Immediate Live Scanner (by Date Slot, Destination, or Due Batch)
      - Consolidated Digest Telegram Tester (faithful to 330-day window)
      - Recent `alert_logs` table & `extraction_source` distribution metrics
    """
    destinations = list_destinations(active_only=False)
    summary = get_dashboard_summary()
    col_scan, col_tg = st.columns([1.45, 1.15])

    with col_scan:
        st.markdown("### Escáner en Vivo (Immediate Dual-Tab Check)")
        st.caption(
            "Ejecuta el motor híbrido (`Patchright` Pestañas Mejores + Más Baratos + `ds:1` + fallback SSR) "
            "respetando la ventana de 330 días."
        )

        scan_scope = st.radio(
            "Alcance del Escaneo",
            options=[
                "Escanear un Destino Específico (Sus fechas en ventana)",
                "Escanear una Fecha Específica (date_slot)",
                "Ejecutar Lote de Cola Vencida (Due Queue Batch)",
            ],
            horizontal=False,
            key="tab3_scan_scope",
        )

        chosen_dest_id: Optional[int] = None
        chosen_slot_id: Optional[int] = None
        batch_limit = 10

        if scan_scope.startswith("Escanear un Destino") and destinations:
            sel_d = st.selectbox(
                "Seleccionar Destino",
                options=destinations,
                format_func=lambda d: f"{d['name']} ({d['origin']} ➔ {d['destination']})",
                key="tab3_dest_picker",
            )
            chosen_dest_id = int(sel_d["id"])
        elif scan_scope.startswith("Escanear una Fecha") and destinations:
            all_slots = list_date_slots(active_only=False)
            if all_slots:
                sel_s = st.selectbox(
                    "Seleccionar Combinación de Fecha",
                    options=all_slots,
                    format_func=lambda s: (
                        f"[{s.get('origin')}➔{s.get('destination_code')}] "
                        f"{s.get('label') or s.get('outbound_date')} "
                        f"({s.get('window_info', {}).get('badge', '330d')})"
                    ),
                    key="tab3_slot_picker",
                )
                chosen_slot_id = int(sel_s["id"])
                batch_limit = 1
        else:
            batch_limit = st.slider(
                "Máximo de Slots a Procesar en esta Corrida",
                min_value=1,
                max_value=25,
                value=10,
                key="tab3_batch_slider",
            )

        if st.button("Iniciar Escaneo Dual-Tab Ahora", type="primary", width="stretch"):
            progress_bar = st.progress(0, text="Inicializando navegador stealth y validando ventana...")
            status_box = st.empty()

            def _ui_progress(done: int, total: int, message: str, last_item: Optional[dict[str, Any]]) -> None:
                ratio = min(1.0, float(done) / max(1, total))
                progress_bar.progress(ratio, text=f"[{done}/{total}] {message}")

            with st.spinner("Extrayendo tarifas en Google Flights..."):
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(
                        run_sharded_dispatcher,
                        max_slots=int(batch_limit),
                        force_all=True,
                        destination_id=chosen_dest_id,
                        slot_id=chosen_slot_id,
                        progress_callback=_ui_progress,
                    )
                    run_summary = future.result()

            clear_db_cache()
            progress_bar.progress(1.0, text="Escaneo finalizado.")
            if run_summary["total_due"] == 0:
                status_box.info(
                    "No hay fechas elegibles para escanear en este momento (todas están fuera de la ventana de 330 días o no están vencidas)."
                )
            else:
                status_box.success(
                    f"Corrida completada en **{run_summary['elapsed_seconds']}s** — "
                    f"Slots procesados: **{run_summary['total_due']}** | "
                    f"Exitosos: **{run_summary['scraped_ok']}** | "
                    f"Alertas enviadas: **{run_summary['alerts_sent']}**"
                )
            if run_summary.get("results"):
                st.dataframe(
                    pd.DataFrame(run_summary["results"]),
                    width="stretch",
                    hide_index=True,
                )

    with col_tg:
        st.markdown("### Probador de Alerta Telegram (Verificado)")
        st.caption(
            "Envía una tarjeta HTML compacta de prueba a Telegram (Meta / Bajo Histórico, excluyendo >330 días)."
        )
        tg_dest_options: list[tuple[Optional[int], str]] = [
            (None, "Primer Destino Activo / Monitoreado")
        ]
        for d in destinations:
            tg_dest_options.append(
                (int(d["id"]), f"{d['name']} ({d['origin']} ➔ {d['destination']})")
            )

        tg_sel = st.selectbox(
            "Destino para la Tarjeta de Prueba",
            options=tg_dest_options,
            format_func=lambda x: str(x[1]) if isinstance(x, tuple) else str(x),
            key="tab3_tg_dest",
        )
        if st.button("Enviar Tarjeta Digest de Prueba a Telegram", width="stretch"):
            with st.spinner("Verificando datos y enviando reporte a Telegram..."):
                ok, detail, md_preview = send_test_digest_alert(destination_id=tg_sel[0])
            if ok:
                st.success(f"Reporte entregado exitosamente a Telegram ({detail}).")
            else:
                st.error(f"No se pudo enviar a Telegram: {detail}")
            with st.expander("Ver HTML del Mensaje Enviado", expanded=True):
                st.code(md_preview, language="html")

    # --------------------------------------------------------------------------
    # SCRAPER HEALTH METRICS & ALERT LOGS TABLE
    # --------------------------------------------------------------------------
    st.markdown("---")
    h_col1, h_col2 = st.columns([1.1, 1.9])

    with h_col1:
        st.markdown("### Salud del Scraper (extraction_source)")
        sources = summary.get("extraction_sources") or []
        if sources:
            df_src = pd.DataFrame(sources)
            fig_pie = px.pie(
                df_src,
                names="extraction_source",
                values="count",
                hole=0.48,
                title="Distribución de Capas de Extracción (7d)",
                template="plotly_dark",
                color_discrete_sequence=["#38BDF8", "#818CF8", "#10B981", "#F59E0B"],
            )
            fig_pie.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                height=300,
                margin=dict(l=10, r=10, t=40, b=10),
            )
            st.plotly_chart(fig_pie, width="stretch")
            st.dataframe(df_src, width="stretch", hide_index=True)
        else:
            st.info("Sin registros de extracción en los últimos 7 días.")

    with h_col2:
        st.markdown("### Historial de Alertas Enviadas (alert_logs)")
        recent_logs = get_recent_alert_logs(limit=40)
        if recent_logs:
            df_logs = pd.DataFrame(recent_logs)
            display_log_cols = [
                c
                for c in [
                    "sent_at",
                    "destination_name",
                    "slot_label",
                    "trigger_type",
                    "cheapest_price",
                    "best_price",
                ]
                if c in df_logs.columns
            ]
            st.dataframe(
                df_logs[display_log_cols],
                width="stretch",
                hide_index=True,
            )
        else:
            st.info("Aún no se han registrado alertas disparadas en `alert_logs`.")

    # --------------------------------------------------------------------------
    # PROXY CONNECTIVITY & STEALTH DIAGNOSTICS
    # --------------------------------------------------------------------------
    st.markdown("---")
    with st.expander("Diagnóstico de Proxies y Conexión de Red (Anti-Bloqueo)", expanded=False):
        st.caption(
            "Verifica la latencia, IP pública de salida y el estado de acceso a Google Flights "
            "tanto para la conexión directa como para tus proxies configurados."
        )
        pool = get_proxy_pool()
        active_proxies_count = len([p for p in pool if p is not None])
        st.write(
            f"**Pool de Proxies Detectados:** `{active_proxies_count}` configurado(s) • Conexión directa disponible como fallback."
        )

        test_col1, test_col2 = st.columns([2.5, 1])
        with test_col1:
            custom_proxy_test = st.text_input(
                "Probar Proxy Personalizado (opcional)",
                placeholder="http://user:pass@host:port o socks5://127.0.0.1:40000",
                key="custom_proxy_test_input",
            )
        with test_col2:
            st.markdown("<div style='height: 28px;'></div>", unsafe_allow_html=True)
            run_proxy_check = st.button("Verificar Conexión de Red", key="btn_run_proxy_check", width="stretch")

        if run_proxy_check:
            targets_to_test = list(pool)
            if custom_proxy_test.strip() and custom_proxy_test.strip() not in targets_to_test:
                targets_to_test.append(custom_proxy_test.strip())
            if None not in targets_to_test:
                targets_to_test.append(None)

            results_list = []
            with st.spinner(f"Probando {len(targets_to_test)} conexiones en paralelo..."):
                with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, max(1, len(targets_to_test)))) as ex:
                    test_results = list(ex.map(test_proxy_connection, targets_to_test))
            for res in test_results:
                status_desc = (
                    "Acceso Correcto (HTTP 200)"
                    if res["success"]
                    else f"Error: {sanitize_log_text(res['error']) or 'Bloqueado'}"
                )
                results_list.append({
                    "Proxy / Red": res["proxy_url"],
                    "IP Pública Detectada": res["ip"],
                    "Latencia": f"{res['latency_ms']} ms",
                    "Acceso a Google Flights": status_desc,
                })
            st.dataframe(pd.DataFrame(results_list), width="stretch", hide_index=True)


# ==============================================================================
# MAIN APPLICATION ENTRYPOINT
# ==============================================================================


def main() -> None:
    if not check_password_gate():
        return

    with st.sidebar:
        st.markdown("## FlightPulse v4.0")
        st.caption(f"Matrix {MAX_DESTINATIONS}×{MAX_SLOTS_PER_DESTINATION} Stealth Flight Engine")
        st.markdown("---")
        st.markdown(
            f"""
            **Capacidad y Estado del Sistema:**
            - **Destinos Activos:** Hasta `{MAX_DESTINATIONS}`
            - **Fechas por Destino:** Hasta `{MAX_SLOTS_PER_DESTINATION}`
            - **Capacidad Total:** `{MAX_TOTAL_SLOTS}` Date Slots
            - **Extracción Dual:** `Más baratos` vs `Mejores`
            - **Ventana Oficial:** `330 días` máx. anticipación
            - **Anti-Ban Queue:** Sharded cada 60m (`50` slots/run)
            """
        )
        st.markdown("---")
        if st.button("Refrescar Datos", width="stretch"):
            clear_db_cache()
            st.rerun()
        if st.button("Cerrar Sesión", width="stretch"):
            st.session_state["authenticated"] = False
            st.rerun()

    if not ensure_database_ready():
        return

    summary = get_dashboard_summary()

    st.markdown("## FlightPulse Matrix v4.0 — Control Center")
    render_top_metrics_ribbon(summary)
    st.markdown("")

    tab1, tab2, tab3 = st.tabs(
        [
            "Matriz de Precios",
            "Destinos y Fechas",
            "Escáner y Alertas",
        ]
    )

    with tab1:
        render_tab1_matrix_explorer()

    with tab2:
        render_tab2_destination_and_generator()

    with tab3:
        render_tab3_scanner_and_logs()


if __name__ == "__main__":
    main()
