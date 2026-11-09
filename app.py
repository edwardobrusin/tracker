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
import sys
from typing import Any, Optional

# Hot-reload safety for Streamlit Cloud: purge cached in-memory src modules on rerun
for _mod in [m for m in list(sys.modules.keys()) if m == "src" or m.startswith("src.")]:
    sys.modules.pop(_mod, None)

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.alerts import send_test_digest_alert
from src.config import (
    APP_PASSWORD,
    COMMON_AIRLINES,
    DEFAULT_CURRENCY,
    DEFAULT_GL,
    DEFAULT_HL,
    GOOGLE_FLIGHTS_MAX_ADVANCE_DAYS,
    MAX_DESTINATIONS,
    MAX_SLOTS_PER_DESTINATION,
    MAX_TOTAL_SLOTS,
    get_flight_window_status,
    get_proxy_pool,
    test_proxy_connection,
)
from src.db import (
    bulk_create_date_slots,
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
    page_title="FlightPulse Matrix v4.0 | 10×10 Stealth Flight Tracker",
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
        st.caption("Self-Hosted 10×10 Google Flights Dual-Tab Stealth Engine (`Best` vs. `Cheapest`)")
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


def render_tab1_matrix_explorer(destinations: list[dict[str, Any]]) -> None:
    """Render Tab 1: Corridor Banner, 10-Date Comparison Table, Heatmap, and Plotly Charts."""
    if not destinations:
        st.info(
            "Aún no tienes destinos configurados. Ve a la pestaña **Tab 2 — Crear / Gestionar Destino y Generador de Fechas** "
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
            format_func=lambda x: str(x[1]),
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
                        &nbsp;•&nbsp; <strong>Escaneo:</strong> Cada {selected_dest.get('check_interval_minutes', 60)}m
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


def render_tab2_destination_and_generator(destinations: list[dict[str, Any]]) -> None:
    """
    Render Tab 2:
      Step 1: Configure Destination & Filter Profile
      Step 2: Add up to 10 Date Combinations (Option A, Option B with Single-Calendar Range, Option C)
    """
    st.markdown("### Paso 1: Configurar Corredor de Vuelo y Perfil de Filtros")

    mode_options: list[tuple[Optional[int], str]] = [
        (None, "Crear Nuevo Destino (Ruta + Perfil de Filtros)")
    ]
    for d in destinations:
        mode_options.append(
            (
                int(d["id"]),
                f"Editar Destino Existente: {d['name']} ({d['origin']} ➔ {d['destination']})",
            )
        )

    selected_mode = st.selectbox(
        "Seleccionar Acción sobre Destino",
        options=mode_options,
        format_func=lambda x: str(x[1]),
        key="tab2_dest_mode",
    )
    editing_dest_id: Optional[int] = selected_mode[0]
    existing_dest = (
        next((d for d in destinations if int(d["id"]) == editing_dest_id), None)
        if editing_dest_id is not None
        else None
    )
    existing_cfg = FilterProfile.from_dict(existing_dest.get("filter_config") if existing_dest else {})

    with st.form("destination_profile_form", clear_on_submit=False):
        r1c1, r1c2, r1c3, r1c4 = st.columns([2.0, 1.2, 1.2, 1.1])
        with r1c1:
            name_val = st.text_input(
                "Nombre del Destino / Corredor",
                value=existing_dest["name"] if existing_dest else "CDMX Multi -> Tokio Multi (Otoño)",
                placeholder="Ej. CDMX Multi -> Tokio (Otoño)",
            )
        with r1c2:
            origin_val = st.text_input(
                "Orígenes IATA (1 o más, sep. por coma)",
                value=existing_dest["origin"] if existing_dest else "MEX, NLU",
                max_chars=64,
                placeholder="Ej. MEX, NLU, TLC",
                help="Ingresa uno o varios aeropuertos separados por coma (ej. MEX, NLU, TLC). Se consultan juntos en 1 sola llamada.",
            ).upper()
        with r1c3:
            dest_val = st.text_input(
                "Destinos IATA (1 o más, sep. por coma)",
                value=existing_dest["destination"] if existing_dest else "NRT, HND",
                max_chars=64,
                placeholder="Ej. NRT, HND o TYO",
                help="Ingresa uno o varios aeropuertos (ej. NRT, HND o MAD, BCN) o códigos metropolitanos (TYO, LON, NYC, PAR).",
            ).upper()
        with r1c4:
            trip_type_val = st.selectbox(
                "Tipo de Viaje",
                options=["round-trip", "one-way"],
                index=0 if (not existing_dest or existing_dest.get("trip_type") == "round-trip") else 1,
                format_func=lambda t: "Redondo (Ida y Vuelta)" if t == "round-trip" else "Sencillo (Solo Ida)",
            )

        r2c1, r2c2, r2c3, r2c4, r2c5 = st.columns([1.4, 1.2, 1.2, 1, 1])
        with r2c1:
            target_price_val = st.number_input(
                "Precio Meta (Target Price)",
                min_value=100.0,
                max_value=500000.0,
                value=float(existing_dest["target_price"]) if existing_dest else 16000.0,
                step=250.0,
            )
        with r2c2:
            interval_choices = [60, 180, 360]
            curr_interval = int(existing_dest.get("check_interval_minutes", 60)) if existing_dest else 60
            interval_idx = interval_choices.index(curr_interval) if curr_interval in interval_choices else 0
            check_interval_val = st.selectbox(
                "Frecuencia de Escaneo",
                options=interval_choices,
                index=interval_idx,
                format_func=lambda m: f"Cada {m // 60}h ({m} min)",
            )
        with r2c3:
            cooldown_val = st.selectbox(
                "Cooldown de Alertas",
                options=[6, 12, 24],
                index=1 if not existing_dest else ([6, 12, 24].index(int(existing_dest.get("cooldown_hours", 12))) if int(existing_dest.get("cooldown_hours", 12)) in (6, 12, 24) else 1),
                format_func=lambda h: f"{h} horas (salvo -5% extra)",
            )
        with r2c4:
            currency_val = st.selectbox(
                "Moneda (`curr`)",
                options=["MXN", "USD", "EUR", "CAD", "COP"],
                index=0 if not existing_dest else (["MXN", "USD", "EUR", "CAD", "COP"].index(existing_dest.get("currency", "MXN")) if existing_dest.get("currency", "MXN") in ["MXN", "USD", "EUR", "CAD", "COP"] else 0),
            )
        with r2c5:
            seat_val = st.selectbox(
                "Cabina",
                options=["economy", "premium-economy", "business", "first"],
                index=["economy", "premium-economy", "business", "first"].index(existing_cfg.seat)
                if existing_cfg.seat in ["economy", "premium-economy", "business", "first"]
                else 0,
                format_func=lambda s: {
                    "economy": "Turista",
                    "premium-economy": "Premium Econ",
                    "business": "Ejecutiva",
                    "first": "Primera",
                }[s],
            )

        st.markdown("##### Filtros Avanzados Protobuf (`?tfs=`)")
        f1, f2, f3, f4 = st.columns([1.2, 2.2, 1, 1])
        with f1:
            stops_options = [None, 0, 1, 2]
            stops_idx = stops_options.index(existing_cfg.max_stops) if existing_cfg.max_stops in stops_options else 0
            max_stops_val = st.selectbox(
                "Escalas Máximas",
                options=stops_options,
                index=stops_idx,
                format_func=lambda s: {
                    None: "Cualquier escala",
                    0: "0 — Solo vuelos directos",
                    1: "1 — Máximo 1 escala",
                    2: "2 — Máximo 2 escalas",
                }[s],
            )
        with f2:
            default_airlines = [a for a in existing_cfg.airlines if a in COMMON_AIRLINES]
            selected_airlines = st.multiselect(
                "Aerolíneas Permitidas (Opcional — vacío = todas)",
                options=list(COMMON_AIRLINES.keys()),
                default=default_airlines,
                format_func=lambda k: str(COMMON_AIRLINES.get(str(k), str(k))),
            )
        with f3:
            carry_on_val = st.selectbox(
                "Equipaje de Mano",
                options=[0, 1, 2],
                index=min(2, max(0, existing_cfg.carry_on_bags)),
                format_func=lambda b: "Sin filtro (0)" if b == 0 else f"{b} maleta(s)",
            )
        with f4:
            adults_val = st.number_input(
                "Pasajeros",
                min_value=1,
                max_value=9,
                value=existing_cfg.adults,
                step=1,
            )

        h1, h2, h3 = st.columns([1.5, 1.5, 1.2])
        with h1:
            dep_window = st.slider(
                "Horario de Salida (00:00 - 23:59)",
                min_value=0,
                max_value=23,
                value=(existing_cfg.dep_start_hour, existing_cfg.dep_end_hour),
            )
        with h2:
            arr_window = st.slider(
                "Horario de Llegada (00:00 - 23:59)",
                min_value=0,
                max_value=23,
                value=(existing_cfg.arr_start_hour, existing_cfg.arr_end_hour),
            )
        with h3:
            custom_iata_str = st.text_input(
                "Aerolíneas IATA Adicionales (separadas por coma)",
                value=",".join([a for a in existing_cfg.airlines if a not in COMMON_AIRLINES]),
                placeholder="Ej. NH, JL",
            )
            hide_self_transfer_val = st.checkbox(
                "Ocultar vuelos Self-Transfer",
                value=existing_cfg.hide_self_transfer,
            )

        submit_btn_text = (
            "Actualizar Perfil del Destino" if editing_dest_id is not None else "Crear Destino en Matrix"
        )
        submitted_dest = st.form_submit_button(submit_btn_text, width="stretch")

        if submitted_dest:
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
                        name=name_val,
                        origin=norm_orig,
                        destination=norm_dest,
                        target_price=float(target_price_val),
                        trip_type=trip_type_val,
                        currency=currency_val,
                        gl=DEFAULT_GL,
                        hl=DEFAULT_HL,
                        check_interval_minutes=int(check_interval_val),
                        cooldown_hours=int(cooldown_val),
                        filter_config=profile_obj.to_dict(),
                        is_active=True,
                    )
                    st.session_state["last_created_dest_id"] = new_id
                    st.success(
                        f"Destino **{name_val}** (`{corridor_disp}`) creado (ID #{new_id}). "
                        "Ahora agrega hasta 10 combinaciones de fechas en el Paso 2 abajo."
                    )
                    st.rerun()
                else:
                    update_destination(
                        destination_id=editing_dest_id,
                        name=name_val,
                        origin=norm_orig,
                        destination=norm_dest,
                        target_price=float(target_price_val),
                        trip_type=trip_type_val,
                        currency=currency_val,
                        gl=DEFAULT_GL,
                        hl=DEFAULT_HL,
                        check_interval_minutes=int(check_interval_val),
                        cooldown_hours=int(cooldown_val),
                        filter_config=profile_obj.to_dict(),
                        is_active=bool(existing_dest.get("is_active", True)) if existing_dest else True,
                    )
                    st.success(
                        f"Perfil del destino **{name_val}** (`{corridor_disp}`) y sus URLs Protobuf actualizados correctamente."
                    )
                    st.rerun()
            except Exception as exc:
                st.error(f"No se pudo guardar el destino: {exc}")

    # --------------------------------------------------------------------------
    # STEP 2: ADD UP TO 10 DATE COMBINATIONS (WITH 330-DAY INTELLIGENCE)
    # --------------------------------------------------------------------------
    st.markdown("---")
    st.markdown("### Paso 2: Agregar hasta 10 Combinaciones de Fechas")

    if not destinations:
        st.info("Crea al menos un destino en el Paso 1 arriba para habilitar el selector de fechas.")
        return

    default_dest_idx = 0
    last_id = st.session_state.get("last_created_dest_id")
    if last_id is not None:
        for i, d in enumerate(destinations):
            if int(d["id"]) == int(last_id):
                default_dest_idx = i
                break

    target_dest = st.selectbox(
        "Seleccionar Destino a Gestionar",
        options=destinations,
        index=default_dest_idx,
        format_func=lambda d: (
            f"{d['name']} ({d['origin']} ➔ {d['destination']}) — "
            f"Slots activos: {d.get('active_slots', 0)}/{MAX_SLOTS_PER_DESTINATION}"
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

    # Notice about 330-day window
    today = datetime.date.today()
    max_searchable_date = today + datetime.timedelta(days=GOOGLE_FLIGHTS_MAX_ADVANCE_DAYS)
    st.caption(
        f"**Ventana de 330 días:** Google Flights publica vuelos hasta el **{max_searchable_date.strftime('%d/%m/%Y')}**. "
        "Si agregas fechas posteriores, el sistema las guardará y comenzará a rastrearlas automáticamente en cuanto las aerolíneas liberen sus itinerarios."
    )

    replace_mode = st.checkbox(
        "Reemplazar las fechas actuales de este destino al guardar las nuevas combinaciones",
        value=False,
        key="step2_replace_existing",
    )

    opt_b_tab, opt_a_tab, opt_c_tab = st.tabs(
        [
            "Opción B — Selector Rango en 1 Solo Calendario (Recomendado)",
            "Opción A — Generador Automático de Ventana de Viaje",
            "Opción C — Pegar URLs de Google Flights",
        ]
    )

    # --------------------------------------------------------------------------
    # OPTION B: SINGLE-CALENDAR RANGE PICKER WITH STAGING TRAY (NEW UX)
    # --------------------------------------------------------------------------
    with opt_b_tab:
        st.markdown("##### Selecciona Rango en 1 Solo Calendario")
        st.caption(
            "Haz clic una vez para la fecha de salida y un segundo clic para el regreso. "
            "Si haces clic en una fecha anterior, se reinicia como nueva salida automáticamente."
        )

        if "staged_slots_list" not in st.session_state:
            st.session_state["staged_slots_list"] = []

        staged_slots: list[dict[str, Any]] = st.session_state["staged_slots_list"]

        default_cal_start = today + datetime.timedelta(days=21)
        default_cal_end = default_cal_start + datetime.timedelta(days=9)

        cal_c1, cal_c2 = st.columns([1.5, 1.5])

        with cal_c1:
            if dest_trip == "one-way":
                single_cal_val = st.date_input(
                    "Fecha de Salida (Solo ida)",
                    value=default_cal_start,
                    min_value=today,
                    key="opb_single_date",
                )
                range_start = single_cal_val
                range_end = None
            else:
                date_range_picker = st.date_input(
                    "Rango de Fechas (Ida y Vuelta)",
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
            st.markdown("##### Vista Previa de Selección")
            if range_start:
                out_str = range_start.strftime("%Y-%m-%d")
                ret_str = range_end.strftime("%Y-%m-%d") if range_end else None
                lbl = format_date_slot_label(out_str, ret_str)
                win_status = get_flight_window_status(out_str, ret_str)

                st.markdown(f"**Combinación:** `{lbl}`")
                if win_status["status"] == "ACTIVE_WINDOW":
                    st.success(f"**Dentro de ventana activa:** Rastreo inmediato ({win_status['days_until_flight']} días para el vuelo)")
                elif win_status["status"] == "FUTURE_RELEASE":
                    st.info(
                        f"**En espera de liberación:** Google Flights abre esta fecha el **{win_status['release_date'].strftime('%d/%m/%Y')}** "
                        f"(faltan {win_status['days_until_release']} días). Se guardará en pausa y se activará sola."
                    )
                else:
                    st.warning("Fecha no disponible")

                can_add = len(staged_slots) < MAX_SLOTS_PER_DESTINATION
                add_btn_txt = f"Añadir a la Lista ({len(staged_slots)}/{MAX_SLOTS_PER_DESTINATION})" if can_add else "Lista Completa (Máx 10)"

                b_add, b_shift7 = st.columns([1.2, 1])
                with b_add:
                    if st.button(add_btn_txt, disabled=not can_add, key="btn_add_staged", type="primary", width="stretch"):
                        if any(s["outbound_date"] == out_str and s.get("return_date") == ret_str for s in staged_slots):
                            st.warning("Esta combinación ya fue agregada a la lista.")
                        else:
                            slot_url = build_google_flights_url(
                                origin=dest_origin,
                                destination=dest_code,
                                outbound_date=out_str,
                                return_date=ret_str,
                                trip_type=dest_trip,
                                filter_profile=dest_profile,
                                curr=dest_curr,
                                gl=dest_gl,
                                hl=dest_hl,
                            )
                            staged_slots.append({
                                "label": lbl,
                                "outbound_date": out_str,
                                "return_date": ret_str,
                                "google_flights_url": slot_url,
                                "window_info": win_status,
                            })
                            st.rerun()
                with b_shift7:
                    if st.button("+7 Días Rápido", disabled=not can_add, key="btn_shift_7", width="stretch"):
                        shift_out = range_start + datetime.timedelta(days=7)
                        shift_ret = (range_end + datetime.timedelta(days=7)) if range_end else None
                        s_out_str = shift_out.strftime("%Y-%m-%d")
                        s_ret_str = shift_ret.strftime("%Y-%m-%d") if shift_ret else None
                        s_lbl = format_date_slot_label(s_out_str, s_ret_str)
                        s_win = get_flight_window_status(s_out_str, s_ret_str)
                        s_url = build_google_flights_url(
                            origin=dest_origin,
                            destination=dest_code,
                            outbound_date=s_out_str,
                            return_date=s_ret_str,
                            trip_type=dest_trip,
                            filter_profile=dest_profile,
                            curr=dest_curr,
                            gl=dest_gl,
                            hl=dest_hl,
                        )
                        staged_slots.append({
                            "label": s_lbl,
                            "outbound_date": s_out_str,
                            "return_date": s_ret_str,
                            "google_flights_url": s_url,
                            "window_info": s_win,
                        })
                        st.rerun()

        # Visual Tray of Staged Slots
        st.markdown("---")
        tray_h1, tray_h2 = st.columns([3, 1])
        with tray_h1:
            st.markdown(f"##### Fechas Seleccionadas para Guardar ({len(staged_slots)} / {MAX_SLOTS_PER_DESTINATION})")
        with tray_h2:
            if staged_slots and st.button("Limpiar Todo", key="btn_clear_staged", width="stretch"):
                st.session_state["staged_slots_list"] = []
                st.rerun()

        if not staged_slots:
            st.info("Tu bandeja de fechas está vacía. Selecciona un rango en el calendario arriba y haz clic en **'Añadir a la Lista'**.")
        else:
            for s_idx, slot in enumerate(staged_slots):
                sc1, sc2, sc3 = st.columns([2.5, 2, 0.5])
                with sc1:
                    st.markdown(f"**{s_idx + 1}.** `{slot['label']}`")
                with sc2:
                    win = slot.get("window_info") or get_flight_window_status(slot.get("outbound_date"), slot.get("return_date"))
                    if win["status"] == "ACTIVE_WINDOW":
                        st.markdown("<span class='status-pill-emerald'>En ventana activa</span>", unsafe_allow_html=True)
                    elif win["status"] == "FUTURE_RELEASE":
                        st.markdown(f"<span class='status-pill-violet'>Abre {win['release_date'].strftime('%d/%m/%Y')}</span>", unsafe_allow_html=True)
                    else:
                        st.markdown("<span class='status-pill-slate'>Expirada</span>", unsafe_allow_html=True)
                with sc3:
                    if st.button("✕", key=f"del_staged_{s_idx}", help="Quitar esta combinación"):
                        staged_slots.pop(s_idx)
                        st.rerun()

            st.markdown("")
            if st.button(
                f"Guardar estas {len(staged_slots)} Fecha(s) en '{target_dest['name']}'",
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
                    st.success(f"Se guardaron {len(inserted)} combinaciones de fechas en **{target_dest['name']}**.")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))

    # --------------------------------------------------------------------------
    # OPTION A: AUTOMATIC 10-DATE WINDOW GENERATOR
    # --------------------------------------------------------------------------
    with opt_a_tab:
        st.caption(
            "Elige un período de viaje, duración de estancia y días de la semana preferidos "
            "para generar hasta 10 combinaciones óptimas automáticamente."
        )
        default_start = today + datetime.timedelta(days=14)
        default_end = today + datetime.timedelta(days=60)

        a1, a2, a3, a4 = st.columns([1.2, 1.2, 1.4, 1])
        with a1:
            win_start = st.date_input("Inicio de Ventana", value=default_start, key="opta_start")
        with a2:
            win_end = st.date_input("Fin de Ventana", value=default_end, key="opta_end")
        with a3:
            stay_range = st.slider(
                "Duración del Viaje (días)",
                min_value=2,
                max_value=30,
                value=(7, 10),
                key="opta_stay",
            )
        with a4:
            max_gen_slots = st.number_input(
                "Máx. Fechas",
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
            "Días de Salida Preferidos",
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
            st.markdown(f"**Vista Previa ({len(generated_preview)} combinaciones generadas):**")
            preview_display = []
            for g in generated_preview:
                w_info = get_flight_window_status(g["outbound_date"], g.get("return_date"))
                preview_display.append({
                    "Etiqueta": g["label"],
                    "Ida": g["outbound_date"],
                    "Vuelta": g.get("return_date") or "—",
                    "Ventana (330d)": w_info["badge"],
                    "Google Flights URL": g["google_flights_url"],
                })
            st.dataframe(
                pd.DataFrame(preview_display),
                width="stretch",
                hide_index=True,
            )
            if st.button(
                f"Guardar estas {len(generated_preview)} Fechas en '{target_dest['name']}'",
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
                    st.success(
                        f"Se guardaron {len(inserted)} combinaciones de fechas para **{target_dest['name']}**."
                    )
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))
        else:
            st.warning("No se encontraron combinaciones para los días seleccionados en ese rango.")

    # --------------------------------------------------------------------------
    # OPTION C: PASTE GOOGLE FLIGHTS URLS (ROBUST REGEX EXTRACTION)
    # --------------------------------------------------------------------------
    with opt_c_tab:
        st.caption(
            "Pega de 1 a 10 URLs directamente desde Google Flights. "
            "El sistema extrae el Protobuf `?tfs=`, limpia enlaces en formato markdown o notas, y normaliza moneda y lenguaje."
        )
        raw_urls_input = st.text_area(
            "Pegar URLs de Google Flights (1 a 10 líneas)",
            height=160,
            placeholder="https://www.google.com/travel/flights/search?tfs=CBwQAhooEgoyMDI2LTExLTA2...\nhttps://www.google.com/travel/flights/search?tfs=CBwQAhooEgoyMDI2LTExLTEz...",
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
                st.error(f"Error al analizar las URLs pegadas: {parse_err}")
                parsed_url_slots = []

            if parsed_url_slots:
                st.markdown(f"**URLs Decodificadas ({len(parsed_url_slots)}):**")
                display_pasted = []
                for p in parsed_url_slots:
                    w_info = get_flight_window_status(p["outbound_date"], p.get("return_date"))
                    display_pasted.append({
                        "Etiqueta": p["label"],
                        "Ida": p["outbound_date"],
                        "Vuelta": p.get("return_date") or "—",
                        "Ventana (330d)": w_info["badge"],
                        "Google Flights URL": p["google_flights_url"],
                    })
                st.dataframe(
                    pd.DataFrame(display_pasted),
                    width="stretch",
                    hide_index=True,
                )
                if st.button(
                    f"Guardar {len(parsed_url_slots)} URL(s) en '{target_dest['name']}'",
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
                        st.success(f"Se guardaron {len(inserted)} URLs en **{target_dest['name']}**.")
                        st.rerun()
                    except Exception as exc:
                        st.error(str(exc))
            else:
                st.warning("No se detectaron URLs válidas de Google Flights en el texto ingresado.")


# ==============================================================================
# TAB 3: LIVE SCANNER, LOGS & TELEGRAM TESTER
# ==============================================================================


def render_tab3_scanner_and_logs(
    destinations: list[dict[str, Any]],
    summary: dict[str, Any],
) -> None:
    """
    Render Tab 3:
      - Immediate Live Scanner (by Date Slot, Destination, or Due Batch)
      - Consolidated Digest Telegram Tester (faithful to 330-day window)
      - Recent `alert_logs` table & `extraction_source` distribution metrics
    """
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
            "Envía una tarjeta consolidada real a Telegram. Si las fechas están fuera de la ventana de 330 días, "
            "informa su fecha exacta de liberación sin inventar precios ni enlaces ficticios."
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
            format_func=lambda x: str(x[1]),
            key="tab3_tg_dest",
        )
        if st.button("Enviar Tarjeta Digest de Prueba a Telegram", width="stretch"):
            with st.spinner("Verificando datos y enviando reporte a Telegram..."):
                ok, detail, md_preview = send_test_digest_alert(destination_id=tg_sel[0])
            if ok:
                st.success(f"Reporte entregado exitosamente a Telegram ({detail}).")
            else:
                st.error(f"No se pudo enviar a Telegram: {detail}")
            with st.expander("Ver Markdown del Mensaje Enviado", expanded=True):
                st.code(md_preview, language="markdown")

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

            results_list = []
            for target in targets_to_test:
                with st.spinner(f"Probando {target or 'Conexión Directa'}..."):
                    res = test_proxy_connection(target)
                    status_desc = "Acceso Correcto (HTTP 200)" if res["success"] else f"Error: {res['error'] or 'Bloqueado'}"
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
        st.caption("Matrix 10×10 Stealth Flight Engine")
        st.markdown("---")
        st.markdown(
            """
            **Capacidad y Estado del Sistema:**
            - **Destinos Activos:** Hasta `10`
            - **Fechas por Destino:** Hasta `10`
            - **Capacidad Total:** `100` Date Slots
            - **Extracción Dual:** `Más baratos` vs `Mejores`
            - **Ventana Oficial:** `330 días` máx. anticipación
            - **Anti-Ban Queue:** Sharded en lotes cada 25m
            """
        )
        st.markdown("---")
        if st.button("Refrescar Datos", width="stretch"):
            st.rerun()
        if st.button("Cerrar Sesión", width="stretch"):
            st.session_state["authenticated"] = False
            st.rerun()

    if not ensure_database_ready():
        return

    summary = get_dashboard_summary()
    destinations = list_destinations(active_only=False)

    st.markdown("## FlightPulse Matrix v4.0 — Control Center")
    render_top_metrics_ribbon(summary)
    st.markdown("")

    tab1, tab2, tab3 = st.tabs(
        [
            "Tab 1 — Explorador del Corredor y Matriz de Precios",
            "Tab 2 — Crear / Gestionar Destino y Generador de Fechas",
            "Tab 3 — Escáner en Vivo, Métricas y Probador de Telegram",
        ]
    )

    with tab1:
        render_tab1_matrix_explorer(destinations)

    with tab2:
        render_tab2_destination_and_generator(destinations)

    with tab3:
        render_tab3_scanner_and_logs(destinations, summary)


if __name__ == "__main__":
    main()
