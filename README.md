# ✈️ FlightPulse Matrix v4.0

**Self-Hosted, Zero-Cost Personal 20×10 N:M Google Flights Matrix Tracker with Dual-Tab Extraction (`Más Baratos` vs. `Mejores`), 330-Day Airline Window Intelligence & Consolidated Telegram Alerts.**

---

## 🎯 Project Scope & Ideal Objectives

FlightPulse Matrix v4.0 is a personal, self-hosted flight price monitoring system designed to track multi-airport travel corridors (`N origins × M destinations`, e.g., `MEX, NLU ➔ MXP, CDG, AMS, MAD, FRA`) across up to **20 active destinations × 10 date combinations = 200 active `date_slots`** using real Google Flights data (zero simulated prices).

### Core Scope
- **N:M Multi-Airport Corridors & Protobuf `?tfs=` Encoding (`src/tfs_builder.py`):** Generates deterministic Google Flights search URLs supporting multiple origin/destination airports, metro code expansion (e.g., `TYO → NRT, HND`), trip types, max stops, allowed airlines, carry-on baggage, and departure/arrival hour windows.
- **330-Day Advance Schedule Intelligence (`src/config.py`, `src/db.py`):** Automatically detects whether each `date_slot` is inside Google Flights' active booking window (`<= 330 days`) or waiting for airline schedule release (`FUTURE_RELEASE`), avoiding wasted scrapes on unreleased dates.
- **Dual-Tab Extraction Engine (`src/scraper.py`, `src/ds1_parser.py`):** Captures both the **Cheapest (`Más baratos`)** and **Best (`Mejores`)** tabs per date slot, including winning airport pair (`origin_airport -> destination_airport`), airline, stops, duration, and self-transfer warnings.
- **Persistent Historical Analytics (`src/db.py`):** Stores destinations, date slots, time-series `price_snapshots` (with rolling 7-day average and `all_time_low_price`), and `alert_logs` in Neon Serverless PostgreSQL (`psycopg` v3).
- **Automated Sharded Background Execution (`.github/workflows/flight_tracker.yml`, `src/runner.py`):** Runs on GitHub Actions (`0 * * * *`, every 60 min with `concurrency` protection) processing due slots in bounded batches (`MAX_SLOTS_PER_RUN=50`, default `check_interval_minutes=240` / 4h) ordered by `last_checked_at ASC NULLS FIRST` to cover all 200 slots ~6×/day without bursts.
- **Zero-Cost Network Priority & Secret-Safe Logging (`src/config.py`, `src/runner.py`):** Routes strictly in order **Nivel 1: Cloudflare WARP (`SOCKS5_PROXY`) ➔ Nivel 2: Direct (`None`) ➔ Nivel 3: `PROXY_LIST` (Webshare)** with interleaved multi-account rotation, per-account circuit breaker (`402`/`407`/3x tunnel failures), and strict proxy/credential redaction (`redact_proxy_for_log`) in all logs.
- **High-Signal Compact HTML Alerts (`src/alerts.py`):** Evaluates strictly **Meta** (`TARGET_BREACH`) and **Bajo Histórico** (`ALL_TIME_LOW`, >=2% drop vs DB reference), excludes `FUTURE_RELEASE` (>330d) slots, and sends ultra-concise `parse_mode="HTML"` Telegram messages with `[Reservar]` links.
- **Executive Control Center (`app.py`):** Password-protected Streamlit dashboard with matrix comparison tables, Plotly multi-date & dual-tab evolution charts, date-matrix generator/URL importer, live on-demand scanner, proxy diagnostics, and Telegram alert tester.

### Ideal Objectives
1. **$0/Month Sustainable Infrastructure:** Operate 24/7 on free-tier infrastructure (GitHub Actions + Neon Serverless PostgreSQL + Telegram Bot API + Streamlit) without exhausting free proxy bandwidth quotas or CI limits before the end of each billing cycle.
2. **Anti-Ban & Proxy Resilience:** Distribute scraping load safely across available network paths (proxies, Cloudflare WARP, or direct/lightweight HTTP SSR) while keeping per-slot data transfer minimal and avoiding Google rate-limits/CAPTCHAs.
3. **High-Signal, Low-Noise Telegram Alerts:** Deliver ultra-concise, scannable Telegram notifications strictly for high-value events (Target Price & Historical Low), with direct booking links and zero visual clutter.
4. **Full Matrix Utilization:** Support filling the full **20×10 (200 slots)** capacity with predictable, sustainable check frequencies.

---

## 🏛️ Core Architecture (Matrix 20×10 Engine)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                     STREAMLIT CONTROL CENTER (app.py)                        │
│  • Tab 1: 10-Date Matrix Comparison Table & Plotly Evolution Charts          │
│  • Tab 2: N:M Corridor Builder, Date Matrix Generator & Batch URL Paste      │
│  • Tab 3: Live Scanner, Scraper Health, Alert Logs, Proxy & Telegram Tester  │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                NEON SERVERLESS POSTGRESQL (src/db.py, psycopg v3)            │
│   destinations (≤20) ──► date_slots (≤200) ──► price_snapshots & alert_logs  │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│             SHARDED ANTI-BAN QUEUE DISPATCHER (src/runner.py)                │
│  • GitHub Actions Cron (0 * * * *) selects ≤50 due date_slots/run (4h cadence)│
│  • Filters to active 330-day window & orders `last_checked_at ASC NULLS FIRST`│
│  • Network priority: WARP (Nivel 1) ➔ Direct (Nivel 2) ➔ Webshare (Nivel 3)   │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│             THREE-LAYER HYBRID EXTRACTION ENGINE (src/scraper.py)            │
│  • Tier 1/2: Pooled Patchright Chrome (`headless=False` under `xvfb-run`)    │
│    - Blocks `image`, `media`, `font` & telemetry domains                     │
│    - Layer 1: SSR `AF_initDataCallback(ds:1)` + `GetShoppingResults` RPC     │
│    - Layer 2: Best (`Mejores opciones`) + deterministic Cheapest (`Los más bajos`)│
│    - Consolidates `min(dom_cheapest, rpc_cheapest, ds1_cheapest)`            │
│  • Tier 3A: `curl_cffi` Chrome TLS-impersonated SSR (`ds:1`) fallback        │
│  • Tier 3B: `primp` / `fast-flights` HTTP fallback                           │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│             CONSOLIDATED DESTINATION DIGEST ALERTS (src/alerts.py)           │
│  • Evaluates strictly Meta (`TARGET_BREACH`) & Bajo Histórico (`ALL_TIME_LOW`)│
│  • Excludes `FUTURE_RELEASE` (>330d) & logs notifications in `alert_logs`    │
│  • Sends ONE compact HTML Telegram card per destination (`[Reservar]`)       │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 📂 Repository Structure

```text
.
├── .github/
│   └── workflows/
│       └── flight_tracker.yml         # Hourly (0 * * * *) GitHub Actions scanner (Xvfb + WARP)
├── .streamlit/
│   └── config.toml                    # Executive dark-mode theme configuration
├── src/
│   ├── __init__.py
│   ├── config.py                      # Env/Secrets loader, 330d window check, proxy pool & tunnel verifier
│   ├── db.py                          # Neon PostgreSQL schema, persistent connection & CRUD/analytics layer
│   ├── tfs_builder.py                 # Pure-Python Protobuf ?tfs= encoder, decoder & 10-date matrix generator
│   ├── ds1_parser.py                  # AF_initDataCallback(ds:1), RPC & ARIA/DOM card parser
│   ├── scraper.py                     # Pooled Patchright browser + curl_cffi / primp / fast-flights fallback
│   ├── alerts.py                      # Consolidated per-destination Telegram digest engine
│   └── runner.py                      # Sharded anti-ban queue dispatcher CLI
├── tests/
│   └── test_dashboard_performance.py  # Functional, regression & millisecond DB/UI benchmark suite
├── app.py                             # Streamlit Executive Control Center
├── requirements.txt
├── .env.example
└── README.md
```

---

## 🚀 Zero-Cost Setup & Deployment Guide ($0/month)

### 1. Create a Free Neon Serverless PostgreSQL Database
1. Sign up at [Neon.tech](https://neon.tech) (Free Tier).
2. Copy your connection string (`postgresql://user:pass@ep-....aws.neon.tech/neondb?sslmode=require`).
3. Put it in `.env` as `DATABASE_URL`. Tables and indexes are auto-created on first run (`init_db()`).

### 2. Create Your Telegram Alert Bot
1. Message `@BotFather` on Telegram and run `/newbot` to obtain `TELEGRAM_BOT_TOKEN`.
2. Start a conversation with your new bot and get your numeric `TELEGRAM_CHAT_ID` (via `@userinfobot` or `https://api.telegram.org/bot<TOKEN>/getUpdates`).

### 3. Configure GitHub Actions Secrets
In your GitHub repository, navigate to **Settings → Secrets and variables → Actions** and add:
- `DATABASE_URL`: Your Neon PostgreSQL connection string
- `TELEGRAM_BOT_TOKEN`: Your Telegram Bot token
- `TELEGRAM_CHAT_ID`: Your Telegram Chat ID
- `PROXY_LIST` *(Optional)*: Comma-separated list of SOCKS5/HTTP proxies (`http://user:pass@ip:port` or `ip:port:user:pass`)

> Note: `.github/workflows/flight_tracker.yml` also starts a local Cloudflare WARP daemon (`SOCKS5_PROXY=socks5://127.0.0.1:40000`) with automatic direct fallback.

### 4. Local Setup & Verification
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
patchright install chrome

cp .env.example .env
# Edit .env with your DATABASE_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, APP_PASSWORD

# Initialize database schema
python -m src.runner --init-db-only

# Run a sharded batch check from CLI
python -m src.runner --max-slots 50

# Launch the Streamlit Control Center
streamlit run app.py

# Run functional & performance tests
python tests/test_dashboard_performance.py
```
