# ✈️ FlightPulse Matrix v5.0

**Self-Hosted, Zero-Cost Personal 50×10 N:M Google Flights Matrix Tracker with Multi-GL Competition (`MX, IT, DE`), RAM-Buffered Neon Sleep, 6-Shard Parallel Execution & External `cron-job.org` Triggering.**

---

## 🎯 Project Scope & Ideal Objectives

FlightPulse Matrix v5.0 is a personal, self-hosted flight price monitoring system designed to track multi-airport travel corridors (`N origins × M destinations`, e.g., `MEX, NLU ➔ MXP, CDG, AMS, MAD, FRA`) across up to **50 active destinations × 10 date combinations = 500 active `date_slots`** using real Google Flights data (zero simulated prices).

### Core Scope
- **N:M Multi-Airport Corridors & Protobuf `?tfs=` Encoding (`src/tfs_builder.py`):** Generates deterministic Google Flights search URLs supporting multiple origin/destination airports, metro code expansion (e.g., `TYO → NRT, HND`), trip types, max stops, allowed airlines, carry-on baggage, and departure/arrival hour windows.
- **Multi-GL Market Competition in RAM (`src/scraper.py`):** Competes each `date_slot` across its configured `gl_pool` (default `MX, IT, DE`) while reusing a single pooled Patchright Chrome instance, keeping only the lowest `effective_price = cheapest_price or best_price` (ties favor pool order, e.g., `MX` for MXN) and storing `winning_gl`, `last_winning_gl`, and `all_time_low_gl`.
- **330-Day Advance Schedule Intelligence (`src/config.py`, `src/db.py`):** Automatically detects whether each `date_slot` is inside Google Flights' active booking window (`<= 330 days`) or waiting for airline schedule release (`FUTURE_RELEASE`), avoiding wasted scrapes on unreleased dates.
- **Direct Cheapest + Best Dual-Tab Extraction (`src/scraper.py`, `src/ds1_parser.py`):** Direct landing on `tfu=EgoIABAAGAAgAigB` (`loaders == 0` settle + `route.abort()` + `HEADLESS_MODE=False`) capturing both **Cheapest (`Más baratos`)** and **Best (`Mejores`)** in one navigation.
- **RAM Buffer & Neon Serverless Sleep (`src/db.py`, `src/runner.py`):** Strict 3-phase execution — Phase 1 reads due slots + baselines under PostgreSQL advisory lock (`742918341`) and calls `close_db_connection()` (`[NEON_SLEEP]`); Phase 2 runs 100% in RAM with zero DB connections; Phase 3 reconnects once to batch-flush `price_snapshots`, `date_slots`, and `alert_logs`.
- **6-Shard Parallel Matrix & `cron-job.org` (`src/runner.py`, `.github/workflows/flight_tracker.yml`):** External hourly trigger via `cron-job.org` (`workflow_dispatch`, default `60` min cadence with `5` min due grace) fans out deterministically across `6` parallel GitHub Actions shards (`assign_destinations_to_shard`).
- **Zero-Cost Network Priority & Secret-Safe Logging (`src/config.py`, `src/runner.py`):** Routes strictly in order **Nivel 1: Cloudflare WARP (`SOCKS5_PROXY`) ➔ Nivel 2: Direct (`None`) ➔ Nivel 3: `PROXY_LIST` (Webshare)** with interleaved multi-account rotation, per-account circuit breaker (`402`/`407`/3x tunnel failures), and strict proxy/credential redaction (`redact_proxy_for_log`) in all logs.
- **High-Signal Compact HTML Alerts (`src/alerts.py`):** Evaluates strictly **Meta** (`TARGET_BREACH`, initial breach, re-cross, or >=3% extra drop) and **Bajo Histórico** (`ALL_TIME_LOW`, >=2% drop vs DB reference) without cooldown delays, excludes `FUTURE_RELEASE` (>330d) slots, and renders clean `[Reservar]` links dynamically pointing to `gl=winning_gl`.
- **Executive Control Center (`app.py`):** Password-protected Streamlit dashboard with `POS` traceability, dynamic `winning_gl` booking links, Multi-GL `gl_pool` multiselect, Plotly multi-date & dual-tab evolution charts, live on-demand scanner, proxy diagnostics, and Telegram alert tester.

### Ideal Objectives
1. **$0/Month Sustainable Infrastructure:** Operate 24/7 on free-tier infrastructure (GitHub Actions + Neon Serverless PostgreSQL + Telegram Bot API + Streamlit) without exhausting free proxy bandwidth quotas or CI limits before the end of each billing cycle.
2. **Anti-Ban & Proxy Resilience:** Distribute scraping load safely across available network paths (proxies, Cloudflare WARP, or direct/lightweight HTTP SSR) while keeping per-slot data transfer minimal and avoiding Google rate-limits/CAPTCHAs.
3. **High-Signal, Low-Noise Telegram Alerts:** Deliver ultra-concise, scannable Telegram notifications strictly for high-value events (Target Price & Historical Low), with direct booking links and zero visual clutter.
4. **Full Matrix Utilization:** Support filling the full **20×10 (200 slots)** capacity with predictable, sustainable check frequencies.

---

## 🏛️ Core Architecture (Matrix 50×10 / 6-Shard Engine)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                     STREAMLIT CONTROL CENTER (app.py)                        │
│  • Tab 1: 10-Date Matrix Comparison (POS + winning_gl link) & Plotly Charts  │
│  • Tab 2: N:M Corridor Builder, Multi-GL Pool (MX,IT,DE) & Matrix Generator  │
│  • Tab 3: Live Multi-GL Scanner, Scraper Health, Logs & Telegram Tester      │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                NEON SERVERLESS POSTGRESQL (src/db.py, psycopg v3)            │
│   destinations (≤50) ──► date_slots (≤500) ──► price_snapshots & alert_logs  │
│   • Advisory-locked init_db() + Phase 1 read / Phase 3 batch flush           │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│         6-SHARD RAM-BUFFERED DISPATCHER (cron-job.org ➔ src/runner.py)       │
│  • workflow_dispatch fans out 6 shards (assign_destinations_to_shard)        │
│  • Phase 1: Single read + close_db_connection() ([NEON_SLEEP])               │
│  • Phase 2: 100% RAM scraping via WARP ➔ Direct ➔ Webshare (0 DB calls)      │
│  • Phase 3: Atomic batch flush + Consolidated Telegram digests               │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│           MULTI-GL DIRECT EXTRACTION ENGINE (src/scraper.py)                 │
│  • Competes gl_pool (MX, IT, DE) on pooled Patchright Chrome (HEADLESS=False)│
│  • Direct tfu=EgoIABAAGAAgAigB + 3 visual loaders == 0 + route.abort()       │
│  • Keeps lowest effective_price (ties favor pool order) + winning_gl         │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 📂 Repository Structure

```text
.
├── .github/
│   └── workflows/
│       └── flight_tracker.yml         # 6-shard GitHub Actions scanner (cron-job.org dispatch, Xvfb + WARP)
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

# Run a sharded batch check from CLI (e.g., shard 0 of 6)
python -m src.runner --shard-index 0 --total-shards 6 --max-slots 500

# Launch the Streamlit Control Center
streamlit run app.py

# Run functional & performance tests
python tests/test_dashboard_performance.py
```
