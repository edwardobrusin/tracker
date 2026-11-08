# ✈️ FlightPulse Matrix v4.0

**Self-Hosted, $0-Cost Personal 10×10 Google Flights Matrix Tracker with Dual-Tab Extraction (`🔥 Cheapest` vs. `⭐ Best`) & Consolidated Telegram Digest Alerts.**

---

## 🏛️ Core Architecture (Matrix 10×10 Engine)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                     STREAMLIT CONTROL CENTER (app.py)                        │
│  • 10×10 Matrix Comparison Table & Plotly Multi-Date Evolution Charts        │
│  • Protobuf ?tfs= 10-Date Window Generator, Custom Picker & Batch URL Paste  │
│  • Live On-Demand Scanner & Consolidated Telegram Digest Tester              │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                NEON SERVERLESS POSTGRESQL (src/db.py, psycopg v3)            │
│    destinations (≤10) ──► date_slots (≤100) ──► price_snapshots & alert_logs │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│             SHARDED ANTI-BAN QUEUE DISPATCHER (src/runner.py)                │
│  • GitHub Actions Cron (*/25 * * * *) selects ≤20–25 due date_slots/run      │
│  • Orders by `last_checked_at ASC NULLS FIRST` (zero burst risk)             │
│  • Shards destinations across Cloudflare WARP / PROXY_LIST / Direct fallback │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│             TWO-TIER HYBRID EXTRACTION ENGINE (src/scraper.py)               │
│  • Pooled Patchright Chrome (`headless=False` under `xvfb-run`) per shard    │
│  • Clears cookies between destinations + re-injects `SOCS` consent cookie    │
│  • Blocks `image`, `media`, `font` & Google Analytics/DoubleClick (<100 KB)  │
│  • Layer 1: SSR `AF_initDataCallback(ds:1)` + `GetShoppingResults` RPC       │
│  • Layer 2: Best Tab (`Mejores`) + clicks Cheapest Tab (`Más baratos`) DOM   │
│  • Layer 3: `curl_cffi` Chrome TLS-impersonated SSR + `fast-flights`         │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│             CONSOLIDATED DESTINATION DIGEST ALERTS (src/alerts.py)           │
│  • Evaluates Target Breach, All-Time Low, and Sharp Drop (-10% vs 7d avg)    │
│  • Enforces per-slot cooldown unless new price drops ≥5% below last alert    │
│  • Sends ONE grouped Telegram card per destination sorted by Cheapest ASC    │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 📂 Repository Structure

```text
.
├── .github/
│   └── workflows/
│       └── flight_tracker.yml   # Every 25m sharded runner with Xvfb + Cloudflare WARP
├── .streamlit/
│   └── config.toml              # Executive dark-mode theme configuration
├── src/
│   ├── __init__.py
│   ├── config.py                # Env/Secrets loader, SOCS cookie, telemetry blocklists
│   ├── db.py                    # Neon PostgreSQL v3 schema & CRUD/analytics layer
│   ├── tfs_builder.py           # Pure-Python Protobuf ?tfs= encoder, decoder & 10-date matrix generator
│   ├── ds1_parser.py            # AF_initDataCallback(ds:1), RPC & ARIA/DOM card parser
│   ├── scraper.py               # Pooled Patchright browser + curl_cffi SSR fallback
│   ├── alerts.py                # Consolidated per-destination Telegram digest engine
│   └── runner.py                # Sharded anti-ban queue dispatcher CLI
├── app.py                       # Streamlit Executive Control Center
├── requirements.txt
├── .env.example
└── README.md
```

---

## 🚀 Zero-Cost Deployment Guide ($0/month)

### 1. Create a Free Neon Serverless PostgreSQL Database
1. Sign up at [Neon.tech](https://neon.tech) (Free Tier).
2. Copy your connection string (`postgresql://user:pass@ep-....aws.neon.tech/neondb?sslmode=require`).
3. Put it in `.env` as `DATABASE_URL`. Tables are auto-created on first run (`init_db()`).

### 2. Create Your Telegram Alert Bot
1. Message `@BotFather` on Telegram and run `/newbot` to obtain `TELEGRAM_BOT_TOKEN`.
2. Start a conversation with your new bot and get your numeric `TELEGRAM_CHAT_ID` (via `@userinfobot` or `https://api.telegram.org/bot<TOKEN>/getUpdates`).

### 3. Configure GitHub Actions Secrets
In your GitHub repository, navigate to **Settings → Secrets and variables → Actions** and add:
- `DATABASE_URL`: Your Neon PostgreSQL connection string
- `TELEGRAM_BOT_TOKEN`: Your Telegram Bot token
- `TELEGRAM_CHAT_ID`: Your Telegram Chat ID
- `PROXY_LIST` *(Optional)*: Comma-separated list of SOCKS5/HTTP proxies (falls back to Cloudflare WARP and direct connection automatically)

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
python -m src.runner --max-slots 20

# Launch the Streamlit Control Center
streamlit run app.py
```
