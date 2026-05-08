# historical-price-cron-archiver

A per-minute OHLCV, ticker, and orderbook archiver for crypto markets. Uses [CCXT](https://github.com/ccxt/ccxt) to fetch market data, [Celery](https://docs.celeryq.dev/) for task scheduling, and [QuestDB](https://questdb.io/) for time-series storage.

## Overview

Every minute, Celery Beat dispatches three task groups that fan out across every configured exchange × symbol pair:

| Task | Data collected |
|------|---------------|
| `fetch_ohlcv` | 1-minute OHLCV candle |
| `fetch_ticker` | Best bid/ask + last price snapshot |
| `fetch_orderbook` | Order book snapshot (configurable depth) |

Results are written to QuestDB via the native ILP (InfluxDB Line Protocol) HTTP ingestion path — the fastest write path QuestDB offers.

## Stack

- **Python 3.11+**
- **CCXT** — unified exchange API (Binance, Bybit, OKX, Kraken, and [many more](https://docs.ccxt.com/#/?id=exchanges))
- **Celery + Redis** — distributed task queue and beat scheduler
- **QuestDB** — high-performance time-series database
- **Docker Compose** — one-command deployment

## Quick Start

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env with your QuestDB host, Redis URL, and optional alert settings
```

### 2. Configure symbols

Edit `config/symbols.yaml` to choose which exchanges and trading pairs to archive:

```yaml
exchanges:
  binance:
    symbols:
      - BTC/USDT
      - ETH/USDT
  kraken:
    symbols:
      - BTC/USD
```

Any [CCXT-supported exchange ID](https://docs.ccxt.com/#/?id=exchanges) can be used as a key.

### 3. Start services

```bash
docker compose up -d
```

This starts:
- **QuestDB** — HTTP API + Web Console on port `9000`, ILP TCP on port `9009`
- **Redis** — on port `6379`
- **init** — one-shot container that creates the `ohlcv` table (WAL + DEDUP)
- **worker** — Celery worker (concurrency 4)
- **beat** — Celery Beat scheduler (fires every minute)

### 4. Check logs

```bash
docker compose logs -f worker
docker compose logs -f beat
```

### 5. Browse data

Open the QuestDB Web Console at **http://localhost:9000** to query your data interactively.

---

## Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        Docker Compose                        │
│                                                             │
│  ┌──────────┐   every minute   ┌──────────────────────────┐ │
│  │  Beat    │ ───dispatch────► │        Redis             │ │
│  │scheduler │                  │     (broker/backend)     │ │
│  └──────────┘                  └────────────┬─────────────┘ │
│                                             │ dequeue        │
│                                   ┌─────────▼─────────┐     │
│                                   │   Celery Worker   │     │
│                                   │  (concurrency 4)  │     │
│                                   └─────────┬─────────┘     │
│                                             │ ILP HTTP       │
│                                   ┌─────────▼─────────┐     │
│                                   │     QuestDB       │     │
│                                   │   (port 9000)     │     │
│                                   └───────────────────┘     │
└─────────────────────────────────────────────────────────────┘
```

### Task Fan-out Pattern

Each minute, Beat fires three **dispatcher** tasks. Each dispatcher reads `config/symbols.yaml` and spawns one **leaf task** per `(exchange, symbol)` pair. Dispatchers and leaf tasks are fully independent so slow or failing pairs never block others.

```
Beat (every 60 s)
 ├── dispatch_ohlcv
 │    ├── fetch_and_store_ohlcv("binance", "BTC/USDT")
 │    ├── fetch_and_store_ohlcv("binance", "ETH/USDT")
 │    └── ...
 ├── dispatch_ticker
 │    ├── fetch_and_store_ticker("binance", "BTC/USDT")
 │    └── ...
 └── dispatch_orderbook
      ├── fetch_and_store_orderbook("binance", "BTC/USDT")
      └── ...
```

### Fetch → Persist Flow (per leaf task)

1. **CCXT client** — fetches data from the exchange REST API via a shared, cached exchange instance.
2. **Retry** — up to 5 attempts with exponential backoff (1 s → 2 s → 4 s → 8 s → 16 s) using Tenacity.
3. **ILP write** — sends a single row to QuestDB via the HTTP ILP sender. Tables are auto-created on first write (except `ohlcv` which is pre-created with DEDUP).
4. **Failure logging** — on unrecoverable error, a line is appended to `logs/failures.log`. Once `ALERT_FAILURE_THRESHOLD` consecutive failures accumulate for a `(task, exchange, symbol)` triple, an SMTP alert email is sent and an `ALERTED` line is written to the log.

### Database Schema

| Table | Designated timestamp | Key columns | Notes |
|-------|---------------------|-------------|-------|
| `ohlcv` | `timestamp` | `exchange` (SYMBOL), `symbol` (SYMBOL), `timeframe` (SYMBOL), OHLCV doubles | WAL + DEDUP on `(timestamp, exchange, symbol, timeframe)` — duplicate candles silently dropped |
| `ticker` | `timestamp` | `exchange`, `symbol`, bid/ask/last/vwap/volume doubles | Auto-created by ILP on first write |
| `orderbook` | `timestamp` | `exchange`, `symbol`, `bids` (VARCHAR JSON), `asks` (VARCHAR JSON), `depth` | Bids/asks stored as `"[[price, amount], ...]"` JSON strings |

### Orderbook Storage

QuestDB has no JSONB type. Bids and asks are serialised to compact JSON strings:

```
bids  VARCHAR  →  "[[29500.0, 1.2], [29499.5, 0.8], ...]"
asks  VARCHAR  →  "[[29501.0, 0.5], [29502.0, 2.1], ...]"
```

### Reliability Design

| Concern | Solution |
|---------|----------|
| Transient exchange errors | Tenacity exponential backoff (5 attempts) |
| Worker crash mid-task | `task_acks_late=True` + `task_reject_on_worker_lost=True` — task re-queued automatically |
| Beat fires while last run is still in flight | Each task has `expires=55 s`; stale tasks are dropped rather than piling up |
| Duplicate OHLCV writes on retry | QuestDB WAL + `DEDUP UPSERT KEYS` — identical rows silently deduplicated |
| Silent failures | `logs/failures.log` + threshold-based email alerts |

---

## Configuration

### `config/symbols.yaml`

| Key | Description |
|-----|-------------|
| `settings.orderbook_depth` | Number of price levels to archive per side (default: `20`) |
| `settings.ohlcv_timeframe` | CCXT timeframe string (default: `"1m"`) |
| `exchanges.<id>.symbols` | List of `BASE/QUOTE` symbols for that exchange |

### `.env`

| Variable | Description |
|----------|-------------|
| `QUESTDB_HOST` | QuestDB hostname (default: `questdb`) |
| `QUESTDB_HTTP_PORT` | QuestDB HTTP API port (default: `9000`) |
| `QUESTDB_ILP_PORT` | QuestDB ILP ingestion port (default: `9000`) |
| `REDIS_URL` | Redis connection URL |
| `CELERY_BROKER_URL` | Celery broker (Redis) |
| `CELERY_RESULT_BACKEND` | Celery result backend (Redis) |
| `FAILURE_LOG_PATH` | Path to the failure log file (default: `logs/failures.log`) |
| `ALERT_EMAIL_ENABLED` | Enable SMTP failure alerts (`true`/`false`) |
| `SMTP_*` / `ALERT_*` | SMTP credentials and alert recipient |
| `ALERT_FAILURE_THRESHOLD` | Consecutive failures before an alert is sent (default: `3`) |
| `LOG_LEVEL` | Logging verbosity (default: `INFO`) |

---

## VPS Deployment (DigitalOcean)

This section covers deploying `worker` and `beat` to a DigitalOcean Droplet with automatic deploys triggered by pushing to `main`. QuestDB is assumed to be running on a separate host (e.g. QuestDB Cloud or another VPS).

### Infrastructure

| Service | Where |
|---------|-------|
| QuestDB | External host (QuestDB Cloud / separate VPS) |
| Redis | External host (Upstash or separate VPS) |
| Worker + Beat | App Droplet (`docker-compose.app.yml`) |

---

### Step 1 — Provision the Droplet

Create a **Basic Droplet** ($6/mo, 1 vCPU / 1 GB RAM, Ubuntu 24.04) in the DigitalOcean console. Add your SSH public key during creation.

---

### Step 2 — First-time VPS setup

SSH into the Droplet and run:

```bash
# Install Docker (includes Compose plugin)
curl -fsSL https://get.docker.com | sh

# Allow your user to run Docker without sudo (re-login after this)
usermod -aG docker $USER

# Verify
docker compose version
```

Clone the repo and create the `.env` file:

```bash
git clone https://github.com/<you>/<repo>.git /app
cd /app
cp .env.example .env
nano .env
```

Key values to set in `.env`:

```bash
QUESTDB_HOST=<your-questdb-host>   # QuestDB Cloud hostname or external IP
QUESTDB_HTTP_PORT=9000
QUESTDB_ILP_PORT=9000
CELERY_BROKER_URL=redis://<your-redis-host>:6379/0
CELERY_RESULT_BACKEND=redis://<your-redis-host>:6379/1
REDIS_URL=redis://<your-redis-host>:6379/0
```

Pull the pre-built image and boot the stack for the first time:

```bash
docker compose -f docker-compose.app.yml pull
docker compose -f docker-compose.app.yml up -d
```

This starts Redis, runs the `init` container to create the `ohlcv` table in QuestDB, then starts `worker` and `beat`.

---

### Step 3 — Configure GitHub Actions secrets

In your GitHub repo go to **Settings → Secrets and variables → Actions** and add:

| Secret | Value |
|--------|-------|
| `VPS_HOST` | Droplet public IP address |
| `VPS_USER` | `root` (or your sudo user) |
| `VPS_SSH_KEY` | Contents of your SSH private key (e.g. `~/.ssh/id_ed25519`) |

> The `.env` file lives on the VPS only and is never committed to the repo. All application secrets stay off GitHub.

---

### Step 4 — Configure the image name in `docker-compose.app.yml`

Replace `<OWNER>` and `<REPO>` in `docker-compose.app.yml` with your GitHub username and repo name:

```yaml
image: ghcr.io/<OWNER>/<REPO>:latest
```

For example: `ghcr.io/johndoe/historical-price-cron-archiver:latest`

---

### Step 5 — Deploy

Every push to `main` automatically:

1. Builds the Docker image on GitHub Actions (7 GB RAM — no OOM risk)
2. Pushes the image to GitHub Container Registry (free)
3. SSHs into the Droplet
4. Pulls the latest image (`docker pull`)
5. Force-recreates `worker` and `beat`
6. Prunes old images

To trigger a deploy:

```bash
git push origin main
```

To monitor the deploy, watch the **Actions** tab in GitHub. To check container status on the VPS:

```bash
docker compose -f docker-compose.app.yml ps
docker compose -f docker-compose.app.yml logs -f worker
docker compose -f docker-compose.app.yml logs -f beat
```

---

### Useful VPS commands

```bash
# Restart a specific service
docker compose -f docker-compose.app.yml restart worker

# View failure log
tail -f /app/logs/failures.log

# Pull and redeploy manually (same as CI does)
cd /app && git pull origin main && docker compose -f docker-compose.app.yml up -d --force-recreate worker beat
```

---

## Development

### Install dependencies

```bash
pip install poetry
poetry install
```

### Run locally (without Docker)

Start QuestDB and Redis manually or via Docker, then:

```bash
# Create ohlcv table
python scripts/init_db.py

# Start worker
celery -A src.celery_app worker --loglevel=INFO

# Start beat scheduler (separate terminal)
celery -A src.celery_app beat --loglevel=INFO
```

### Linting & formatting

```bash
ruff check src/
black src/
```

### Tests

```bash
pytest
```

---

## Project Structure

```
.
├── config/
│   └── symbols.yaml        # Exchange and symbol configuration
├── logs/                   # failures.log written here at runtime
├── migrations/
│   └── 001_initial.sql     # QuestDB DDL for ohlcv (WAL + DEDUP)
├── scripts/
│   └── init_db.py          # Creates ohlcv table via QuestDB HTTP API
├── src/
│   ├── celery_app.py       # Celery app + Beat schedule
│   ├── db/
│   │   └── connection.py   # QuestDB ILP Sender factory
│   ├── tasks/
│   │   ├── ohlcv.py        # OHLCV fetch + ILP write
│   │   ├── ticker.py       # Ticker fetch + ILP write
│   │   └── orderbook.py    # Orderbook fetch + ILP write
│   └── utils/
│       ├── alerting.py     # Log file failure tracking + SMTP alerts
│       ├── ccxt_client.py  # Cached CCXT exchange factory
│       ├── config_loader.py# symbols.yaml loader
│       └── retry.py        # Tenacity exponential backoff decorator
├── .github/
│   └── workflows/
│       └── deploy.yml      # Auto-deploy to VPS on push to main
├── docker-compose.yml      # Full local stack (includes QuestDB)
├── docker-compose.app.yml  # VPS stack (worker + beat + redis only)
├── Dockerfile
└── .env.example
```
