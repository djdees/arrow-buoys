# arrow-buoys — Straits of Florida (NDBC → Parquet → Arrow IPC → Polars)

End-to-end pipeline for NOAA National Data Buoy Center (NDBC) realtime data:

```
NDBC realtime2/*.txt ──► go-ingest ──► data/*.parquet
data/*.parquet        ──► go-source ──► :8080/stream  (Arrow IPC over HTTP)
.../stream            ──► py-receiver ──► out/*.png   (Matplotlib charts)
```

## Repo Layout

```
.
├─ docker-compose.yml
├─ Makefile               # Top-level compose lifecycle
├─ .env.example           # Copy to .env and edit
├─ data/                  # Parquet files (gitignored; volume mount)
├─ out/                   # Charts (gitignored; volume mount)
├─ go-ingest/             # NDBC txt → Parquet writer
│  ├─ main.go
│  ├─ go.mod
│  ├─ Dockerfile
│  └─ Makefile
├─ go-source/             # Parquet → Arrow IPC HTTP server
│  ├─ main.go
│  ├─ go.mod
│  ├─ Dockerfile
│  └─ Makefile
└─ py-receiver/           # Arrow IPC → Polars → Matplotlib
   ├─ main.py
   ├─ pyproject.toml
   ├─ Dockerfile
   └─ Makefile
```

## Quick Start

```bash
# 1. Clone and configure
cp .env.example .env
# Optionally edit STATIONS, REFRESH_MINUTES, ARROW_PORT in .env

# 2. Build images and start the stack
make build
make up

# 3. Watch logs
make tail

# Charts appear in ./out/ once py-receiver completes.
# Parquet files appear in ./data/ after go-ingest runs.
```

### One-Shot Ingest (no daemon)

```bash
make once        # Runs go-ingest with REFRESH_MINUTES=0, then exits
```

### Stop / Clean

```bash
make down        # Stop containers
make clean       # Remove containers + locally-built images (keeps data/out)
make prune       # Remove images + volumes (keeps external network)
```

## Make Targets (Top Level)

| Target      | Description                                              |
|-------------|----------------------------------------------------------|
| `build`     | Build all service images (multi-stage Chainguard)        |
| `up`        | Start all services in detached mode                      |
| `down`      | Stop and remove containers                               |
| `restart`   | Restart all running services                             |
| `ps`        | Show container status                                    |
| `logs`      | Print all logs (no follow)                               |
| `tail`      | Follow all logs (last 200 lines)                         |
| `once`      | One-shot ingest run (`REFRESH_MINUTES=0`)                |
| `network`   | Ensure external Docker network exists                    |
| `env`       | Print current env variables                              |
| `clean`     | Remove containers + local images (keeps data/out)        |
| `prune`     | Full cleanup: images, volumes, orphans                   |
| `help`      | Show colorized help                                      |

## Environment Configuration

Copy `.env.example` to `.env` and adjust:

```dotenv
PROJECT=arrow-buoys
NET_NAME=arrow-buoys-net
DATA_DIR=./data
OUT_DIR=./out
STATIONS=SANF1,SMKF1,LONF1,VAKF1,KYWF1
REFRESH_MINUTES=60
ARROW_PORT=8080
```

| Variable          | Description                                                         |
|-------------------|---------------------------------------------------------------------|
| `PROJECT`         | Docker Compose project name (used with `-p`)                        |
| `NET_NAME`        | External Docker network name (shared across stacks)                 |
| `DATA_DIR`        | Host path for Parquet files (mounted to `/data` in containers)      |
| `OUT_DIR`         | Host path for chart output (mounted to `/app/out` in containers)    |
| `STATIONS`        | Comma-separated NDBC station IDs                                    |
| `REFRESH_MINUTES` | Poll interval for go-ingest (`0` = one-shot, exit after first run)  |
| `ARROW_PORT`      | HTTP port for go-source Arrow IPC endpoint                          |

### Default Stations (Straits of Florida)

| ID      | Location                        |
|---------|---------------------------------|
| SANF1   | Sand Key, FL                    |
| SMKF1   | Sombrero Key, FL                |
| LONF1   | Long Key, FL                    |
| VAKF1   | Vaca Key, FL                    |
| KYWF1   | Key West, FL                    |

Find more station IDs at [ndbc.noaa.gov](https://www.ndbc.noaa.gov/).

## Data Flow Details

### go-ingest
- Fetches `https://www.ndbc.noaa.gov/data/realtime2/<STATION>.txt`
- Dynamically parses the `#YY/YYYY MM DD hh mm …` header
- Filters sentinel values: `99`, `999`, `9999` → stored as `null`
- Writes one Parquet per station: `data/<STATION>_latest.parquet`
- Atomic write: `.tmp` → rename (safe for concurrent readers)
- Env: `STATIONS`, `DATA_DIR`, `REFRESH_MINUTES`

### go-source
- Globs `data/*_latest.parquet` on each `/stream` request
- Converts rows to Apache Arrow record batches
- Streams Arrow IPC format via `GET /stream`
- Also exposes `GET /healthz` for liveness checks
- Env: `DATA_DIR`, `ARROW_PORT`

### py-receiver
- Fetches `/stream` with retry logic (waits for go-source readiness)
- Reads all Arrow IPC batches into a PyArrow table
- Converts to Polars DataFrame; parses epoch timestamps
- Writes charts to `OUT_DIR`:
  - `winds_line.png` — wind speed timeseries per station
  - `wtmp_bar.png` — latest sea surface temperature bar chart
- Env: `SOURCE_URL`, `OUT_DIR`

## Python (uv) — Local Dev Workflow

```bash
cd py-receiver

# Install runtime deps
make install

# Install dev deps (black, isort, ruff, mypy, pytest via uvx)
make install-dev

# Run against live go-source (adjust SOURCE_URL if needed)
make run SOURCE_URL=http://localhost:8080/stream OUT_DIR=./out

# Format, lint, type-check, test
make fmt
make lint
make type
make test

# Clean caches
make clean

# Nuclear option: remove .venv entirely
make clean-build NUKE=1
```

### Python FAQ / Tips (uv)

**Q: Where is the virtualenv?**  
`.venv/` inside `py-receiver/`. Activate manually with `. .venv/bin/activate` or let `uv run` handle it.

**Q: Do I need ruff/black/mypy installed globally?**  
No — the Makefile uses `uvx` to run tools ephemerally. No global installs needed.

**Q: How do I change the Python version?**  
Edit the `UV_ARGS` variable in `py-receiver/Makefile` or set `requires-python` in `pyproject.toml`.

**Q: How do I pin/export dependencies?**  
```bash
make freeze   # writes requirements.txt from pyproject.toml
```

**Q: Charts look wrong / empty?**  
1. Check logs: `make tail`
2. Ensure ingest completed: `make once` then re-run py-receiver
3. Validate the Arrow stream manually:
   ```bash
   curl -s http://localhost:8080/stream > /tmp/buoys.arrow
   python3 - <<'PY'
   import pyarrow as pa
   r = pa.ipc.open_stream(open("/tmp/buoys.arrow", "rb"))
   t = r.read_all()
   print(t.schema)
   print(t.to_pandas().head())
   PY
   ```
4. Inspect Parquet directly:
   ```bash
   python3 -c "import pyarrow.parquet as pq; print(pq.read_table('data/SANF1_latest.parquet').to_pandas().head())"
   ```

## Security Notes

- All containers run **non-root** (UID 65532, Chainguard defaults).
- Go binaries are statically linked (`CGO_ENABLED=0`) for the `cgr.dev/chainguard/static` runtime.
- No shell in runtime images; minimal attack surface.
- Parquet writes are atomic (`.tmp` → `os.Rename`) to avoid torn reads.

## Extensibility Ideas

- **Partitioned Parquet**: Write `data/<STATION>/date=YYYY-MM-DD/` partitions for time-range queries.
- **More NDBC data types**: Add spectral wave (`.spec`), raw (`*.data_spec`), or directional wave data.
- **Arrow Flight**: Replace HTTP IPC with `arrow-flight` for bidirectional streaming and authentication.
- **FastAPI dashboard**: Serve charts via FastAPI with auto-refresh instead of static PNGs.
- **DuckDB**: Replace Polars with DuckDB for SQL queries directly over Parquet.
- **Multi-stack**: Share `arrow-buoys-net` with other compose stacks for federated data pipelines.

---

> Built with [Chainguard](https://chainguard.dev) base images for minimal, non-root container runtime.
