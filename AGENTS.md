## Cursor Cloud specific instructions

### Overview

AI Email Assistant — a FastAPI service orchestrated by LangGraph that classifies incoming Exchange emails, generates reply drafts via LLM, and sends interactive approval cards to Lark (飞书). See `CLAUDE.md` for full architecture details.

### Prerequisites (already installed in VM snapshot)

- Python 3.12 venv at `.venv/`
- System libs for WeasyPrint (`libpango`, `libpangoft2`, `libjpeg-dev`, `libopenjp2-7-dev`, `fonts-noto-cjk`)
- `libpq-dev` (required for `psycopg` pure-python implementation; without it, tests importing `psycopg` fail with `no pq wrapper available`)
- Docker with `fuse-overlayfs` storage driver and `iptables-legacy`
- `ruff` is installed inside `.venv/` (not in `requirements.txt`; the update script installs it)

### Running infrastructure services

```bash
# Ensure Docker daemon is running
sudo dockerd &>/tmp/dockerd.log &
sleep 3
sudo chmod 666 /var/run/docker.sock

# Start PostgreSQL + Qdrant
docker compose up -d qdrant postgres
```

### LangGraph checkpoint migration gotcha

`langgraph-checkpoint-postgres` v3.0.4 uses `CREATE INDEX CONCURRENTLY` in migrations 6–8. The explicit bootstrap command handles the required autocommit behavior. Bootstrap must use the isolated migration-owner Docker secret, never the runtime DSN. Do not execute this command until the Task 1B0-B catalog role verifier and the separate DBA ownership-transfer checkpoint have passed:

```bash
docker compose --profile migration run --rm database-bootstrap
```

Once the tables exist, subsequent `checkpointer.setup()` calls are no-ops.

### Environment setup

Local Python and production Compose now share one minimal `.env`. Copy `.env.example` to `.env`, fill only its 17 integration/model values, then run `.venv/bin/python scripts/configure_deployment.py`; database identities, security tokens, DSNs, limits, and advanced deployment state are generated under ignored `secrets/`. Compose still injects an explicit runtime allowlist, and migration/maintenance credentials never enter the application container. Use `PYTHON_DOTENV_DISABLED=1` when a test must be isolated from the local deployment configuration.

### Common commands

| Task | Command |
|------|---------|
| Run tests | `.venv/bin/python -m pytest -q` |
| Lint | `.venv/bin/ruff check src/ tests/` |
| Start app | `source .venv/bin/activate && python -m src.main` |
| Health check | `curl http://localhost:8000/health` |

### Notes

- Lark WS will fail to connect with placeholder credentials — this is expected and does not block the health check or webhook processing.
- Exchange API calls will fail without valid `EXCHANGE_API_URL`/`EXCHANGE_API_KEY` — also expected in dev; the folder-cache init is wrapped in a try/except.
- Tests are heavily mocked and do **not** require running Docker services.
- `Settings` uses `SecretStr` for sensitive fields; access via `resolve_secret()` helper for mock compatibility.
- `ExchangeClient` uses a shared `httpx.AsyncClient` with connection pooling — call `close()` on shutdown.
- `rate_limiter.py` defers `get_settings()` to first `acquire()` — no module-level side effects.
