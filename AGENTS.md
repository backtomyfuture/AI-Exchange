## Cursor Cloud specific instructions

### Overview

AI Email Assistant — a FastAPI service orchestrated by LangGraph that classifies incoming Exchange emails, generates reply drafts via LLM, and sends interactive approval cards to Lark (飞书). See `CLAUDE.md` for full architecture details.

### Prerequisites (already installed in VM snapshot)

- Python 3.12 venv at `.venv/`
- System libs for WeasyPrint (`libpango`, `libpangoft2`, `libjpeg-dev`, `libopenjp2-7-dev`, `fonts-noto-cjk`)
- Docker with `fuse-overlayfs` storage driver and `iptables-legacy`

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

`langgraph-checkpoint-postgres` v3.0.4 uses `CREATE INDEX CONCURRENTLY` in migrations 6–8, which fails inside the default transaction block. Before the first app launch against a fresh database, run migrations with autocommit:

```python
import asyncio, psycopg
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

async def migrate():
    conn = await psycopg.AsyncConnection.connect(
        "postgresql://user:password@localhost:5432/email_agent", autocommit=True
    )
    cur = conn.cursor()
    for i, m in enumerate(AsyncPostgresSaver.MIGRATIONS):
        await cur.execute(m)
        if i > 0:
            await cur.execute("INSERT INTO checkpoint_migrations (v) VALUES (%s)", (i,))
    await conn.close()

asyncio.run(migrate())
```

Once the tables exist, subsequent `checkpointer.setup()` calls are no-ops.

### .env setup

Copy `.env.example` to `.env`. The only mandatory fix for tests: set `EXCHANGE_ACCOUNT_ID` to an integer (default `8`); the placeholder `your_account_id` causes a Pydantic validation error at import time.

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
