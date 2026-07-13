# AI-Exchange Phase 5 Security and Data Governance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将飞书身份、邮件预览、HTML/PDF/附件、外部模型、日志、密钥和容器网络全部收敛到默认拒绝、可审计且可自动验证的生产安全边界。

**Architecture:** 所有入口先完成身份、受众、状态和策略检查，再加载最小必要内容。预览服务以独立 Origin 和一次性令牌换取短会话；不可信邮件内容经过清洗、资源阻断和结构化模型边界。生产配置、出站目标、日志字段与容器权限由集中策略在启动和 CI 中共同验证。

**Tech Stack:** Python 3.12、FastAPI、Pydantic 2、PyJWT、nh3、Pillow、filetype、WeasyPrint、structlog、Docker Compose、pytest。

## Global Constraints

- Phase 1 的最小安全措施继续生效；本阶段只收紧边界，不恢复公开 `/email`、公开 `/metrics`、TLS 降级或默认凭据。
- 预览身份必须来自飞书 OAuth/SSO，或由一次性令牌换取的 HttpOnly 会话；URL 中的 `open_id` 不是身份。
- 一次性令牌必须包含 `aud`、`sub`、`nonce`、`exp`，消费后原子失效，并重定向到不含令牌的 URL。
- 邮件 HTML、附件和模型输入全部视为不可信数据；模型无发送、任意文件或内部网络权限。
- 外部模型必须同时满足账户开关、供应商、模型、地域、用途、保留和数据类别策略。
- 生产日志不得记录正文、附件、完整主题、完整地址、请求头、签名、密钥、Token、完整卡片或模型原文。
- PostgreSQL、Qdrant 和 Metrics 不得向公网或 `0.0.0.0` 暴露。
- 生产容器必须非 root、只读根文件系统、丢弃 Linux capabilities，并使用显式 CPU/内存/PID 限制。
- 所有安全拒绝都记录脱敏审计元数据；拒绝路径不得回显策略细节或敏感依赖错误。

---

## File Map

| Area | Files | Responsibility |
|---|---|---|
| Production policy | `src/security/settings.py`, `src/security/egress.py`, `src/security/service_auth.py` | Reject unsafe secrets/TLS/targets; authenticate Webhook, Metrics and administrative APIs |
| Identity | `src/security/auth.py`, `src/approval/service.py`, `src/commands/handlers.py` | Resource-scoped Lark RBAC, card audience/version/expiry, delegated-admin audit |
| Preview | `src/security/preview.py`, `src/api/preview.py` | Single-use token exchange, short server session, isolated Origin |
| Untrusted content | `src/security/html.py`, `src/security/pdf.py`, `src/security/attachments.py` | HTML allowlist, PDF resource isolation, attachment validation/quarantine |
| Model governance | `src/llm/policy.py`, `src/llm/minimizer.py`, `src/llm/gateway.py` | Account/provider/region/data policy and PII minimization before model calls |
| Confidentiality | `src/security/redaction.py`, `src/utils/logging_setup.py` | Allowlist-first logs and safe public errors |
| Data lifecycle | `src/maintenance/retention.py` | Configurable retention, holds, dry-run, bounded/resumable cleanup and projection deletion |
| Deployment | `Dockerfile`, `docker-compose.yml`, `docker-compose.dev.yml` | Non-root read-only runtime and internal data networks |
| Evidence | `tests/security/`, `tests/unit/security/`, `tests/integration/security/`, `docs/security/` | Negative security regression suite and threat/data-flow documentation |

### Task 1: Enforce Production Secrets, TLS, and Outbound Destination Policy

**Files:**
- Modify: `src/security/__init__.py`
- Create: `src/security/settings.py`
- Create: `src/security/egress.py`
- Create: `tests/unit/security/test_settings.py`
- Create: `tests/unit/security/test_egress.py`
- Create: `tests/unit/security/conftest.py`
- Create: `tests/integration/security/conftest.py`
- Modify: `src/config.py`
- Modify: `src/init_app.py`
- Modify: `.env.example`

**Interfaces:**
- Consumes: `Settings`, `resolve_secret()` and Phase 1 production-mode validation
- Produces: `validate_production_settings(settings: Settings) -> None`; `EgressPolicy.require_allowed(url: str, service: str) -> None`

- [ ] **Step 1: Write failing production-setting and egress tests**

```python
from types import SimpleNamespace

import pytest

from src.security.egress import EgressPolicy, EgressRejected
from src.security.settings import UnsafeProductionConfiguration, validate_production_settings


def test_production_rejects_defaults_and_disabled_exchange_tls():
    settings = SimpleNamespace(
        APP_ENV="production",
        POSTGRES_PASSWORD="password",
        EXCHANGE_API_KEY="your_api_key",
        EXCHANGE_WEBHOOK_SECRET="",
        PREVIEW_TOKEN_SECRET="change-me",
        EXCHANGE_SSL_VERIFY=False,
        EXCHANGE_CA_FILE="",
    )
    with pytest.raises(UnsafeProductionConfiguration) as error:
        validate_production_settings(settings)
    assert set(error.value.fields) == {
        "POSTGRES_PASSWORD",
        "EXCHANGE_API_KEY",
        "EXCHANGE_WEBHOOK_SECRET",
        "PREVIEW_TOKEN_SECRET",
        "EXCHANGE_SSL_VERIFY",
    }


@pytest.mark.parametrize(
    ("url", "service"),
    [
        ("http://127.0.0.1:6333", "model"),
        ("https://169.254.169.254/latest/meta-data", "exchange"),
        ("https://unlisted.example/v1", "lark"),
    ],
)
def test_egress_policy_rejects_private_and_unlisted_targets(url, service):
    policy = EgressPolicy(
        allowed_hosts={
            "exchange": frozenset({"exchange.corp.example"}),
            "model": frozenset({"api.openai.com"}),
            "lark": frozenset({"open.feishu.cn"}),
        }
    )
    with pytest.raises(EgressRejected):
        policy.require_allowed(url, service)
```

- [ ] **Step 2: Run tests and confirm the new security modules are missing**

Run: `.venv/bin/python -m pytest tests/unit/security/test_settings.py tests/unit/security/test_egress.py -q`

Expected: collection fails with `ModuleNotFoundError: No module named 'src.security'`.

- [ ] **Step 3: Implement exact startup and destination rules**

Add `APP_ENV`, `EXCHANGE_CA_FILE`, `PREVIEW_TOKEN_SECRET`, `EXCHANGE_ALLOWED_HOSTS`, `MODEL_ALLOWED_HOSTS`, and `LARK_ALLOWED_HOSTS` to `Settings`. `validate_production_settings()` accumulates the exact field names whose values are empty, sample values (`password`, `change-me`, `your_*`) or unsafe; production requires `EXCHANGE_SSL_VERIFY=True`. `EgressPolicy` accepts only `https`, rejects credentials/fragments/non-default malformed ports, resolves every address, rejects loopback/link-local/private/multicast/reserved IPs, and compares the normalized hostname to the service allowlist before a client is built.

```python
class UnsafeProductionConfiguration(RuntimeError):
    def __init__(self, fields: set[str]) -> None:
        self.fields = tuple(sorted(fields))
        super().__init__("unsafe production configuration: " + ", ".join(self.fields))


class EgressRejected(RuntimeError):
    pass
```

Call `validate_production_settings()` before database, Lark, Exchange, model or Qdrant clients are initialized. Pass `EXCHANGE_CA_FILE or True` to httpx when production TLS verification is enabled; never catch certificate errors to retry with `False`.

`tests/unit/security/conftest.py` defines deterministic `settings`, `auth`, `audit_repo`, `token_service`, preview claims, Lark principals and fake external adapters. `tests/integration/security/conftest.py` builds the FastAPI `app` with migrated PostgreSQL, `signed_webhook_headers(raw_body)`, admin/monitor principals and `valid_preview_token`; every fixture uses sentinel secrets and closes resources in `finally`.

- [ ] **Step 4: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/security/test_settings.py tests/unit/security/test_egress.py tests/unit/test_exchange_ssl.py tests/unit/test_config.py -q
.venv/bin/ruff check src/security src/config.py src/init_app.py tests/unit/security
git add src/security/__init__.py src/security/settings.py src/security/egress.py src/config.py src/init_app.py .env.example tests/unit/security/conftest.py tests/integration/security/conftest.py tests/unit/security/test_settings.py tests/unit/security/test_egress.py tests/unit/test_exchange_ssl.py tests/unit/test_config.py
git commit -m "feat: fail closed on secrets tls and outbound policy"
```

Expected: all listed tests pass and no TLS fallback remains in `rg -n 'verify\s*=\s*False' src`.

---

### Task 2: Authenticate Webhook, Metrics, and Administrative Interfaces

**Files:**
- Create: `src/security/service_auth.py`
- Create: `tests/unit/security/test_service_auth.py`
- Create: `tests/integration/security/test_admin_routes.py`
- Modify: `src/server.py`
- Modify: `src/ingestion/normalization.py`
- Modify: `src/config.py`
- Modify: `.env.example`

**Interfaces:**
- Consumes: Phase 1 raw-body request limit and Phase 2 `IngressService.accept(*, raw_body: bytes, payload: Mapping[str, Any], header_event: str | None) -> IngressReceipt`
- Produces: `verify_webhook_signature(raw_body: bytes, supplied: str, secret: SecretStr) -> None`; `require_monitor_principal(request) -> MonitorPrincipal`; `require_admin_principal(request) -> AdminPrincipal`

- [ ] **Step 1: Write failing signature, replay, and administration tests**

```python
import hashlib
import hmac

import pytest
from httpx import ASGITransport, AsyncClient

from src.security.service_auth import WebhookSignatureRejected, verify_webhook_signature


def test_webhook_signature_is_checked_over_original_bytes():
    secret = "test-secret"
    original = b'{"email_id":"m1", "event":"create"}'
    normalized = b'{"email_id":"m1","event":"create"}'
    signature = hmac.new(secret.encode(), original, hashlib.sha256).hexdigest()
    verify_webhook_signature(original, signature, secret)
    with pytest.raises(WebhookSignatureRejected):
        verify_webhook_signature(normalized, signature, secret)


@pytest.mark.asyncio
async def test_valid_replay_returns_same_202_receipt(app, signed_webhook_headers):
    body = b'{"email_id":"m1","event":"create"}'
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        first = await client.post("/webhook/exchange", content=body, headers=signed_webhook_headers(body))
        second = await client.post("/webhook/exchange", content=body, headers=signed_webhook_headers(body))
    assert first.status_code == second.status_code == 202
    assert first.json()["inbox_id"] == second.json()["inbox_id"]


def test_unsigned_header_timestamp_cannot_override_signed_body_time(
    signed_body, signed_headers
):
    signed_headers["X-Webhook-Timestamp"] = "2099-01-01T00:00:00Z"
    event = verify_and_parse_webhook(signed_body, signed_headers)
    assert event.source_event_at == event.signed_body_timestamp


@pytest.mark.asyncio
async def test_recovery_routes_require_admin_identity_and_idempotency_key(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/admin/dead-letters/dead-1/recover")
    assert response.status_code == 401
    assert response.json()["error"] == "authentication_required"
```

- [ ] **Step 2: Implement raw-byte HMAC and strict request shape**

Accept only POST and `application/json`; read at most the Phase 1 body limit, validate a 64-character lowercase hex signature using `hmac.compare_digest`, and parse JSON only after verification. The extension inserts `timestamp` into the event JSON before HMACing the complete raw body, so the validated **body** timestamp is signed and may remain Task 2's trusted `source_event_at`/dedupe fact. `X-Webhook-Timestamp` is generated separately and is not covered by that HMAC; it is informational only, cannot override the body or authorize freshness, and a mismatch is a bounded audit signal. Valid replay is accepted through the durable Inbox dedupe key and returns the original receipt; malformed/invalid signatures never reach JSON parsing or `IngressService`. A future anti-replay contract may version a signature over an explicit timestamp/body/nonce tuple and enforce a freshness window, but it must not misclassify today's signed body timestamp as unsigned.

- [ ] **Step 3: Protect observability and management routes**

`/metrics` accepts either an internal proxy principal or a bearer token compared in constant time. Sync trigger, cold-start approval, retry, dead-letter recovery, card invalidation, manual send resolution, cutover and cleanup endpoints require an `AdminPrincipal`, an `Idempotency-Key`, a reason, resource scope and an audit insert in the same transaction as the command. Authentication failures return generic 401/403 and never reveal resource existence.

- [ ] **Step 4: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/security/test_service_auth.py tests/integration/security/test_admin_routes.py tests/unit/test_exchange_webhook.py tests/unit/test_metrics_auth.py -q
git add src/security/service_auth.py src/server.py src/ingestion/normalization.py src/config.py .env.example tests/unit/security/test_service_auth.py tests/integration/security/test_admin_routes.py tests/unit/test_exchange_webhook.py tests/unit/test_metrics_auth.py
git commit -m "feat: authenticate webhook metrics and admin routes"
```

Expected: byte-normalized signature substitution fails, valid replay returns one Inbox receipt, and every management mutation is authenticated, idempotent and audited.

---

### Task 3: Complete Lark RBAC and Card-bound Authorization

**Files:**
- Modify: `src/security/auth.py`
- Create: `tests/unit/security/test_lark_authorization.py`
- Create: `tests/integration/security/test_admin_delegation_audit.py`
- Modify: `src/approval/models.py`
- Modify: `src/approval/service.py`
- Modify: `src/commands/handlers.py`
- Modify: `src/utils/lark_ws.py`
- Modify: `src/config.py`
- Modify: `.env.example`

**Interfaces:**
- Consumes: immutable `ApprovalCommand`, approval version, actor `open_id`, card expiry and Phase 3 approval CAS
- Produces: `LarkPrincipal`, `AuthorizationService.authorize(action, principal, resource) -> AuthorizationDecision`

- [ ] **Step 1: Write failing owner, group, expiry, and administrator tests**

```python
from datetime import UTC, datetime, timedelta

import pytest

from src.security.auth import AuthorizationDenied, AuthorizationService, LarkPrincipal, ProtectedResource


def test_user_cannot_open_or_approve_another_users_mail():
    auth = AuthorizationService(admin_ids=frozenset(), group_memberships={})
    principal = LarkPrincipal(open_id="ou-attacker", group_ids=frozenset())
    resource = ProtectedResource(
        owner_open_id="ou-owner",
        allowed_approver_ids=frozenset({"ou-owner"}),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    with pytest.raises(AuthorizationDenied):
        auth.authorize("approve", principal, resource, now=datetime.now(UTC))


def test_expired_card_is_denied_even_for_owner():
    now = datetime.now(UTC)
    auth = AuthorizationService(admin_ids=frozenset(), group_memberships={})
    principal = LarkPrincipal(open_id="ou-owner", group_ids=frozenset())
    resource = ProtectedResource(
        owner_open_id="ou-owner",
        allowed_approver_ids=frozenset({"ou-owner"}),
        expires_at=now - timedelta(seconds=1),
    )
    with pytest.raises(AuthorizationDenied):
        auth.authorize("approve", principal, resource, now=now)


def test_admin_delegation_is_explicit_and_audited(auth, audit_repo):
    decision = auth.authorize(
        "approve",
        LarkPrincipal("ou-admin", frozenset()),
        ProtectedResource("ou-owner", frozenset({"ou-owner"}), datetime.max.replace(tzinfo=UTC)),
        now=datetime.now(UTC),
    )
    assert decision.delegated is True
    assert decision.effective_owner_open_id == "ou-owner"
```

- [ ] **Step 2: Implement one authorization decision path**

```python
@dataclass(frozen=True)
class LarkPrincipal:
    open_id: str
    group_ids: frozenset[str]


@dataclass(frozen=True)
class ProtectedResource:
    owner_open_id: str
    allowed_approver_ids: frozenset[str]
    expires_at: datetime


@dataclass(frozen=True)
class AuthorizationDecision:
    actor_open_id: str
    effective_owner_open_id: str
    delegated: bool
```

`AuthorizationService` denies unknown actors, expired resources and disallowed actions. Search/list/detail require owner, configured group membership or admin. Approval requires the card-bound allowlist or admin. Admin decisions set `delegated=True`; `ApprovalService` writes actor, effective owner, decision, reason, request ID and timestamp in the same approval transaction. Lark event signature verification must finish before actor extraction.

- [ ] **Step 3: Bind cards and commands to immutable authorization data**

Add `owner_open_id`, `allowed_approver_ids`, `approval_version`, `expires_at` and `audience_hash` to card payloads and persisted notification payloads. On click, load those values from PostgreSQL rather than trusting card-submitted recipients or owner fields; compare card version and audience hash before the Phase 3 CAS.

- [ ] **Step 4: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/security/test_lark_authorization.py tests/integration/security/test_admin_delegation_audit.py tests/unit/test_lark_app.py tests/unit/test_lark_card_actions.py -q
git add src/security/auth.py src/approval/models.py src/approval/service.py src/commands/handlers.py src/utils/lark_ws.py src/config.py .env.example tests/unit/security/test_lark_authorization.py tests/integration/security/test_admin_delegation_audit.py tests/unit/test_lark_app.py tests/unit/test_lark_card_actions.py
git commit -m "feat: enforce lark resource authorization"
```

Expected: unauthorized, stale and expired actions return a generic denial, while one delegated admin action creates exactly one audit row.

---

### Task 4: Add One-time Preview Exchange and an Isolated Session

**Files:**
- Create: `alembic/versions/20260713_0011_preview_nonce.py`
- Create: `src/security/preview.py`
- Create: `src/api/__init__.py`
- Create: `src/api/preview.py`
- Create: `tests/unit/security/test_preview_tokens.py`
- Create: `tests/integration/security/test_preview_nonce.py`
- Create: `tests/integration/security/test_preview_routes.py`
- Modify: `src/server.py`
- Modify: `src/config.py`
- Modify: `.env.example`
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `src/db/access_contract.py`
- Modify: `src/db/bootstrap.py`
- Modify: `src/db/schema.py`
- Modify: `src/db/schema_contract.py`
- Modify: `src/maintenance/checkpoint_repository.py`
- Modify: `tests/unit/test_database_revision.py`
- Modify: `tests/unit/test_database_schema_contract.py`
- Modify: `tests/unit/test_checkpoint_repository_safety.py`
- Modify: `tests/unit/test_alembic_offline.py`
- Modify: `tests/integration/ingestion/test_schema.py`
- Modify: `tests/integration/ingestion/test_access_roles.py`
- Create: `tests/integration/migrations/test_0010_to_0011.py`

**Interfaces:**
- Consumes: authenticated Lark `open_id`, owner-filtered email lookup and ContentStore
- Produces: `PreviewTokenService.issue(claims: PreviewTokenClaims) -> str`; `PreviewTokenService.consume(token: str) -> PreviewSession`; `/preview/exchange`; `/preview/email/{email_id}`

Migration revision is exactly `20260713_0011` with linear `down_revision = "20260713_0010"`.

- [ ] **Step 1: Write failing single-use and redirect tests**

```python
from datetime import UTC, datetime, timedelta

import pytest
from httpx import ASGITransport, AsyncClient

from src.security.preview import PreviewTokenClaims, PreviewTokenReplay


@pytest.mark.asyncio
async def test_nonce_can_be_consumed_once(token_service):
    token = await token_service.issue(
        PreviewTokenClaims(
            aud="email-preview",
            sub="ou-owner",
            nonce="nonce-1",
            email_id="mail-1",
            expires_at=datetime.now(UTC) + timedelta(minutes=2),
        )
    )
    session = await token_service.consume(token)
    assert session.open_id == "ou-owner"
    with pytest.raises(PreviewTokenReplay):
        await token_service.consume(token)


@pytest.mark.asyncio
async def test_exchange_redirect_removes_token_and_sets_secure_cookie(app, valid_preview_token):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="https://preview.example") as client:
        response = await client.get("/preview/exchange", params={"token": valid_preview_token})
    assert response.status_code == 303
    assert response.headers["location"] == "/preview/email/mail-1"
    assert "token=" not in response.headers["location"]
    cookie = response.headers["set-cookie"]
    assert all(flag in cookie for flag in ("HttpOnly", "Secure", "SameSite=Strict"))
```

- [ ] **Step 2: Add nonce storage and atomic consumption**

Migration `20260713_0011` creates `preview_nonces(nonce_hash PRIMARY KEY, subject_hash, email_id, expires_at, consumed_at, created_at)` and `preview_sessions(session_hash PRIMARY KEY, subject_hash, email_id, expires_at, last_seen_at, revoked_at, created_at)`; neither table stores raw token/session values. `consume()` validates the JWT signature, algorithm, `aud`, `sub`, `nonce`, `exp` and email ID, then performs `UPDATE preview_nonces SET consumed_at=now() WHERE nonce_hash=%s AND consumed_at IS NULL AND expires_at>now() RETURNING subject_hash, email_id, expires_at`; zero rows means expired, unknown or replayed. Session creation inserts only SHA-256 of a 256-bit random cookie value.

Treat `0011` as a complete revision-contract change: advance the single exact app head/schema digest, bootstrap pre/post checks, four ACL manifests, checkpoint revision allowlist and offline SQL. Runtime receives only issue/consume/session columns, maintenance only bounded revoke/expiry cleanup, auditor SELECT-only, and DDL remains migration-only. `tests/integration/migrations/test_0010_to_0011.py` starts real PostgreSQL at `0010` with preview/security profiles disabled, seeds all four Outboxes and projection/content rows, performs the code-first bridge, verifies preservation/roles/startup and a second no-op upgrade, and proves old-head binary rejection plus single-head empty-DB behavior.

```python
@dataclass(frozen=True)
class PreviewSession:
    session_id: str
    open_id: str
    email_id: str
    expires_at: datetime
```

- [ ] **Step 3: Implement the preview routes and response policy**

`/preview/exchange` accepts only `GET`, exchanges the token, creates a server-side session, sets a host-only `__Host-preview_session` cookie with a 15-minute maximum age, and returns 303. `/preview/email/{email_id}` accepts only that cookie, verifies the session subject may view the resource, sanitizes content through Task 4, and returns no secrets in errors. Every preview response sets `Content-Security-Policy`, `Referrer-Policy: no-referrer`, `Cache-Control: no-store`, `X-Content-Type-Options: nosniff`, and `Cross-Origin-Resource-Policy: same-origin`.

- [ ] **Step 4: Require a distinct production Origin**

Add `PREVIEW_ORIGIN`. Production startup rejects it when its normalized origin equals `EXTERNAL_URL`, is not HTTPS, contains credentials/path/query/fragment, or is absent. Nginx routes the distinct preview server name to the preview router and replaces forwarded host/proto; FastAPI trusts those headers only from the configured proxy network and rejects direct/spoofed hosts. Application and reverse-proxy logs omit query values for `/preview/exchange`.

- [ ] **Step 5: Verify and commit**

```bash
uv add "PyJWT==2.10.1"
uv lock --check
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/security/test_preview_tokens.py tests/integration/security/test_preview_nonce.py tests/integration/security/test_preview_routes.py tests/integration/migrations/test_0010_to_0011.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py -q
.venv/bin/python -m pytest tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py -q
git add alembic/versions/20260713_0011_preview_nonce.py src/security/preview.py src/api/__init__.py src/api/preview.py src/server.py src/config.py .env.example pyproject.toml uv.lock src/db/access_contract.py src/db/bootstrap.py src/db/schema.py src/db/schema_contract.py src/maintenance/checkpoint_repository.py tests/unit/security/test_preview_tokens.py tests/integration/security/test_preview_nonce.py tests/integration/security/test_preview_routes.py tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py tests/integration/migrations/test_0010_to_0011.py
git commit -m "feat: add isolated single-use email preview"
```

Expected: replay, wrong audience, wrong subject, expired token, cross-user lookup and non-preview Host all fail without revealing whether an email exists.

---

### Task 5: Sanitize HTML and Block Remote Tracking

**Files:**
- Create: `src/security/html.py`
- Create: `tests/unit/security/test_html.py`
- Modify: `src/utils/email_renderer.py`
- Modify: `src/api/preview.py`
- Modify: `pyproject.toml`
- Modify: `uv.lock`

**Interfaces:**
- Produces: `sanitize_email_html(raw_html: str) -> SanitizedHtml`; `preview_security_headers() -> dict[str, str]`

- [ ] **Step 1: Write a malicious-document corpus test**

```python
import pytest

from src.security.html import sanitize_email_html


@pytest.mark.parametrize(
    "payload",
    [
        '<script>alert(1)</script>',
        '<img src="https://tracker.example/pixel" onerror="alert(1)">',
        '<a href="javascript:alert(1)">open</a>',
        '<form action="https://evil.example"><input name="secret"></form>',
        '<iframe src="file:///etc/passwd"></iframe>',
        '<style>@import url(https://evil.example/a.css)</style>',
        '<svg><use href="http://169.254.169.254/latest/meta-data"></use></svg>',
    ],
)
def test_untrusted_html_cannot_execute_or_load_external_resources(payload):
    result = sanitize_email_html(payload)
    lowered = result.html.lower()
    for forbidden in ("<script", "<form", "<iframe", "javascript:", "http://", "https://", "file:", "onerror", "@import"):
        assert forbidden not in lowered
```

- [ ] **Step 2: Implement an explicit nh3 allowlist**

Allow only `a`, `abbr`, `b`, `blockquote`, `br`, `code`, `div`, `em`, `h1`-`h6`, `hr`, `i`, `li`, `ol`, `p`, `pre`, `span`, `strong`, `table`, `tbody`, `td`, `th`, `thead`, `tr`, `u`, and `ul`. Allow only presentation attributes `class`, `colspan`, `rowspan`, `title`; strip all `style`, `id`, `src`, `srcset`, event attributes and SVG/MathML. Links keep only `https` or `mailto`, receive `rel="noopener noreferrer nofollow"`, and their display text remains escaped. Return removal counts, never removed values.

```python
@dataclass(frozen=True)
class SanitizedHtml:
    html: str
    removed_elements: int
    removed_attributes: int
```

- [ ] **Step 3: Replace every raw rendering path**

`email_renderer` and preview routes accept only `SanitizedHtml`; delete any `|safe`/raw-body branch. CSP is exactly `default-src 'none'; img-src 'self' data:; style-src 'self'; font-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'`. No remote image proxy is added in this phase.

- [ ] **Step 4: Verify dependencies, tests, and commit**

```bash
uv add "nh3==0.3.2"
uv lock --check
.venv/bin/python -m pytest tests/unit/security/test_html.py tests/unit/test_email_renderer.py tests/integration/security/test_preview_routes.py -q
git add pyproject.toml uv.lock src/security/html.py src/utils/email_renderer.py src/api/preview.py tests/unit/security/test_html.py tests/unit/test_email_renderer.py tests/integration/security/test_preview_routes.py
git commit -m "feat: sanitize untrusted email html"
```

Expected: corpus tests pass, `rg -n 'raw_html|mark_safe|\|safe' src/api src/utils/email_renderer.py` finds no bypass, and the lock file is synchronized.

---

### Task 6: Deny External Resources During PDF Generation

**Files:**
- Create: `src/security/pdf.py`
- Create: `tests/unit/security/test_pdf_fetcher.py`
- Modify: `src/utils/pdf_generator.py`
- Modify: `src/utils/lark_pdf_flow.py`

**Interfaces:**
- Consumes: sanitized HTML and explicit in-memory font/image assets
- Produces: `restricted_url_fetcher(url: str) -> dict[str, object]`; `render_pdf(document: SanitizedHtml, assets: Mapping[str, bytes]) -> bytes`

- [ ] **Step 1: Write failing SSRF and local-file tests**

```python
import pytest

from src.security.pdf import PdfResourceRejected, restricted_url_fetcher


@pytest.mark.parametrize(
    "url",
    [
        "https://tracker.example/pixel.png",
        "http://127.0.0.1:8000/metrics",
        "http://169.254.169.254/latest/meta-data",
        "file:///etc/passwd",
        "ftp://example.test/a",
        "data:text/html,<script>alert(1)</script>",
    ],
)
def test_pdf_fetcher_denies_external_local_and_unsafe_data_urls(url):
    with pytest.raises(PdfResourceRejected):
        restricted_url_fetcher(url)
```

- [ ] **Step 2: Implement a closed asset registry**

`restricted_url_fetcher` serves only `asset://<sha256>` entries pre-registered for the current render and the exact MIME types `image/png`, `image/jpeg`, `image/gif`, `font/ttf`, `font/otf`. Reject missing hashes, MIME mismatch, more than 5 MiB per asset and all other schemes. Do not call WeasyPrint's default fetcher.

- [ ] **Step 3: Make temporary-file cleanup unconditional**

Render from bytes/file-like objects where supported. If a temporary directory is required, create it under configured `SECURE_TMP_DIR` with mode `0700`, random names and `try/finally` removal. A startup sweep removes only app-owned entries older than 24 hours after verifying directory ownership and prefix.

- [ ] **Step 4: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/security/test_pdf_fetcher.py tests/unit/test_pdf_generator.py tests/unit/test_lark_pdf_flow.py -q
git add src/security/pdf.py src/utils/pdf_generator.py src/utils/lark_pdf_flow.py tests/unit/security/test_pdf_fetcher.py tests/unit/test_pdf_generator.py tests/unit/test_lark_pdf_flow.py
git commit -m "feat: isolate pdf resource loading"
```

Expected: no test starts a network request, all injected private/file URLs are rejected, and failure-path temporary directories are empty.

---

### Task 7: Validate and Quarantine Attachments Before Storage or Analysis

**Files:**
- Create: `src/security/attachments.py`
- Create: `tests/unit/security/test_attachments.py`
- Modify: `tests/unit/security/conftest.py`
- Modify: `src/utils/email_processor.py`
- Modify: `src/storage/content_store.py`
- Modify: `src/config.py`
- Modify: `.env.example`
- Modify: `pyproject.toml`
- Modify: `uv.lock`

**Interfaces:**
- Produces: `AttachmentPolicy.inspect(name, declared_type, content) -> AttachmentDecision`; immutable `AttachmentMetadata`

- [ ] **Step 1: Write exact count, size, magic, image, and archive tests**

```python
import pytest

from src.security.attachments import AttachmentPolicy, AttachmentRejected


def test_declared_pdf_with_executable_magic_is_rejected():
    policy = AttachmentPolicy.production_defaults()
    with pytest.raises(AttachmentRejected, match="type_mismatch"):
        policy.inspect("invoice.pdf", "application/pdf", b"MZ" + b"0" * 64)


def test_image_pixel_limit_is_checked_before_decode_allocation(make_png):
    policy = AttachmentPolicy.production_defaults(max_image_pixels=100)
    with pytest.raises(AttachmentRejected, match="image_pixels"):
        policy.inspect("photo.png", "image/png", make_png(width=11, height=10))


def test_archive_ratio_and_macro_formats_are_quarantined(make_high_ratio_zip, make_macro_docm):
    policy = AttachmentPolicy.production_defaults(max_archive_ratio=20.0)
    for name, declared, content in (
        ("high-ratio.zip", "application/zip", make_high_ratio_zip()),
        ("macro.docm", "application/vnd.ms-word.document.macroEnabled.12", make_macro_docm()),
    ):
        decision = policy.inspect(name, declared, content)
        assert decision.action == "quarantine"
```

The security conftest builds the PNG and ZIP/Office containers in memory with fixed dimensions/content, never extracts them to the repository or depends on opaque binary fixtures.

- [ ] **Step 2: Implement the production defaults**

Defaults are: 20 attachments, 10 MiB each, 25 MiB decoded total, 40 megapixels per image, archive ratio 20:1, 1000 archive entries and nesting depth 1. Validate Base64 encoded length before decoding, stream-decode with a hard counter, detect type from magic bytes, call Pillow `verify()` with decompression-bomb warnings promoted to rejection, and inspect ZIP central-directory sizes without extracting. Reject executables/scripts; quarantine macro-enabled Office files, password-protected or nested archives and unknown types.

```python
@dataclass(frozen=True)
class AttachmentDecision:
    action: Literal["allow", "quarantine"]
    detected_type: str
    sha256: str
    size: int
    reason: str | None
```

- [ ] **Step 3: Enforce policy before ContentStore and model access**

`email_processor` validates aggregate count/decoded size before writing; allowed bytes go to ContentStore and Graph sees only artifact IDs. Quarantined metadata is persisted without making bytes available to model roles. External model attachment analysis remains disabled unless Task 7 policy explicitly permits both the role and detected type.

- [ ] **Step 4: Verify dependencies, tests, and commit**

```bash
uv add "filetype==1.2.0" "Pillow==12.0.0"
uv lock --check
.venv/bin/python -m pytest tests/unit/security/test_attachments.py tests/unit/test_content_guard.py tests/unit/test_email_processor.py -q
git add pyproject.toml uv.lock src/security/attachments.py src/utils/email_processor.py src/storage/content_store.py src/config.py .env.example tests/unit/security/conftest.py tests/unit/security/test_attachments.py tests/unit/test_content_guard.py tests/unit/test_email_processor.py
git commit -m "feat: validate and quarantine email attachments"
```

Expected: malicious fixtures are rejected/quarantined without extraction, allowed attachment bytes never appear in Graph State, and all configured limits have boundary tests.

---

### Task 8: Enforce Account-scoped External Model Data Policy

**Files:**
- Create: `src/llm/policy.py`
- Create: `src/llm/minimizer.py`
- Create: `tests/unit/llm/test_policy.py`
- Create: `tests/unit/llm/test_minimizer.py`
- Modify: `src/llm/gateway.py`
- Modify: `src/graph/dependencies.py`
- Modify: `src/config.py`
- Modify: `.env.example`

**Interfaces:**
- Consumes: account ID, role, configured provider/model/region, requested data categories and optional artifact metadata
- Produces: `ModelDataPolicy.authorize(request) -> AuthorizedModelRequest`; content-minimized message list; metadata-only audit event

- [ ] **Step 1: Write failing policy-composition tests**

```python
import pytest

from src.llm.policy import ModelDataPolicy, ModelPolicyRejected, ModelRequestContext


def test_provider_switch_cannot_bypass_account_region_or_attachment_policy():
    policy = ModelDataPolicy.from_dict(
        {
            "8": {
                "external_enabled": True,
                "providers": ["openai"],
                "models": ["gpt-5-mini"],
                "regions": ["sg"],
                "roles": ["categorizer", "drafter"],
                "attachment_types": [],
                "max_retention_days": 0,
                "training_allowed": False,
            }
        }
    )
    request = ModelRequestContext(
        account_id=8,
        provider="other-provider",
        model="same-name",
        region="us",
        role="drafter",
        data_categories=frozenset({"email_body", "attachment"}),
        attachment_types=frozenset({"application/pdf"}),
        provider_retention_days=30,
        provider_training_enabled=True,
    )
    with pytest.raises(ModelPolicyRejected) as error:
        policy.authorize(request)
    assert set(error.value.reasons) == {
        "provider",
        "region",
        "attachment_type",
        "retention",
        "training",
    }
```

- [ ] **Step 2: Implement policy intersection before provider selection**

The gateway derives provider/model only after loading the account policy. Authorization is an intersection: application allowlist ∩ account allowlist ∩ role allowlist ∩ provider declaration. Missing policy means external processing disabled. No fallback provider can widen region, retention, training, attachment or role permissions. Policy versions use a content hash and are included in every model audit row.

- [ ] **Step 3: Minimize untrusted model content**

Remove quoted thread history beyond the configured message count, signatures, tracking URLs and unrelated headers; replace full addresses with deterministic account-scoped hashes unless the role needs the address; cap historical hints; surround email/attachment text with explicit untrusted-data delimiters. The gateway accepts only minimized structured messages and never filesystem paths, raw attachment bytes or transport clients.

- [ ] **Step 4: Audit metadata without prompts**

Write `provider`, `model`, `region`, `role`, `account_category`, data-category flags, input/output Token counts, outcome, error kind and policy version. Do not store messages, output body, full addresses or attachment names. Unit-test captured structlog events to prove forbidden fields are absent.

- [ ] **Step 5: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/llm/test_policy.py tests/unit/llm/test_minimizer.py tests/unit/llm/test_gateway.py tests/unit/test_prompt_injection.py -q
git add src/llm/policy.py src/llm/minimizer.py src/llm/gateway.py src/graph/dependencies.py src/config.py .env.example tests/unit/llm/test_policy.py tests/unit/llm/test_minimizer.py tests/unit/llm/test_gateway.py tests/unit/test_prompt_injection.py
git commit -m "feat: enforce external model data governance"
```

Expected: disabled accounts and every policy mismatch enter `manual_review`; switching providers never bypasses policy or Token budget.

---

### Task 9: Centralize Log Redaction and Safe Error Responses

**Files:**
- Modify: `src/security/redaction.py`
- Create: `tests/unit/security/test_redaction.py`
- Create: `tests/integration/security/test_safe_errors.py`
- Modify: `src/utils/logging_setup.py`
- Modify: `src/server.py`
- Modify: `src/main.py`

**Interfaces:**
- Produces: `redact_event(logger, method_name, event_dict) -> dict[str, object]`; safe public exception handler

- [ ] **Step 1: Write a sentinel-secret test across logs and responses**

```python
import json

import pytest
from httpx import ASGITransport, AsyncClient

from src.security.redaction import redact_event


def test_redactor_removes_sensitive_keys_and_masks_identifiers():
    event = redact_event(
        None,
        "info",
        {
            "event": "failed",
            "body": "SENTINEL-BODY",
            "authorization": "Bearer SENTINEL-TOKEN",
            "card": {"content": "SENTINEL-CARD"},
            "email": "person@example.com",
            "email_id": "mail-1",
        },
    )
    encoded = json.dumps(event, ensure_ascii=False)
    assert "SENTINEL" not in encoded
    assert "person@example.com" not in encoded
    assert event["email_id"] == "mail-1"


@pytest.mark.asyncio
async def test_dependency_exception_is_not_returned_to_client(app):
    app.state.force_error = RuntimeError("postgresql://user:SENTINEL@db/internal")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/ready")
    assert response.status_code == 503
    assert response.json() == {"status": "unavailable", "request_id": response.json()["request_id"]}
    assert "SENTINEL" not in response.text
```

- [ ] **Step 2: Implement allowlist-first structured logging**

Keep only event name, timestamp, level, component, stage, result, `ErrorKind`, durations/counts and correlation IDs. Recursively drop keys matching body/html/content/attachment/card/prompt/messages/token/secret/password/signature/authorization/cookie/header/url-query. Hash actor/address identifiers with a rotating audit key when correlation is required. Replace exception text from external clients with exception class plus normalized error kind.

- [ ] **Step 3: Install generic public exception handling**

Return request ID and a generic status for unexpected errors; `/health` returns only status/version/time; `/ready` returns component names and boolean readiness without DSNs, URLs or exception text. Ensure uvicorn access logs use a filter that removes preview query strings and authorization/cookie headers.

- [ ] **Step 4: Verify and commit**

```bash
.venv/bin/python -m pytest tests/unit/security/test_redaction.py tests/integration/security/test_safe_errors.py tests/unit/test_logging_setup.py tests/unit/test_metrics_auth.py -q
git add src/security/redaction.py src/utils/logging_setup.py src/server.py src/main.py tests/unit/security/test_redaction.py tests/integration/security/test_safe_errors.py tests/unit/test_logging_setup.py tests/unit/test_metrics_auth.py
git commit -m "feat: redact logs and public error responses"
```

Expected: seeded sentinels do not appear in captured logs, response bodies or access-log URLs.

---

### Task 10: Harden the Production Container and Network Boundary

**Files:**
- Modify: `docker-compose.dev.yml`
- Create: `deploy/nginx/nginx.conf`
- Create: `deploy/nginx/ai-exchange.conf`
- Create: `tests/security/test_container_config.py`
- Modify: `Dockerfile`
- Modify: `docker-compose.yml`
- Modify: `.dockerignore`
- Modify: `README.md`

**Interfaces:**
- Consumes: application health/readiness endpoints and encrypted content-store volume
- Produces: multi-stage, non-root, read-only production service; internal-only PostgreSQL/Qdrant/Metrics

- [ ] **Step 1: Write a static Compose and Dockerfile policy test**

```python
from pathlib import Path

import yaml


def test_production_services_are_not_public_and_app_is_hardened():
    compose = yaml.safe_load(Path("docker-compose.yml").read_text())
    assert "ports" not in compose["services"]["postgres"]
    assert "ports" not in compose["services"]["qdrant"]
    app = compose["services"]["ai-assistant-service"]
    assert app["read_only"] is True
    assert app["cap_drop"] == ["ALL"]
    assert app["security_opt"] == ["no-new-privileges:true"]
    assert app["pids_limit"] == 256
    assert app["tmpfs"] == ["/tmp:rw,noexec,nosuid,size=64m"]


def test_runtime_image_has_no_compiler_or_test_tree():
    dockerfile = Path("Dockerfile").read_text()
    runtime = dockerfile.split("FROM python:3.12-slim AS runtime", maxsplit=1)[1]
    assert "build-essential" not in runtime
    assert "COPY tests" not in runtime
    assert "USER 10001:10001" in runtime


def test_preview_proxy_log_format_omits_query_string():
    config = Path("deploy/nginx/ai-exchange.conf").read_text()
    assert "$request_uri" not in config
    assert "$args" not in config
    assert "limit_req" in config
    assert "proxy_set_header X-Forwarded-Proto $scheme" in config
```

- [ ] **Step 2: Build an exact two-stage image**

Builder installs locked production dependencies and compiles wheels. Runtime installs only shared libraries, copies wheels and `src`, uses UID/GID 10001, sets `PYTHONDONTWRITEBYTECODE=1`, and contains no compiler, curl, tests, `.env`, VCS data or source bind mount. Healthcheck uses Python stdlib HTTP so curl is unnecessary. Phase 6 will pin the base digest and emit SBOM; this task establishes the security shape.

- [ ] **Step 3: Separate production and development Compose**

Production removes PostgreSQL/Qdrant host ports, source/test bind mounts and `host.docker.internal`; uses internal backend network, a separate reverse-proxy-facing network for the app, `read_only`, `cap_drop: [ALL]`, `no-new-privileges`, `pids_limit: 256`, tmpfs and resource limits. Nginx terminates TLS for separate application/preview server names, applies method/body/rate limits, forwards only normalized host/proto/request ID, keeps Metrics/admin on an internal location, and uses an access-log format without query strings, authorization or cookies. `docker-compose.dev.yml` contains localhost-only database/Qdrant ports and source mounts for developer use.

- [ ] **Step 4: Verify and commit**

```bash
.venv/bin/python -m pytest tests/security/test_container_config.py -q
docker compose config --quiet
docker compose -f docker-compose.yml -f docker-compose.dev.yml config --quiet
docker build --target runtime -t ai-exchange:security-gate .
docker run --rm --entrypoint id ai-exchange:security-gate
git add Dockerfile docker-compose.yml docker-compose.dev.yml deploy/nginx/nginx.conf deploy/nginx/ai-exchange.conf .dockerignore README.md tests/security/test_container_config.py
git commit -m "build: harden production container boundary"
```

Expected: runtime reports UID/GID 10001, both Compose configurations validate, and only the application proxy-facing port is externally reachable in production.

---

### Task 11: Implement the Retention Policy and Guarded Cleanup Engine

**Files:**
- Create: `alembic/versions/20260713_0012_retention_control.py`
- Create: `src/maintenance/retention.py`
- Create: `tests/unit/maintenance/test_retention_policy.py`
- Create: `tests/integration/maintenance/test_retention_engine.py`
- Create: `tests/unit/maintenance/conftest.py`
- Create: `tests/integration/maintenance/conftest.py`
- Modify: `src/config.py`
- Modify: `.env.example`
- Modify: `src/db/access_contract.py`
- Modify: `src/db/bootstrap.py`
- Modify: `src/db/schema.py`
- Modify: `src/db/schema_contract.py`
- Modify: `src/maintenance/checkpoint_repository.py`
- Modify: `tests/unit/test_database_revision.py`
- Modify: `tests/unit/test_database_schema_contract.py`
- Modify: `tests/unit/test_checkpoint_repository_safety.py`
- Modify: `tests/unit/test_alembic_offline.py`
- Modify: `tests/integration/ingestion/test_schema.py`
- Modify: `tests/integration/ingestion/test_access_roles.py`
- Create: `tests/integration/migrations/test_0011_to_0012.py`

**Interfaces:**
- Consumes: ContentStore references/holds, Inbox/Outbox/audit facts and Projection Outbox
- Produces: `RetentionPolicy.production_defaults()`; `RetentionPlanner.plan(now, limit) -> RetentionPlan`; resumable `RetentionExecutor.execute(plan_id, authorization) -> RetentionReport`

Migration revision is exactly `20260713_0012` with linear `down_revision = "20260713_0011"`.

- [ ] **Step 1: Write exact policy and preservation tests**

```python
from datetime import UTC, datetime, timedelta

import pytest

from src.maintenance.retention import RetentionPolicy


def test_production_default_periods_are_locked():
    policy = RetentionPolicy.production_defaults()
    assert policy.terminal_content == timedelta(days=30)
    assert policy.inbox_payload == timedelta(days=7)
    assert policy.resolved_dead_letter == timedelta(days=30)
    assert policy.preview_cache == timedelta(hours=24)
    assert policy.terminal_checkpoint == timedelta(hours=24)
    assert policy.redacted_logs == timedelta(days=30)
    assert policy.business_audit == timedelta(days=180)
    assert policy.security_audit == timedelta(days=365)
    assert policy.encrypted_backup == timedelta(days=30)


@pytest.mark.asyncio
async def test_unresolved_send_and_referenced_content_are_never_candidates(planner, db):
    now = datetime.now(UTC)
    await db.seed_email(state="send_unknown", content_ref="held-1", terminal_at=now - timedelta(days=400))
    await db.seed_email(state="accepted", content_ref="held-2", terminal_at=now - timedelta(days=90))
    await db.seed_content(content_ref="referenced", ref_count=1, expires_at=now - timedelta(days=1))
    plan = await planner.plan(now=now, limit=500)
    assert {"held-1", "held-2", "referenced"}.isdisjoint(plan.content_refs_to_delete)
```

- [ ] **Step 2: Encode all lifecycle rules**

Waiting approval is held until completion/expiry. Unresolved `accepted`/`send_unknown` retains the full frozen send snapshot until manual resolution, then payload is kept 30 days. Draft/send-intent/notification/send Outbox payloads clear 30 days after terminal state while minimal audit remains. Qdrant lifetime follows content; 365-day audit contains outcome, hashes, minimized actor, time and causality only. Dead letters, backups and Outbox payloads use the same content deadline and cannot become indefinite copies.

- [ ] **Step 3: Implement dry-run, bounded execution, and recovery**

Migration `0012` creates `retention_plans` (policy/config/schema hashes, immutable source high-water, counts, plan hash, state, actor/reason, created/expiry), `retention_plan_items` (plan ID, deterministic ordinal, object type/ID hash, expected version/hold/reference facts, action, status; append-only identity), and `retention_executions` (plan ID, authorization hash/expiry, cursor, bounded counters, last safe error, started/completed timestamps). Triggers forbid plan/item identity rewrite, reopen of completed work, DELETE and TRUNCATE outside the later policy-authorized cleanup path.

Planner writes the immutable row list and safety counts without deleting. Executor requires a signed `RetentionExecutionAuthorization(plan_hash, actor_id, expires_at)`, claims at most 500 rows, clears sensitive payload before optional metadata deletion, decrements account-scoped references, enqueues one stable Qdrant delete per removed projection, records cursor/statistics and resumes after failure. It rejects stale/changed plans and any row that acquired a hold or reference after planning.

`0012` advances the exact single head/schema digest, bootstrap checks, all four ACL manifests, checkpoint allowlist and offline SQL in this task. Runtime can enqueue stable projection deletion only; maintenance owns bounded plan/item/execution transitions but no DDL or protected-row bypass; auditor is SELECT-only; migration owns DDL. `tests/integration/migrations/test_0011_to_0012.py` proves a disabled-profile code-first real-PostgreSQL bridge, preservation of preview/business rows, role behavior, second no-op upgrade, old-head rejection and single-head empty-DB startup.

`tests/unit/maintenance/conftest.py` provides a fixed clock and policy; `tests/integration/maintenance/conftest.py` provides migrated `db`, `planner`, `executor`, reference/hold seed methods and a fake Projection Outbox that records stable business keys.

- [ ] **Step 4: Verify and commit**

```bash
TEST_DATABASE_URL=postgresql://user:password@localhost:5432/ai_exchange_test .venv/bin/python -m pytest tests/unit/maintenance/test_retention_policy.py tests/integration/maintenance/test_retention_engine.py tests/integration/migrations/test_0011_to_0012.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py -q
.venv/bin/python -m pytest tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py -q
git add alembic/versions/20260713_0012_retention_control.py src/maintenance/retention.py src/config.py .env.example src/db/access_contract.py src/db/bootstrap.py src/db/schema.py src/db/schema_contract.py src/maintenance/checkpoint_repository.py tests/unit/maintenance/conftest.py tests/integration/maintenance/conftest.py tests/unit/maintenance/test_retention_policy.py tests/integration/maintenance/test_retention_engine.py tests/unit/test_database_revision.py tests/unit/test_database_schema_contract.py tests/unit/test_checkpoint_repository_safety.py tests/unit/test_alembic_offline.py tests/integration/ingestion/test_schema.py tests/integration/ingestion/test_access_roles.py tests/integration/migrations/test_0011_to_0012.py
git commit -m "feat: enforce guarded data retention"
```

Expected: every design retention period has a test, protected content never enters a plan, dry-run matches execution, and interrupted cleanup resumes without duplicate projection deletion.

---

### Task 12: Add Security Regression Gate and Phase Acceptance Record

**Files:**
- Create: `tests/security/test_webhook_security.py`
- Create: `tests/security/test_prompt_boundary.py`
- Create: `tests/security/test_no_sensitive_logging.py`
- Create: `docs/security/threat-model.md`
- Create: `docs/security/data-flow.md`
- Create: `docs/superpowers/reports/2026-07-10-phase-5-security-acceptance.md`
- Create: `tests/integration/security/test_phase5_activation_successor.py`
- Modify: `src/ingestion/cutover_barrier.py`
- Modify: `README.md`
- Modify: `CLAUDE.md`

**Interfaces:**
- Consumes: all Phase 5 security modules and design specification sections 8.3 and 9
- Produces: reproducible security test command and evidence record used by Phase 6 CI

- [ ] **Step 1: Add end-to-end negative tests**

Cover wrong HMAC over raw bytes, altered body after signing, valid replay dedupe, wrong Lark actor/group, stale card version, preview replay, malicious HTML, PDF SSRF, oversized/zip-bomb attachment, prompt-injection text, forbidden model region, unsafe startup settings and sentinel-secret logging. Every case asserts the exact generic HTTP/business outcome and that no notification/send/model side effect was invoked.

- [ ] **Step 2: Document assets, boundaries, actors, and residual risk**

`threat-model.md` enumerates Exchange server, reverse proxy, Lark, model providers, PostgreSQL, Qdrant and encrypted ContentStore; trust boundaries include webhook ingress, preview Origin, worker egress and administrative APIs. `data-flow.md` records which data category crosses each boundary, encryption, retention and the responsible policy module. Residual risks explicitly include the Exchange server's current lack of idempotency/status query and are deferred without modifying that repository.

- [ ] **Step 3: Run the complete Phase 5 gate**

```bash
.venv/bin/python -m pytest tests/security tests/unit/security tests/integration/security -q
.venv/bin/python -m pytest --cov=src.security --cov=src.llm.policy --cov=src.llm.minimizer --cov-report=term-missing --cov-fail-under=90
.venv/bin/ruff check src/ tests/
.venv/bin/python -m pytest -q
uv lock --check
docker compose config --quiet
git diff --check
```

Expected: every command exits 0; security tests contain no skip markers; security module coverage is at least 90%.

- [ ] **Step 4: Record evidence and commit**

Write the exact commit SHA, command/exit-code table, dependency versions, image ID, tested negative cases and any accepted residual risk to the Phase 5 report. Do not include secrets, internal URLs, raw addresses, mail bodies or card payloads. After every Phase-5 gate passes, append exactly one immutable `phase5_security_governance` activation successor to the latest `phase4_graph_projection` row. It freezes exact head `20260713_0012`, schema/build/capability-config, security/RBAC/preview/TLS/model-policy/retention evidence hashes and the predecessor manifest; its target generation/fence and live-barrier FK remain null because this is not cutover. Same evidence replays idempotently, drift conflicts, and this stage remains non-consumable. A real-PostgreSQL test proves the self-predecessor link, current-leaf uniqueness, null live fields and that `ActivationService` rejects it as not yet `production_ready`.

```bash
git add tests/security tests/integration/security/test_phase5_activation_successor.py src/ingestion/cutover_barrier.py docs/security docs/superpowers/reports/2026-07-10-phase-5-security-acceptance.md README.md CLAUDE.md
git commit -m "test: establish production security regression gate"
```

Expected: the report maps every security requirement to a passing test or an explicitly owned residual risk; no current-project security requirement remains unassigned.
