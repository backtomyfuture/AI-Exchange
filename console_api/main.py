"""FastAPI entry point for the local Operations Console.

Run with:

    uvicorn console_api.main:app --host 127.0.0.1 --port 8090 --reload

This module is intentionally not imported by ``src.server`` and is not part
of the production Compose application.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from console_api.database import ConsoleDatabase, ConsoleDatabaseError
from console_api.models import (
    EmailListResponse,
    MatchTestRequest,
    MatchTestResponse,
    PipelineTrace,
    RuleDetail,
    RuleDraftRequest,
    RuleSaveResponse,
    RuleSummary,
    RuleValidationResponse,
)
from console_api.rules import RuleStore, RuleStoreError
from console_api.settings import ConsoleSettings, get_console_settings, running_in_production


def _database(settings: ConsoleSettings = Depends(get_console_settings)) -> ConsoleDatabase:
    try:
        return ConsoleDatabase(settings)
    except ConsoleDatabaseError as exc:
        raise HTTPException(status_code=503, detail="console_database_unavailable") from exc


def _rules(settings: ConsoleSettings = Depends(get_console_settings)) -> RuleStore:
    return RuleStore(settings)


def _safe_error(exc: Exception) -> HTTPException:
    if isinstance(exc, RuleStoreError):
        return HTTPException(status_code=exc.status_code, detail=exc.code)
    if isinstance(exc, ConsoleDatabaseError):
        return HTTPException(status_code=503, detail="console_database_unavailable")
    return HTTPException(status_code=500, detail="console_operation_failed")


def create_app() -> FastAPI:
    application = FastAPI(
        title="AI Exchange Operations Console",
        version="0.1.0",
        description="Local-only Pipeline Trace and Tier 1 Rule Draft editor.",
    )
    settings = get_console_settings()
    local_client_hosts = frozenset(settings.client_host_list())
    application.add_middleware(
        CORSMiddleware,
        allow_origins=settings.origin_list(),
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type"],
    )

    @application.middleware("http")
    async def local_only_guard(request, call_next):
        client_host = request.client.host if request.client else None
        if running_in_production() or client_host not in local_client_hosts:
            return JSONResponse(status_code=404, content={"detail": "Not found"})
        return await call_next(request)

    @application.get("/health")
    async def health():
        return {"status": "ok", "local_only": True}

    @application.get("/api/emails", response_model=EmailListResponse)
    async def list_emails(
        database: ConsoleDatabase = Depends(_database),
        page: int = Query(default=1, ge=1, le=100_000),
        page_size: int = Query(default=25, ge=1, le=100),
        status: str | None = Query(default=None, max_length=64),
        sender: str | None = Query(default=None, max_length=320),
        received_from: datetime | None = None,
        received_to: datetime | None = None,
    ):
        try:
            return await database.list_emails(
                page=page,
                page_size=page_size,
                status=status,
                sender=sender,
                received_from=received_from,
                received_to=received_to,
            )
        except Exception as exc:
            raise _safe_error(exc) from exc

    @application.get("/api/emails/{external_email_id}/trace", response_model=PipelineTrace)
    async def email_trace(
        external_email_id: str,
        database: ConsoleDatabase = Depends(_database),
    ):
        try:
            trace = await database.trace(external_email_id)
        except Exception as exc:
            raise _safe_error(exc) from exc
        if trace is None:
            raise HTTPException(status_code=404, detail="email_not_found")
        return trace

    @application.get("/api/rules", response_model=list[RuleSummary])
    async def list_rules(rule_store: RuleStore = Depends(_rules)):
        return rule_store.list_rules()

    @application.get("/api/rules/{rule_id}", response_model=RuleDetail)
    async def get_rule(rule_id: str, rule_store: RuleStore = Depends(_rules)):
        try:
            result = rule_store.get_rule(rule_id)
        except Exception as exc:
            raise _safe_error(exc) from exc
        if result is None:
            raise HTTPException(status_code=404, detail="rule_not_found")
        return result

    @application.post("/api/rules", response_model=RuleSaveResponse)
    async def save_rule(
        request: RuleDraftRequest,
        rule_store: RuleStore = Depends(_rules),
    ):
        try:
            result = rule_store.save(request)
        except Exception as exc:
            raise _safe_error(exc) from exc
        return RuleSaveResponse(
            rule=result,
            message=(
                "Written to local tier1_rules/. Commit the file and run "
                "scripts/deploy_system.py to activate it after a planned restart."
            ),
            written_path=f"tier1_rules/{result.filename}",
        )

    @application.post("/api/rules/validate", response_model=RuleValidationResponse)
    async def validate_rule(
        request: RuleDraftRequest,
        rule_store: RuleStore = Depends(_rules),
    ):
        try:
            return rule_store.validate_candidate(request)
        except Exception as exc:
            raise _safe_error(exc) from exc

    @application.post("/api/rules/validate-registry", response_model=RuleValidationResponse)
    async def validate_registry(rule_store: RuleStore = Depends(_rules)):
        try:
            return rule_store.validate_registry()
        except Exception as exc:
            raise _safe_error(exc) from exc

    @application.post("/api/rules/compile", response_model=RuleValidationResponse)
    async def compile_registry_artifact(rule_store: RuleStore = Depends(_rules)):
        try:
            artifact, _path = rule_store.compile_artifact()
        except Exception as exc:
            raise _safe_error(exc) from exc
        return RuleValidationResponse(
            valid=True,
            digest=artifact.digest,
            enabled_rule_count=len(artifact.rules),
            warnings=[
                {"rule_id": issue.rule_id, "code": issue.code, "message": issue.message}
                for issue in artifact.warnings
            ],
        )

    @application.post(
        "/api/rules/{rule_id}/test-match",
        response_model=MatchTestResponse,
    )
    async def test_match(
        rule_id: str,
        request: MatchTestRequest,
        database: ConsoleDatabase = Depends(_database),
        rule_store: RuleStore = Depends(_rules),
    ):
        try:
            return await rule_store.test_match(
                database,
                rule_id,
                request.external_email_id,
                request.save_as,
            )
        except Exception as exc:
            raise _safe_error(exc) from exc

    return application


app = create_app()
