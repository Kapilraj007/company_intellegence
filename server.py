"""
FastAPI Gateway — Company Intelligence Pipeline.

Adds live task events, task listing, output file listing, and safe JSON file reads
for the frontend dashboard/pipeline/output views.
"""

import io
import json
import os
import re
import sys
import time
import uuid
from contextlib import asynccontextmanager, redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from core.auth import (
    CurrentUser,
    PENDING_APPROVAL_MESSAGE,
    REJECTED_ACCOUNT_MESSAGE,
    bootstrap_admin_from_env,
    clear_auth_cookie,
    create_access_token,
    generate_session_nonce,
    get_current_user,
    get_role_permissions,
    get_optional_current_user,
    hash_password,
    normalize_email,
    require_admin,
    require_permission,
    serialize_user,
    set_auth_cookie,
    validate_password_strength,
    verify_password,
)
from core.local_store import get_local_store_client
from core.supabase_store import get_supabase_client
from core.user_scope import require_user_id
from logger import get_logger

load_dotenv()
logger = get_logger("server")

# ── Paths and constants ────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = (BASE_DIR / "output").resolve()
OUTPUT_FILE_RE = re.compile(
    r"^(?P<company>.+?)_(?P<kind>golden_record|validation_report|pytest_report|semantic_chunks)_(?P<stamp>\d{8}_\d{6})\.json$"
)


# ── In-memory stores ───────────────────────────────────────────────────────────
_tasks: Dict[str, Dict[str, Any]] = {}
_tasks_lock = Lock()
_pipeline_run_lock = Lock()
_admin_cache_lock = Lock()
_admin_cache: Dict[str, Dict[str, Any]] = {}
_output_file_metrics_cache: Dict[str, Dict[str, Any]] = {}


# ── Helpers ────────────────────────────────────────────────────────────────────
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _slug(value: str) -> str:
    """Convert a string to a URL-friendly slug."""
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", (value or "").strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "unknown"


def _admin_cache_ttl_seconds() -> float:
    raw = str(os.getenv("ADMIN_CACHE_TTL_SECONDS", "20")).strip()
    try:
        return max(2.0, float(raw))
    except (TypeError, ValueError):
        return 20.0


def _admin_cache_get(key: str) -> Any:
    cache_key = str(key or "").strip()
    if not cache_key:
        return None
    with _admin_cache_lock:
        entry = _admin_cache.get(cache_key)
        if not isinstance(entry, dict):
            return None
        cached_at = float(entry.get("cached_at") or 0.0)
        if (time.time() - cached_at) > _admin_cache_ttl_seconds():
            _admin_cache.pop(cache_key, None)
            return None
        payload = entry.get("payload")
    if isinstance(payload, (dict, list)):
        try:
            return json.loads(json.dumps(payload, ensure_ascii=False))
        except Exception:
            return payload
    return payload


def _admin_cache_set(key: str, payload: Any) -> Any:
    cache_key = str(key or "").strip()
    if not cache_key:
        return payload
    if isinstance(payload, (dict, list)):
        try:
            clone = json.loads(json.dumps(payload, ensure_ascii=False))
        except Exception:
            clone = payload
    else:
        clone = payload
    with _admin_cache_lock:
        _admin_cache[cache_key] = {"cached_at": time.time(), "payload": clone}
    return payload


def _invalidate_admin_cache(prefix: str = "") -> None:
    wanted = str(prefix or "").strip()
    with _admin_cache_lock:
        if not wanted:
            _admin_cache.clear()
            return
        for key in list(_admin_cache.keys()):
            if key.startswith(wanted):
                _admin_cache.pop(key, None)


def _record_local_activity(
    *,
    actor_user_id: str,
    activity_type: str,
    activity_status: str = "completed",
    scope: str = "system",
    target_user_id: str | None = None,
    run_id: str | None = None,
    company_id: str | None = None,
    company_name: str | None = None,
    details: Dict[str, Any] | None = None,
    source: str = "server",
) -> None:
    if os.getenv("PYTEST_CURRENT_TEST") and str(os.getenv("ENABLE_SERVER_LOCAL_LOGS_IN_TESTS", "")).strip().lower() not in {"1", "true", "yes"}:
        return
    try:
        get_local_store_client().record_admin_activity(
            actor_user_id=actor_user_id,
            activity_type=activity_type,
            activity_status=activity_status,
            scope=scope,
            target_user_id=target_user_id,
            run_id=run_id,
            company_id=company_id,
            company_name=company_name,
            details=details,
            source=source,
        )
    except Exception as exc:
        logger.warning(f"Could not record local activity '{activity_type}': {exc}")


def _record_error_event(
    *,
    user_id: str | None,
    error_type: str,
    message: str,
    source: str = "server",
    run_id: str | None = None,
    company_id: str | None = None,
    company_name: str | None = None,
    details: Dict[str, Any] | None = None,
) -> None:
    if os.getenv("PYTEST_CURRENT_TEST") and str(os.getenv("ENABLE_SERVER_LOCAL_LOGS_IN_TESTS", "")).strip().lower() not in {"1", "true", "yes"}:
        return
    try:
        get_local_store_client().record_error_event(
            user_id=user_id,
            error_type=error_type,
            message=message,
            source=source,
            run_id=run_id,
            company_id=company_id,
            company_name=company_name,
            details=details,
        )
    except Exception as exc:
        logger.warning(f"Could not record error event '{error_type}': {exc}")


def _task_status_payload(task: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "task_id": task["task_id"],
        "status": task["status"],
        "company": task["company"],
        "created_at": task["created_at"],
        "completed_at": task.get("completed_at"),
        "error": task.get("error"),
    }


def _current_user_id(current_user: CurrentUser) -> str:
    try:
        return require_user_id(current_user.user_id, context="authenticated request")
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc


async def _authenticated_user_id(current_user: CurrentUser = Depends(require_permission("features:use"))) -> str:
    return _current_user_id(current_user)


async def _data_reader_user_id(current_user: CurrentUser = Depends(require_permission("data:read"))) -> str:
    return _current_user_id(current_user)


def _assert_task_owner(task: Dict[str, Any], task_id: str, user_id: str) -> None:
    owner = str(task.get("user_id") or "").strip()
    if owner != user_id:
        raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found.")


def _task_summary_payload(task: Dict[str, Any]) -> Dict[str, Any]:
    result = task.get("result") or {}
    golden_record = result.get("golden_record") or {}
    if isinstance(golden_record, dict):
        fields = len(golden_record)
    elif isinstance(golden_record, list):
        fields = len(golden_record)
    else:
        fields = 0
    test_results = result.get("test_results") or {}
    return {
        **_task_status_payload(task),
        "fields": fields,
        "passed": int(test_results.get("passed") or 0),
        "failed": int(test_results.get("failed") or 0),
        "skipped": int(test_results.get("skipped") or 0),
        "golden_record_path": result.get("golden_record_path"),
        "validation_report_path": result.get("validation_report_path"),
        "pytest_report_path": result.get("pytest_report_path"),
        "chunk_record_path": result.get("chunk_record_path"),
    }


def _infer_source(line: str) -> str:
    lower = line.lower()
    if "[llm1" in lower:
        return "llm1"
    if "[llm2" in lower:
        return "llm2"
    if "[llm3" in lower:
        return "llm3"
    if "[agent1" in lower:
        return "agent1"
    if "[agent2" in lower:
        return "agent2"
    if "[agent3" in lower:
        return "agent3"
    if "[agent4" in lower:
        return "agent4"
    if "[router" in lower:
        return "router"
    if "[retry" in lower:
        return "retry"
    return "pipeline"


def _infer_level(line: str) -> str:
    lower = line.lower()
    if "❌" in line or "failed" in lower or "error" in lower:
        return "error"
    if "⚠" in line or "warning" in lower:
        return "warn"
    if "✅" in line or " passed" in lower or " complete" in lower or " done" in lower:
        return "success"
    return "info"


def _append_event(task_id: str, message: str, source: str = "server", level: str = "info") -> None:
    clean = message.strip()
    if not clean:
        return
    with _tasks_lock:
        task = _tasks.get(task_id)
        if not task:
            return
        seq = int(task.get("event_seq", 0)) + 1
        task["event_seq"] = seq
        task.setdefault("events", []).append(
            {
                "seq": seq,
                "time": _utc_now_iso(),
                "source": source,
                "level": level,
                "message": clean,
            }
        )
        if len(task["events"]) > 4000:
            task["events"] = task["events"][-2000:]


class _TaskEventStream(io.TextIOBase):
    def __init__(self, task_id: str, sink: io.TextIOBase, fallback_level: str = "info"):
        self.task_id = task_id
        self.sink = sink
        self.fallback_level = fallback_level
        self._buffer = ""

    def write(self, data: str) -> int:
        if not data:
            return 0
        self.sink.write(data)
        self._buffer += data
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._emit(line)
        return len(data)

    def flush(self) -> None:
        if self._buffer:
            self._emit(self._buffer)
            self._buffer = ""
        self.sink.flush()

    def _emit(self, line: str) -> None:
        clean = line.strip()
        if not clean:
            return
        level = _infer_level(clean) if self.fallback_level == "info" else self.fallback_level
        _append_event(self.task_id, clean, source=_infer_source(clean), level=level)


def _safe_output_path(path: str) -> Path:
    raw = Path(path).expanduser()
    resolved = raw.resolve() if raw.is_absolute() else (BASE_DIR / raw).resolve()
    output_root = OUTPUT_DIR.resolve()
    if resolved != output_root and output_root not in resolved.parents:
        raise HTTPException(status_code=403, detail="Only files inside output/ are allowed.")
    return resolved


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _cached_output_metrics(path: str, metric_type: str) -> Dict[str, Any] | None:
    normalized_path = str(path or "").strip()
    if not normalized_path:
        return None
    candidate = Path(normalized_path)
    if not candidate.exists():
        return None
    try:
        stats = candidate.stat()
        signature = f"{stats.st_mtime_ns}:{stats.st_size}:{metric_type}"
    except OSError:
        return None

    with _admin_cache_lock:
        cached = _output_file_metrics_cache.get(normalized_path)
        if isinstance(cached, dict) and str(cached.get("signature") or "") == signature:
            payload = cached.get("payload")
            return dict(payload) if isinstance(payload, dict) else None

    try:
        payload = _read_json(candidate)
    except Exception:
        return None

    if metric_type == "golden":
        if isinstance(payload, dict):
            metrics = {"fields": len(payload)}
        elif isinstance(payload, list):
            metrics = {"fields": len(payload)}
        else:
            metrics = {"fields": None}
    elif metric_type == "pytest":
        if isinstance(payload, dict):
            metrics = {
                "passed": int(payload.get("passed") or 0),
                "failed": int(payload.get("failed") or 0),
                "skipped": int(payload.get("skipped") or 0),
            }
        else:
            metrics = {"passed": None, "failed": None, "skipped": None}
    else:
        metrics = {}

    with _admin_cache_lock:
        _output_file_metrics_cache[normalized_path] = {"signature": signature, "payload": dict(metrics)}
    return metrics


def _stamp_to_iso(stamp: str) -> str:
    try:
        dt = datetime.strptime(stamp, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except ValueError:
        return ""


def _serialize_current_user(current_user: CurrentUser) -> Dict[str, Any]:
    return {
        "user_id": current_user.user_id,
        "name": current_user.name,
        "email": current_user.email,
        "role": current_user.role,
        "approval_status": current_user.approval_status,
        "permissions": get_role_permissions(current_user.role),
    }


def _log_supabase_activity(
    *,
    user_id: str,
    activity_type: str,
    run_id: str | None = None,
    company_id: str | None = None,
    company_name: str | None = None,
    activity_status: str | None = None,
    details: Dict[str, Any] | None = None,
) -> None:
    scope = "pipeline"
    lowered = str(activity_type or "").strip().lower()
    if lowered.startswith("signup") or lowered.startswith("user_") or lowered.startswith("role_"):
        scope = "admin"
    elif lowered.startswith("data_fetch") or lowered.startswith("data_store"):
        scope = "storage"
    _record_local_activity(
        actor_user_id=str(user_id or "").strip(),
        activity_type=activity_type,
        activity_status=activity_status or "completed",
        scope=scope,
        run_id=run_id,
        company_id=company_id,
        company_name=company_name,
        details=details,
        source="server",
    )
    try:
        get_supabase_client().log_pipeline_activity(
            user_id=user_id,
            activity_type=activity_type,
            run_id=run_id,
            company_id=company_id,
            company_name=company_name,
            activity_status=activity_status,
            details=details,
        )
    except Exception as exc:
        logger.warning(f"Could not record Supabase activity '{activity_type}': {exc}")


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Company Intelligence API starting up")
    try:
        bootstrap_admin_from_env()
    except Exception as exc:
        logger.warning(f"Admin bootstrap skipped: {exc}")
    yield
    logger.info("Company Intelligence API shutting down")


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Company Intelligence API",
    description="LangGraph multi-agent pipeline for company research",
    version="1.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    # Vite dev server (5173) and Docker/Nginx (3000).
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3001",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)


@app.middleware("http")
async def error_monitoring_middleware(request: Request, call_next):
    started = time.time()
    try:
        response = await call_next(request)
    except Exception as exc:
        _record_error_event(
            user_id=None,
            error_type="unhandled_exception",
            message=str(exc),
            source="api_middleware",
            details={"path": request.url.path, "method": request.method},
        )
        raise

    if response.status_code >= 500:
        _record_error_event(
            user_id=None,
            error_type="http_5xx",
            message=f"{request.method} {request.url.path} -> {response.status_code}",
            source="api_middleware",
            details={
                "path": request.url.path,
                "method": request.method,
                "status_code": response.status_code,
                "duration_ms": round((time.time() - started) * 1000, 2),
            },
        )
    return response


# ── Models ────────────────────────────────────────────────────────────────────
class RunRequest(BaseModel):
    company: str


class SignupRequest(BaseModel):
    name: str
    email: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class VerifyUserRequest(BaseModel):
    verified: bool = True
    note: str | None = None


class UpdateUserRoleRequest(BaseModel):
    role: str


class SimilarSearchRequest(BaseModel):
    query: str
    top_k: int = 5
    top_k_chunks: int = 200
    exclude_company: Optional[str] = None
    include_full_data: bool = True
    filters: Optional[Dict[str, Any]] = None


class TaskStatus(BaseModel):
    task_id: str
    status: str
    company: str
    created_at: str
    completed_at: Optional[str] = None
    error: Optional[str] = None


class InnovationClusterRequest(BaseModel):
    company_ids: Optional[list[str]] = None
    company_names: Optional[list[str]] = None
    limit: Optional[int] = None
    algorithm: str = "auto"
    reduction: str = "auto"
    n_clusters: Optional[int] = None
    min_cluster_size: int = 2
    include_noise: bool = False


class AnalyticsRequest(BaseModel):
    company_ids: Optional[list[str]] = None
    company_names: Optional[list[str]] = None
    limit: Optional[int] = None
    top_n: int = 5


class PredictiveAnalyticsRequest(AnalyticsRequest):
    min_training_samples: int = 6


def _run_company_search(body: SimilarSearchRequest, *, user_id: str) -> Dict[str, Any]:
    user_id = require_user_id(user_id, context="company search route")
    from search.search_service import get_search_service

    try:
        result = get_search_service().search_companies(
            query=body.query,
            top_k=body.top_k,
            top_k_chunks=body.top_k_chunks,
            exclude_company=body.exclude_company or "",
            include_full_data=body.include_full_data,
            filters=body.filters,
            user_id=user_id,
        )
        _log_supabase_activity(
            user_id=user_id,
            activity_type="data_fetch_search",
            activity_status="completed",
            details={
                "query": body.query,
                "top_k": body.top_k,
                "result_count": len(result.get("results") or []),
            },
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _run_company_similarity(body: SimilarSearchRequest, *, user_id: str) -> Dict[str, Any]:
    user_id = require_user_id(user_id, context="company similarity route")
    from search.search_service import CompanyNotFoundError, get_search_service

    try:
        result = get_search_service().find_similar_companies(
            company=body.query,
            top_k=body.top_k,
            top_k_chunks=body.top_k_chunks,
            exclude_company=body.exclude_company or "",
            include_full_data=body.include_full_data,
            filters=body.filters,
            user_id=user_id,
        )
        _log_supabase_activity(
            user_id=user_id,
            activity_type="data_fetch_similarity",
            activity_status="completed",
            details={
                "company": body.query,
                "top_k": body.top_k,
                "result_count": len(result.get("results") or []),
            },
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except CompanyNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _run_innovation_clustering(body: InnovationClusterRequest, *, user_id: str) -> Dict[str, Any]:
    user_id = require_user_id(user_id, context="innovation clustering route")
    from ml.clustering.cluster_pipeline import get_innovation_cluster_pipeline

    try:
        result = get_innovation_cluster_pipeline().run(
            company_ids=body.company_ids or [],
            company_names=body.company_names or [],
            limit=body.limit,
            algorithm=body.algorithm,
            reduction=body.reduction,
            n_clusters=body.n_clusters,
            min_cluster_size=body.min_cluster_size,
            include_noise=body.include_noise,
            user_id=user_id,
        )
        _log_supabase_activity(
            user_id=user_id,
            activity_type="data_fetch_innovation_clusters",
            activity_status="completed",
            details={"cluster_count": len(result.get("clusters") or [])},
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _run_descriptive_analytics(body: AnalyticsRequest, *, user_id: str) -> Dict[str, Any]:
    user_id = require_user_id(user_id, context="descriptive analytics route")
    from ml.analytics.descriptive_analytics import DescriptiveAnalyticsService

    try:
        result = DescriptiveAnalyticsService().run(
            company_ids=body.company_ids or [],
            company_names=body.company_names or [],
            limit=body.limit,
            top_n=body.top_n,
            user_id=user_id,
        )
        _log_supabase_activity(
            user_id=user_id,
            activity_type="data_fetch_descriptive_analytics",
            activity_status="completed",
            details={"company_count": len(result.get("companies") or [])},
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _run_predictive_analytics(body: PredictiveAnalyticsRequest, *, user_id: str) -> Dict[str, Any]:
    user_id = require_user_id(user_id, context="predictive analytics route")
    from ml.analytics.predictive_models import PredictiveAnalyticsService

    try:
        result = PredictiveAnalyticsService().run(
            company_ids=body.company_ids or [],
            company_names=body.company_names or [],
            limit=body.limit,
            top_n=body.top_n,
            min_training_samples=body.min_training_samples,
            user_id=user_id,
        )
        _log_supabase_activity(
            user_id=user_id,
            activity_type="data_fetch_predictive_analytics",
            activity_status="completed",
            details={"company_count": len(result.get("companies") or [])},
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _admin_list_users(*, limit: int = 1000, approval_status: str | None = None) -> list[Dict[str, Any]]:
    store = get_supabase_client()
    if hasattr(store, "list_users"):
        try:
            rows = store.list_users(limit=limit, approval_status=approval_status)
            return [serialize_user(row) for row in rows if isinstance(row, dict)]
        except Exception as exc:
            logger.warning(f"Admin user listing via list_users failed: {exc}")

    statuses = [approval_status] if approval_status else ["pending", "approved", "rejected"]
    merged: Dict[str, Dict[str, Any]] = {}
    for status_value in statuses:
        if not status_value:
            continue
        try:
            for row in store.list_users_by_approval_status(status_value):
                if not isinstance(row, dict):
                    continue
                user = serialize_user(row)
                user_id = str(user.get("user_id") or "").strip()
                if user_id:
                    merged[user_id] = user
        except Exception as exc:
            logger.warning(f"Fallback user listing for status='{status_value}' failed: {exc}")
    rows = list(merged.values())
    rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
    return rows[: max(1, min(int(limit or 1000), 5000))]


def _admin_recent_activity(limit: int = 300) -> list[Dict[str, Any]]:
    normalized_limit = max(1, min(int(limit or 300), 2000))
    cache_key = f"admin:activity:{normalized_limit}"
    cached = _admin_cache_get(cache_key)
    if isinstance(cached, list):
        return cached

    rows: list[Dict[str, Any]] = []
    try:
        rows.extend(get_local_store_client().list_activity_logs(limit=normalized_limit))
    except Exception as exc:
        logger.warning(f"Local activity listing failed: {exc}")
    try:
        remote_rows = get_supabase_client().list_pipeline_activity(limit=normalized_limit)
        for row in remote_rows:
            if not isinstance(row, dict):
                continue
            rows.append(
                {
                    "activity_id": str(row.get("activity_id") or ""),
                    "created_at": row.get("created_at"),
                    "scope": "pipeline",
                    "source": "supabase",
                    "actor_user_id": str(row.get("user_id") or ""),
                    "activity_type": str(row.get("activity_type") or ""),
                    "activity_status": str(row.get("activity_status") or ""),
                    "run_id": row.get("run_id"),
                    "company_id": row.get("company_id"),
                    "company_name": row.get("company_name"),
                    "details": row.get("details") if isinstance(row.get("details"), dict) else {},
                }
            )
    except Exception as exc:
        logger.warning(f"Supabase activity listing failed: {exc}")
    rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
    deduped: list[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        dedupe_key = "|".join(
            [
                str(row.get("activity_id") or ""),
                str(row.get("created_at") or ""),
                str(row.get("actor_user_id") or ""),
                str(row.get("activity_type") or ""),
                str(row.get("run_id") or ""),
            ]
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        deduped.append(row)
        if len(deduped) >= normalized_limit:
            break
    _admin_cache_set(cache_key, deduped)
    return deduped


def _admin_error_logs(limit: int = 200) -> list[Dict[str, Any]]:
    normalized_limit = max(1, min(int(limit or 200), 2000))
    cache_key = f"admin:errors:{normalized_limit}"
    cached = _admin_cache_get(cache_key)
    if isinstance(cached, list):
        return cached
    rows: list[Dict[str, Any]] = []
    try:
        rows.extend(get_local_store_client().list_error_logs(limit=normalized_limit))
    except Exception as exc:
        logger.warning(f"Error log listing failed: {exc}")
    rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
    rows = rows[:normalized_limit]
    _admin_cache_set(cache_key, rows)
    return rows


def _admin_pipeline_runs(limit: int = 300) -> list[Dict[str, Any]]:
    normalized_limit = max(1, min(int(limit or 300), 5000))
    cache_key = f"admin:runs:{normalized_limit}"
    cached = _admin_cache_get(cache_key)
    if isinstance(cached, list):
        return cached
    rows: list[Dict[str, Any]] = []
    try:
        rows = get_local_store_client().list_all_pipeline_runs(limit=normalized_limit)
    except Exception as exc:
        logger.warning(f"Admin pipeline run listing failed: {exc}")
    _admin_cache_set(cache_key, rows)
    return rows


def _build_rerun_version_rows(runs: list[Dict[str, Any]], *, limit: int) -> list[Dict[str, Any]]:
    normalized_limit = max(1, min(int(limit or 300), 5000))
    grouped: Dict[str, list[Dict[str, Any]]] = {}
    for run in runs:
        if not isinstance(run, dict):
            continue
        company_id = str(run.get("company_id") or "").strip().lower()
        company_name = str(run.get("company_name") or "").strip()
        if not company_id and not company_name:
            continue
        group_key = company_id or company_name.lower()
        grouped.setdefault(group_key, []).append(run)

    reruns: list[Dict[str, Any]] = []
    for group_runs in grouped.values():
        ordered = sorted(
            group_runs,
            key=lambda row: str(row.get("completed_at") or row.get("started_at") or row.get("created_at") or ""),
        )
        if len(ordered) <= 1:
            continue
        previous = ordered[0]
        for idx, current in enumerate(ordered[1:], start=2):
            run_id = str(current.get("run_id") or "").strip()
            company_id = str(current.get("company_id") or "").strip()
            company_name = str(current.get("company_name") or "").strip()
            created_at = str(current.get("completed_at") or current.get("started_at") or current.get("created_at") or "")
            reruns.append(
                {
                    "version_id": f"rerun-{(company_id or company_name or 'company')}-{idx:05d}-{run_id[:8]}",
                    "version_number": idx,
                    "version_kind": "rerun",
                    "created_at": created_at,
                    "run_id": run_id,
                    "user_id": str(current.get("user_id") or "").strip(),
                    "user_name": str(current.get("user_name") or "").strip(),
                    "company_id": company_id,
                    "company_name": company_name,
                    "status": str(current.get("status") or "").strip().lower(),
                    "previous_run_id": str(previous.get("run_id") or "").strip(),
                    "previous_created_at": str(
                        previous.get("completed_at")
                        or previous.get("started_at")
                        or previous.get("created_at")
                        or ""
                    ),
                }
            )
            previous = current

    reruns.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
    return reruns[:normalized_limit]


def _admin_data_versions(limit: int = 300) -> list[Dict[str, Any]]:
    normalized_limit = max(1, min(int(limit or 300), 5000))
    cache_key = f"admin:versions:{normalized_limit}"
    cached = _admin_cache_get(cache_key)
    if isinstance(cached, list):
        return cached
    rows: list[Dict[str, Any]] = []
    try:
        all_runs = get_local_store_client().list_all_pipeline_runs(limit=50000)
        rows = _build_rerun_version_rows(all_runs, limit=normalized_limit)
    except Exception as exc:
        logger.warning(f"Admin data version listing failed: {exc}")
    _admin_cache_set(cache_key, rows)
    return rows


def _admin_dashboard_summary() -> Dict[str, Any]:
    cache_key = "admin:dashboard:summary"
    cached = _admin_cache_get(cache_key)
    if isinstance(cached, dict):
        return cached

    users = _admin_list_users(limit=5000)
    runs = _admin_pipeline_runs(limit=5000)
    activity = _admin_recent_activity(limit=2000)
    errors = _admin_error_logs(limit=1000)
    versions = _admin_data_versions(limit=5000)

    now = datetime.now(timezone.utc)
    day_ago = now.timestamp() - 86400

    def _is_last_day(ts: Any) -> bool:
        raw = str(ts or "").strip()
        if not raw:
            return False
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp() >= day_ago
        except ValueError:
            return False

    pending = sum(1 for user in users if str(user.get("approval_status") or "") == "pending")
    approved = sum(1 for user in users if str(user.get("approval_status") or "") == "approved")
    rejected = sum(1 for user in users if str(user.get("approval_status") or "") == "rejected")
    admins = sum(1 for user in users if str(user.get("role") or "") == "admin")

    status_counts = {"running": 0, "completed": 0, "failed": 0, "other": 0}
    for run in runs:
        status_value = str(run.get("status") or "").strip().lower()
        if status_value in {"running", "pending"}:
            status_counts["running"] += 1
        elif status_value in {"completed", "done"}:
            status_counts["completed"] += 1
        elif status_value in {"failed", "error"}:
            status_counts["failed"] += 1
        else:
            status_counts["other"] += 1

    actor_counts: Dict[str, int] = {}
    for row in activity:
        actor = str(row.get("actor_user_id") or "").strip()
        if not actor:
            continue
        actor_counts[actor] = actor_counts.get(actor, 0) + 1
    top_actors = sorted(actor_counts.items(), key=lambda pair: pair[1], reverse=True)[:10]

    storage_summary = {}
    try:
        storage_summary = get_local_store_client().get_storage_summary()
    except Exception as exc:
        logger.warning(f"Storage summary unavailable: {exc}")

    payload = {
        "generated_at": _utc_now_iso(),
        "users": {
            "total": len(users),
            "admins": admins,
            "approved": approved,
            "pending": pending,
            "rejected": rejected,
            "last_24h_signups": sum(1 for user in users if _is_last_day(user.get("created_at"))),
        },
        "pipelines": {
            "total_runs": len(runs),
            "running": status_counts["running"],
            "completed": status_counts["completed"],
            "failed": status_counts["failed"],
            "last_24h_runs": sum(1 for run in runs if _is_last_day(run.get("started_at") or run.get("created_at"))),
            "last_24h_completed": sum(1 for run in runs if _is_last_day(run.get("completed_at"))),
        },
        "storage": {
            **(storage_summary if isinstance(storage_summary, dict) else {}),
            "version_events": len(versions),
        },
        "activity": {
            "total_events": len(activity),
            "last_24h_events": sum(1 for row in activity if _is_last_day(row.get("created_at"))),
            "top_actors": [{"user_id": uid, "events": count} for uid, count in top_actors],
        },
        "errors": {
            "total": len(errors),
            "last_24h": sum(1 for row in errors if _is_last_day(row.get("created_at"))),
            "latest": errors[0] if errors else None,
        },
    }
    _admin_cache_set(cache_key, payload)
    return payload


# ── Background worker ─────────────────────────────────────────────────────────
def _execute_pipeline(task_id: str, company: str, user_id: str, user_name: str = "") -> None:
    user_id = require_user_id(user_id, context="background pipeline execution")
    with _pipeline_run_lock:
        with _tasks_lock:
            task = _tasks.get(task_id)
            if not task:
                return
            task["status"] = "running"
        _append_event(task_id, f"Pipeline started for company '{company}'", source="server")
        logger.info(f"[task={task_id}] Pipeline started for company='{company}'")

        stream_out = _TaskEventStream(task_id, sys.stdout, fallback_level="info")
        stream_err = _TaskEventStream(task_id, sys.stderr, fallback_level="error")

        try:
            from main import run_full_pipeline

            with redirect_stdout(stream_out), redirect_stderr(stream_err):
                result = run_full_pipeline(company, user_id=user_id, user_name=user_name)
            stream_out.flush()
            stream_err.flush()

            with _tasks_lock:
                task = _tasks.get(task_id)
                if task is None:
                    return
                task.update(
                    status="done",
                    result=result,
                    completed_at=_utc_now_iso(),
                    error=None,
                )

            _append_event(task_id, "Pipeline complete", source="server", level="success")
            if result.get("golden_record_path"):
                _append_event(
                    task_id,
                    f"Golden record saved: {Path(result['golden_record_path']).name}",
                    source="agent3",
                    level="success",
                )
            if result.get("chunk_record_path"):
                _append_event(
                    task_id,
                    f"Semantic chunks saved: {Path(result['chunk_record_path']).name}",
                    source="agent3",
                    level="success",
                )
            tests = result.get("test_results") or {}
            if tests:
                _append_event(
                    task_id,
                    f"Tests: {tests.get('passed', 0)} passed / {tests.get('failed', 0)} failed / {tests.get('skipped', 0)} skipped",
                    source="agent4",
                    level="warn" if tests.get("failed", 0) else "success",
                )
            golden_record = result.get("golden_record") or {}
            if isinstance(golden_record, dict):
                field_count = len(golden_record)
            elif isinstance(golden_record, list):
                field_count = len(golden_record)
            else:
                field_count = 0
            logger.info(
                f"[task={task_id}] Pipeline complete — {field_count} fields"
            )
            _invalidate_admin_cache("admin:")
        except Exception as exc:
            stream_out.flush()
            stream_err.flush()
            with _tasks_lock:
                task = _tasks.get(task_id)
                if task is not None:
                    task.update(
                        status="failed",
                        error=str(exc),
                        completed_at=_utc_now_iso(),
                    )
            _append_event(task_id, f"Pipeline failed: {exc}", source="server", level="error")
            logger.error(f"[task={task_id}] Pipeline failed: {exc}", exc_info=True)
            _record_error_event(
                user_id=user_id,
                error_type="pipeline_failed",
                message=str(exc),
                source="pipeline_worker",
                run_id=task_id,
                company_name=company,
                details={"task_id": task_id},
            )
            _invalidate_admin_cache("admin:")


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"message": "Company Intelligence API is running", "docs": "/docs"}


@app.get("/health")
def health_check():
    with _tasks_lock:
        tasks_in_memory = len(_tasks)
    return {"status": "ok", "tasks_in_memory": tasks_in_memory}


@app.post("/auth/signup", status_code=201)
def signup(body: SignupRequest):
    name = str(body.name or "").strip()
    email = normalize_email(body.email)
    password = str(body.password or "")

    if not name:
        raise HTTPException(status_code=422, detail="'name' must not be empty.")
    if not email:
        raise HTTPException(status_code=422, detail="'email' must not be empty.")
    try:
        validate_password_strength(password)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    store = get_supabase_client()
    existing = store.get_user_by_email(email)
    if existing:
        raise HTTPException(status_code=409, detail="An account with this email already exists.")

    created = store.create_user(
        name=name,
        email=email,
        password_hash=hash_password(password),
        role="user",
        approval_status="pending",
        session_nonce=generate_session_nonce(),
    )
    user = created or store.get_user_by_email(email)
    if not user:
        raise HTTPException(status_code=500, detail="Unable to create user account.")

    _log_supabase_activity(
        user_id=str(user.get("user_id") or ""),
        activity_type="signup_requested",
        activity_status="pending",
        details={"email": email},
    )
    _invalidate_admin_cache("admin:")
    return {
        "message": "Request has been sent to admin for approval.",
        "user": serialize_user(user),
    }


@app.post("/auth/login")
def login(body: LoginRequest, response: Response):
    email = normalize_email(body.email)
    password = str(body.password or "")
    store = get_supabase_client()
    user = store.get_user_by_email(email)

    if not user or not verify_password(password, str(user.get("password") or "")):
        raise HTTPException(status_code=401, detail="Invalid email or password.")

    approval_status = str(user.get("approval_status") or "").strip().lower()
    if approval_status == "pending":
        raise HTTPException(status_code=403, detail=PENDING_APPROVAL_MESSAGE)
    if approval_status == "rejected":
        raise HTTPException(status_code=403, detail=REJECTED_ACCOUNT_MESSAGE)

    session_nonce = generate_session_nonce()
    updated = store.update_user(
        str(user.get("user_id") or ""),
        session_nonce=session_nonce,
        last_login_at=_utc_now_iso(),
    ) or user
    token = create_access_token(updated, session_nonce=session_nonce)
    set_auth_cookie(response, token)
    _log_supabase_activity(
        user_id=str(updated.get("user_id") or ""),
        activity_type="login",
        activity_status="success",
    )
    _invalidate_admin_cache("admin:")
    return {"user": serialize_user(updated)}


@app.post("/auth/logout")
def logout(response: Response, current_user: CurrentUser | None = Depends(get_optional_current_user)):
    if current_user is not None:
        try:
            get_supabase_client().update_user(
                current_user.user_id,
                session_nonce=generate_session_nonce(),
            )
            _log_supabase_activity(
                user_id=current_user.user_id,
                activity_type="logout",
                activity_status="success",
            )
        except Exception as exc:
            logger.warning(f"Logout session rotation failed: {exc}")
    clear_auth_cookie(response)
    _invalidate_admin_cache("admin:")
    return {"message": "Signed out."}


@app.get("/auth/me")
async def get_authenticated_user(current_user: CurrentUser = Depends(get_current_user)):
    return {"user": _serialize_current_user(current_user)}


@app.get("/auth/admin/users/pending")
def list_pending_users(_admin: CurrentUser = Depends(require_admin)):
    rows = [serialize_user(row) for row in get_supabase_client().list_users_by_approval_status("pending")]
    return {"users": rows}


@app.get("/auth/admin/users")
def list_users(
    approval_status: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
    _admin: CurrentUser = Depends(require_admin),
):
    status_value = str(approval_status or "").strip().lower() or None
    rows = _admin_list_users(limit=limit, approval_status=status_value)
    return {"users": rows, "count": len(rows)}


@app.post("/auth/admin/users/{target_user_id}/approve")
def approve_user(target_user_id: str, admin_user: CurrentUser = Depends(require_admin)):
    store = get_supabase_client()
    user = store.get_user_by_id(target_user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    updated = store.update_user(
        target_user_id,
        approval_status="approved",
        verification_status=str(user.get("verification_status") or "verified").strip().lower() or "verified",
        verified_at=user.get("verified_at") or _utc_now_iso(),
        verified_by=user.get("verified_by") or admin_user.user_id,
    ) or user
    _log_supabase_activity(
        user_id=admin_user.user_id,
        activity_type="user_approved",
        activity_status="completed",
        details={"target_user_id": target_user_id, "target_email": updated.get("email")},
    )
    _record_local_activity(
        actor_user_id=admin_user.user_id,
        scope="admin",
        activity_type="user_approved",
        target_user_id=target_user_id,
        activity_status="completed",
        details={"target_email": updated.get("email")},
    )
    _invalidate_admin_cache("admin:")
    return {"user": serialize_user(updated)}


@app.post("/auth/admin/users/{target_user_id}/reject")
def reject_user(target_user_id: str, admin_user: CurrentUser = Depends(require_admin)):
    store = get_supabase_client()
    user = store.get_user_by_id(target_user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    updated = store.update_user(target_user_id, approval_status="rejected") or user
    _log_supabase_activity(
        user_id=admin_user.user_id,
        activity_type="user_rejected",
        activity_status="completed",
        details={"target_user_id": target_user_id, "target_email": updated.get("email")},
    )
    _record_local_activity(
        actor_user_id=admin_user.user_id,
        scope="admin",
        activity_type="user_rejected",
        target_user_id=target_user_id,
        activity_status="completed",
        details={"target_email": updated.get("email")},
    )
    _invalidate_admin_cache("admin:")
    return {"user": serialize_user(updated)}


@app.post("/auth/admin/users/{target_user_id}/verify")
def verify_user(
    target_user_id: str,
    body: VerifyUserRequest,
    admin_user: CurrentUser = Depends(require_admin),
):
    store = get_supabase_client()
    user = store.get_user_by_id(target_user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    verified = bool(body.verified)
    verification_status = "verified" if verified else "unverified"
    updated = store.update_user(
        target_user_id,
        verification_status=verification_status,
        verified_at=_utc_now_iso() if verified else None,
        verified_by=admin_user.user_id if verified else None,
        approval_note=(body.note or "").strip() or None,
    ) or user
    _log_supabase_activity(
        user_id=admin_user.user_id,
        activity_type="user_verified" if verified else "user_unverified",
        activity_status="completed",
        details={
            "target_user_id": target_user_id,
            "target_email": updated.get("email"),
            "verification_status": verification_status,
            "note": (body.note or "").strip(),
        },
    )
    _record_local_activity(
        actor_user_id=admin_user.user_id,
        scope="admin",
        activity_type="user_verified" if verified else "user_unverified",
        target_user_id=target_user_id,
        activity_status="completed",
        details={
            "verification_status": verification_status,
            "target_email": updated.get("email"),
            "note": (body.note or "").strip(),
        },
    )
    _invalidate_admin_cache("admin:")
    return {"user": serialize_user(updated)}


@app.post("/auth/admin/users/{target_user_id}/role")
def update_user_role(
    target_user_id: str,
    body: UpdateUserRoleRequest,
    admin_user: CurrentUser = Depends(require_admin),
):
    new_role = str(body.role or "").strip().lower()
    if new_role not in {"user", "admin"}:
        raise HTTPException(status_code=422, detail="Role must be either 'user' or 'admin'.")
    if target_user_id == admin_user.user_id and new_role != "admin":
        raise HTTPException(status_code=409, detail="You cannot remove your own admin role.")

    store = get_supabase_client()
    user = store.get_user_by_id(target_user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    updated = store.update_user(target_user_id, role=new_role) or user
    _log_supabase_activity(
        user_id=admin_user.user_id,
        activity_type="role_updated",
        activity_status="completed",
        details={
            "target_user_id": target_user_id,
            "target_email": updated.get("email"),
            "new_role": new_role,
        },
    )
    _record_local_activity(
        actor_user_id=admin_user.user_id,
        scope="admin",
        activity_type="role_updated",
        target_user_id=target_user_id,
        activity_status="completed",
        details={"new_role": new_role, "target_email": updated.get("email")},
    )
    _invalidate_admin_cache("admin:")
    return {"user": serialize_user(updated)}


@app.get("/auth/admin/dashboard")
def get_admin_dashboard(_admin: CurrentUser = Depends(require_admin)):
    return _admin_dashboard_summary()


@app.get("/auth/admin/pipelines/runs")
def get_admin_pipeline_runs(
    limit: int = Query(default=300, ge=1, le=5000),
    _admin: CurrentUser = Depends(require_admin),
):
    rows = _admin_pipeline_runs(limit=limit)
    return {"runs": rows, "count": len(rows)}


@app.get("/auth/admin/activity-logs")
def get_admin_activity_logs(
    limit: int = Query(default=300, ge=1, le=2000),
    _admin: CurrentUser = Depends(require_admin),
):
    rows = _admin_recent_activity(limit=limit)
    return {"logs": rows, "count": len(rows)}


@app.get("/auth/admin/error-logs")
def get_admin_error_logs(
    limit: int = Query(default=200, ge=1, le=2000),
    _admin: CurrentUser = Depends(require_admin),
):
    rows = _admin_error_logs(limit=limit)
    return {"errors": rows, "count": len(rows)}


@app.get("/auth/admin/data-versions")
def get_admin_data_versions(
    limit: int = Query(default=300, ge=1, le=5000),
    _admin: CurrentUser = Depends(require_admin),
):
    rows = _admin_data_versions(limit=limit)
    return {"versions": rows, "count": len(rows)}


@app.get("/data/versions")
def list_user_data_versions(
    limit: int = Query(default=200, ge=1, le=2000),
    company_id: str = Query(default=""),
    company_name: str = Query(default=""),
    reruns_only: bool = Query(default=True),
    user_id: str = Depends(_data_reader_user_id),
):
    company_id_value = company_id.strip()
    company_name_value = company_name.strip().lower()
    if reruns_only:
        runs = get_local_store_client().list_pipeline_runs(user_id=user_id)
        if company_id_value or company_name_value:
            runs = [
                row
                for row in runs
                if (
                    (company_id_value and str(row.get("company_id") or "").strip() == company_id_value)
                    or (company_name_value and str(row.get("company_name") or "").strip().lower() == company_name_value)
                )
            ]
        rows = _build_rerun_version_rows(runs, limit=limit)
    else:
        rows = get_local_store_client().list_company_versions(
            user_id=user_id,
            company_id=company_id_value,
            company_name=company_name.strip(),
            limit=limit,
        )
    _record_local_activity(
        actor_user_id=user_id,
        scope="storage",
        activity_type="data_versions_viewed",
        details={
            "count": len(rows),
            "company_id": company_id_value,
            "company_name": company_name.strip(),
            "reruns_only": bool(reruns_only),
        },
    )
    return {"versions": rows, "count": len(rows)}


@app.post("/search-companies")
def search_companies(body: SimilarSearchRequest, user_id: str = Depends(_authenticated_user_id)):
    return _run_company_search(body, user_id=user_id)


@app.post("/search/similar")
def search_similar_companies(body: SimilarSearchRequest, user_id: str = Depends(_authenticated_user_id)):
    return _run_company_similarity(body, user_id=user_id)


@app.post("/ml/innovation-clusters")
def innovation_clusters(body: InnovationClusterRequest, user_id: str = Depends(_authenticated_user_id)):
    return _run_innovation_clustering(body, user_id=user_id)


@app.post("/ml/analytics/descriptive")
def descriptive_analytics(body: AnalyticsRequest, user_id: str = Depends(_authenticated_user_id)):
    return _run_descriptive_analytics(body, user_id=user_id)


@app.post("/ml/analytics/predictive")
def predictive_analytics(body: PredictiveAnalyticsRequest, user_id: str = Depends(_authenticated_user_id)):
    return _run_predictive_analytics(body, user_id=user_id)


@app.post("/run", response_model=TaskStatus, status_code=202)
def run_pipeline(
    body: RunRequest,
    background_tasks: BackgroundTasks,
    current_user: CurrentUser = Depends(require_permission("pipeline:run")),
):
    user_id = _current_user_id(current_user)
    user_name = str(current_user.name or "").strip()
    company = body.company.strip()
    if not company:
        raise HTTPException(status_code=422, detail="'company' must not be empty.")

    task_id = str(uuid.uuid4())
    created = _utc_now_iso()
    task = {
        "task_id": task_id,
        "user_id": user_id,
        "user_name": user_name,
        "status": "pending",
        "company": company,
        "created_at": created,
        "completed_at": None,
        "error": None,
        "result": None,
        "events": [],
        "event_seq": 0,
    }

    with _tasks_lock:
        _tasks[task_id] = task
    _append_event(task_id, f"Queued pipeline for company '{company}'", source="server")
    _record_local_activity(
        actor_user_id=user_id,
        scope="pipeline",
        activity_type="pipeline_queued",
        activity_status="pending",
        run_id=task_id,
        company_name=company,
        details={"task_id": task_id, "user_name": user_name},
    )
    _log_supabase_activity(
        user_id=user_id,
        activity_type="pipeline_queued",
        run_id=task_id,
        company_name=company,
        activity_status="pending",
        details={"user_name": user_name},
    )

    background_tasks.add_task(_execute_pipeline, task_id, company, user_id, user_name)
    logger.info(f"[task={task_id}] Queued for company='{company}'")
    _invalidate_admin_cache("admin:")
    return TaskStatus(**_task_status_payload(task))


@app.get("/status/{task_id}", response_model=TaskStatus)
def get_status(task_id: str, user_id: str = Depends(_authenticated_user_id)):
    with _tasks_lock:
        task = _tasks.get(task_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found.")
        _assert_task_owner(task, task_id, user_id)
        payload = _task_status_payload(task)
    return TaskStatus(**payload)


@app.get("/tasks")
def get_tasks(user_id: str = Depends(_authenticated_user_id)):
    with _tasks_lock:
        rows = [
            _task_summary_payload(task)
            for task in _tasks.values()
            if str(task.get("user_id") or "").strip() == user_id
        ]
    rows.sort(key=lambda x: x["created_at"], reverse=True)
    return rows


@app.get("/events/{task_id}")
def get_events(
    task_id: str,
    since: int = 0,
    limit: int = 200,
    user_id: str = Depends(_authenticated_user_id),
):
    if since < 0:
        since = 0
    if limit <= 0:
        limit = 200
    limit = min(limit, 1000)

    with _tasks_lock:
        task = _tasks.get(task_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found.")
        _assert_task_owner(task, task_id, user_id)
        status = task["status"]
        events = list(task.get("events", []))

    sliced = [event for event in events if int(event.get("seq", 0)) > since][:limit]
    cursor = since if not sliced else int(sliced[-1]["seq"])
    return {"task_id": task_id, "status": status, "cursor": cursor, "events": sliced}


@app.get("/result/{task_id}")
def get_result(task_id: str, user_id: str = Depends(_authenticated_user_id)):
    with _tasks_lock:
        task = _tasks.get(task_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found.")
        _assert_task_owner(task, task_id, user_id)
        status = task["status"]
        error = task.get("error")
        company = task["company"]
        result = task.get("result", {})

    if status == "pending":
        raise HTTPException(status_code=202, detail="Task is still pending.")
    if status == "running":
        raise HTTPException(status_code=202, detail="Task is still running.")
    if status == "failed":
        raise HTTPException(status_code=500, detail=error or "Unknown error")

    golden = result.get("golden_record") or {}
    if isinstance(golden, dict):
        golden_count = len(golden)
    elif isinstance(golden, list):
        golden_count = len(golden)
    else:
        golden_count = 0
    return JSONResponse(
        {
            "task_id": task_id,
            "company": company,
            "golden_record_path": result.get("golden_record_path"),
            "validation_report_path": result.get("validation_report_path"),
            "pytest_report_path": result.get("pytest_report_path"),
            "chunk_record_path": result.get("chunk_record_path"),
            "golden_record_count": golden_count,
            "test_results": result.get("test_results"),
        }
    )


@app.get("/outputs")
def list_outputs(limit: int = 200, user_id: str = Depends(_data_reader_user_id)):
    require_user_id(user_id, context="output listing route")
    if limit <= 0:
        limit = 200
    limit = min(limit, 1000)
    runs = get_local_store_client().list_pipeline_runs(user_id=user_id)
    items: list[Dict[str, Any]] = []
    seen_paths: set[str] = set()

    # Process runs from local_store (current/tracked runs)
    for run in runs:
        paths = run.get("paths") or {}
        if not isinstance(paths, dict) or not any(paths.values()):
            continue
        items.append(
            {
                "id": str(run.get("run_id") or ""),
                "company": str(run.get("company_name") or "").strip(),
                "company_slug": str(run.get("company_id") or "").strip(),
                "timestamp": str(run.get("completed_at") or run.get("started_at") or ""),
                "created_at": run.get("completed_at") or run.get("started_at"),
                "golden_record_path": paths.get("golden_record_path"),
                "validation_report_path": paths.get("validation_report_path"),
                "pytest_report_path": paths.get("pytest_report_path"),
                "semantic_chunks_path": paths.get("semantic_chunks_path"),
                "fields": None,
                "passed": None,
                "failed": None,
                "skipped": None,
            }
        )
        # Track paths we've already added
        for path in paths.values():
            if path:
                seen_paths.add(str(path))
    
    # Discover old output files that don't have run metadata
    try:
        if OUTPUT_DIR.exists():
            for json_file in sorted(OUTPUT_DIR.glob("*.json"), reverse=True):
                file_path_str = str(json_file)
                if file_path_str in seen_paths:
                    continue  # Already included from run metadata
                
                match = OUTPUT_FILE_RE.match(json_file.name)
                if not match:
                    continue  # Doesn't match expected pattern
                
                company = match.group("company")
                kind = match.group("kind")
                stamp = match.group("stamp")
                
                if kind == "golden_record":
                    # Try to find or create an entry for this company
                    existing = next((item for item in items if item["company_slug"].lower() == _slug(company).lower()), None)
                    if existing:
                        if not existing.get("golden_record_path"):
                            existing["golden_record_path"] = file_path_str
                            seen_paths.add(file_path_str)
                    else:
                        timestamp = _stamp_to_iso(stamp)
                        items.append(
                            {
                                "id": f"old-{_slug(company)}-{stamp}",
                                "company": company,
                                "company_slug": _slug(company),
                                "timestamp": timestamp,
                                "created_at": timestamp,
                                "golden_record_path": file_path_str,
                                "validation_report_path": None,
                                "pytest_report_path": None,
                                "semantic_chunks_path": None,
                                "fields": None,
                                "passed": None,
                                "failed": None,
                                "skipped": None,
                            }
                        )
                        seen_paths.add(file_path_str)
                elif kind == "validation_report" and any(item["company_slug"].lower() == _slug(company).lower() for item in items):
                    for item in items:
                        if item["company_slug"].lower() == _slug(company).lower() and not item.get("validation_report_path"):
                            item["validation_report_path"] = file_path_str
                            seen_paths.add(file_path_str)
                            break
                elif kind == "pytest_report" and any(item["company_slug"].lower() == _slug(company).lower() for item in items):
                    for item in items:
                        if item["company_slug"].lower() == _slug(company).lower() and not item.get("pytest_report_path"):
                            item["pytest_report_path"] = file_path_str
                            seen_paths.add(file_path_str)
                            break
                elif kind == "semantic_chunks" and any(item["company_slug"].lower() == _slug(company).lower() for item in items):
                    for item in items:
                        if item["company_slug"].lower() == _slug(company).lower() and not item.get("semantic_chunks_path"):
                            item["semantic_chunks_path"] = file_path_str
                            seen_paths.add(file_path_str)
                            break
    except Exception as exc:
        logger.warning(f"Error discovering old output files: {exc}")
    
    # Sort by timestamp (newest first)
    items.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    items = items[:limit]

    for item in items:
        golden_path = item.get("golden_record_path")
        pytest_path = item.get("pytest_report_path")

        if golden_path:
            metrics = _cached_output_metrics(str(golden_path), "golden")
            item["fields"] = metrics.get("fields") if isinstance(metrics, dict) else None

        if pytest_path:
            metrics = _cached_output_metrics(str(pytest_path), "pytest")
            if isinstance(metrics, dict):
                item["passed"] = metrics.get("passed")
                item["failed"] = metrics.get("failed")
                item["skipped"] = metrics.get("skipped")
            else:
                item["passed"] = None
                item["failed"] = None
                item["skipped"] = None

    _log_supabase_activity(
        user_id=user_id,
        activity_type="data_fetch_outputs",
        activity_status="completed",
        details={"result_count": len(items)},
    )
    return items


@app.get("/file")
def read_output_json(path: str, user_id: str = Depends(_data_reader_user_id)):
    require_user_id(user_id, context="output file route")
    target = _safe_output_path(path)
    run = get_local_store_client().get_pipeline_run_for_path(path=str(target), user_id=user_id)
    if not run:
        raise HTTPException(status_code=404, detail="Requested file is not available for this user.")
    if not target.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {target.name}")
    if target.suffix.lower() != ".json":
        raise HTTPException(status_code=415, detail="Only JSON files are supported.")
    try:
        payload = _read_json(target)
        _log_supabase_activity(
            user_id=user_id,
            activity_type="data_fetch_output_file",
            run_id=str(run.get("run_id") or ""),
            company_id=str(run.get("company_id") or ""),
            company_name=str(run.get("company_name") or ""),
            activity_status="completed",
            details={"path": str(target)},
        )
        return JSONResponse(payload)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid JSON: {exc}") from exc


# ── Dev entry point ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=True)
