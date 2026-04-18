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
import uuid
from contextlib import asynccontextmanager, redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from core.auth import CurrentUser, get_current_user
from core.user_scope import require_user_id
from logger import get_logger

load_dotenv()
logger = get_logger("server")

# ── Supabase configuration ─────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

from supabase import create_client as _create_supabase
_supabase = _create_supabase(SUPABASE_URL, SUPABASE_SERVICE_KEY)

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


# ── Helpers ────────────────────────────────────────────────────────────────────
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


async def _authenticated_user_id(current_user: CurrentUser = Depends(get_current_user)) -> str:
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


def _stamp_to_iso(stamp: str) -> str:
    try:
        dt = datetime.strptime(stamp, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except ValueError:
        return ""


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Company Intelligence API starting up")
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
)


# ── Models ────────────────────────────────────────────────────────────────────
class RunRequest(BaseModel):
    company: str


class AnalyzeRequest(BaseModel):
    company: str
    force_refresh: bool = False


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
        return get_search_service().search_companies(
            query=body.query,
            top_k=body.top_k,
            top_k_chunks=body.top_k_chunks,
            exclude_company=body.exclude_company or "",
            include_full_data=body.include_full_data,
            filters=body.filters,
            user_id=user_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _run_company_similarity(body: SimilarSearchRequest, *, user_id: str) -> Dict[str, Any]:
    user_id = require_user_id(user_id, context="company similarity route")
    from search.search_service import CompanyNotFoundError, get_search_service

    try:
        return get_search_service().find_similar_companies(
            company=body.query,
            top_k=body.top_k,
            top_k_chunks=body.top_k_chunks,
            exclude_company=body.exclude_company or "",
            include_full_data=body.include_full_data,
            filters=body.filters,
            user_id=user_id,
        )
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
        return get_innovation_cluster_pipeline().run(
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
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _run_descriptive_analytics(body: AnalyticsRequest, *, user_id: str) -> Dict[str, Any]:
    user_id = require_user_id(user_id, context="descriptive analytics route")
    from ml.analytics.descriptive_analytics import DescriptiveAnalyticsService

    try:
        return DescriptiveAnalyticsService().run(
            company_ids=body.company_ids or [],
            company_names=body.company_names or [],
            limit=body.limit,
            top_n=body.top_n,
            user_id=user_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _run_predictive_analytics(body: PredictiveAnalyticsRequest, *, user_id: str) -> Dict[str, Any]:
    user_id = require_user_id(user_id, context="predictive analytics route")
    from ml.analytics.predictive_models import PredictiveAnalyticsService

    try:
        return PredictiveAnalyticsService().run(
            company_ids=body.company_ids or [],
            company_names=body.company_names or [],
            limit=body.limit,
            top_n=body.top_n,
            min_training_samples=body.min_training_samples,
            user_id=user_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


# ── Background worker for versioning system ────────────────────────────────────
def _run_and_save(run_id: str, company: str, user_id: str) -> None:
    """
    Runs the full pipeline for a company and saves the result
    back to the company_runs row identified by run_id.
    On failure, marks the row with is_active=False and stores
    the error message in golden_record so the UI can display it.
    """
    user_id = require_user_id(user_id, context="versioning background run")
    try:
        from main import run_full_pipeline
        result = run_full_pipeline(company, user_id=user_id)
        _supabase.table("company_runs").update({
            "golden_record": result.get("golden_record"),
            "chunk_path": result.get("chunk_record_path"),
        }).eq("id", run_id).eq("run_by", user_id).execute()
    except Exception as exc:
        logger.error(f"Pipeline failed for run_id={run_id}: {exc}")
        _supabase.table("company_runs").update({
            "is_active": False,
            "golden_record": {"error": str(exc)},
        }).eq("id", run_id).eq("run_by", user_id).execute()


# ── Background worker ─────────────────────────────────────────────────────────
def _execute_pipeline(task_id: str, company: str, user_id: str) -> None:
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
                result = run_full_pipeline(company, user_id=user_id)
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


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"message": "Company Intelligence API is running", "docs": "/docs"}


@app.get("/health")
def health_check():
    with _tasks_lock:
        tasks_in_memory = len(_tasks)
    return {"status": "ok", "tasks_in_memory": tasks_in_memory}


@app.get("/auth/me")
async def get_authenticated_user(user_id: str = Depends(_authenticated_user_id)):
    return {"user_id": user_id}


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
    user_id: str = Depends(_authenticated_user_id),
):
    company = body.company.strip()
    if not company:
        raise HTTPException(status_code=422, detail="'company' must not be empty.")

    task_id = str(uuid.uuid4())
    created = _utc_now_iso()
    task = {
        "task_id": task_id,
        "user_id": user_id,
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

    background_tasks.add_task(_execute_pipeline, task_id, company, user_id)
    logger.info(f"[task={task_id}] Queued for company='{company}'")
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
def list_outputs(limit: int = 200, user_id: str = Depends(_authenticated_user_id)):
    require_user_id(user_id, context="output listing route")
    if limit <= 0:
        limit = 200
    limit = min(limit, 1000)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    grouped: Dict[str, Dict[str, Any]] = {}
    for path in OUTPUT_DIR.glob("*.json"):
        match = OUTPUT_FILE_RE.match(path.name)
        if not match:
            continue
        company_slug = match.group("company")
        kind = match.group("kind")
        stamp = match.group("stamp")
        run_id = f"{company_slug}_{stamp}"
        item = grouped.setdefault(
            run_id,
            {
                "id": run_id,
                "company": company_slug.replace("_", " "),
                "company_slug": company_slug,
                "timestamp": stamp,
                "created_at": _stamp_to_iso(stamp),
                "golden_record_path": None,
                "validation_report_path": None,
                "pytest_report_path": None,
                "semantic_chunks_path": None,
                "fields": None,
                "passed": None,
                "failed": None,
                "skipped": None,
            },
        )
        item[f"{kind}_path"] = str(path.resolve())

    items = sorted(grouped.values(), key=lambda x: x["timestamp"], reverse=True)[:limit]

    for item in items:
        golden_path = item.get("golden_record_path")
        pytest_path = item.get("pytest_report_path")

        if golden_path:
            try:
                golden_data = _read_json(Path(golden_path))
                if isinstance(golden_data, dict):
                    item["fields"] = len(golden_data)
                elif isinstance(golden_data, list):
                    item["fields"] = len(golden_data)
            except Exception:
                item["fields"] = None

        if pytest_path:
            try:
                report = _read_json(Path(pytest_path))
                if isinstance(report, dict):
                    item["passed"] = int(report.get("passed") or 0)
                    item["failed"] = int(report.get("failed") or 0)
                    item["skipped"] = int(report.get("skipped") or 0)
            except Exception:
                item["passed"] = None
                item["failed"] = None
                item["skipped"] = None

    return items


@app.get("/file")
def read_output_json(path: str, user_id: str = Depends(_authenticated_user_id)):
    require_user_id(user_id, context="output file route")
    target = _safe_output_path(path)
    if not target.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {target.name}")
    if target.suffix.lower() != ".json":
        raise HTTPException(status_code=415, detail="Only JSON files are supported.")
    try:
        return JSONResponse(_read_json(target))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid JSON: {exc}") from exc


# ── Versioning API endpoints ──────────────────────────────────────────────────
@app.post("/analyze")
async def analyze(
    req: AnalyzeRequest,
    background_tasks: BackgroundTasks,
    user_id: str = Depends(_authenticated_user_id),
):
    """
    Main entry point for company intelligence lookup.

    Behaviour:
      - If data exists AND force_refresh=False  → return cached data instantly
      - If data exists AND force_refresh=True   → archive old row, run pipeline,
                                                  return status=running + run_id
      - If no data exists                       → run pipeline,
                                                  return status=running + run_id

    The frontend should poll GET /run-status/{run_id} to know when done.
    """
    company = req.company.strip()
    if not company:
        raise HTTPException(status_code=400, detail="Company name is required")
    user_id = require_user_id(user_id, context="analyze route")

    # 1. Look up active record
    existing = (
        _supabase.table("company_runs")
        .select("id, version, golden_record, created_at")
        .eq("company_name", company)
        .eq("run_by", user_id)
        .eq("is_active", True)
        .maybe_single()
        .execute()
    )

    # 2. Serve from cache if no refresh requested
    if existing.data and not req.force_refresh:
        return {
            "status": "cached",
            "company": company,
            "version": existing.data["version"],
            "created_at": existing.data["created_at"],
            "golden_record": existing.data["golden_record"],
        }

    # 3. Determine next version number
    next_version = 1
    if existing.data:
        next_version = existing.data["version"] + 1
        # Archive the currently active row
        _supabase.table("company_runs").update({
            "is_active": False,
            "archived_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", existing.data["id"]).eq("run_by", user_id).execute()

    # 4. Insert placeholder row so frontend can poll immediately
    insert_result = _supabase.table("company_runs").insert({
        "company_name": company,
        "version": next_version,
        "is_active": True,
        "run_by": user_id,
    }).execute()

    run_id = insert_result.data[0]["id"]

    # 5. Kick off pipeline in background
    background_tasks.add_task(_run_and_save, run_id, company, user_id)

    return {
        "status": "running",
        "company": company,
        "version": next_version,
        "run_id": run_id,
    }


@app.get("/run-status/{run_id}")
async def run_status(
    run_id: str,
    user_id: str = Depends(_authenticated_user_id),
):
    """
    Polls a single run by its UUID.
    Returns:
      - status=running  if golden_record is still null
      - status=done     if golden_record is populated
      - status=failed   if is_active=False and golden_record has an 'error' key
    Frontend should call this every 3–5 seconds until status != 'running'.
    """
    row = (
        _supabase.table("company_runs")
        .select("id, version, is_active, golden_record, created_at")
        .eq("id", run_id)
        .eq("run_by", user_id)
        .single()
        .execute()
    )

    if not row.data:
        raise HTTPException(status_code=404, detail="Run not found")

    data = row.data

    if data["golden_record"] is None:
        return {"status": "running", "run_id": run_id}

    if not data["is_active"] and "error" in (data["golden_record"] or {}):
        return {
            "status": "failed",
            "run_id": run_id,
            "error": data["golden_record"]["error"],
        }

    return {
        "status": "done",
        "run_id": run_id,
        "version": data["version"],
        "created_at": data["created_at"],
        "golden_record": data["golden_record"],
    }


@app.get("/company-history/{company_name}")
async def company_history(
    company_name: str,
    user_id: str = Depends(_authenticated_user_id),
):
    """
    Returns all versions (active + archived) for a company,
    ordered newest first. Used to build a version history UI.
    """
    rows = (
        _supabase.table("company_runs")
        .select("id, version, is_active, created_at, archived_at, run_by")
        .eq("company_name", company_name)
        .eq("run_by", user_id)
        .order("version", desc=True)
        .execute()
    )
    return {"company": company_name, "versions": rows.data}


# ── Dev entry point ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=True)
