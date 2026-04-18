"""
Celery Task Queue — Step 3 of the production upgrade
=====================================================
Offloads heavy LangGraph pipeline runs from FastAPI into background workers.

Prerequisites:
  pip install celery redis

Start Redis (Docker):
  docker run -d -p 6379:6379 redis:7-alpine

Start worker:
  celery -A tasks worker --concurrency=2 --loglevel=info

Update server.py to use Celery:
  Replace `background_tasks.add_task(...)` with `run_agent_task.delay(...)`
  and poll Celery's AsyncResult instead of _tasks dict.
"""

import os

from celery import Celery

from logger import get_logger
from core.user_scope import require_user_id

logger = get_logger("tasks")

# ── Celery app ────────────────────────────────────────────────────────────────

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery = Celery(
    "agent_tasks",
    broker=REDIS_URL,
    backend=REDIS_URL,
)

celery.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,           # lets us see "STARTED" state
    task_acks_late=True,               # re-queue if worker crashes mid-task
    worker_prefetch_multiplier=1,      # one task at a time per worker process
    result_expires=3600,               # keep results for 1 hour
)


# ── Tasks ─────────────────────────────────────────────────────────────────────

@celery.task(
    bind=True,
    name="tasks.run_agent_task",
    autoretry_for=(Exception,),
    retry_backoff=10,           # 10s, 20s, 40s … exponential back-off
    max_retries=3,
)
def run_agent_task(self, company: str, user_id: str) -> dict:
    """
    Run the full LangGraph company intelligence pipeline.
    Auto-retries up to 3 times on any exception (covers LLM rate limits,
    network timeouts, transient Groq errors).
    """
    user_id = require_user_id(user_id, context="Celery pipeline task")
    logger.info(f"[Celery task={self.request.id}] Starting pipeline for '{company}'")

    try:
        from main import run_full_pipeline
        result = run_full_pipeline(company, user_id=user_id)

        logger.info(
            f"[Celery task={self.request.id}] Done — "
            f"{len(result.get('golden_record', []))} fields, "
            f"saved to {result.get('golden_record_path')}"
        )

        # Strip non-serialisable objects before returning
        return {
            "company": company,
            "golden_record_path":     result.get("golden_record_path"),
            "validation_report_path": result.get("validation_report_path"),
            "pytest_report_path":     result.get("pytest_report_path"),
            "golden_record_count":    len(result.get("golden_record", [])),
            "test_results":           result.get("test_results"),
        }

    except Exception as exc:
        logger.error(
            f"[Celery task={self.request.id}] Failed (attempt "
            f"{self.request.retries + 1}/3): {exc}",
            exc_info=True,
        )
        raise  # triggers autoretry


# ── How to integrate with server.py ──────────────────────────────────────────
"""
Replace the background_tasks approach in server.py with:

    from tasks import run_agent_task
    from celery.result import AsyncResult

    @app.post("/run")
    def run_pipeline(body: RunRequest):
        job = run_agent_task.delay(body.company)
        return {"task_id": job.id, "status": "pending"}

    @app.get("/status/{task_id}")
    def get_status(task_id: str):
        job = AsyncResult(task_id, app=celery)
        return {"task_id": task_id, "status": job.status}

    @app.get("/result/{task_id}")
    def get_result(task_id: str):
        job = AsyncResult(task_id, app=celery)
        if job.status != "SUCCESS":
            raise HTTPException(202, detail=f"Not ready: {job.status}")
        return job.result
"""
