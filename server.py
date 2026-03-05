"""
FastAPI server for Company Intelligence Pipeline
Wraps main.py pipeline — input via API, output saves locally to /output
"""
import os
import json
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

# ── Import your existing pipeline ────────────────────────────────────────────
from graph import graph
from agents.agent1_research import run_targeted_research
from agents.agent2_consolidation import check_consolidation, inc_retry_consolidation, run_consolidation
from agents.agent3_save import save_output
from agents.agent4_test_runner import extract_failed_parameter_ids, route_after_tests, run_tests

app = FastAPI(
    title="Company Intelligence API",
    description="AI Agent pipeline — research, consolidate, validate, test company data",
    version="1.0.0",
)

# ── In-memory job tracker ─────────────────────────────────────────────────────
jobs: Dict[str, Dict[str, Any]] = {}


# ── Request / Response Models ─────────────────────────────────────────────────
class FullPipelineRequest(BaseModel):
    company_name: str

class RegenRequest(BaseModel):
    company_name: str
    base_record_path: str
    failed_ids: list[int]
    max_rounds: int = 2
    run_tests: bool = True

class JobStatusResponse(BaseModel):
    job_id: str
    status: str                    # pending | running | done | failed
    company_name: Optional[str]
    started_at: Optional[str]
    finished_at: Optional[str]
    golden_record_path: Optional[str]
    validation_report_path: Optional[str]
    pytest_report_path: Optional[str]
    error: Optional[str]


# ── Background pipeline runner ────────────────────────────────────────────────
def _run_pipeline_bg(job_id: str, company_name: str):
    jobs[job_id]["status"] = "running"
    try:
        result = graph.invoke(
            {"company_name": company_name},
            config={"recursion_limit": 500},
        )
        jobs[job_id].update({
            "status": "done",
            "finished_at": datetime.now().isoformat(),
            "golden_record_path": result.get("golden_record_path"),
            "validation_report_path": result.get("validation_report_path"),
            "pytest_report_path": result.get("pytest_report_path"),
            "test_results": result.get("test_results"),
        })
    except Exception as exc:
        jobs[job_id].update({
            "status": "failed",
            "finished_at": datetime.now().isoformat(),
            "error": str(exc),
        })


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def root():
    return {"status": "ok", "service": "Company Intelligence API"}


@app.get("/health", tags=["Health"])
def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.post("/run/full", tags=["Pipeline"])
def run_full(req: FullPipelineRequest, background_tasks: BackgroundTasks):
    """
    Trigger full pipeline for a company.
    Returns a job_id — poll /jobs/{job_id} for status.
    Output JSON files are saved locally in the /output folder.
    """
    job_id = f"{req.company_name.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    jobs[job_id] = {
        "job_id": job_id,
        "status": "pending",
        "company_name": req.company_name,
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
        "golden_record_path": None,
        "validation_report_path": None,
        "pytest_report_path": None,
        "error": None,
    }
    background_tasks.add_task(_run_pipeline_bg, job_id, req.company_name)
    return {"job_id": job_id, "status": "pending", "message": f"Pipeline started for '{req.company_name}'"}


@app.get("/jobs/{job_id}", tags=["Jobs"], response_model=JobStatusResponse)
def get_job_status(job_id: str):
    """Poll this endpoint to check if your pipeline job is done."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    return jobs[job_id]


@app.get("/jobs", tags=["Jobs"])
def list_jobs():
    """List all jobs and their statuses."""
    return [
        {"job_id": k, "status": v["status"], "company_name": v["company_name"]}
        for k, v in jobs.items()
    ]


@app.get("/results/{job_id}", tags=["Results"])
def get_results(job_id: str):
    """
    Return the golden record JSON content directly from the saved file.
    Only available when job status is 'done'.
    """
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = jobs[job_id]
    if job["status"] != "done":
        raise HTTPException(status_code=400, detail=f"Job not done yet. Current status: {job['status']}")

    path = job.get("golden_record_path")
    if not path or not Path(path).exists():
        raise HTTPException(status_code=404, detail="Output file not found on disk")

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    return {
        "job_id": job_id,
        "company_name": job["company_name"],
        "golden_record_path": path,
        "record_count": len(data),
        "data": data,
    }


@app.get("/output/list", tags=["Results"])
def list_output_files():
    """List all saved output JSON files in the /output directory."""
    output_dir = Path(__file__).parent / "output"
    if not output_dir.exists():
        return {"files": []}
    files = sorted(output_dir.glob("*.json"), key=lambda f: f.stat().st_mtime, reverse=True)
    return {
        "files": [
            {
                "name": f.name,
                "size_kb": round(f.stat().st_size / 1024, 2),
                "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
            }
            for f in files
        ]
    }


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)