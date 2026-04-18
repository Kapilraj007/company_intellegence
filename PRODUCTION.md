# Company Intelligence — Production Upgrade Guide

This folder contains the production upgrade files following the learning order
recommended in the course material.

---

## What was added

| File | Step | Purpose |
|------|------|---------|
| `server.py` | 1 | FastAPI gateway — async job submission + polling |
| `logger.py` | 2 | Structured logging with loguru (console + rotating file) |
| `tasks.py` | 3 | Celery task definition (ready to activate with Redis) |
| `render.yaml` | 1 | Render deployment config |
| `requirements_prod.txt` | all | Minimal production deps with step-gated comments |

---

## Step 1 — Deploy FastAPI + LangGraph on Render

### Run locally
```bash
pip install fastapi uvicorn loguru python-dotenv tenacity
uvicorn server:app --host 0.0.0.0 --port 8000 --reload
```

### API usage
```bash
# Submit a job
curl -X POST http://localhost:8000/run \
  -H "Content-Type: application/json" \
  -d '{"company": "IBM"}'
# → {"task_id": "abc-123", "status": "pending", ...}

# Poll status
curl http://localhost:8000/status/abc-123
# → {"status": "running", ...}

# Get result when done
curl http://localhost:8000/result/abc-123
```

### Deploy to Render
1. Push this repo to GitHub
2. In Render dashboard: New → Web Service → connect repo
3. Build command: `pip install -r requirements_prod.txt`
4. Start command: `uvicorn server:app --host 0.0.0.0 --port $PORT`
5. Add env var: `GROQ_API_KEY = <your key>`
6. Click Deploy

Alternatively, add `render.yaml` to repo root and use Render Blueprints for
one-click deploy.

---

## Step 2 — Logging

Logging is already wired into `server.py`. To add it to any agent:

```python
from logger import get_logger
logger = get_logger("agent1")

logger.info("LLM1 started for company='{}'", company)
logger.warning("JSON parse failed — retrying")
logger.error("Pipeline failed: {}", exc, exc_info=True)
```

Logs go to:
- **Console** — coloured output visible in Render's log stream
- **`logs/agent.log`** — rotates at 10 MB, retains 14 days

Control log level with env var: `LOG_LEVEL=DEBUG`

---

## Step 3 — Celery + Redis Queue

When you're ready to add a proper task queue:

1. Uncomment the Redis + worker blocks in `render.yaml`
2. Uncomment `celery[redis]` and `redis` in `requirements_prod.txt`
3. In `server.py`:
   - Replace `background_tasks.add_task(...)` with `run_agent_task.delay(company)`
   - Replace the `_tasks` dict lookups with `AsyncResult(task_id)`
   - See the code comment at the bottom of `tasks.py` for the exact snippet

Start worker locally:
```bash
# Terminal 1 — Redis
docker run -d -p 6379:6379 redis:7-alpine

# Terminal 2 — Celery worker
celery -A tasks worker --concurrency=2 --loglevel=info

# Terminal 3 — FastAPI
uvicorn server:app --reload
```

---

## Step 4 — Vector Database

Replace `.langgraph_api/store.vectors.pckl` with Qdrant:

```bash
pip install qdrant-client
docker run -p 6333:6333 qdrant/qdrant
```

```python
from qdrant_client import QdrantClient
client = QdrantClient(url=os.getenv("QDRANT_URL", "http://localhost:6333"))
```

Add `QDRANT_URL` to Render env vars.

---

## Step 5 — Monitoring

```bash
pip install prometheus-client
```

```python
# monitoring/metrics.py
from prometheus_client import Counter, Histogram, start_http_server

pipeline_runs    = Counter("pipeline_runs_total", "Total pipeline runs", ["status"])
pipeline_latency = Histogram("pipeline_duration_seconds", "Pipeline duration")
```

Expose `/metrics` endpoint and connect Grafana Cloud (free tier) to Render.

---

## Current Architecture

```
Client
  ↓
FastAPI server.py   ← Step 1 ✅
  ↓  (BackgroundTasks → Celery in Step 3)
LangGraph Pipeline (main.py → graph.py)
  ↓
Agent1 (3× Groq LLMs) → Agent2 (consolidation) → Agent3 (save) → Agent4 (pytest)
  ↓
output/*.json
  ↓
logs/agent.log      ← Step 2 ✅
```
