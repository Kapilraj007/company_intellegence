# 🚀 Company Intelligence — Railway Deployment Guide

## What Gets Deployed
- ✅ All 4 AI Agents (LangGraph pipeline)
- ✅ FastAPI REST API (`server.py`)
- ✅ Output JSON files saved **locally** on the server (`/output/`)
- ✅ 24/7 uptime on Railway

---

## 📁 Files to Add to Your Project Root

Copy these files into your `company-intelligence/` folder:

```
company-intelligence/
├── server.py          ← NEW: FastAPI wrapper (copy from deployment package)
├── Procfile           ← NEW: tells Railway how to start
├── requirements.txt   ← REPLACE: with clean version
├── .gitignore         ← NEW: keeps secrets + cache out of git
├── .env.example       ← UPDATE: safe template (no real keys)
└── .env               ← YOUR REAL KEYS (never committed)
```

---

## 🔑 Step 1 — Secure Your API Keys

Your `.env` currently has **real API keys** — rotate them before pushing to GitHub:

1. Go to [console.groq.com](https://console.groq.com) → regenerate GROQ key
2. Go to [aistudio.google.com](https://aistudio.google.com) → regenerate Google key
3. Go to [openrouter.ai](https://openrouter.ai) → regenerate OpenRouter key
4. Go to [smith.langchain.com](https://smith.langchain.com) → regenerate LangSmith key

---

## 📦 Step 2 — Push to GitHub

```bash
cd company-intelligence

# Initialize git (if not already done)
git init
git add .
git commit -m "initial deploy"

# Create repo on github.com, then:
git remote add origin https://github.com/YOUR_USERNAME/company-intelligence.git
git push -u origin main
```

---

## 🚂 Step 3 — Deploy on Railway

### Option A: Via CLI (recommended for learning)

```bash
# Install Railway CLI
npm install -g @railway/cli

# Login
railway login

# Link to your project
railway init

# Deploy
railway up
```

### Option B: Via Railway Dashboard

1. Go to [railway.app](https://railway.app) → **New Project**
2. Click **Deploy from GitHub repo**
3. Select `company-intelligence`
4. Railway auto-detects `Procfile` and starts deployment

---

## 🔐 Step 4 — Add Environment Variables in Railway

In Railway Dashboard → Your Project → **Variables** tab, add:

```
GROQ_API_KEY          = your_new_groq_key
GOOGLE_API_KEY        = your_new_google_key
OPENROUTER_API_KEY    = your_new_openrouter_key
LANGCHAIN_TRACING_V2  = true
LANGCHAIN_API_KEY     = your_new_langsmith_key
LANGCHAIN_PROJECT     = company-intelligence
```

Or via CLI:
```bash
railway variables set GROQ_API_KEY=your_key_here
railway variables set GOOGLE_API_KEY=your_key_here
# ... repeat for all keys
```

---

## ✅ Step 5 — Test Your Live API

Railway gives you a public URL like:
```
https://company-intelligence-production.up.railway.app
```

### Test the API endpoints:

```bash
# 1. Health check
curl https://your-app.up.railway.app/health

# 2. Run full pipeline for a company
curl -X POST https://your-app.up.railway.app/run/full \
  -H "Content-Type: application/json" \
  -d '{"company_name": "IBM"}'

# Response: {"job_id": "ibm_20260305_120000", "status": "pending"}

# 3. Poll job status
curl https://your-app.up.railway.app/jobs/ibm_20260305_120000

# 4. Get results when done
curl https://your-app.up.railway.app/results/ibm_20260305_120000

# 5. List all saved output files
curl https://your-app.up.railway.app/output/list
```

---

## 📂 Where Output Files Are Stored

Output JSON files are saved **locally on the Railway server** at:
```
/output/
├── ibm_golden_record_20260305_120000.json
├── ibm_validation_report_20260305_120000.json
└── ibm_pytest_report_20260305_120000.json
```

Retrieve them anytime via:
```bash
GET /results/{job_id}       → returns golden record as JSON
GET /output/list            → lists all saved files
```

> ⚠️ Railway's free tier resets disk on redeploy.
> Use `/results/{job_id}` to download files before redeploying.

---

## 🌐 API Endpoints Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Health check |
| GET | `/health` | Service status + timestamp |
| POST | `/run/full` | Start pipeline for a company |
| GET | `/jobs` | List all jobs |
| GET | `/jobs/{job_id}` | Poll job status |
| GET | `/results/{job_id}` | Get golden record JSON |
| GET | `/output/list` | List all saved files |

### Interactive API Docs (auto-generated):
```
https://your-app.up.railway.app/docs       ← Swagger UI
https://your-app.up.railway.app/redoc      ← ReDoc
```

---

## 💰 Railway Free Tier Usage

| Resource | Your Usage | Monthly Cost |
|----------|-----------|--------------|
| 512MB RAM | Light (LLM calls are external) | ~$2.50 |
| 0.5 vCPU | Idle most of time | ~$1.50 |
| **Total** | | **~$4/mo ✅** |

Stays within the **$5 free credit** per month.

---

## 🔄 Redeploy After Code Changes

```bash
git add .
git commit -m "update"
git push origin main
# Railway auto-redeploys on every push ✅
```
