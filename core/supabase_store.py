"""
Supabase persistence for Agent 1 raw outputs.

Stores:
  - pipeline_runs   : one row per pipeline run
  - agent1_raw_outputs : one row per LLM per run (llm1, llm2, llm3)

Usage:
  from core.supabase_store import get_supabase_client
  db = get_supabase_client()
  db.create_pipeline_run(run_id=..., company_name=..., company_id=..., user_id=...)
  db.insert_agent1_output(run_id=..., company_id=..., company_name=...,
                          source_llm="llm1", raw_data={...}, filled_count=120,
                          user_id=...)
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

from supabase import create_client, Client
from logger import get_logger
from core.user_scope import require_user_id

logger = get_logger("supabase_store")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SupabaseStore:
    def __init__(self) -> None:
        url = os.getenv("SUPABASE_URL", "").strip()
        key = os.getenv("SUPABASE_SERVICE_KEY", "").strip()
        if not url or not key:
            raise RuntimeError(
                "Supabase credentials missing. Set SUPABASE_URL and SUPABASE_SERVICE_KEY."
            )
        self._client: Client = create_client(url, key)
        self._column_support: Dict[Tuple[str, str], bool] = {}

    def _supports_column(self, table: str, column: str) -> bool:
        cache_key = (table, column)
        if cache_key in self._column_support:
            return self._column_support[cache_key]

        supported = True
        try:
            self._client.table(table).select(column).limit(1).execute()
        except Exception as exc:
            message = str(exc).lower()
            if "does not exist" in message and column.lower() in message:
                supported = False
                logger.warning(
                    f"[Supabase] Column '{table}.{column}' not found; "
                    "continuing with schema-compatible payload."
                )
            else:
                logger.warning(
                    f"[Supabase] Column probe failed for '{table}.{column}': {exc}. "
                    "Assuming column exists."
                )

        self._column_support[cache_key] = supported
        return supported

    # ── Pipeline run ──────────────────────────────────────────────────────────

    def create_pipeline_run(
        self,
        *,
        run_id: str,
        company_name: str,
        company_id: str,
        user_id: str,
    ) -> None:
        user_id = require_user_id(user_id, context="Supabase pipeline run creation")
        include_user_id = self._supports_column("pipeline_runs", "user_id")
        payload = {
            "run_id": run_id,
            "company_name": company_name,
            "company_id": company_id,
            "status": "running",
            "started_at": _now_iso(),
        }
        if include_user_id:
            payload["user_id"] = user_id

        # Idempotent when pipeline registration is retried.
        self._client.table("pipeline_runs").upsert(payload, on_conflict="run_id").execute()
        logger.info(f"[Supabase] pipeline_run created: {run_id}")

    def complete_pipeline_run(self, *, run_id: str, user_id: str) -> None:
        user_id = require_user_id(user_id, context="Supabase pipeline run completion")
        include_user_id = self._supports_column("pipeline_runs", "user_id")
        payload = {
            "status":       "completed",
            "completed_at": _now_iso(),
        }
        if include_user_id:
            payload["user_id"] = user_id

        query = self._client.table("pipeline_runs").update(payload).eq("run_id", run_id)
        if include_user_id:
            query = query.eq("user_id", user_id)
        query.execute()
        logger.info(f"[Supabase] pipeline_run completed: {run_id}")

    # ── Agent 1 raw output ────────────────────────────────────────────────────

    def insert_agent1_output(
        self,
        *,
        run_id: str,
        company_id: str,
        company_name: str,
        source_llm: str,
        raw_data: Dict[str, Any],
        filled_count: int,
        user_id: str,
    ) -> None:
        """
        Insert one LLM's flat output for a pipeline run.
        Called once per LLM (llm1, llm2, llm3) from agent1_research.py.
        """
        user_id = require_user_id(user_id, context="Supabase agent1 output insert")
        include_user_id = self._supports_column("agent1_raw_outputs", "user_id")
        payload_raw = dict(raw_data or {})
        if not include_user_id:
            payload_raw.setdefault("__user_id", user_id)
        payload = {
            "run_id":       run_id,
            "company_id":   company_id,
            "company_name": company_name,
            "source_llm":   source_llm,
            "raw_data":     payload_raw,
            "filled_count": filled_count,
            "created_at":   _now_iso(),
        }
        if include_user_id:
            payload["user_id"] = user_id

        try:
            self._client.table("agent1_raw_outputs").insert(payload).execute()
        except Exception as exc:
            message = str(exc)
            # Recover from missing pipeline_runs row (FK violation) by creating it.
            if (
                "agent1_raw_outputs_run_id_fkey" in message
                or "is not present in table \"pipeline_runs\"" in message
            ):
                logger.warning(
                    f"[Supabase] pipeline_runs row missing for run={run_id}; "
                    "creating and retrying insert"
                )
                self.create_pipeline_run(
                    run_id=run_id,
                    company_name=company_name,
                    company_id=company_id,
                    user_id=user_id,
                )
                self._client.table("agent1_raw_outputs").insert(payload).execute()
            else:
                raise

        logger.info(
            f"[Supabase] agent1_raw_outputs inserted: "
            f"run={run_id} llm={source_llm} filled={filled_count}"
        )


# ── Singleton ─────────────────────────────────────────────────────────────────

_CLIENT: SupabaseStore | None = None


def get_supabase_client() -> SupabaseStore:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = SupabaseStore()
    return _CLIENT
