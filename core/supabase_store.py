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
from typing import Any, Dict, List, Tuple

from supabase import create_client, Client
from logger import get_logger
from core.user_scope import require_user_id

logger = get_logger("supabase_store")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_role(value: Any) -> str:
    role = str(value or "user").strip().lower() or "user"
    return role if role in {"user", "admin"} else "user"


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

    @staticmethod
    def _rows(result: Any) -> List[Dict[str, Any]]:
        data = getattr(result, "data", None)
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        if isinstance(data, dict):
            return [data]
        return []

    @classmethod
    def _first_row(cls, result: Any) -> Dict[str, Any] | None:
        rows = cls._rows(result)
        return rows[0] if rows else None

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
        user_name: str = "",
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
        self.log_pipeline_activity(
            user_id=user_id,
            activity_type="pipeline_started",
            run_id=run_id,
            company_id=company_id,
            company_name=company_name,
            activity_status="running",
            details={"user_name": str(user_name or "").strip()},
        )
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
        self.log_pipeline_activity(
            user_id=user_id,
            activity_type="pipeline_completed",
            run_id=run_id,
            activity_status="completed",
        )
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
        self.log_pipeline_activity(
            user_id=user_id,
            activity_type="agent1_output_saved",
            run_id=run_id,
            company_id=company_id,
            company_name=company_name,
            activity_status="stored",
            details={"source_llm": source_llm, "filled_count": int(filled_count or 0)},
        )

    # ── Custom users ──────────────────────────────────────────────────────────

    def get_user_by_email(self, email: str) -> Dict[str, Any] | None:
        normalized = str(email or "").strip().lower()
        if not normalized:
            return None
        result = (
            self._client.table("users")
            .select("*")
            .eq("email", normalized)
            .limit(1)
            .execute()
        )
        return self._first_row(result)

    def get_user_by_id(self, user_id: str) -> Dict[str, Any] | None:
        user_id = require_user_id(user_id, context="Supabase user lookup")
        result = (
            self._client.table("users")
            .select("*")
            .eq("user_id", user_id)
            .limit(1)
            .execute()
        )
        return self._first_row(result)

    def create_user(
        self,
        *,
        name: str,
        email: str,
        password_hash: str,
        role: str = "user",
        approval_status: str = "pending",
        session_nonce: str = "",
    ) -> Dict[str, Any] | None:
        payload = {
            "name": str(name or "").strip(),
            "email": str(email or "").strip().lower(),
            "password": str(password_hash or "").strip(),
            "role": _normalize_role(role),
            "approval_status": str(approval_status or "pending").strip().lower() or "pending",
            "created_at": _now_iso(),
            "session_nonce": str(session_nonce or "").strip(),
        }
        if self._supports_column("users", "verification_status"):
            payload["verification_status"] = "unverified"
        result = self._client.table("users").insert(payload).execute()
        return self._first_row(result)

    def update_user(self, user_id: str, **fields: Any) -> Dict[str, Any] | None:
        user_id = require_user_id(user_id, context="Supabase user update")
        payload: Dict[str, Any] = {}
        if "name" in fields:
            payload["name"] = str(fields["name"] or "").strip()
        if "email" in fields:
            payload["email"] = str(fields["email"] or "").strip().lower()
        if "password_hash" in fields:
            payload["password"] = str(fields["password_hash"] or "").strip()
        if "role" in fields:
            payload["role"] = _normalize_role(fields["role"])
        if "approval_status" in fields:
            payload["approval_status"] = str(fields["approval_status"] or "pending").strip().lower() or "pending"
        if "session_nonce" in fields:
            payload["session_nonce"] = str(fields["session_nonce"] or "").strip()
        if "last_login_at" in fields:
            payload["last_login_at"] = fields["last_login_at"]
        if "verification_status" in fields and self._supports_column("users", "verification_status"):
            payload["verification_status"] = str(fields["verification_status"] or "unverified").strip().lower() or "unverified"
        if "verified_at" in fields and self._supports_column("users", "verified_at"):
            payload["verified_at"] = fields["verified_at"]
        if "verified_by" in fields and self._supports_column("users", "verified_by"):
            payload["verified_by"] = fields["verified_by"]
        if "approval_note" in fields and self._supports_column("users", "approval_note"):
            payload["approval_note"] = str(fields["approval_note"] or "").strip()
        if not payload:
            return self.get_user_by_id(user_id)
        result = self._client.table("users").update(payload).eq("user_id", user_id).execute()
        updated = self._first_row(result)
        return updated or self.get_user_by_id(user_id)

    def list_users(self, *, limit: int = 500, approval_status: str | None = None) -> List[Dict[str, Any]]:
        fields = [
            "user_id",
            "name",
            "email",
            "role",
            "approval_status",
            "created_at",
            "last_login_at",
        ]
        for optional in ("verification_status", "verified_at", "verified_by"):
            if self._supports_column("users", optional):
                fields.append(optional)
        query = self._client.table("users").select(",".join(fields))
        status_value = str(approval_status or "").strip().lower()
        if status_value:
            query = query.eq("approval_status", status_value)
        try:
            query = query.order("created_at", desc=True)
        except Exception:
            pass
        try:
            query = query.limit(max(1, min(int(limit or 500), 5000)))
        except Exception:
            pass
        rows = self._rows(query.execute())
        if not rows:
            return []
        rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
        return rows

    def list_users_by_approval_status(self, approval_status: str) -> List[Dict[str, Any]]:
        status_value = str(approval_status or "").strip().lower()
        if not status_value:
            return []
        rows = self.list_users(limit=1000, approval_status=status_value)
        rows.sort(key=lambda row: str(row.get("created_at") or ""))
        return rows

    # ── Activity logs ─────────────────────────────────────────────────────────

    def log_pipeline_activity(
        self,
        *,
        user_id: str,
        activity_type: str,
        run_id: str | None = None,
        company_id: str | None = None,
        company_name: str | None = None,
        activity_status: str | None = None,
        details: Dict[str, Any] | None = None,
    ) -> None:
        user_id = require_user_id(user_id, context="Supabase activity log")
        payload: Dict[str, Any] = {
            "user_id": user_id,
            "activity_type": str(activity_type or "").strip(),
            "created_at": _now_iso(),
            "details": dict(details or {}),
        }
        if run_id:
            payload["run_id"] = str(run_id).strip()
        if company_id:
            payload["company_id"] = str(company_id).strip()
        if company_name:
            payload["company_name"] = str(company_name).strip()
        if activity_status:
            payload["activity_status"] = str(activity_status).strip().lower()

        try:
            self._client.table("pipeline_activity_logs").insert(payload).execute()
        except Exception as exc:
            logger.warning(f"[Supabase] pipeline activity log failed: {exc}")

    def list_pipeline_activity(
        self,
        *,
        limit: int = 200,
        user_id: str | None = None,
        activity_type: str | None = None,
        run_id: str | None = None,
    ) -> List[Dict[str, Any]]:
        query = self._client.table("pipeline_activity_logs").select("*")
        if user_id:
            query = query.eq("user_id", require_user_id(user_id, context="Supabase activity listing"))
        if activity_type:
            query = query.eq("activity_type", str(activity_type).strip())
        if run_id:
            query = query.eq("run_id", str(run_id).strip())
        try:
            query = query.order("created_at", desc=True)
        except Exception:
            pass
        try:
            query = query.limit(max(1, min(int(limit or 200), 2000)))
        except Exception:
            pass
        rows = self._rows(query.execute())
        rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
        return rows


# ── Singleton ─────────────────────────────────────────────────────────────────

_CLIENT: SupabaseStore | None = None


def get_supabase_client() -> SupabaseStore:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = SupabaseStore()
    return _CLIENT
