"""Local JSON persistence layer used in place of external DB/vector services."""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Optional

from core.user_scope import require_user_id


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _slug(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", (value or "").strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "unknown"


def _tokenize(text: str) -> set[str]:
    return {
        tok
        for tok in re.split(r"[^a-zA-Z0-9]+", (text or "").lower())
        if len(tok) >= 3
    }


def _merge_dict(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _merge_dict(base[key], value)
        else:
            base[key] = value
    return base


class LocalStoreClient:
    def __init__(self, root: Optional[str] = None):
        configured = root or os.getenv("LOCAL_STORE_DIR", "").strip()
        base = Path(configured) if configured else Path(__file__).resolve().parent.parent / "output" / "local_store"
        self.root = base.resolve()
        self.runs_dir = self.root / "runs"
        self.companies_dir = self.root / "companies"
        self.versions_dir = self.root / "versions"
        self.admin_dir = self.root / "admin"
        self.activity_log_path = self.admin_dir / "activity_logs.jsonl"
        self.error_log_path = self.admin_dir / "error_logs.jsonl"
        self.root.mkdir(parents=True, exist_ok=True)
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.companies_dir.mkdir(parents=True, exist_ok=True)
        self.versions_dir.mkdir(parents=True, exist_ok=True)
        self.admin_dir.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        self._query_cache: Dict[str, Dict[str, Any]] = {}
        try:
            self._query_cache_ttl = max(2.0, float(os.getenv("LOCAL_STORE_QUERY_CACHE_TTL_SECONDS", "15")))
        except (TypeError, ValueError):
            self._query_cache_ttl = 15.0

    def _run_path(self, run_id: str) -> Path:
        return self.runs_dir / f"{_slug(run_id)}.json"

    def _company_path(self, company_id: str, company_name: str = "", user_id: str = "") -> Path:
        key = company_id or company_name
        if user_id:
            key = f"{_slug(user_id)}__{_slug(key)}"
        return self.companies_dir / f"{_slug(key)}.json"

    @staticmethod
    def _read(path: Path, default: Any) -> Any:
        try:
            with path.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return default

    @staticmethod
    def _write(path: Path, payload: Any) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False)
        tmp.replace(path)

    @staticmethod
    def _append_jsonl_line(path: Path, payload: Dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _invalidate_query_cache(self, prefix: str = "") -> None:
        if not prefix:
            self._query_cache.clear()
            return
        wanted = str(prefix).strip()
        if not wanted:
            return
        for key in list(self._query_cache.keys()):
            if key.startswith(wanted):
                self._query_cache.pop(key, None)

    def _cache_get(self, key: str) -> Any:
        entry = self._query_cache.get(key)
        if not isinstance(entry, dict):
            return None
        cached_at = float(entry.get("cached_at") or 0.0)
        if (time.time() - cached_at) > self._query_cache_ttl:
            self._query_cache.pop(key, None)
            return None
        return entry.get("value")

    def _cache_set(self, key: str, value: Any) -> Any:
        self._query_cache[key] = {"cached_at": time.time(), "value": value}
        return value

    @staticmethod
    def _json_copy(value: Any) -> Any:
        return json.loads(json.dumps(value, ensure_ascii=False))

    @staticmethod
    def _activity_ts() -> str:
        return _now_iso()

    @staticmethod
    def _sort_desc_by_created_at(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
        return rows

    def _iter_jsonl_rows(self, path: Path) -> List[Dict[str, Any]]:
        if not path.exists():
            return []
        rows: List[Dict[str, Any]] = []
        try:
            with path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    clean = line.strip()
                    if not clean:
                        continue
                    try:
                        payload = json.loads(clean)
                    except Exception:
                        continue
                    if isinstance(payload, dict):
                        rows.append(payload)
        except Exception:
            return []
        return rows

    @staticmethod
    def _visible_to_user(doc: Dict[str, Any], user_id: str) -> bool:
        del user_id
        return isinstance(doc, dict)

    @staticmethod
    def _run_visible_to_user(doc: Dict[str, Any], user_id: str) -> bool:
        del user_id
        return isinstance(doc, dict)

    def _load_company_doc(self, company_id: str, company_name: str = "", user_id: str = "") -> Dict[str, Any]:
        user_id = require_user_id(user_id, context="local company document load")
        doc: Dict[str, Any] = {}
        for path in (
            self._company_path(company_id, company_name, user_id=user_id),
            self._company_path(company_id, company_name),
        ):
            candidate = self._read(path, {})
            if isinstance(candidate, dict) and self._visible_to_user(candidate, user_id):
                doc = candidate
                break
        doc.setdefault("company_id", company_id or _slug(company_name))
        doc.setdefault("company_name", company_name or company_id)
        doc.setdefault("agent1_flat", {})
        doc.setdefault("raw_data", {})
        doc.setdefault("consolidated", {})
        doc.setdefault("chunks", [])
        doc.setdefault("failed_parameter_ids_by_run", {})
        if not isinstance(doc.get("versions"), list):
            doc["versions"] = []
        try:
            doc["version_counter"] = int(doc.get("version_counter") or len(doc["versions"]))
        except (TypeError, ValueError):
            doc["version_counter"] = len(doc["versions"])
        return doc

    def _write_company_doc(self, company_id: str, company_name: str, user_id: str, doc: Dict[str, Any]) -> None:
        user_id = require_user_id(user_id, context="local company document write")
        doc["user_id"] = user_id
        self._write(self._company_path(company_id, company_name, user_id=user_id), doc)

    def _record_activity(
        self,
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
        source: str = "local_store",
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "activity_id": str(uuid.uuid4()),
            "created_at": self._activity_ts(),
            "source": str(source or "local_store"),
            "scope": str(scope or "system"),
            "actor_user_id": str(actor_user_id or "").strip(),
            "activity_type": str(activity_type or "").strip(),
            "activity_status": str(activity_status or "completed").strip().lower() or "completed",
            "details": dict(details or {}),
        }
        if target_user_id:
            payload["target_user_id"] = str(target_user_id).strip()
        if run_id:
            payload["run_id"] = str(run_id).strip()
        if company_id:
            payload["company_id"] = str(company_id).strip()
        if company_name:
            payload["company_name"] = str(company_name).strip()

        self._append_jsonl_line(self.activity_log_path, payload)
        self._invalidate_query_cache("activity:")
        self._invalidate_query_cache("dashboard:")
        return payload

    def _record_error(
        self,
        *,
        user_id: str | None,
        error_type: str,
        message: str,
        source: str = "local_store",
        run_id: str | None = None,
        company_id: str | None = None,
        company_name: str | None = None,
        details: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "error_id": str(uuid.uuid4()),
            "created_at": self._activity_ts(),
            "source": str(source or "local_store"),
            "user_id": str(user_id or "").strip() or None,
            "error_type": str(error_type or "runtime").strip().lower() or "runtime",
            "message": str(message or "").strip(),
            "details": dict(details or {}),
        }
        if run_id:
            payload["run_id"] = str(run_id).strip()
        if company_id:
            payload["company_id"] = str(company_id).strip()
        if company_name:
            payload["company_name"] = str(company_name).strip()
        self._append_jsonl_line(self.error_log_path, payload)
        self._invalidate_query_cache("error:")
        self._invalidate_query_cache("dashboard:")
        return payload

    def _record_company_version(
        self,
        *,
        doc: Dict[str, Any],
        user_id: str,
        company_id: str,
        company_name: str,
        run_id: str,
        version_kind: str,
        snapshot_payload: Dict[str, Any] | List[Any] | str | int | float | None,
    ) -> Dict[str, Any]:
        versions = doc.get("versions")
        if not isinstance(versions, list):
            versions = []
            doc["versions"] = versions
        try:
            current_counter = int(doc.get("version_counter") or len(versions))
        except (TypeError, ValueError):
            current_counter = len(versions)
        version_number = current_counter + 1
        created_at = self._activity_ts()
        version_id = f"v{version_number:05d}"

        snapshot_dir = self.versions_dir / _slug(user_id) / _slug(company_id or company_name)
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = snapshot_dir / f"{version_number:05d}_{_slug(version_kind)}.json"
        snapshot_doc = {
            "version_id": version_id,
            "version_number": version_number,
            "version_kind": str(version_kind or "snapshot").strip().lower() or "snapshot",
            "created_at": created_at,
            "user_id": user_id,
            "run_id": str(run_id or "").strip(),
            "company_id": company_id,
            "company_name": company_name,
            "payload": snapshot_payload,
        }
        self._write(snapshot_path, snapshot_doc)

        meta = {
            "version_id": version_id,
            "version_number": version_number,
            "version_kind": snapshot_doc["version_kind"],
            "created_at": created_at,
            "user_id": user_id,
            "run_id": str(run_id or "").strip(),
            "company_id": company_id,
            "company_name": company_name,
            "snapshot_path": str(snapshot_path),
            "snapshot_size_bytes": int(snapshot_path.stat().st_size),
        }
        versions.append(meta)
        if len(versions) > 500:
            doc["versions"] = versions[-500:]
        doc["version_counter"] = version_number
        self._invalidate_query_cache("dashboard:")
        return meta

    def _visible_company_docs(self, user_id: str) -> List[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="local company listing")
        selected: Dict[str, Dict[str, Any]] = {}
        for path in sorted(self.companies_dir.glob("*.json")):
            doc = self._read(path, {})
            if not isinstance(doc, dict) or not self._visible_to_user(doc, user_id):
                continue
            cid = str(doc.get("company_id") or path.stem).strip()
            owner = str(doc.get("user_id") or "").strip()
            if cid not in selected or owner == user_id:
                selected[cid] = doc
        return list(selected.values())

    def create_pipeline_run(
        self,
        *,
        run_id: str,
        company_name: str,
        company_id: str,
        user_id: str,
        user_name: str = "",
    ) -> None:
        user_id = require_user_id(user_id, context="local pipeline run creation")
        with self._lock:
            run = {
                "run_id": run_id,
                "user_id": user_id,
                "user_name": str(user_name or "").strip(),
                "company_id": company_id,
                "company_name": company_name,
                "status": "running",
                "started_at": _now_iso(),
                "completed_at": None,
                "golden_record_count": 0,
                "all_tests_passed": False,
                "failed_param_ids": [],
                "agent2_retry_count": 0,
                "pytest_retry_count": 0,
                "paths": {},
            }
            self._write(self._run_path(run_id), run)
            self._invalidate_query_cache("runs:")
            self._invalidate_query_cache("dashboard:")
            self._record_activity(
                actor_user_id=user_id,
                scope="pipeline",
                activity_type="pipeline_run_created",
                activity_status="running",
                run_id=run_id,
                company_id=company_id,
                company_name=company_name,
            )

    def complete_pipeline_run(
        self,
        *,
        run_id: str,
        golden_record_count: int,
        all_tests_passed: bool,
        failed_param_ids: List[int],
        agent2_retry_count: int,
        pytest_retry_count: int,
        golden_record_path: str,
        validation_path: str,
        pytest_report_path: Optional[str],
        chunk_record_path: Optional[str],
        user_id: str,
    ) -> None:
        user_id = require_user_id(user_id, context="local pipeline run completion")
        with self._lock:
            path = self._run_path(run_id)
            run = self._read(path, {})
            if not isinstance(run, dict):
                run = {"run_id": run_id}
            owner = str(run.get("user_id") or "").strip()
            if owner and owner != user_id:
                raise PermissionError(f"Pipeline run '{run_id}' is not owned by user_id '{user_id}'.")
            run.update(
                {
                    "user_id": user_id,
                    "status": "completed",
                    "completed_at": _now_iso(),
                    "golden_record_count": int(golden_record_count or 0),
                    "all_tests_passed": bool(all_tests_passed),
                    "failed_param_ids": sorted({int(x) for x in failed_param_ids if str(x).isdigit()}),
                    "agent2_retry_count": int(agent2_retry_count or 0),
                    "pytest_retry_count": int(pytest_retry_count or 0),
                    "paths": {
                        "golden_record_path": golden_record_path,
                        "validation_report_path": validation_path,
                        "pytest_report_path": pytest_report_path,
                        "semantic_chunks_path": chunk_record_path,
                    },
                }
            )
            self._write(path, run)
            company_id_value = str(run.get("company_id") or "").strip()
            company_name_value = str(run.get("company_name") or "").strip()
            self._invalidate_query_cache("runs:")
            self._invalidate_query_cache("dashboard:")
            self._record_activity(
                actor_user_id=user_id,
                scope="pipeline",
                activity_type="pipeline_run_completed",
                activity_status="completed",
                run_id=run_id,
                company_id=company_id_value or None,
                company_name=company_name_value or None,
                details={
                    "golden_record_count": int(golden_record_count or 0),
                    "all_tests_passed": bool(all_tests_passed),
                    "failed_param_count": len(failed_param_ids or []),
                },
            )

    def insert_agent1_flat(
        self,
        *,
        run_id: str,
        company_name: str,
        company_id: str,
        source_llm: str,
        full_json: Dict[str, Any],
        user_id: str,
    ) -> None:
        user_id = require_user_id(user_id, context="local agent1 flat insert")
        with self._lock:
            doc = self._load_company_doc(company_id, company_name, user_id=user_id)
            doc["company_name"] = company_name
            doc["company_id"] = company_id
            doc["user_id"] = user_id
            doc.setdefault("agent1_flat", {})
            doc["agent1_flat"][source_llm] = {
                "run_id": run_id,
                "generated_at": _now_iso(),
                "data": full_json,
            }
            doc["updated_at"] = _now_iso()
            self._record_company_version(
                doc=doc,
                user_id=user_id,
                company_id=company_id,
                company_name=company_name,
                run_id=run_id,
                version_kind=f"agent1_{source_llm}",
                snapshot_payload={"source_llm": source_llm, "data": full_json},
            )
            self._write_company_doc(company_id, company_name, user_id, doc)
            self._invalidate_query_cache("companies:")
            self._invalidate_query_cache("versions:")
            self._record_activity(
                actor_user_id=user_id,
                scope="storage",
                activity_type="agent1_flat_saved",
                run_id=run_id,
                company_id=company_id,
                company_name=company_name,
                details={"source_llm": source_llm, "key_count": len(full_json or {})},
            )

    def insert_company_raw_data(
        self,
        *,
        run_id: str,
        company_name: str,
        company_id: str,
        raw_json: Dict[str, Any],
        user_id: str,
    ) -> None:
        user_id = require_user_id(user_id, context="local raw data insert")
        with self._lock:
            doc = self._load_company_doc(company_id, company_name, user_id=user_id)
            doc["company_name"] = company_name
            doc["company_id"] = company_id
            doc["user_id"] = user_id
            doc.setdefault("raw_data", {})
            if isinstance(raw_json, dict):
                _merge_dict(doc["raw_data"], raw_json)
            doc["raw_data"]["last_run_id"] = run_id
            doc["raw_data"]["updated_at"] = _now_iso()
            doc["updated_at"] = _now_iso()
            self._record_company_version(
                doc=doc,
                user_id=user_id,
                company_id=company_id,
                company_name=company_name,
                run_id=run_id,
                version_kind="raw_data",
                snapshot_payload=raw_json,
            )
            self._write_company_doc(company_id, company_name, user_id, doc)
            self._invalidate_query_cache("companies:")
            self._invalidate_query_cache("versions:")
            self._record_activity(
                actor_user_id=user_id,
                scope="storage",
                activity_type="raw_data_saved",
                run_id=run_id,
                company_id=company_id,
                company_name=company_name,
                details={"raw_key_count": len(raw_json or {}) if isinstance(raw_json, dict) else 0},
            )

    def upsert_company_consolidated_data(
        self,
        *,
        run_id: str,
        company_name: str,
        company_id: str,
        consolidated_json: Dict[str, Any],
        chunk_count: int,
        chunk_coverage_pct: float,
        user_id: str,
    ) -> None:
        user_id = require_user_id(user_id, context="local consolidated upsert")
        with self._lock:
            doc = self._load_company_doc(company_id, company_name, user_id=user_id)
            doc["company_name"] = company_name
            doc["company_id"] = company_id
            doc["user_id"] = user_id
            doc["consolidated"] = {
                "run_id": run_id,
                "generated_at": _now_iso(),
                "chunk_count": int(chunk_count or 0),
                "chunk_coverage_pct": float(chunk_coverage_pct or 0.0),
                "json": consolidated_json,
            }
            doc["updated_at"] = _now_iso()
            self._record_company_version(
                doc=doc,
                user_id=user_id,
                company_id=company_id,
                company_name=company_name,
                run_id=run_id,
                version_kind="consolidated",
                snapshot_payload=consolidated_json,
            )
            self._write_company_doc(company_id, company_name, user_id, doc)
            self._invalidate_query_cache("companies:")
            self._invalidate_query_cache("versions:")
            self._record_activity(
                actor_user_id=user_id,
                scope="storage",
                activity_type="consolidated_data_saved",
                run_id=run_id,
                company_id=company_id,
                company_name=company_name,
                details={
                    "chunk_count": int(chunk_count or 0),
                    "chunk_coverage_pct": float(chunk_coverage_pct or 0.0),
                    "field_count": len(consolidated_json or {}) if isinstance(consolidated_json, dict) else 0,
                },
            )

    def insert_company_chunks(
        self,
        *,
        run_id: str,
        company_name: str,
        company_id: str,
        chunks: List[Dict[str, Any]],
        user_id: str,
    ) -> None:
        user_id = require_user_id(user_id, context="local chunk insert")
        with self._lock:
            doc = self._load_company_doc(company_id, company_name, user_id=user_id)
            doc["company_name"] = company_name
            doc["company_id"] = company_id
            doc["user_id"] = user_id
            doc["chunks"] = chunks or []
            doc["chunks_run_id"] = run_id
            doc["updated_at"] = _now_iso()
            self._record_company_version(
                doc=doc,
                user_id=user_id,
                company_id=company_id,
                company_name=company_name,
                run_id=run_id,
                version_kind="semantic_chunks",
                snapshot_payload=chunks,
            )
            self._write_company_doc(company_id, company_name, user_id, doc)
            self._invalidate_query_cache("companies:")
            self._invalidate_query_cache("versions:")
            self._record_activity(
                actor_user_id=user_id,
                scope="storage",
                activity_type="semantic_chunks_saved",
                run_id=run_id,
                company_id=company_id,
                company_name=company_name,
                details={"chunk_count": len(chunks or [])},
            )

    def repair_company_chunks(
        self,
        *,
        run_id: str,
        company_name: str,
        company_id: str,
        consolidated_json: Dict[str, Any],
        chunk_count: int,
        chunk_coverage_pct: float,
        chunks: List[Dict[str, Any]],
        schema_field_count: Optional[int] = None,
        user_id: str,
    ) -> Dict[str, Any]:
        user_id = require_user_id(user_id, context="local chunk repair")
        with self._lock:
            doc = self._load_company_doc(company_id, company_name, user_id=user_id)
            doc["company_name"] = company_name
            doc["company_id"] = company_id
            doc["user_id"] = user_id

            consolidated = doc.get("consolidated")
            if not isinstance(consolidated, dict):
                consolidated = {}
            consolidated["run_id"] = consolidated.get("run_id") or run_id
            consolidated["generated_at"] = consolidated.get("generated_at") or _now_iso()
            consolidated["chunk_count"] = int(chunk_count or 0)
            consolidated["chunk_coverage_pct"] = float(chunk_coverage_pct or 0.0)
            if isinstance(consolidated_json, dict) and consolidated_json:
                consolidated["json"] = consolidated_json
            doc["consolidated"] = consolidated

            if chunks:
                doc["chunks"] = list(chunks)
                doc["chunks_run_id"] = run_id or doc.get("chunks_run_id")

            raw_data = doc.get("raw_data")
            if not isinstance(raw_data, dict):
                raw_data = {}
            if schema_field_count is not None and not raw_data.get("schema_field_count"):
                raw_data["schema_field_count"] = int(schema_field_count)
            if run_id and not raw_data.get("last_run_id"):
                raw_data["last_run_id"] = run_id
            doc["raw_data"] = raw_data

            doc["updated_at"] = _now_iso()
            self._record_company_version(
                doc=doc,
                user_id=user_id,
                company_id=company_id,
                company_name=company_name,
                run_id=run_id,
                version_kind="repair",
                snapshot_payload={
                    "consolidated": consolidated_json,
                    "chunks": chunks,
                    "schema_field_count": schema_field_count,
                },
            )
            self._write_company_doc(company_id, company_name, user_id, doc)
            self._invalidate_query_cache("companies:")
            self._invalidate_query_cache("versions:")
            self._record_activity(
                actor_user_id=user_id,
                scope="storage",
                activity_type="company_data_repaired",
                run_id=run_id,
                company_id=company_id,
                company_name=company_name,
                details={
                    "chunk_count": len(chunks or []),
                    "schema_field_count": schema_field_count,
                },
            )
            return doc

    def mark_parameters_as_failed(
        self,
        *,
        run_id: str,
        company_id: str,
        failed_param_ids: List[int],
        user_id: str,
    ) -> None:
        user_id = require_user_id(user_id, context="local failed parameter marking")
        ids = sorted({int(x) for x in failed_param_ids if isinstance(x, int) or str(x).isdigit()})
        with self._lock:
            run_path = self._run_path(run_id)
            run = self._read(run_path, {})
            if isinstance(run, dict):
                owner = str(run.get("user_id") or "").strip()
                if owner and owner != user_id:
                    raise PermissionError(f"Pipeline run '{run_id}' is not owned by user_id '{user_id}'.")
                run["user_id"] = user_id
                run["failed_param_ids"] = ids
                run["updated_at"] = _now_iso()
                self._write(run_path, run)

            if company_id:
                doc = self._load_company_doc(company_id, user_id=user_id)
                doc["user_id"] = user_id
                doc.setdefault("failed_parameter_ids_by_run", {})
                doc["failed_parameter_ids_by_run"][run_id] = ids
                doc["updated_at"] = _now_iso()
                self._write_company_doc(company_id, doc.get("company_name", ""), user_id, doc)
            self._invalidate_query_cache("runs:")
            self._invalidate_query_cache("companies:")
            self._record_activity(
                actor_user_id=user_id,
                scope="pipeline",
                activity_type="failed_parameters_marked",
                activity_status="completed",
                run_id=run_id,
                company_id=company_id,
                details={"failed_parameter_ids": ids},
            )

    def record_admin_activity(
        self,
        *,
        actor_user_id: str,
        activity_type: str,
        activity_status: str = "completed",
        scope: str = "admin",
        target_user_id: str | None = None,
        run_id: str | None = None,
        company_id: str | None = None,
        company_name: str | None = None,
        details: Dict[str, Any] | None = None,
        source: str = "server",
    ) -> Dict[str, Any]:
        with self._lock:
            return self._record_activity(
                actor_user_id=str(actor_user_id or "").strip(),
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

    def record_error_event(
        self,
        *,
        user_id: str | None,
        error_type: str,
        message: str,
        source: str = "server",
        run_id: str | None = None,
        company_id: str | None = None,
        company_name: str | None = None,
        details: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        with self._lock:
            return self._record_error(
                user_id=user_id,
                error_type=error_type,
                message=message,
                source=source,
                run_id=run_id,
                company_id=company_id,
                company_name=company_name,
                details=details,
            )

    def list_activity_logs(
        self,
        *,
        limit: int = 200,
        actor_user_id: str | None = None,
        scope: str | None = None,
        activity_type: str | None = None,
    ) -> List[Dict[str, Any]]:
        normalized_limit = max(1, min(int(limit or 200), 5000))
        cache_key = f"activity:{normalized_limit}:{actor_user_id or ''}:{scope or ''}:{activity_type or ''}"
        with self._lock:
            cached = self._cache_get(cache_key)
            if isinstance(cached, list):
                return self._json_copy(cached)
            rows = self._iter_jsonl_rows(self.activity_log_path)
            if actor_user_id:
                wanted_actor = str(actor_user_id).strip()
                rows = [row for row in rows if str(row.get("actor_user_id") or "").strip() == wanted_actor]
            if scope:
                wanted_scope = str(scope).strip().lower()
                rows = [row for row in rows if str(row.get("scope") or "").strip().lower() == wanted_scope]
            if activity_type:
                wanted_type = str(activity_type).strip().lower()
                rows = [row for row in rows if str(row.get("activity_type") or "").strip().lower() == wanted_type]
            self._sort_desc_by_created_at(rows)
            limited = rows[:normalized_limit]
            self._cache_set(cache_key, self._json_copy(limited))
            return limited

    def list_error_logs(
        self,
        *,
        limit: int = 200,
        user_id: str | None = None,
        error_type: str | None = None,
    ) -> List[Dict[str, Any]]:
        normalized_limit = max(1, min(int(limit or 200), 5000))
        cache_key = f"error:{normalized_limit}:{user_id or ''}:{error_type or ''}"
        with self._lock:
            cached = self._cache_get(cache_key)
            if isinstance(cached, list):
                return self._json_copy(cached)
            rows = self._iter_jsonl_rows(self.error_log_path)
            if user_id:
                wanted_user = str(user_id).strip()
                rows = [row for row in rows if str(row.get("user_id") or "").strip() == wanted_user]
            if error_type:
                wanted_type = str(error_type).strip().lower()
                rows = [row for row in rows if str(row.get("error_type") or "").strip().lower() == wanted_type]
            self._sort_desc_by_created_at(rows)
            limited = rows[:normalized_limit]
            self._cache_set(cache_key, self._json_copy(limited))
            return limited

    def list_company_versions(
        self,
        *,
        user_id: str,
        company_id: str = "",
        company_name: str = "",
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="local company version listing")
        normalized_limit = max(1, min(int(limit or 200), 2000))
        if company_id or company_name:
            doc = self.get_company(company_id=company_id, company_name=company_name, user_id=user_id)
            if not isinstance(doc, dict):
                return []
            versions = list(doc.get("versions") or [])
            if not isinstance(versions, list):
                return []
            self._sort_desc_by_created_at(versions)
            return versions[:normalized_limit]

        versions: List[Dict[str, Any]] = []
        with self._lock:
            for doc in self._visible_company_docs(user_id):
                rows = doc.get("versions")
                if not isinstance(rows, list):
                    continue
                for row in rows:
                    if isinstance(row, dict):
                        versions.append(dict(row))
        self._sort_desc_by_created_at(versions)
        return versions[:normalized_limit]

    def list_all_company_versions(
        self,
        *,
        limit: int = 500,
        user_id: str | None = None,
        company_id: str | None = None,
    ) -> List[Dict[str, Any]]:
        normalized_limit = max(1, min(int(limit or 500), 5000))
        cache_key = f"versions:all:{normalized_limit}:{user_id or ''}:{company_id or ''}"
        with self._lock:
            cached = self._cache_get(cache_key)
            if isinstance(cached, list):
                return self._json_copy(cached)
            out: List[Dict[str, Any]] = []
            for path in sorted(self.companies_dir.glob("*.json")):
                doc = self._read(path, {})
                if not isinstance(doc, dict):
                    continue
                owner = str(doc.get("user_id") or "").strip()
                if user_id and owner != str(user_id).strip():
                    continue
                if company_id and str(doc.get("company_id") or "").strip() != str(company_id).strip():
                    continue
                rows = doc.get("versions")
                if not isinstance(rows, list):
                    continue
                for row in rows:
                    if isinstance(row, dict):
                        out.append(dict(row))
            self._sort_desc_by_created_at(out)
            limited = out[:normalized_limit]
            self._cache_set(cache_key, self._json_copy(limited))
            return limited

    def list_all_companies(self, *, limit: int = 5000) -> List[Dict[str, Any]]:
        normalized_limit = max(1, min(int(limit or 5000), 20000))
        cache_key = f"companies:all:{normalized_limit}"
        with self._lock:
            cached = self._cache_get(cache_key)
            if isinstance(cached, list):
                return self._json_copy(cached)
            rows: List[Dict[str, Any]] = []
            for path in sorted(self.companies_dir.glob("*.json")):
                doc = self._read(path, {})
                if isinstance(doc, dict):
                    rows.append(doc)
            self._sort_desc_by_created_at(rows)
            limited = rows[:normalized_limit]
            self._cache_set(cache_key, self._json_copy(limited))
            return limited

    def list_all_pipeline_runs(self, *, limit: int = 5000) -> List[Dict[str, Any]]:
        normalized_limit = max(1, min(int(limit or 5000), 50000))
        cache_key = f"runs:all:{normalized_limit}"
        with self._lock:
            cached = self._cache_get(cache_key)
            if isinstance(cached, list):
                return self._json_copy(cached)
            rows: List[Dict[str, Any]] = []
            for path in sorted(self.runs_dir.glob("*.json")):
                doc = self._read(path, {})
                if isinstance(doc, dict):
                    rows.append(doc)
            rows.sort(key=lambda row: str(row.get("completed_at") or row.get("started_at") or ""), reverse=True)
            limited = rows[:normalized_limit]
            self._cache_set(cache_key, self._json_copy(limited))
            return limited

    def get_storage_summary(self) -> Dict[str, Any]:
        cache_key = "dashboard:storage_summary"
        with self._lock:
            cached = self._cache_get(cache_key)
            if isinstance(cached, dict):
                return self._json_copy(cached)

            def _dir_size(path: Path) -> int:
                total = 0
                if not path.exists():
                    return 0
                for child in path.rglob("*"):
                    if child.is_file():
                        total += int(child.stat().st_size)
                return total

            summary = {
                "runs_count": len(list(self.runs_dir.glob("*.json"))),
                "companies_count": len(list(self.companies_dir.glob("*.json"))),
                "versions_count": len(list(self.versions_dir.rglob("*.json"))),
                "activity_log_entries": len(self._iter_jsonl_rows(self.activity_log_path)),
                "error_log_entries": len(self._iter_jsonl_rows(self.error_log_path)),
                "bytes": {
                    "runs": _dir_size(self.runs_dir),
                    "companies": _dir_size(self.companies_dir),
                    "versions": _dir_size(self.versions_dir),
                    "admin_logs": _dir_size(self.admin_dir),
                },
            }
            summary["bytes"]["total"] = int(sum(summary["bytes"].values()))
            self._cache_set(cache_key, self._json_copy(summary))
            return summary

    def get_companies_full_data(
        self,
        *,
        company_ids: List[str],
        company_names: List[str],
        user_id: str,
    ) -> Dict[str, Dict[str, Any]]:
        user_id = require_user_id(user_id, context="local company data fetch")
        names = {str(n).strip().lower() for n in company_names if str(n).strip()}
        wanted_ids = {str(i).strip() for i in company_ids if str(i).strip()}
        out: Dict[str, Dict[str, Any]] = {}

        with self._lock:
            for doc in self._visible_company_docs(user_id):
                cid = str(doc.get("company_id") or "").strip()
                cname = str(doc.get("company_name") or "").strip()
                if wanted_ids and cid in wanted_ids:
                    out[cid] = doc
                    continue
                if names and cname.lower() in names and cid:
                    out[cid] = doc

        return out

    def get_company(
        self,
        *,
        company_id: str = "",
        company_name: str = "",
        user_id: str,
    ) -> Optional[Dict[str, Any]]:
        rows = self.get_companies_full_data(
            company_ids=[company_id] if company_id else [],
            company_names=[company_name] if company_name else [],
            user_id=user_id,
        )
        if company_id and company_id in rows:
            return rows[company_id]
        return next(iter(rows.values()), None)

    def list_companies(self, *, user_id: str) -> List[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="local company listing")
        cache_key = f"companies:user:{user_id}"
        with self._lock:
            cached = self._cache_get(cache_key)
            if isinstance(cached, list):
                return self._json_copy(cached)
            rows = self._visible_company_docs(user_id)
            self._sort_desc_by_created_at(rows)
            self._cache_set(cache_key, self._json_copy(rows))
            return rows

    def list_pipeline_runs(self, *, user_id: str) -> List[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="local pipeline run listing")
        cache_key = f"runs:user:{user_id}"
        rows: List[Dict[str, Any]] = []
        with self._lock:
            cached = self._cache_get(cache_key)
            if isinstance(cached, list):
                return self._json_copy(cached)
            for path in sorted(self.runs_dir.glob("*.json")):
                doc = self._read(path, {})
                if not isinstance(doc, dict) or not self._run_visible_to_user(doc, user_id):
                    continue
                rows.append(doc)
            rows.sort(key=lambda row: str(row.get("completed_at") or row.get("started_at") or ""), reverse=True)
            self._cache_set(cache_key, self._json_copy(rows))
            return rows

    def get_pipeline_run_for_path(self, *, path: str, user_id: str) -> Optional[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="local pipeline path ownership")
        target = str(path or "").strip()
        if not target:
            return None
        normalized_target = str(Path(target).resolve())
        for run in self.list_pipeline_runs(user_id=user_id):
            paths = run.get("paths") or {}
            if not isinstance(paths, dict):
                continue
            for candidate in paths.values():
                if not candidate:
                    continue
                if str(Path(str(candidate)).resolve()) == normalized_target:
                    return run
        return None

    def search_similar_companies_from_chunks(
        self,
        *,
        query_text: str,
        top_k_companies: int = 5,
        top_k_chunks: int = 200,
        filters: Optional[Dict[str, Any]] = None,
        exclude_company_name: Optional[str] = None,
        user_id: str,
    ) -> List[Dict[str, Any]]:
        user_id = require_user_id(user_id, context="local company chunk search")
        del filters  # reserved for future filtering
        query_tokens = _tokenize(query_text)
        if not query_tokens:
            return []

        exclude = (exclude_company_name or "").strip().lower()
        bucket: Dict[str, Dict[str, Any]] = {}

        with self._lock:
            for doc in self._visible_company_docs(user_id):

                company_name = str(doc.get("company_name") or "").strip()
                if exclude and company_name.lower() == exclude:
                    continue

                company_id = str(doc.get("company_id") or "").strip() or _slug(company_name)
                chunks = doc.get("chunks") or []
                if not isinstance(chunks, list):
                    chunks = []

                hits = []
                for chunk in chunks:
                    if not isinstance(chunk, dict):
                        continue
                    text = str(chunk.get("chunk_text") or "")
                    chunk_tokens = _tokenize(text)
                    if not chunk_tokens:
                        continue
                    overlap = query_tokens.intersection(chunk_tokens)
                    if not overlap:
                        continue
                    precision = len(overlap) / max(len(query_tokens), 1)
                    recall = len(overlap) / max(len(chunk_tokens), 1)
                    score = round((0.7 * precision + 0.3 * recall), 4)
                    hits.append(
                        {
                            "chunk_id": chunk.get("chunk_id"),
                            "chunk_title": chunk.get("chunk_title"),
                            "score": score,
                            "overlap_terms": sorted(overlap),
                        }
                    )

                if not hits:
                    continue

                hits.sort(key=lambda x: x["score"], reverse=True)
                hits = hits[: max(1, int(top_k_chunks))]
                bucket[company_id] = {
                    "company_id": company_id,
                    "company_name": company_name,
                    "score": round(sum(h["score"] for h in hits) / len(hits), 4),
                    "match_count": len(hits),
                    "top_chunks": hits[:5],
                }

        ranked = sorted(
            bucket.values(),
            key=lambda x: (x["score"], x["match_count"]),
            reverse=True,
        )
        return ranked[: max(1, int(top_k_companies))]


_CLIENT = LocalStoreClient()


def get_local_store_client() -> LocalStoreClient:
    return _CLIENT
