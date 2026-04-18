"""Local JSON persistence layer used in place of external DB/vector services."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
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
        self.root.mkdir(parents=True, exist_ok=True)
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.companies_dir.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()

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
    def _visible_to_user(doc: Dict[str, Any], user_id: str) -> bool:
        owner = str(doc.get("user_id") or "").strip()
        return not owner or owner == user_id

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
        return doc

    def _write_company_doc(self, company_id: str, company_name: str, user_id: str, doc: Dict[str, Any]) -> None:
        user_id = require_user_id(user_id, context="local company document write")
        doc["user_id"] = user_id
        self._write(self._company_path(company_id, company_name, user_id=user_id), doc)

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

    def create_pipeline_run(self, *, run_id: str, company_name: str, company_id: str, user_id: str) -> None:
        user_id = require_user_id(user_id, context="local pipeline run creation")
        with self._lock:
            run = {
                "run_id": run_id,
                "user_id": user_id,
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
                    },
                }
            )
            self._write(path, run)

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
            self._write_company_doc(company_id, company_name, user_id, doc)

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
            self._write_company_doc(company_id, company_name, user_id, doc)

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
            self._write_company_doc(company_id, company_name, user_id, doc)

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
            self._write_company_doc(company_id, company_name, user_id, doc)

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
            self._write_company_doc(company_id, company_name, user_id, doc)
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
        with self._lock:
            rows = self._visible_company_docs(user_id)
        return rows

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
