from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Set, Tuple

from core.supabase_store import SupabaseStore


@dataclass
class _Result:
    data: Any


class _TableQuery:
    def __init__(self, client: "_FakeSupabaseClient", table: str) -> None:
        self._client = client
        self._table = table
        self._operation = ""
        self._fields = ""
        self._payload: Dict[str, Any] = {}
        self._filters: List[Tuple[str, Any]] = []

    def select(self, fields: str) -> "_TableQuery":
        self._operation = "select"
        self._fields = fields
        return self

    def limit(self, _value: int) -> "_TableQuery":
        return self

    def upsert(self, payload: Dict[str, Any], on_conflict: str = "") -> "_TableQuery":
        self._operation = "upsert"
        self._payload = dict(payload)
        self._client.operations.append(
            {
                "table": self._table,
                "operation": "upsert",
                "payload": dict(payload),
                "on_conflict": on_conflict,
            }
        )
        return self

    def update(self, payload: Dict[str, Any]) -> "_TableQuery":
        self._operation = "update"
        self._payload = dict(payload)
        return self

    def insert(self, payload: Dict[str, Any]) -> "_TableQuery":
        self._operation = "insert"
        self._payload = dict(payload)
        return self

    def eq(self, key: str, value: Any) -> "_TableQuery":
        self._filters.append((key, value))
        return self

    def execute(self) -> _Result:
        if self._operation == "select":
            for column in [c.strip() for c in self._fields.split(",") if c.strip()]:
                if (self._table, column) in self._client.missing_columns:
                    raise RuntimeError(
                        {"message": f"column {self._table}.{column} does not exist", "code": "42703"}
                    )
            return _Result(data=[])

        self._client.operations.append(
            {
                "table": self._table,
                "operation": self._operation,
                "payload": dict(self._payload),
                "filters": list(self._filters),
            }
        )
        return _Result(data=[])


class _FakeSupabaseClient:
    def __init__(self, *, missing_columns: Set[Tuple[str, str]] | None = None) -> None:
        self.missing_columns = set(missing_columns or set())
        self.operations: List[Dict[str, Any]] = []

    def table(self, table_name: str) -> _TableQuery:
        return _TableQuery(self, table_name)


def _build_store(monkeypatch, fake_client: _FakeSupabaseClient) -> SupabaseStore:
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "service-key")
    monkeypatch.setattr("core.supabase_store.create_client", lambda *_args, **_kwargs: fake_client)
    return SupabaseStore()


def test_create_pipeline_run_omits_user_id_when_column_missing(monkeypatch):
    fake_client = _FakeSupabaseClient(missing_columns={("pipeline_runs", "user_id")})
    store = _build_store(monkeypatch, fake_client)

    store.create_pipeline_run(
        run_id="run-1",
        company_name="Acme",
        company_id="acme",
        user_id="user-a",
    )

    upsert_call = next(op for op in fake_client.operations if op["operation"] == "upsert")
    assert upsert_call["table"] == "pipeline_runs"
    assert "user_id" not in upsert_call["payload"]


def test_create_pipeline_run_includes_user_id_when_column_exists(monkeypatch):
    fake_client = _FakeSupabaseClient()
    store = _build_store(monkeypatch, fake_client)

    store.create_pipeline_run(
        run_id="run-2",
        company_name="Acme",
        company_id="acme",
        user_id="user-a",
    )

    upsert_call = next(op for op in fake_client.operations if op["operation"] == "upsert")
    assert upsert_call["payload"]["user_id"] == "user-a"


def test_complete_pipeline_run_falls_back_to_run_id_filter_when_user_column_missing(monkeypatch):
    fake_client = _FakeSupabaseClient(missing_columns={("pipeline_runs", "user_id")})
    store = _build_store(monkeypatch, fake_client)

    store.complete_pipeline_run(run_id="run-3", user_id="user-a")

    update_call = next(op for op in fake_client.operations if op["operation"] == "update")
    assert update_call["table"] == "pipeline_runs"
    assert "user_id" not in update_call["payload"]
    assert update_call["filters"] == [("run_id", "run-3")]


def test_insert_agent1_output_embeds_user_id_in_raw_data_when_column_missing(monkeypatch):
    fake_client = _FakeSupabaseClient(missing_columns={("agent1_raw_outputs", "user_id")})
    store = _build_store(monkeypatch, fake_client)

    source_raw_data = {"key": "value"}
    store.insert_agent1_output(
        run_id="run-4",
        company_id="acme",
        company_name="Acme",
        source_llm="llm1",
        raw_data=source_raw_data,
        filled_count=10,
        user_id="user-a",
    )

    insert_call = next(op for op in fake_client.operations if op["operation"] == "insert")
    assert insert_call["table"] == "agent1_raw_outputs"
    assert "user_id" not in insert_call["payload"]
    assert insert_call["payload"]["raw_data"]["__user_id"] == "user-a"
    assert source_raw_data == {"key": "value"}
