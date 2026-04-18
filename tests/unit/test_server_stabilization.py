from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import httpx
import jwt

import server
from core.auth import get_supabase_jwt_verifier


TEST_JWT_SECRET = "test-jwt-secret-that-is-long-enough"


def _make_token(user_id: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,
        "aud": "authenticated",
        "iss": "https://example.supabase.co/auth/v1",
        "exp": int((now + timedelta(hours=1)).timestamp()),
        "iat": int(now.timestamp()),
    }
    return jwt.encode(payload, TEST_JWT_SECRET, algorithm="HS256")


@dataclass
class _Result:
    data: Any


class _CompanyRunsQuery:
    def __init__(self, parent: "_FakeSupabase") -> None:
        self._parent = parent
        self._operation = ""
        self._payload: Dict[str, Any] = {}
        self._selected_fields: List[str] = []
        self._filters: List[tuple[str, Any]] = []
        self._single_mode: str | None = None
        self._order_field = ""
        self._order_desc = False

    def select(self, fields: str) -> "_CompanyRunsQuery":
        self._operation = "select"
        self._selected_fields = [field.strip() for field in fields.split(",") if field.strip()]
        return self

    def eq(self, key: str, value: Any) -> "_CompanyRunsQuery":
        self._filters.append((key, value))
        return self

    def maybe_single(self) -> "_CompanyRunsQuery":
        self._single_mode = "maybe_single"
        return self

    def single(self) -> "_CompanyRunsQuery":
        self._single_mode = "single"
        return self

    def order(self, field: str, desc: bool = False) -> "_CompanyRunsQuery":
        self._order_field = field
        self._order_desc = bool(desc)
        return self

    def update(self, payload: Dict[str, Any]) -> "_CompanyRunsQuery":
        self._operation = "update"
        self._payload = dict(payload)
        return self

    def insert(self, payload: Dict[str, Any]) -> "_CompanyRunsQuery":
        self._operation = "insert"
        self._payload = dict(payload)
        return self

    def execute(self) -> _Result:
        if self._operation == "insert":
            next_id = f"run-{len(self._parent.rows) + 1}"
            row = {"id": next_id, **self._payload}
            self._parent.rows.append(row)
            return _Result([dict(row)])

        matching = [row for row in self._parent.rows if self._matches(row)]

        if self._operation == "update":
            updated = []
            for row in matching:
                row.update(self._payload)
                updated.append(dict(row))
            return _Result(updated)

        if self._operation != "select":
            return _Result(None)

        rows = [self._project(row) for row in matching]
        if self._order_field:
            rows.sort(key=lambda item: item.get(self._order_field), reverse=self._order_desc)

        if self._single_mode in {"single", "maybe_single"}:
            return _Result(rows[0] if rows else None)
        return _Result(rows)

    def _matches(self, row: Dict[str, Any]) -> bool:
        for key, value in self._filters:
            if row.get(key) != value:
                return False
        return True

    def _project(self, row: Dict[str, Any]) -> Dict[str, Any]:
        if not self._selected_fields:
            return dict(row)
        return {field: row.get(field) for field in self._selected_fields}


class _FakeSupabase:
    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self.rows = rows

    def table(self, name: str) -> _CompanyRunsQuery:
        if name != "company_runs":
            raise AssertionError(f"Unexpected table: {name}")
        return _CompanyRunsQuery(self)


def _request(method: str, path: str, *, json_body: Dict[str, Any] | None = None, headers: Dict[str, str] | None = None):
    async def run():
        transport = httpx.ASGITransport(app=server.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.request(method, path, json=json_body, headers=headers)

    return asyncio.run(run())


def test_auth_me_rejects_missing_bearer_token(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", TEST_JWT_SECRET)
    get_supabase_jwt_verifier.cache_clear()
    server.app.dependency_overrides.clear()

    try:
        response = _request("GET", "/auth/me")
    finally:
        get_supabase_jwt_verifier.cache_clear()

    assert response.status_code == 401
    assert response.json()["detail"] == "Missing bearer token."


def test_auth_me_returns_token_user_id(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", TEST_JWT_SECRET)
    get_supabase_jwt_verifier.cache_clear()
    server.app.dependency_overrides.clear()

    try:
        response = _request(
            "GET",
            "/auth/me",
            headers={"Authorization": f"Bearer {_make_token('user-42')}"},
        )
    finally:
        get_supabase_jwt_verifier.cache_clear()

    assert response.status_code == 200
    assert response.json() == {"user_id": "user-42"}


def test_analyze_scopes_cache_lookup_to_authenticated_user(monkeypatch):
    fake_db = _FakeSupabase(
        rows=[
            {
                "id": "run-legacy",
                "company_name": "Acme",
                "version": 7,
                "is_active": True,
                "run_by": "user-b",
                "golden_record": {"source": "other-user"},
                "created_at": "2026-04-18T00:00:00+00:00",
            }
        ]
    )
    monkeypatch.setattr(server, "_supabase", fake_db)
    async def _noop_run_and_save(*_args, **_kwargs) -> None:
        return None

    monkeypatch.setattr(server, "_run_and_save", _noop_run_and_save)
    async def _override_user() -> str:
        return "user-a"

    server.app.dependency_overrides[server._authenticated_user_id] = _override_user

    try:
        response = _request(
            "POST",
            "/analyze",
            json_body={"company": "Acme", "force_refresh": False},
        )
    finally:
        server.app.dependency_overrides.clear()

    payload = response.json()
    assert response.status_code == 200
    assert payload["status"] == "running"
    assert payload["version"] == 1
    inserted = next(row for row in fake_db.rows if row["id"] == payload["run_id"])
    assert inserted["run_by"] == "user-a"
    assert inserted["company_name"] == "Acme"


def test_run_status_hides_foreign_user_run(monkeypatch):
    fake_db = _FakeSupabase(
        rows=[
            {
                "id": "run-foreign",
                "company_name": "Acme",
                "version": 2,
                "is_active": True,
                "run_by": "user-b",
                "golden_record": {"ok": True},
                "created_at": "2026-04-18T00:00:00+00:00",
            }
        ]
    )
    monkeypatch.setattr(server, "_supabase", fake_db)
    async def _override_user() -> str:
        return "user-a"

    server.app.dependency_overrides[server._authenticated_user_id] = _override_user

    try:
        response = _request("GET", "/run-status/run-foreign")
    finally:
        server.app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json()["detail"] == "Run not found"
