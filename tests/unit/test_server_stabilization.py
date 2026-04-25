from __future__ import annotations

import asyncio

import httpx
from fastapi import BackgroundTasks

import server
from core.auth import CurrentUser


def _request(method: str, path: str):
    async def run():
        transport = httpx.ASGITransport(app=server.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.request(method, path)

    return asyncio.run(run())


def _fake_user(*, user_id: str = "user-1", name: str = "User One", role: str = "user") -> CurrentUser:
    return CurrentUser(
        user_id=user_id,
        name=name,
        email=f"{user_id}@example.com",
        role=role,
        approval_status="approved",
        claims={"sub": user_id},
    )


def test_auth_me_rejects_missing_token():
    server.app.dependency_overrides.clear()
    response = _request("GET", "/auth/me")
    assert response.status_code == 401
    assert response.json()["detail"] == "Missing authentication token."


def test_auth_me_returns_current_user_from_dependency_override():
    async def _override_current_user() -> CurrentUser:
        return _fake_user(user_id="user-42", name="Analyst")

    server.app.dependency_overrides[server.get_current_user] = _override_current_user
    try:
        response = _request("GET", "/auth/me")
    finally:
        server.app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()["user"]
    assert payload["user_id"] == "user-42"
    assert payload["name"] == "Analyst"
    assert payload["approval_status"] == "approved"


def test_run_pipeline_includes_runner_identity():
    user = _fake_user(user_id="user-99", name="Kapil")
    response = server.run_pipeline(
        body=server.RunRequest(company="Acme"),
        background_tasks=BackgroundTasks(),
        current_user=user,
    )
    task_id = response.task_id
    task = server._tasks[task_id]

    assert response.company == "Acme"
    assert task["user_id"] == "user-99"
    assert task["user_name"] == "Kapil"
    assert isinstance(task["created_at"], str) and task["created_at"].strip()


def test_status_hides_foreign_task():
    task_id = "task-foreign"
    server._tasks[task_id] = {
        "task_id": task_id,
        "user_id": "user-b",
        "user_name": "Other User",
        "status": "done",
        "company": "Globex",
        "created_at": "2026-04-18T00:00:00+00:00",
        "completed_at": "2026-04-18T00:10:00+00:00",
        "error": None,
        "result": {},
        "events": [],
        "event_seq": 0,
    }

    try:
        try:
            server.get_status(task_id, user_id="user-a")
        except Exception as exc:
            assert getattr(exc, "status_code", None) == 404
            assert "not found" in str(getattr(exc, "detail", "")).lower()
        else:
            raise AssertionError("Expected 404 for foreign task access")
    finally:
        server._tasks.pop(task_id, None)
