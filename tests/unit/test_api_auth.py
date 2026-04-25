from __future__ import annotations

import asyncio
import os
from datetime import timedelta

import httpx
from fastapi import Depends, FastAPI
from fastapi import HTTPException

import core.auth as auth_module
from core.auth import CurrentUser, get_auth_jwt_verifier, get_current_user


TEST_JWT_SECRET = "test-jwt-secret-that-is-long-enough"


class _FakeAuthStore:
    def __init__(self, user: dict[str, object]) -> None:
        self._user = dict(user)

    def get_user_by_id(self, user_id: str) -> dict[str, object] | None:
        return dict(self._user) if self._user.get("user_id") == user_id else None

    def get_user_by_email(self, email: str) -> dict[str, object] | None:
        return dict(self._user) if self._user.get("email") == email else None


def _make_user(**overrides: object) -> dict[str, object]:
    base = {
        "user_id": "user-123",
        "name": "Analyst User",
        "email": "analyst@example.com",
        "password": auth_module.hash_password("password-123"),
        "role": "user",
        "approval_status": "approved",
        "created_at": "2026-04-25T00:00:00+00:00",
        "session_nonce": "nonce-123",
    }
    base.update(overrides)
    return base


def _make_token(user: dict[str, object], session_nonce: str | None = None) -> str:
    return auth_module.create_access_token(
        user,
        session_nonce=session_nonce or str(user.get("session_nonce") or ""),
        expires_delta=timedelta(hours=1),
    )


def _make_app() -> FastAPI:
    app = FastAPI()

    @app.get("/auth/me")
    async def auth_me(current_user: CurrentUser = Depends(get_current_user)):
        return {
            "user_id": current_user.user_id,
            "email": current_user.email,
            "role": current_user.role,
        }

    return app


def test_password_hash_round_trip():
    os.environ["PASSWORD_HASH_ITERATIONS"] = "100000"
    hashed = auth_module.hash_password("password-123")

    assert hashed.startswith("pbkdf2_sha256$")
    assert auth_module.verify_password("password-123", hashed) is True
    assert auth_module.verify_password("wrong-password", hashed) is False


def test_verifier_extracts_user_profile(monkeypatch):
    monkeypatch.setenv("PASSWORD_HASH_ITERATIONS", "100000")
    user = _make_user()
    monkeypatch.setenv("APP_JWT_SECRET", TEST_JWT_SECRET)
    monkeypatch.setattr(auth_module, "_get_auth_store", lambda: _FakeAuthStore(user))
    get_auth_jwt_verifier.cache_clear()

    try:
        current_user = get_auth_jwt_verifier().verify_token(_make_token(user))
    finally:
        get_auth_jwt_verifier.cache_clear()

    assert current_user.user_id == "user-123"
    assert current_user.email == "analyst@example.com"
    assert current_user.role == "user"


def test_verifier_rejects_stale_session_nonce(monkeypatch):
    monkeypatch.setenv("PASSWORD_HASH_ITERATIONS", "100000")
    user = _make_user()
    monkeypatch.setenv("APP_JWT_SECRET", TEST_JWT_SECRET)
    monkeypatch.setattr(auth_module, "_get_auth_store", lambda: _FakeAuthStore(user))
    get_auth_jwt_verifier.cache_clear()

    try:
        try:
            get_auth_jwt_verifier().verify_token(_make_token(user, session_nonce="stale-nonce"))
        except HTTPException as exc:
            assert exc.status_code == 401
            assert exc.detail == "Session is no longer valid."
        else:
            raise AssertionError("Expected stale nonce to be rejected")
    finally:
        get_auth_jwt_verifier.cache_clear()


def test_verifier_rejects_pending_user(monkeypatch):
    monkeypatch.setenv("PASSWORD_HASH_ITERATIONS", "100000")
    user = _make_user(approval_status="pending")
    monkeypatch.setenv("APP_JWT_SECRET", TEST_JWT_SECRET)
    monkeypatch.setattr(auth_module, "_get_auth_store", lambda: _FakeAuthStore(user))
    get_auth_jwt_verifier.cache_clear()

    try:
        try:
            get_auth_jwt_verifier().verify_token(_make_token(user))
        except HTTPException as exc:
            assert exc.status_code == 403
            assert exc.detail == auth_module.PENDING_APPROVAL_MESSAGE
        else:
            raise AssertionError("Expected pending user to be rejected")
    finally:
        get_auth_jwt_verifier.cache_clear()


def test_get_current_user_accepts_bearer_token(monkeypatch):
    monkeypatch.setenv("PASSWORD_HASH_ITERATIONS", "100000")
    user = _make_user()
    monkeypatch.setenv("APP_JWT_SECRET", TEST_JWT_SECRET)
    monkeypatch.setattr(auth_module, "_get_auth_store", lambda: _FakeAuthStore(user))
    get_auth_jwt_verifier.cache_clear()

    async def run_request():
        transport = httpx.ASGITransport(app=_make_app())
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.get(
                "/auth/me",
                headers={"Authorization": f"Bearer {_make_token(user)}"},
            )

    try:
        response = asyncio.run(run_request())
    finally:
        get_auth_jwt_verifier.cache_clear()

    assert response.status_code == 200
    assert response.json()["user_id"] == "user-123"


def test_get_current_user_accepts_cookie_token(monkeypatch):
    monkeypatch.setenv("PASSWORD_HASH_ITERATIONS", "100000")
    user = _make_user()
    monkeypatch.setenv("APP_JWT_SECRET", TEST_JWT_SECRET)
    monkeypatch.setattr(auth_module, "_get_auth_store", lambda: _FakeAuthStore(user))
    get_auth_jwt_verifier.cache_clear()

    async def run_request():
        transport = httpx.ASGITransport(app=_make_app())
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            client.cookies.set(auth_module.auth_cookie_name(), _make_token(user))
            return await client.get("/auth/me")

    try:
        response = asyncio.run(run_request())
    finally:
        get_auth_jwt_verifier.cache_clear()

    assert response.status_code == 200
    assert response.json()["email"] == "analyst@example.com"
