from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import httpx
import jwt
import pytest
from fastapi import Depends, FastAPI
from fastapi import HTTPException

import core.auth as auth_module
from core.auth import CurrentUser, get_current_user, get_supabase_jwt_verifier


TEST_JWT_SECRET = "test-jwt-secret-that-is-long-enough"


def _make_token(user_id: str, email: str = "analyst@example.com") -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,
        "email": email,
        "aud": "authenticated",
        "iss": "https://example.supabase.co/auth/v1",
        "role": "authenticated",
        "exp": int((now + timedelta(hours=1)).timestamp()),
        "iat": int(now.timestamp()),
    }
    return jwt.encode(payload, TEST_JWT_SECRET, algorithm="HS256")


def _make_app() -> FastAPI:
    app = FastAPI()

    @app.get("/auth/me")
    async def auth_me(current_user: CurrentUser = Depends(get_current_user)):
        return {"user_id": current_user.user_id}

    return app


def _make_protected_action_app(captured: dict | None = None) -> FastAPI:
    app = FastAPI()

    @app.post("/search-companies")
    async def protected_search(
        body: dict,
        current_user: CurrentUser = Depends(get_current_user),
    ):
        user_id = current_user.user_id
        if captured is not None:
            captured["body_user_id"] = body.get("user_id")
            captured["user_id"] = user_id
        return {"user_id": user_id}

    return app


def test_supabase_verifier_extracts_user_id(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", TEST_JWT_SECRET)
    get_supabase_jwt_verifier.cache_clear()

    try:
        current_user = get_supabase_jwt_verifier().verify_token(_make_token("user-123"))
    finally:
        get_supabase_jwt_verifier.cache_clear()

    assert current_user.user_id == "user-123"
    assert current_user.email == "analyst@example.com"


def test_auth_me_returns_current_user_id(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", TEST_JWT_SECRET)
    get_supabase_jwt_verifier.cache_clear()

    async def run_request():
        transport = httpx.ASGITransport(app=_make_app())
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.get(
                "/auth/me",
                headers={"Authorization": f"Bearer {_make_token('user-456')}"},
            )

    response = asyncio.run(run_request())

    get_supabase_jwt_verifier.cache_clear()

    assert response.status_code == 200
    assert response.json() == {"user_id": "user-456"}


def test_auth_me_rejects_missing_token(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", TEST_JWT_SECRET)
    get_supabase_jwt_verifier.cache_clear()

    async def run_request():
        transport = httpx.ASGITransport(app=_make_app())
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.get("/auth/me")

    response = asyncio.run(run_request())

    get_supabase_jwt_verifier.cache_clear()

    assert response.status_code == 401
    assert response.json()["detail"] == "Missing bearer token."


def test_protected_search_route_rejects_missing_token(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", TEST_JWT_SECRET)
    get_supabase_jwt_verifier.cache_clear()

    async def run_request():
        transport = httpx.ASGITransport(app=_make_protected_action_app())
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.post("/search-companies", json={"query": "AI healthcare"})

    response = asyncio.run(run_request())

    get_supabase_jwt_verifier.cache_clear()

    assert response.status_code == 401
    assert response.json()["detail"] == "Missing bearer token."


def test_protected_search_route_uses_token_user_id(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", TEST_JWT_SECRET)
    get_supabase_jwt_verifier.cache_clear()

    captured = {}

    async def run_request():
        transport = httpx.ASGITransport(app=_make_protected_action_app(captured))
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.post(
                "/search-companies",
                json={"query": "AI healthcare", "user_id": "spoofed-user"},
                headers={"Authorization": f"Bearer {_make_token('token-user')}"},
            )

    response = asyncio.run(run_request())

    get_supabase_jwt_verifier.cache_clear()

    assert response.status_code == 200
    assert captured == {"body_user_id": "spoofed-user", "user_id": "token-user"}


def test_verifier_accepts_rs256_tokens_when_jwt_secret_is_present(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", TEST_JWT_SECRET)
    get_supabase_jwt_verifier.cache_clear()
    verifier = get_supabase_jwt_verifier()

    class _SigningKey:
        key = "public-key"

    class _JWKClient:
        def get_signing_key_from_jwt(self, _token: str):
            return _SigningKey()

    monkeypatch.setattr(verifier, "_jwks_client", _JWKClient())
    monkeypatch.setattr(auth_module.jwt, "get_unverified_header", lambda _token: {"alg": "RS256"})

    captured: dict[str, object] = {}

    def _fake_decode(token: str, key: str, algorithms: list[str], **_kwargs):
        captured["token"] = token
        captured["key"] = key
        captured["algorithms"] = list(algorithms)
        return {"sub": "user-rs", "exp": int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())}

    monkeypatch.setattr(auth_module.jwt, "decode", _fake_decode)

    try:
        current_user = verifier.verify_token("rs-token")
    finally:
        get_supabase_jwt_verifier.cache_clear()

    assert current_user.user_id == "user-rs"
    assert captured["token"] == "rs-token"
    assert captured["key"] == "public-key"
    assert captured["algorithms"] == ["RS256"]


def test_verifier_rejects_unsupported_signing_algorithm(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", TEST_JWT_SECRET)
    get_supabase_jwt_verifier.cache_clear()
    verifier = get_supabase_jwt_verifier()
    monkeypatch.setattr(auth_module.jwt, "get_unverified_header", lambda _token: {"alg": "none"})

    try:
        with pytest.raises(HTTPException) as exc_info:
            verifier.verify_token("unsupported-token")
    finally:
        get_supabase_jwt_verifier.cache_clear()

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Unsupported token signing algorithm."
