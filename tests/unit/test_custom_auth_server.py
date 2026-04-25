from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import pytest
from fastapi import HTTPException, Response

import core.auth as auth_module
import server
from core.local_store import LocalStoreClient


TEST_JWT_SECRET = "test-jwt-secret-that-is-long-enough"


class _FakeAuthStore:
    def __init__(self) -> None:
        self.users: dict[str, dict[str, object]] = {}
        self.activities: list[dict[str, object]] = []

    def get_user_by_email(self, email: str) -> dict[str, object] | None:
        normalized = str(email or "").strip().lower()
        for user in self.users.values():
            if str(user.get("email") or "").strip().lower() == normalized:
                return dict(user)
        return None

    def get_user_by_id(self, user_id: str) -> dict[str, object] | None:
        user = self.users.get(user_id)
        return dict(user) if user else None

    def create_user(
        self,
        *,
        name: str,
        email: str,
        password_hash: str,
        role: str = "user",
        approval_status: str = "pending",
        session_nonce: str = "",
    ) -> dict[str, object]:
        user_id = f"user-{len(self.users) + 1}"
        user = {
            "user_id": user_id,
            "name": name,
            "email": email,
            "password": password_hash,
            "role": role,
            "approval_status": approval_status,
            "session_nonce": session_nonce,
            "created_at": "2026-04-25T00:00:00+00:00",
        }
        self.users[user_id] = dict(user)
        return dict(user)

    def update_user(self, user_id: str, **fields: object) -> dict[str, object] | None:
        if user_id not in self.users:
            return None
        user = self.users[user_id]
        for key, value in fields.items():
            if key == "password_hash":
                user["password"] = value
            else:
                user[key] = value
        return dict(user)

    def list_users_by_approval_status(self, approval_status: str) -> list[dict[str, object]]:
        wanted = str(approval_status or "").strip().lower()
        return [
            dict(user)
            for user in self.users.values()
            if str(user.get("approval_status") or "").strip().lower() == wanted
        ]

    def list_users(self, *, limit: int = 500, approval_status: str | None = None) -> list[dict[str, object]]:
        rows = [dict(user) for user in self.users.values()]
        if approval_status:
            wanted = str(approval_status or "").strip().lower()
            rows = [row for row in rows if str(row.get("approval_status") or "").strip().lower() == wanted]
        rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
        return rows[: max(1, int(limit or 500))]

    def log_pipeline_activity(self, **payload: object) -> None:
        self.activities.append(dict(payload))

    def list_pipeline_activity(self, *, limit: int = 200, **_kwargs: object) -> list[dict[str, object]]:
        rows = [dict(item) for item in self.activities]
        rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
        return rows[: max(1, int(limit or 200))]


def _cookie_token(response: Response) -> str:
    header = response.headers.get("set-cookie", "")
    prefix = f"{auth_module.auth_cookie_name()}="
    if prefix not in header:
        return ""
    return header.split(prefix, 1)[1].split(";", 1)[0]


def test_signup_requires_admin_approval_before_login(monkeypatch):
    monkeypatch.setenv("APP_JWT_SECRET", TEST_JWT_SECRET)
    monkeypatch.setenv("PASSWORD_HASH_ITERATIONS", "100000")
    store = _FakeAuthStore()
    admin = store.create_user(
        name="Admin",
        email="admin@example.com",
        password_hash=auth_module.hash_password("admin-pass-123"),
        role="admin",
        approval_status="approved",
        session_nonce="admin-session",
    )
    monkeypatch.setattr(server, "get_supabase_client", lambda: store)
    monkeypatch.setattr(auth_module, "_get_auth_store", lambda: store)
    auth_module.get_auth_jwt_verifier.cache_clear()

    try:
        signup_payload = server.signup(
            server.SignupRequest(
                name="Kapil",
                email="kapil@example.com",
                password="password-123",
            )
        )
        assert signup_payload["message"] == "Request has been sent to admin for approval."
        assert signup_payload["user"]["approval_status"] == "pending"

        with pytest.raises(HTTPException) as login_exc:
            server.login(
                server.LoginRequest(email="kapil@example.com", password="password-123"),
                Response(),
            )
        assert login_exc.value.status_code == 403
        assert login_exc.value.detail == auth_module.PENDING_APPROVAL_MESSAGE

        admin_user = auth_module.get_auth_jwt_verifier().verify_token(
            auth_module.create_access_token(admin, session_nonce="admin-session", expires_delta=timedelta(hours=1))
        )
        approved = server.approve_user("user-2", admin_user=admin_user)
        assert approved["user"]["approval_status"] == "approved"

        response = Response()
        login_payload = server.login(
            server.LoginRequest(email="kapil@example.com", password="password-123"),
            response,
        )
        token = _cookie_token(response)
        current_user = auth_module.get_auth_jwt_verifier().verify_token(token)
        me_payload = asyncio.run(server.get_authenticated_user(current_user=current_user))

        assert login_payload["user"]["email"] == "kapil@example.com"
        assert me_payload["user"]["name"] == "Kapil"
    finally:
        auth_module.get_auth_jwt_verifier.cache_clear()


def test_outputs_and_files_are_visible_to_authenticated_users(tmp_path, monkeypatch):
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = LocalStoreClient(root=str(tmp_path / "local_store"))
    activity_store = _FakeAuthStore()

    user_a_file = output_dir / "acme_golden_record_20260425_120000.json"
    user_b_file = output_dir / "globex_golden_record_20260425_120500.json"
    user_a_file.write_text(json.dumps({"company_name": "Acme"}), encoding="utf-8")
    user_b_file.write_text(json.dumps({"company_name": "Globex"}), encoding="utf-8")

    for run_id, company, company_id, path_value, user_id in [
        ("run-a", "Acme", "acme", str(user_a_file), "user-a"),
        ("run-b", "Globex", "globex", str(user_b_file), "user-b"),
    ]:
        store.create_pipeline_run(
            run_id=run_id,
            company_name=company,
            company_id=company_id,
            user_id=user_id,
        )
        store.complete_pipeline_run(
            run_id=run_id,
            golden_record_count=1,
            all_tests_passed=True,
            failed_param_ids=[],
            agent2_retry_count=0,
            pytest_retry_count=0,
            golden_record_path=path_value,
            validation_path="",
            pytest_report_path=None,
            chunk_record_path=None,
            user_id=user_id,
        )

    monkeypatch.setattr(server, "get_local_store_client", lambda: store)
    monkeypatch.setattr(server, "get_supabase_client", lambda: activity_store)
    monkeypatch.setattr(server, "BASE_DIR", tmp_path)
    monkeypatch.setattr(server, "OUTPUT_DIR", output_dir)

    outputs = server.list_outputs(limit=50, user_id="user-a")
    assert len(outputs) == 2
    assert {item["company"] for item in outputs} == {"Acme", "Globex"}

    allowed = server.read_output_json(path=str(user_a_file), user_id="user-a")
    assert json.loads(allowed.body.decode("utf-8"))["company_name"] == "Acme"
    allowed_foreign = server.read_output_json(path=str(user_b_file), user_id="user-a")
    assert json.loads(allowed_foreign.body.decode("utf-8"))["company_name"] == "Globex"


def test_admin_can_verify_and_update_role(monkeypatch):
    monkeypatch.setenv("APP_JWT_SECRET", TEST_JWT_SECRET)
    monkeypatch.setenv("PASSWORD_HASH_ITERATIONS", "100000")
    store = _FakeAuthStore()
    admin = store.create_user(
        name="Admin",
        email="admin@example.com",
        password_hash=auth_module.hash_password("admin-pass-123"),
        role="admin",
        approval_status="approved",
        session_nonce="admin-session",
    )
    store.create_user(
        name="Analyst",
        email="analyst@example.com",
        password_hash=auth_module.hash_password("analyst-pass-123"),
        role="user",
        approval_status="pending",
        session_nonce="user-session",
    )
    monkeypatch.setattr(server, "get_supabase_client", lambda: store)
    monkeypatch.setattr(auth_module, "_get_auth_store", lambda: store)
    auth_module.get_auth_jwt_verifier.cache_clear()

    try:
        admin_user = auth_module.get_auth_jwt_verifier().verify_token(
            auth_module.create_access_token(admin, session_nonce="admin-session", expires_delta=timedelta(hours=1))
        )
        verified = server.verify_user(
            "user-2",
            server.VerifyUserRequest(verified=True, note="Validated identity"),
            admin_user=admin_user,
        )
        assert verified["user"]["verification_status"] == "verified"

        role_updated = server.update_user_role(
            "user-2",
            server.UpdateUserRoleRequest(role="admin"),
            admin_user=admin_user,
        )
        assert role_updated["user"]["role"] == "admin"
    finally:
        auth_module.get_auth_jwt_verifier.cache_clear()
