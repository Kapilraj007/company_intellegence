from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Callable

import jwt
from fastapi import Depends, HTTPException, Request, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import InvalidTokenError


http_bearer = HTTPBearer(auto_error=False)
PENDING_APPROVAL_MESSAGE = "Your account is pending admin approval."
REJECTED_ACCOUNT_MESSAGE = "Your account request was rejected. Contact an administrator."
ROLE_PERMISSIONS: dict[str, set[str]] = {
    "user": {
        "features:use",
        "pipeline:run",
        "data:read",
        "data:write",
    },
    "admin": {
        "features:use",
        "pipeline:run",
        "data:read",
        "data:write",
        "admin:access",
        "admin:users:read",
        "admin:users:write",
        "admin:roles:write",
        "admin:audit:read",
        "admin:errors:read",
        "admin:dashboard:read",
        "admin:versions:read",
    },
}


@dataclass(frozen=True)
class CurrentUser:
    user_id: str
    name: str
    email: str
    role: str
    approval_status: str
    claims: dict[str, Any]


def normalize_email(value: str) -> str:
    return str(value or "").strip().lower()


def normalize_role(value: str) -> str:
    role = str(value or "user").strip().lower() or "user"
    return role if role in ROLE_PERMISSIONS else "user"


def get_role_permissions(role: str) -> list[str]:
    return sorted(ROLE_PERMISSIONS.get(normalize_role(role), set()))


def has_permission(role: str, permission: str) -> bool:
    return str(permission or "").strip() in ROLE_PERMISSIONS.get(normalize_role(role), set())


def validate_password_strength(password: str) -> None:
    value = str(password or "")
    if len(value) < 8:
        raise ValueError("Password must be at least 8 characters long.")


def _password_iterations() -> int:
    raw = str(os.getenv("PASSWORD_HASH_ITERATIONS", "390000")).strip()
    try:
        return max(100000, int(raw))
    except (TypeError, ValueError):
        return 390000


def hash_password(password: str) -> str:
    validate_password_strength(password)
    salt = base64.urlsafe_b64encode(secrets.token_bytes(16)).decode("utf-8").rstrip("=")
    iterations = _password_iterations()
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        iterations,
    )
    encoded = base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")
    return f"pbkdf2_sha256${iterations}${salt}${encoded}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algorithm, raw_iterations, salt, expected = str(stored_hash or "").split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        iterations = int(raw_iterations)
    except (TypeError, ValueError):
        return False

    digest = hashlib.pbkdf2_hmac(
        "sha256",
        str(password or "").encode("utf-8"),
        salt.encode("utf-8"),
        iterations,
    )
    actual = base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")
    return hmac.compare_digest(actual, expected)


def generate_session_nonce() -> str:
    return secrets.token_urlsafe(32)


def _jwt_secret() -> str:
    secret = (
        os.getenv("APP_JWT_SECRET", "").strip()
        or os.getenv("SUPABASE_JWT_SECRET", "").strip()
    )
    if not secret:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="APP_JWT_SECRET is required for custom authentication.",
        )
    return secret


def _jwt_audience() -> str | None:
    return str(os.getenv("APP_JWT_AUDIENCE", "company-intelligence")).strip() or None


def _jwt_issuer() -> str | None:
    return str(os.getenv("APP_JWT_ISSUER", "company-intelligence-api")).strip() or None


def _access_token_ttl_minutes() -> int:
    raw = str(os.getenv("ACCESS_TOKEN_TTL_MINUTES", "720")).strip()
    try:
        return max(15, int(raw))
    except (TypeError, ValueError):
        return 720


def auth_cookie_name() -> str:
    return str(os.getenv("AUTH_COOKIE_NAME", "ci_access_token")).strip() or "ci_access_token"


def _cookie_secure() -> bool:
    return str(os.getenv("AUTH_COOKIE_SECURE", "false")).strip().lower() in {"1", "true", "yes", "on"}


def _cookie_samesite() -> str:
    value = str(os.getenv("AUTH_COOKIE_SAMESITE", "lax")).strip().lower()
    return value if value in {"lax", "strict", "none"} else "lax"


def _cookie_max_age() -> int:
    return _access_token_ttl_minutes() * 60


def _get_auth_store():
    from core.supabase_store import get_supabase_client

    return get_supabase_client()


def serialize_user(record: dict[str, Any]) -> dict[str, Any]:
    role = normalize_role(record.get("role"))
    return {
        "user_id": str(record.get("user_id") or "").strip(),
        "name": str(record.get("name") or "").strip(),
        "email": normalize_email(record.get("email")),
        "role": role,
        "approval_status": str(record.get("approval_status") or "pending").strip().lower() or "pending",
        "verification_status": str(record.get("verification_status") or "unverified").strip().lower() or "unverified",
        "verified_at": record.get("verified_at"),
        "verified_by": str(record.get("verified_by") or "").strip() or None,
        "last_login_at": record.get("last_login_at"),
        "permissions": get_role_permissions(role),
        "created_at": record.get("created_at"),
    }


class CustomJWTVerifier:
    def __init__(self) -> None:
        self.jwt_secret = _jwt_secret()
        self.audience = _jwt_audience()
        self.issuer = _jwt_issuer()

    def verify_token(self, token: str) -> CurrentUser:
        if not token:
            raise _unauthorized("Missing authentication token.")

        claims = self._decode_token(token)
        user_id = str(claims.get("sub") or claims.get("user_id") or "").strip()
        if not user_id:
            raise _unauthorized("Token does not contain a user identifier.")

        record = _get_auth_store().get_user_by_id(user_id)
        if not isinstance(record, dict):
            raise _unauthorized("User account no longer exists.")

        session_nonce = str(record.get("session_nonce") or "").strip()
        token_nonce = str(claims.get("session_nonce") or "").strip()
        if not session_nonce or not token_nonce or not hmac.compare_digest(session_nonce, token_nonce):
            raise _unauthorized("Session is no longer valid.")

        approval_status = str(record.get("approval_status") or "").strip().lower()
        if approval_status == "pending":
            raise _forbidden(PENDING_APPROVAL_MESSAGE)
        if approval_status == "rejected":
            raise _forbidden(REJECTED_ACCOUNT_MESSAGE)

        user = serialize_user(record)
        return CurrentUser(
            user_id=user["user_id"],
            name=user["name"] or user["email"].split("@")[0] or "User",
            email=user["email"],
            role=user["role"],
            approval_status=user["approval_status"],
            claims=claims,
        )

    def _decode_token(self, token: str) -> dict[str, Any]:
        try:
            return jwt.decode(
                token,
                self.jwt_secret,
                algorithms=["HS256"],
                audience=self.audience,
                issuer=self.issuer,
                options={
                    "require": ["exp", "sub"],
                    "verify_aud": bool(self.audience),
                    "verify_iss": bool(self.issuer),
                },
            )
        except InvalidTokenError as exc:
            raise _unauthorized("Invalid or expired authentication token.") from exc
        except HTTPException:
            raise
        except Exception as exc:
            raise _unauthorized("Unable to validate authentication token.") from exc


def create_access_token(
    user: dict[str, Any],
    *,
    session_nonce: str,
    expires_delta: timedelta | None = None,
) -> str:
    serialized = serialize_user(user)
    now = datetime.now(timezone.utc)
    expires_at = now + (expires_delta or timedelta(minutes=_access_token_ttl_minutes()))
    payload = {
        "sub": serialized["user_id"],
        "email": serialized["email"],
        "name": serialized["name"],
        "role": serialized["role"],
        "approval_status": serialized["approval_status"],
        "session_nonce": session_nonce,
        "aud": _jwt_audience(),
        "iss": _jwt_issuer(),
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    return jwt.encode(payload, _jwt_secret(), algorithm="HS256")


def set_auth_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=auth_cookie_name(),
        value=token,
        httponly=True,
        secure=_cookie_secure(),
        samesite=_cookie_samesite(),
        max_age=_cookie_max_age(),
        expires=_cookie_max_age(),
        path="/",
    )


def clear_auth_cookie(response: Response) -> None:
    response.delete_cookie(
        key=auth_cookie_name(),
        httponly=True,
        secure=_cookie_secure(),
        samesite=_cookie_samesite(),
        path="/",
    )


def extract_access_token(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = None,
) -> str:
    if credentials is not None and credentials.scheme.lower() == "bearer":
        return str(credentials.credentials or "").strip()
    return str(request.cookies.get(auth_cookie_name()) or "").strip()


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


def _forbidden(detail: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=detail)


@lru_cache
def get_auth_jwt_verifier() -> CustomJWTVerifier:
    return CustomJWTVerifier()


def get_supabase_jwt_verifier() -> CustomJWTVerifier:
    return get_auth_jwt_verifier()


async def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(http_bearer),
) -> CurrentUser:
    token = extract_access_token(request, credentials)
    if not token:
        raise _unauthorized("Missing authentication token.")
    return get_auth_jwt_verifier().verify_token(token)


async def get_optional_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(http_bearer),
) -> CurrentUser | None:
    token = extract_access_token(request, credentials)
    if not token:
        return None
    try:
        return get_auth_jwt_verifier().verify_token(token)
    except HTTPException:
        return None


async def require_admin(current_user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
    if not has_permission(current_user.role, "admin:access"):
        raise _forbidden("Admin access is required.")
    return current_user


def require_permission(permission: str) -> Callable[..., CurrentUser]:
    required = str(permission or "").strip()

    async def _dependency(current_user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
        if not has_permission(current_user.role, required):
            raise _forbidden(f"Permission '{required}' is required.")
        return current_user

    return _dependency


def bootstrap_admin_from_env() -> None:
    email = normalize_email(os.getenv("BOOTSTRAP_ADMIN_EMAIL", ""))
    password = str(os.getenv("BOOTSTRAP_ADMIN_PASSWORD", "")).strip()
    name = str(os.getenv("BOOTSTRAP_ADMIN_NAME", "Admin")).strip() or "Admin"
    if not email or not password:
        return

    store = _get_auth_store()
    existing = store.get_user_by_email(email)
    if existing:
        store.update_user(
            str(existing.get("user_id") or ""),
            name=name,
            role="admin",
            approval_status="approved",
        )
        return

    store.create_user(
        name=name,
        email=email,
        password_hash=hash_password(password),
        role="admin",
        approval_status="approved",
        session_nonce=generate_session_nonce(),
    )
