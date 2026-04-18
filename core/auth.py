from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import InvalidTokenError, PyJWKClient


http_bearer = HTTPBearer(auto_error=False)
SUPPORTED_ASYMMETRIC_ALGORITHMS = {"RS256", "ES256"}


@dataclass(frozen=True)
class CurrentUser:
    user_id: str
    email: str | None
    claims: dict[str, Any]


class SupabaseJWTVerifier:
    def __init__(self) -> None:
        self.supabase_url = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
        self.jwt_secret = os.getenv("SUPABASE_JWT_SECRET", "").strip()
        self.audience = os.getenv("SUPABASE_JWT_AUDIENCE", "authenticated").strip() or None
        self.issuer = f"{self.supabase_url}/auth/v1" if self.supabase_url else None
        self._jwks_client = (
            PyJWKClient(f"{self.supabase_url}/auth/v1/.well-known/jwks.json")
            if self.supabase_url
            else None
        )

    def verify_token(self, token: str) -> CurrentUser:
        if not token:
            raise _unauthorized("Missing bearer token.")

        claims = self._decode_token(token)
        user_id = claims.get("sub") or claims.get("user_id")
        if not user_id:
            raise _unauthorized("Token does not contain a user identifier.")

        return CurrentUser(
            user_id=str(user_id),
            email=claims.get("email"),
            claims=claims,
        )

    def _decode_token(self, token: str) -> dict[str, Any]:
        try:
            options = {
                "require": ["exp", "sub"],
                "verify_aud": bool(self.audience),
                "verify_iss": bool(self.issuer),
            }
            algorithm = (jwt.get_unverified_header(token).get("alg") or "").upper()

            if algorithm == "HS256":
                if not self.jwt_secret:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="SUPABASE_JWT_SECRET is required for HS256 token validation.",
                    )
                return jwt.decode(
                    token,
                    self.jwt_secret,
                    algorithms=["HS256"],
                    audience=self.audience,
                    issuer=self.issuer,
                    options=options,
                )

            if algorithm in SUPPORTED_ASYMMETRIC_ALGORITHMS:
                if not self._jwks_client:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Supabase auth is not configured on the server.",
                    )
                signing_key = self._jwks_client.get_signing_key_from_jwt(token)
                return jwt.decode(
                    token,
                    signing_key.key,
                    algorithms=[algorithm],
                    audience=self.audience,
                    issuer=self.issuer,
                    options=options,
                )

            raise _unauthorized("Unsupported token signing algorithm.")
        except HTTPException:
            raise
        except InvalidTokenError as exc:
            raise _unauthorized("Invalid or expired token.") from exc
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unable to validate bearer token.",
                headers={"WWW-Authenticate": "Bearer"},
            ) from exc


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


@lru_cache
def get_supabase_jwt_verifier() -> SupabaseJWTVerifier:
    return SupabaseJWTVerifier()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(http_bearer),
) -> CurrentUser:
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise _unauthorized("Missing bearer token.")

    return get_supabase_jwt_verifier().verify_token(credentials.credentials)
