"""Helpers for explicit authenticated-user scoping."""

from __future__ import annotations

from typing import Any


def require_user_id(user_id: Any, *, context: str = "operation") -> str:
    """Return a normalized user_id or fail before scoped work begins."""
    normalized = str(user_id or "").strip()
    if not normalized:
        raise ValueError(f"{context} requires a valid user_id.")
    return normalized
