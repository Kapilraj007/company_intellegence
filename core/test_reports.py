"""Pydantic models for persisted pytest execution reports."""

from typing import List, Optional

from pydantic import BaseModel, Field


class PytestCaseIssue(BaseModel):
    name: str
    message: str
    phase: Optional[str] = None


class PytestReport(BaseModel):
    company_name: str
    generated_at: str
    golden_record_path: Optional[str] = None
    total: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    errors: int = 0
    duration_sec: float = 0.0
    all_passed: bool = False
    failed_tests: List[PytestCaseIssue] = Field(default_factory=list)
    error_tests: List[PytestCaseIssue] = Field(default_factory=list)
    failed_parameter_ids: List[int] = Field(default_factory=list)
    exit_code: int = -1
    skip_reason: Optional[str] = None
