"""
checker.py — Finds data quality issues in a DataFrame.

Returns a list of Issue objects describing every problem found.
"""
from __future__ import annotations
import re
from dataclasses import dataclass, field
from typing import Any
import pandas as pd
from dateutil import parser as dateparser

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_PHONE_RE = re.compile(r"^\+?[\d\s\-().]{7,20}$")


@dataclass
class Issue:
    row: int            # 0-based row index (-1 = whole-column issue)
    column: str
    issue_type: str     # see ISSUE_* constants below
    original_value: Any
    description: str

# Issue type constants
ISSUE_MISSING       = "missing_value"
ISSUE_WRONG_TYPE    = "wrong_type"
ISSUE_DUPLICATE     = "duplicate_row"
ISSUE_BAD_FORMAT    = "bad_format"
ISSUE_INVALID       = "invalid_value"
ISSUE_WHITESPACE    = "extra_whitespace"
ISSUE_CASING        = "inconsistent_casing"


def check(df: pd.DataFrame, col_types: dict[str, str]) -> list[Issue]:
    """Run all checks and return all found issues."""
    issues: list[Issue] = []
    issues.extend(_check_missing(df))
    issues.extend(_check_whitespace(df))
    issues.extend(_check_types(df, col_types))
    issues.extend(_check_duplicates(df))
    issues.extend(_check_casing(df, col_types))
    return issues


# --- individual checks ---

def _check_missing(df: pd.DataFrame) -> list[Issue]:
    issues = []
    for col in df.columns:
        for idx, val in df[col].items():
            if pd.isna(val) or str(val).strip() == "" or str(val).strip().lower() == "nan":
                issues.append(Issue(
                    row=idx, column=col,
                    issue_type=ISSUE_MISSING,
                    original_value=val,
                    description=f"Missing or empty value in column '{col}' at row {idx}."
                ))
    return issues


def _check_whitespace(df: pd.DataFrame) -> list[Issue]:
    issues = []
    for col in df.columns:
        for idx, val in df[col].items():
            s = str(val)
            if s != s.strip() or "  " in s:
                issues.append(Issue(
                    row=idx, column=col,
                    issue_type=ISSUE_WHITESPACE,
                    original_value=val,
                    description=f"Extra whitespace in column '{col}' at row {idx}."
                ))
    return issues


def _check_types(df: pd.DataFrame, col_types: dict[str, str]) -> list[Issue]:
    issues = []
    for col, dtype in col_types.items():
        if col not in df.columns:
            continue
        for idx, val in df[col].items():
            s = str(val).strip()
            if s == "" or s.lower() == "nan":
                continue
            if not _value_matches_type(s, dtype):
                issues.append(Issue(
                    row=idx, column=col,
                    issue_type=ISSUE_WRONG_TYPE,
                    original_value=val,
                    description=(
                        f"Value '{val}' in column '{col}' (row {idx}) "
                        f"does not match expected type '{dtype}'."
                    )
                ))
    return issues


def _check_duplicates(df: pd.DataFrame) -> list[Issue]:
    issues = []
    dup_mask = df.duplicated(keep="first")
    for idx in df[dup_mask].index:
        issues.append(Issue(
            row=idx, column="<all>",
            issue_type=ISSUE_DUPLICATE,
            original_value=None,
            description=f"Row {idx} is an exact duplicate of an earlier row."
        ))
    return issues


def _check_casing(df: pd.DataFrame, col_types: dict[str, str]) -> list[Issue]:
    """Flag string columns where values have mixed casing across the column."""
    issues = []
    for col, dtype in col_types.items():
        if dtype != "string" or col not in df.columns:
            continue
        values = df[col].dropna().astype(str)
        values = values[values.str.strip() != ""]
        lower_set = set(values.str.strip().str.lower())
        actual_set = set(values.str.strip())
        # If multiple casing variants of the same word exist → inconsistent
        case_groups: dict[str, list[str]] = {}
        for v in actual_set:
            case_groups.setdefault(v.lower(), []).append(v)
        for key, variants in case_groups.items():
            if len(variants) > 1:
                for idx, val in df[col].items():
                    if str(val).strip().lower() == key and str(val).strip() != variants[0]:
                        issues.append(Issue(
                            row=idx, column=col,
                            issue_type=ISSUE_CASING,
                            original_value=val,
                            description=(
                                f"Inconsistent casing in column '{col}' at row {idx}: "
                                f"'{val}' vs '{variants[0]}'."
                            )
                        ))
    return issues


# --- helper ---

def _value_matches_type(s: str, dtype: str) -> bool:
    if dtype == "integer":
        try:
            int(s.replace(",", ""))
            return True
        except ValueError:
            return False
    if dtype == "float":
        try:
            float(s.replace(",", ""))
            return True
        except ValueError:
            return False
    if dtype == "boolean":
        return s.lower() in {"true", "false", "yes", "no", "1", "0", "t", "f", "y", "n"}
    if dtype == "email":
        return bool(_EMAIL_RE.match(s))
    if dtype == "phone":
        return bool(_PHONE_RE.match(s))
    if dtype == "date":
        try:
            dateparser.parse(s, fuzzy=False)
            return True
        except Exception:
            return False
    return True  # string — anything is valid
