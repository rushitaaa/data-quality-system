"""
checker.py — Finds data quality issues in a DataFrame.
"""
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Any
import pandas as pd
from dateutil import parser as dateparser

_EMAIL_RE    = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_PHONE_RE    = re.compile(r"^\+?[\d\s\-().]{7,20}$")
_POSTCODE_RE = re.compile(r"^(\d{5}(-\d{4})?|[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}|\d{6})$", re.I)

# Issue type constants
ISSUE_MISSING       = "missing_value"
ISSUE_WRONG_TYPE    = "wrong_type"
ISSUE_DUPLICATE     = "duplicate_row"
ISSUE_BAD_FORMAT    = "bad_format"
ISSUE_INVALID       = "invalid_value"
ISSUE_WHITESPACE    = "extra_whitespace"
ISSUE_CASING        = "inconsistent_casing"
ISSUE_OUTLIER       = "outlier"
ISSUE_EMPTY_COL     = "empty_column"
ISSUE_BAD_PHONE     = "bad_phone_format"
ISSUE_BAD_POSTCODE  = "bad_postcode"


@dataclass
class Issue:
    row: int
    column: str
    issue_type: str
    original_value: Any
    description: str


def check(df: pd.DataFrame, col_types: dict[str, str], duplicate_mode: str = "exact") -> list[Issue]:
    issues: list[Issue] = []
    issues.extend(_check_empty_columns(df))
    issues.extend(_check_missing(df))
    issues.extend(_check_whitespace(df))
    issues.extend(_check_types(df, col_types))
    issues.extend(_check_duplicates(df, duplicate_mode))
    issues.extend(_check_casing(df, col_types))
    issues.extend(_check_outliers(df, col_types))
    issues.extend(_check_phone_format(df, col_types))
    issues.extend(_check_postcode(df, col_types))
    return issues


def _check_empty_columns(df: pd.DataFrame) -> list[Issue]:
    issues = []
    for col in df.columns:
        vals = df[col].astype(str).str.strip()
        non_empty = vals[(vals != "") & (vals.str.lower() != "nan")]
        if len(non_empty) == 0:
            issues.append(Issue(
                row=-1, column=col,
                issue_type=ISSUE_EMPTY_COL,
                original_value=None,
                description=f"Column '{col}' is entirely empty."
            ))
    return issues


def _check_missing(df: pd.DataFrame) -> list[Issue]:
    issues = []
    for col in df.columns:
        for idx, val in df[col].items():
            if pd.isna(val) or str(val).strip() == "" or str(val).strip().lower() == "nan":
                issues.append(Issue(
                    row=idx, column=col,
                    issue_type=ISSUE_MISSING,
                    original_value=val,
                    description=f"Missing value in '{col}' at row {idx}."
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
                    description=f"Extra whitespace in '{col}' at row {idx}."
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
                    description=f"'{val}' in '{col}' (row {idx}) does not match type '{dtype}'."
                ))
    return issues


def _check_duplicates(df: pd.DataFrame, mode: str = "exact") -> list[Issue]:
    issues = []
    if mode == "fuzzy":
        try:
            from rapidfuzz import fuzz
            str_rows = df.astype(str).apply(lambda r: " ".join(r.values), axis=1).tolist()
            seen = []
            for idx, row_str in enumerate(str_rows):
                for prev_idx, prev_str in seen:
                    score = fuzz.ratio(row_str, prev_str)
                    if score >= 90:
                        issues.append(Issue(
                            row=idx, column="<all>",
                            issue_type=ISSUE_DUPLICATE,
                            original_value=None,
                            description=f"Row {idx} is ~{score}% similar to row {prev_idx} (fuzzy duplicate)."
                        ))
                        break
                else:
                    seen.append((idx, row_str))
        except ImportError:
            pass
    else:
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
    issues = []
    for col, dtype in col_types.items():
        if dtype != "string" or col not in df.columns:
            continue
        actual_set = set(df[col].dropna().astype(str).str.strip())
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
                            description=f"Inconsistent casing in '{col}' at row {idx}: '{val}' vs '{variants[0]}'."
                        ))
    return issues


def _check_outliers(df: pd.DataFrame, col_types: dict[str, str]) -> list[Issue]:
    issues = []
    for col, dtype in col_types.items():
        if dtype not in ("integer", "float") or col not in df.columns:
            continue
        nums = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce").dropna()
        if len(nums) < 4:
            continue
        Q1, Q3 = nums.quantile(0.25), nums.quantile(0.75)
        IQR = Q3 - Q1
        if IQR == 0:
            continue
        lower, upper = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR
        for idx, val in df[col].items():
            try:
                n = float(str(val).replace(",", ""))
                if n < lower or n > upper:
                    issues.append(Issue(
                        row=idx, column=col,
                        issue_type=ISSUE_OUTLIER,
                        original_value=val,
                        description=f"Outlier in '{col}' at row {idx}: {val} (expected {lower:.2f}–{upper:.2f})."
                    ))
            except (ValueError, TypeError):
                pass
    return issues


def _check_phone_format(df: pd.DataFrame, col_types: dict[str, str]) -> list[Issue]:
    issues = []
    for col, dtype in col_types.items():
        if dtype != "phone" or col not in df.columns:
            continue
        for idx, val in df[col].items():
            s = str(val).strip()
            if s == "" or s.lower() == "nan":
                continue
            digits = re.sub(r"\D", "", s)
            if not (7 <= len(digits) <= 15):
                issues.append(Issue(
                    row=idx, column=col,
                    issue_type=ISSUE_BAD_PHONE,
                    original_value=val,
                    description=f"Phone '{val}' in '{col}' at row {idx} has unexpected digit count ({len(digits)})."
                ))
    return issues


def _check_postcode(df: pd.DataFrame, col_types: dict[str, str]) -> list[Issue]:
    issues = []
    postcode_cols = [c for c in df.columns if any(k in c.lower() for k in ("zip", "postcode", "postal", "pincode", "pin"))]
    for col in postcode_cols:
        for idx, val in df[col].items():
            s = str(val).strip()
            if s == "" or s.lower() == "nan":
                continue
            if not _POSTCODE_RE.match(s):
                issues.append(Issue(
                    row=idx, column=col,
                    issue_type=ISSUE_BAD_POSTCODE,
                    original_value=val,
                    description=f"Postcode '{val}' in '{col}' at row {idx} has an unrecognised format."
                ))
    return issues


def _value_matches_type(s: str, dtype: str) -> bool:
    if dtype == "integer":
        try: int(s.replace(",", "")); return True
        except ValueError: return False
    if dtype == "float":
        try: float(s.replace(",", "")); return True
        except ValueError: return False
    if dtype == "boolean":
        return s.lower() in {"true","false","yes","no","1","0","t","f","y","n"}
    if dtype == "email":
        return bool(_EMAIL_RE.match(s))
    if dtype == "phone":
        return bool(_PHONE_RE.match(s))
    if dtype == "date":
        try: dateparser.parse(s, fuzzy=False); return True
        except Exception: return False
    return True
