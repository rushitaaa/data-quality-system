"""
cleaner.py — Fixes data quality issues found by checker.py.
"""
from __future__ import annotations
import re
from dataclasses import dataclass
from typing import Any
import pandas as pd
from dateutil import parser as dateparser

from checker import (Issue, ISSUE_MISSING, ISSUE_WRONG_TYPE, ISSUE_DUPLICATE,
                     ISSUE_BAD_FORMAT, ISSUE_WHITESPACE, ISSUE_CASING,
                     ISSUE_OUTLIER, ISSUE_BAD_PHONE, ISSUE_BAD_POSTCODE, ISSUE_EMPTY_COL)


@dataclass
class ChangeRecord:
    row: int
    column: str
    issue_type: str
    original_value: Any
    new_value: Any
    fixed: bool
    note: str


def clean(
    df: pd.DataFrame,
    issues: list[Issue],
    col_types: dict[str, str],
    missing_strategy: str = "flag",       # flag | mean | median | mode | custom
    missing_custom_value: str = "",
    duplicate_mode: str = "exact",
) -> tuple[pd.DataFrame, list[ChangeRecord]]:
    df = df.copy()
    records: list[ChangeRecord] = []

    # 1. Remove duplicates
    dup_indices = [i.row for i in issues if i.issue_type == ISSUE_DUPLICATE]
    if dup_indices:
        for idx in dup_indices:
            records.append(ChangeRecord(
                row=idx, column="<all>", issue_type=ISSUE_DUPLICATE,
                original_value=None, new_value=None,
                fixed=True, note="Duplicate row removed."
            ))
        df = df.drop(index=[i for i in dup_indices if i in df.index]).reset_index(drop=True)

    # 2. Trim whitespace
    for col in df.columns:
        for idx in df.index:
            val = str(df.at[idx, col])
            stripped = " ".join(val.split())
            if stripped != val:
                records.append(ChangeRecord(
                    row=idx, column=col, issue_type=ISSUE_WHITESPACE,
                    original_value=val, new_value=stripped,
                    fixed=True, note="Trimmed extra whitespace."
                ))
                df.at[idx, col] = stripped

    # 3. Casing — title-case string columns
    for col, dtype in col_types.items():
        if dtype != "string" or col not in df.columns:
            continue
        for idx in df.index:
            val = str(df.at[idx, col]).strip()
            if val.lower() == "nan" or val == "":
                continue
            titled = val.title()
            if titled != val:
                records.append(ChangeRecord(
                    row=idx, column=col, issue_type=ISSUE_CASING,
                    original_value=val, new_value=titled,
                    fixed=True, note="Normalized to title case."
                ))
                df.at[idx, col] = titled

    # 4. Date normalization
    for col, dtype in col_types.items():
        if dtype != "date" or col not in df.columns:
            continue
        for idx in df.index:
            val = str(df.at[idx, col]).strip()
            if val.lower() == "nan" or val == "":
                continue
            normalized = _normalize_date(val)
            if normalized and normalized != val:
                records.append(ChangeRecord(
                    row=idx, column=col, issue_type=ISSUE_BAD_FORMAT,
                    original_value=val, new_value=normalized,
                    fixed=True, note="Date standardized to dd/mm/yyyy."
                ))
                df.at[idx, col] = normalized
            elif normalized is None:
                records.append(ChangeRecord(
                    row=idx, column=col, issue_type=ISSUE_BAD_FORMAT,
                    original_value=val, new_value=val,
                    fixed=False, note="Could not parse date -- left unchanged."
                ))

    # 5. Phone normalization
    for col, dtype in col_types.items():
        if dtype != "phone" or col not in df.columns:
            continue
        for idx in df.index:
            val = str(df.at[idx, col]).strip()
            if val.lower() == "nan" or val == "":
                continue
            normalized = _normalize_phone(val)
            if normalized and normalized != val:
                records.append(ChangeRecord(
                    row=idx, column=col, issue_type=ISSUE_BAD_PHONE,
                    original_value=val, new_value=normalized,
                    fixed=True, note="Phone number standardized."
                ))
                df.at[idx, col] = normalized

    # 6. Missing values
    for col in df.columns:
        dtype = col_types.get(col, "string")
        missing_indices = [
            i.row for i in issues
            if i.issue_type == ISSUE_MISSING and i.column == col and i.row in df.index
        ]
        if not missing_indices:
            continue

        if missing_strategy == "flag":
            for idx in missing_indices:
                records.append(ChangeRecord(
                    row=idx, column=col, issue_type=ISSUE_MISSING,
                    original_value=df.at[idx, col], new_value=None,
                    fixed=False, note="Missing value -- requires manual review."
                ))
        else:
            fill_val = _compute_fill(df, col, dtype, missing_strategy, missing_custom_value)
            for idx in missing_indices:
                orig = df.at[idx, col]
                if fill_val is not None:
                    df.at[idx, col] = str(fill_val)
                    records.append(ChangeRecord(
                        row=idx, column=col, issue_type=ISSUE_MISSING,
                        original_value=orig, new_value=fill_val,
                        fixed=True, note=f"Missing value filled with {missing_strategy} ({fill_val})."
                    ))
                else:
                    records.append(ChangeRecord(
                        row=idx, column=col, issue_type=ISSUE_MISSING,
                        original_value=orig, new_value=None,
                        fixed=False, note="Could not compute fill value -- left unchanged."
                    ))

    # 7. Wrong type — flag only
    for issue in issues:
        if issue.issue_type == ISSUE_WRONG_TYPE:
            records.append(ChangeRecord(
                row=issue.row, column=issue.column, issue_type=ISSUE_WRONG_TYPE,
                original_value=issue.original_value, new_value=None,
                fixed=False, note="Wrong type -- requires manual review."
            ))

    # 8. Outliers — flag only
    for issue in issues:
        if issue.issue_type == ISSUE_OUTLIER:
            records.append(ChangeRecord(
                row=issue.row, column=issue.column, issue_type=ISSUE_OUTLIER,
                original_value=issue.original_value, new_value=None,
                fixed=False, note=issue.description
            ))

    # 9. Bad postcode — flag only
    for issue in issues:
        if issue.issue_type == ISSUE_BAD_POSTCODE:
            records.append(ChangeRecord(
                row=issue.row, column=issue.column, issue_type=ISSUE_BAD_POSTCODE,
                original_value=issue.original_value, new_value=None,
                fixed=False, note="Unrecognised postcode format -- requires manual review."
            ))

    # 10. Empty column — flag only
    for issue in issues:
        if issue.issue_type == ISSUE_EMPTY_COL:
            records.append(ChangeRecord(
                row=-1, column=issue.column, issue_type=ISSUE_EMPTY_COL,
                original_value=None, new_value=None,
                fixed=False, note=f"Column '{issue.column}' is entirely empty."
            ))

    return df, records


def _compute_fill(df, col, dtype, strategy, custom_value):
    if strategy == "custom":
        return custom_value if custom_value else None
    nums = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce").dropna()
    if len(nums) == 0:
        if strategy == "mode":
            mode = df[col].mode()
            return str(mode.iloc[0]) if len(mode) > 0 else None
        return None
    if strategy == "mean":
        return round(nums.mean(), 4)
    if strategy == "median":
        return round(nums.median(), 4)
    if strategy == "mode":
        mode = df[col].mode()
        return str(mode.iloc[0]) if len(mode) > 0 else None
    return None


def _normalize_date(s: str):
    try:
        return dateparser.parse(s, fuzzy=False).strftime("%d/%m/%Y")
    except Exception:
        return None


def _normalize_phone(s: str) -> str | None:
    digits = re.sub(r"\D", "", s)
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    if len(digits) == 11 and digits[0] in ("0", "1"):
        return f"+{digits[0]} {digits[1:4]}-{digits[4:7]}-{digits[7:]}"
    if len(digits) == 12:
        return f"+{digits[:2]} {digits[2:7]}-{digits[7:]}"
    return None
