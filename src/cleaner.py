"""
cleaner.py — Fixes data quality issues found by checker.py.

Returns a cleaned DataFrame and a list of ChangeRecord objects
describing every fix applied (or flagged as unfixable).
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any
import re
import pandas as pd
from dateutil import parser as dateparser

from checker import Issue, ISSUE_MISSING, ISSUE_WRONG_TYPE, ISSUE_DUPLICATE
from checker import ISSUE_BAD_FORMAT, ISSUE_WHITESPACE, ISSUE_CASING, ISSUE_INVALID


@dataclass
class ChangeRecord:
    row: int
    column: str
    issue_type: str
    original_value: Any
    new_value: Any
    fixed: bool          # True = auto-fixed, False = flagged only
    note: str


def clean(df: pd.DataFrame, issues: list[Issue], col_types: dict[str, str]) -> tuple[pd.DataFrame, list[ChangeRecord]]:
    """
    Apply fixes to df based on issues list.
    Returns (cleaned_df, change_records).
    """
    df = df.copy()
    records: list[ChangeRecord] = []

    # 1. Remove duplicate rows first (so row indices stay stable)
    dup_indices = [i.row for i in issues if i.issue_type == ISSUE_DUPLICATE]
    if dup_indices:
        for idx in dup_indices:
            records.append(ChangeRecord(
                row=idx, column="<all>",
                issue_type=ISSUE_DUPLICATE,
                original_value=None, new_value=None,
                fixed=True, note="Duplicate row removed."
            ))
        df = df.drop(index=dup_indices).reset_index(drop=True)

    # 2. Whitespace — trim all string cells
    for col in df.columns:
        for idx in df.index:
            val = str(df.at[idx, col])
            stripped = " ".join(val.split())  # collapse internal spaces too
            if stripped != val:
                records.append(ChangeRecord(
                    row=idx, column=col,
                    issue_type=ISSUE_WHITESPACE,
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
                    row=idx, column=col,
                    issue_type=ISSUE_CASING,
                    original_value=val, new_value=titled,
                    fixed=True, note="Normalized to title case."
                ))
                df.at[idx, col] = titled

    # 4. Date normalization → dd/mm/yyyy
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
                    row=idx, column=col,
                    issue_type=ISSUE_BAD_FORMAT,
                    original_value=val, new_value=normalized,
                    fixed=True, note="Date standardized to dd/mm/yyyy."
                ))
                df.at[idx, col] = normalized
            elif normalized is None:
                records.append(ChangeRecord(
                    row=idx, column=col,
                    issue_type=ISSUE_BAD_FORMAT,
                    original_value=val, new_value=val,
                    fixed=False, note="Could not parse date — left unchanged."
                ))

    # 5. Missing values — flag (cannot fill without domain knowledge)
    for issue in issues:
        if issue.issue_type == ISSUE_MISSING:
            records.append(ChangeRecord(
                row=issue.row, column=issue.column,
                issue_type=ISSUE_MISSING,
                original_value=issue.original_value, new_value=None,
                fixed=False,
                note="Missing value detected — requires manual review."
            ))

    # 6. Wrong type — flag (cannot safely coerce without knowing intent)
    for issue in issues:
        if issue.issue_type == ISSUE_WRONG_TYPE:
            records.append(ChangeRecord(
                row=issue.row, column=issue.column,
                issue_type=ISSUE_WRONG_TYPE,
                original_value=issue.original_value, new_value=None,
                fixed=False,
                note=(
                    f"Value does not match expected type for this column "
                    f"— requires manual review."
                )
            ))

    return df, records


# --- helper ---

def _normalize_date(s: str) -> str | None:
    try:
        dt = dateparser.parse(s, fuzzy=False)
        return dt.strftime("%d/%m/%Y")
    except Exception:
        return None
