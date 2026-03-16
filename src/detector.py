"""
detector.py — Detects the semantic type of each column in a DataFrame.

Detected types: integer, float, boolean, date, email, phone, string
"""
import re
import pandas as pd
from dateutil import parser as dateparser

# Regex patterns
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_PHONE_RE = re.compile(r"^\+?[\d\s\-().]{7,20}$")
_BOOL_VALUES = {"true", "false", "yes", "no", "1", "0", "t", "f", "y", "n"}


def detect_types(df: pd.DataFrame) -> dict[str, str]:
    """
    Returns a dict mapping column name → detected type.
    Works on a string-typed DataFrame (as returned by reader.py).
    """
    types = {}
    for col in df.columns:
        types[col] = _detect_column_type(df[col])
    return types


def _detect_column_type(series: pd.Series) -> str:
    """Detect the dominant semantic type of a column."""
    # Drop blanks / NaN for detection
    values = series.dropna().astype(str)
    values = values[values.str.strip() != ""]
    values = values[values.str.strip().str.lower() != "nan"]

    if values.empty:
        return "string"

    scores = {
        "boolean": _score(values, _is_boolean),
        "integer": _score(values, _is_integer),
        "float":   _score(values, _is_float),
        "email":   _score(values, _is_email),
        "phone":   _score(values, _is_phone),
        "date":    _score(values, _is_date),
    }

    # Pick the type with the highest score (min 80% match to be chosen)
    best_type, best_score = max(scores.items(), key=lambda x: x[1])
    if best_score >= 0.8:
        return best_type
    return "string"


def _score(values: pd.Series, check_fn) -> float:
    """Return fraction of non-empty values that pass check_fn."""
    hits = sum(1 for v in values if check_fn(v))
    return hits / len(values) if len(values) > 0 else 0.0


def _is_boolean(v: str) -> bool:
    return v.strip().lower() in _BOOL_VALUES


def _is_integer(v: str) -> bool:
    try:
        int(v.strip().replace(",", ""))
        return True
    except ValueError:
        return False


def _is_float(v: str) -> bool:
    try:
        float(v.strip().replace(",", ""))
        return True
    except ValueError:
        return False


def _is_email(v: str) -> bool:
    return bool(_EMAIL_RE.match(v.strip()))


def _is_phone(v: str) -> bool:
    return bool(_PHONE_RE.match(v.strip()))


def _is_date(v: str) -> bool:
    try:
        dateparser.parse(v.strip(), fuzzy=False)
        return True
    except Exception:
        return False


def print_type_summary(col_types: dict[str, str]) -> None:
    """Print a human-readable type summary to stdout."""
    print("\n--- Column Type Detection ---")
    for col, dtype in col_types.items():
        print(f"  {col:<30} -> {dtype}")
    print()
