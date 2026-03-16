"""
reporter.py — Builds a structured change report from ChangeRecord objects.
"""
from __future__ import annotations
import json
from collections import defaultdict
from cleaner import ChangeRecord


def build_report(records: list[ChangeRecord], total_rows: int, total_cols: int) -> dict:
    """
    Build a summary report dict from change records.
    """
    fixed = [r for r in records if r.fixed]
    flagged = [r for r in records if not r.fixed]

    # Group by issue type
    by_type: dict[str, list[ChangeRecord]] = defaultdict(list)
    for r in records:
        by_type[r.issue_type].append(r)

    # Columns affected
    affected_cols = sorted({r.column for r in records if r.column != "<all>"})

    report = {
        "summary": {
            "total_rows_input": total_rows,
            "total_columns": total_cols,
            "total_issues_found": len(records),
            "auto_fixed": len(fixed),
            "flagged_for_review": len(flagged),
            "columns_affected": affected_cols,
        },
        "issues_by_type": {
            issue_type: [_record_to_dict(r) for r in recs]
            for issue_type, recs in sorted(by_type.items())
        },
    }
    return report


def report_to_text(report: dict) -> str:
    """Convert report dict to a human-readable text string."""
    s = report["summary"]
    lines = [
        "=" * 60,
        "DATA QUALITY REPORT",
        "=" * 60,
        f"Input rows        : {s['total_rows_input']}",
        f"Columns           : {s['total_columns']}",
        f"Total issues found: {s['total_issues_found']}",
        f"Auto-fixed        : {s['auto_fixed']}",
        f"Flagged for review: {s['flagged_for_review']}",
        f"Columns affected  : {', '.join(s['columns_affected']) or 'none'}",
        "",
    ]

    for issue_type, entries in report["issues_by_type"].items():
        lines.append(f"--- {issue_type.upper().replace('_', ' ')} ({len(entries)}) ---")
        for e in entries:
            status = "FIXED" if e["fixed"] else "FLAGGED"
            row_info = f"row {e['row']}" if e["row"] != -1 else "n/a"
            lines.append(
                f"  [{status}] col='{e['column']}' {row_info} | "
                f"was='{e['original_value']}' -> now='{e['new_value']}' | {e['note']}"
            )
        lines.append("")

    lines.append("=" * 60)
    return "\n".join(lines)


def report_to_json(report: dict) -> str:
    return json.dumps(report, indent=2, default=_json_default)


def _json_default(obj):
    import math
    if isinstance(obj, float) and math.isnan(obj):
        return None
    return str(obj)


# --- helper ---

def _record_to_dict(r: ChangeRecord) -> dict:
    return {
        "row": r.row,
        "column": r.column,
        "issue_type": r.issue_type,
        "original_value": r.original_value,
        "new_value": r.new_value,
        "fixed": r.fixed,
        "note": r.note,
    }
