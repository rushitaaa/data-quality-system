"""
writer.py — Writes the cleaned DataFrame and report to output files.
"""
from __future__ import annotations
import os
import json
import pandas as pd
from reporter import report_to_text, report_to_json


def write_output(
    df: pd.DataFrame,
    report: dict,
    input_path: str,
    output_dir: str = "output",
) -> tuple[str, str, str]:
    """
    Save cleaned data and report files.

    Returns (cleaned_data_path, report_txt_path, report_json_path).
    """
    os.makedirs(output_dir, exist_ok=True)

    base = os.path.splitext(os.path.basename(input_path))[0]
    ext  = os.path.splitext(input_path)[1].lower()

    cleaned_path    = os.path.join(output_dir, f"{base}_cleaned{ext}")
    report_txt_path = os.path.join(output_dir, f"{base}_report.txt")
    report_json_path = os.path.join(output_dir, f"{base}_report.json")

    # Write cleaned data in the same format as input
    _write_data(df, cleaned_path, ext)

    # Write reports
    with open(report_txt_path, "w", encoding="utf-8") as f:
        f.write(report_to_text(report))

    with open(report_json_path, "w", encoding="utf-8") as f:
        f.write(report_to_json(report))

    return cleaned_path, report_txt_path, report_json_path


def _write_data(df: pd.DataFrame, path: str, ext: str) -> None:
    if ext == ".csv":
        df.to_csv(path, index=False)
    elif ext == ".json":
        df.to_json(path, orient="records", indent=2, force_ascii=False)
    elif ext == ".xml":
        df.to_xml(path, index=False)
    elif ext in (".yaml", ".yml"):
        try:
            import yaml
        except ImportError:
            raise ImportError("pyyaml is required for YAML output. Run: pip install pyyaml")
        records = df.to_dict(orient="records")
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(records, f, allow_unicode=True, default_flow_style=False)
    else:
        # Fallback — write as CSV
        df.to_csv(path, index=False)
