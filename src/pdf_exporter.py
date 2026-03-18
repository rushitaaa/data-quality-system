"""
pdf_exporter.py — Generates a PDF report from the change report dict.
"""
from __future__ import annotations
from fpdf import FPDF
from datetime import datetime


def generate_pdf(report: dict, filename: str) -> bytes:
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # Title
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_fill_color(30, 64, 175)
    pdf.set_text_color(255, 255, 255)
    pdf.rect(0, 0, 210, 30, "F")
    pdf.set_xy(10, 8)
    pdf.cell(0, 14, "Data Quality Report", ln=True)

    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "", 9)
    pdf.set_xy(10, 32)
    pdf.cell(0, 6, f"File: {filename}   |   Generated: {datetime.now().strftime('%d/%m/%Y %H:%M')}", ln=True)
    pdf.ln(4)

    # Summary box
    s = report["summary"]
    pdf.set_font("Helvetica", "B", 12)
    pdf.set_fill_color(240, 244, 255)
    pdf.cell(0, 8, " Summary", ln=True, fill=True)
    pdf.set_font("Helvetica", "", 10)
    pdf.ln(1)

    stats = [
        ("Input Rows",        s["total_rows_input"]),
        ("Columns",           s["total_columns"]),
        ("Issues Found",      s["total_issues_found"]),
        ("Auto-Fixed",        s["auto_fixed"]),
        ("Flagged for Review",s["flagged_for_review"]),
        ("Columns Affected",  ", ".join(s["columns_affected"]) or "none"),
    ]
    for label, val in stats:
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(60, 6, f"  {label}:", border=0)
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(0, 6, str(val), ln=True)

    pdf.ln(4)

    # Issues by type
    icons = {
        "bad_format": "Date Format",
        "duplicate_row": "Duplicate",
        "extra_whitespace": "Whitespace",
        "inconsistent_casing": "Casing",
        "missing_value": "Missing Value",
        "wrong_type": "Wrong Type",
        "outlier": "Outlier",
        "empty_column": "Empty Column",
        "bad_phone_format": "Phone Format",
        "bad_postcode": "Postcode",
    }

    for issue_type, entries in report["issues_by_type"].items():
        label = icons.get(issue_type, issue_type.replace("_", " ").title())
        pdf.set_font("Helvetica", "B", 11)
        pdf.set_fill_color(220, 230, 255)
        pdf.cell(0, 7, f"  {label}  ({len(entries)} issues)", ln=True, fill=True)
        pdf.ln(1)

        # Table header
        col_w = [18, 38, 25, 38, 38, 38]
        headers = ["Status", "Column", "Row", "Original", "New Value", "Note"]
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_fill_color(200, 210, 240)
        for i, h in enumerate(headers):
            pdf.cell(col_w[i], 6, h, border=1, fill=True)
        pdf.ln()

        pdf.set_font("Helvetica", "", 8)
        for e in entries[:50]:  # cap at 50 rows per type
            row_data = [
                "FIXED" if e["fixed"] else "FLAGGED",
                str(e["column"])[:18],
                str(e["row"]) if e["row"] != -1 else "n/a",
                str(e["original_value"] or "")[:18],
                str(e["new_value"] or "")[:18],
                str(e["note"])[:30],
            ]
            fill = e["fixed"]
            pdf.set_fill_color(220, 255, 220) if fill else pdf.set_fill_color(255, 245, 200)
            for i, cell in enumerate(row_data):
                pdf.cell(col_w[i], 5, cell, border=1, fill=True)
            pdf.ln()

        if len(entries) > 50:
            pdf.set_font("Helvetica", "I", 8)
            pdf.cell(0, 5, f"  ... and {len(entries)-50} more rows", ln=True)
        pdf.ln(3)

    return pdf.output()
