"""
app.py — FastAPI web server for the Data Quality Improvement System.
"""
import io, os, sys, uuid, json, math, tempfile
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.responses import FileResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

sys.path.insert(0, str(Path(__file__).parent / "src"))

from reader       import read_file
from detector     import detect_types
from checker      import check
from cleaner      import clean
from reporter     import build_report, report_to_json
from writer       import write_output
from pdf_exporter import generate_pdf

ALLOWED_EXTENSIONS = {".csv", ".json", ".xml", ".yaml", ".yml"}
TEMP_DIR = Path(tempfile.gettempdir()) / "dqis"
TEMP_DIR.mkdir(exist_ok=True)

app = FastAPI(title="Data Quality Improvement System", version="2.0.0")
app.mount("/static", StaticFiles(directory="static"), name="static")


def _sanitize(obj):
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(content=Path("static/index.html").read_text(encoding="utf-8"))


@app.post("/api/process")
async def process_file(
    file: UploadFile = File(...),
    missing_strategy: str = Form("flag"),
    missing_custom_value: str = Form(""),
    duplicate_mode: str = Form("exact"),
    output_format: str = Form("same"),
    type_overrides: str = Form("{}"),   # JSON string: {"col": "type", ...}
):
    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(400, detail=f"Unsupported type '{ext}'. Allowed: {', '.join(ALLOWED_EXTENSIONS)}")

    job_id  = uuid.uuid4().hex
    job_dir = TEMP_DIR / job_id
    job_dir.mkdir()
    input_path = job_dir / file.filename
    input_path.write_bytes(await file.read())

    try:
        overrides = json.loads(type_overrides) if type_overrides else {}
    except Exception:
        overrides = {}

    try:
        df = read_file(str(input_path))
        total_rows, total_cols = df.shape

        # Original preview (before cleaning)
        orig_preview = {
            "columns": list(df.columns),
            "rows": df.head(10).fillna("").to_dict(orient="records"),
        }

        col_types = detect_types(df)
        col_types.update(overrides)   # apply any manual overrides

        issues = check(df, col_types, duplicate_mode=duplicate_mode)

        cleaned_df, change_records = clean(
            df, issues, col_types,
            missing_strategy=missing_strategy,
            missing_custom_value=missing_custom_value,
            duplicate_mode=duplicate_mode,
        )

        report = build_report(change_records, total_rows, total_cols)

        # Resolve output extension
        out_ext = ext if output_format == "same" else f".{output_format}"
        out_filename = Path(file.filename).stem + out_ext
        out_input_path = str(job_dir / out_filename)   # fake input path to drive ext detection

        output_dir = str(job_dir / "output")
        write_output(cleaned_df, report, out_input_path, output_dir)

        # Cleaned preview
        clean_preview = {
            "columns": list(cleaned_df.columns),
            "rows": cleaned_df.head(10).fillna("").to_dict(orient="records"),
        }

    except Exception as e:
        raise HTTPException(422, detail=str(e))

    payload = _sanitize({
        "job_id":           job_id,
        "filename":         file.filename,
        "summary":          report["summary"],
        "issues_by_type":   report["issues_by_type"],
        "col_types":        col_types,
        "orig_preview":     orig_preview,
        "clean_preview":    clean_preview,
        "download_cleaned": f"/api/download/{job_id}/cleaned",
        "download_report":  f"/api/download/{job_id}/report",
        "download_pdf":     f"/api/download/{job_id}/pdf",
    })
    return Response(content=json.dumps(payload, indent=2), media_type="application/json")


@app.get("/api/download/{job_id}/cleaned")
async def download_cleaned(job_id: str):
    job_dir = TEMP_DIR / job_id
    if not job_dir.exists():
        raise HTTPException(404, detail="Job not found.")
    files = list((job_dir / "output").glob("*_cleaned.*"))
    if not files:
        raise HTTPException(404, detail="Cleaned file not found.")
    return FileResponse(str(files[0]), filename=files[0].name)


@app.get("/api/download/{job_id}/report")
async def download_report(job_id: str):
    job_dir = TEMP_DIR / job_id
    if not job_dir.exists():
        raise HTTPException(404, detail="Job not found.")
    files = list((job_dir / "output").glob("*_report.json"))
    if not files:
        raise HTTPException(404, detail="Report not found.")
    return FileResponse(str(files[0]), filename=files[0].name, media_type="application/json")


@app.get("/api/download/{job_id}/pdf")
async def download_pdf(job_id: str):
    job_dir = TEMP_DIR / job_id
    if not job_dir.exists():
        raise HTTPException(404, detail="Job not found.")
    report_files = list((job_dir / "output").glob("*_report.json"))
    if not report_files:
        raise HTTPException(404, detail="Report not found.")
    report = json.loads(report_files[0].read_text(encoding="utf-8"))
    # reconstruct filename from path
    filename = report_files[0].name.replace("_report.json", "")
    pdf_bytes = generate_pdf(report, filename)
    return Response(content=bytes(pdf_bytes), media_type="application/pdf",
                    headers={"Content-Disposition": f"attachment; filename={filename}_report.pdf"})
