"""
app.py — FastAPI web server for the Data Quality Improvement System.
"""
import io
import os
import sys
import uuid
import tempfile
from pathlib import Path

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

# Make sure src/ modules are importable
sys.path.insert(0, str(Path(__file__).parent / "src"))

from reader   import read_file
from detector import detect_types
from checker  import check
from cleaner  import clean
from reporter import build_report, report_to_json
import json, math

def _sanitize(obj):
    """Recursively replace NaN/Inf floats with None so JSON stays valid."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj
from writer   import write_output

ALLOWED_EXTENSIONS = {".csv", ".json", ".xml", ".yaml", ".yml"}
TEMP_DIR = Path(tempfile.gettempdir()) / "dqis"
TEMP_DIR.mkdir(exist_ok=True)

app = FastAPI(title="Data Quality Improvement System", version="1.0.0")

# Serve static frontend
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = Path("static/index.html")
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.post("/api/process")
async def process_file(file: UploadFile = File(...)):
    """
    Upload a data file, run the full quality pipeline, return report + download links.
    """
    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{ext}'. Allowed: {', '.join(ALLOWED_EXTENSIONS)}"
        )

    # Save upload to a temp file
    job_id = uuid.uuid4().hex
    job_dir = TEMP_DIR / job_id
    job_dir.mkdir()
    input_path = job_dir / file.filename

    content = await file.read()
    input_path.write_bytes(content)

    try:
        df = read_file(str(input_path))
        total_rows, total_cols = df.shape

        col_types = detect_types(df)
        issues    = check(df, col_types)
        cleaned_df, change_records = clean(df, issues, col_types)
        report    = build_report(change_records, total_rows, total_cols)

        output_dir = str(job_dir / "output")
        cleaned_path, _, _ = write_output(cleaned_df, report, str(input_path), output_dir)

    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))

    # Build data preview (first 10 rows of cleaned data)
    preview_rows = cleaned_df.head(10).fillna("").to_dict(orient="records")
    preview_cols = list(cleaned_df.columns)

    payload = _sanitize({
        "job_id": job_id,
        "filename": file.filename,
        "summary": report["summary"],
        "issues_by_type": report["issues_by_type"],
        "col_types": col_types,
        "preview": {"columns": preview_cols, "rows": preview_rows},
        "download_cleaned": f"/api/download/{job_id}/cleaned",
        "download_report":  f"/api/download/{job_id}/report",
    })
    return Response(content=json.dumps(payload, indent=2), media_type="application/json")


@app.get("/api/download/{job_id}/cleaned")
async def download_cleaned(job_id: str):
    job_dir = TEMP_DIR / job_id
    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found.")
    output_dir = job_dir / "output"
    files = list(output_dir.glob("*_cleaned.*"))
    if not files:
        raise HTTPException(status_code=404, detail="Cleaned file not found.")
    return FileResponse(str(files[0]), filename=files[0].name)


@app.get("/api/download/{job_id}/report")
async def download_report(job_id: str):
    job_dir = TEMP_DIR / job_id
    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found.")
    output_dir = job_dir / "output"
    files = list(output_dir.glob("*_report.json"))
    if not files:
        raise HTTPException(status_code=404, detail="Report file not found.")
    return FileResponse(str(files[0]), filename=files[0].name, media_type="application/json")
