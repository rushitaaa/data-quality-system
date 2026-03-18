"""
database.py — SQLite persistence for job history using SQLAlchemy.
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine, Column, String, Integer, Text, DateTime
from sqlalchemy.orm import DeclarativeBase, Session

DB_PATH = Path(__file__).parent.parent / "history.db"
engine  = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})


class Base(DeclarativeBase):
    pass


class JobRecord(Base):
    __tablename__ = "jobs"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    job_id          = Column(String, unique=True, nullable=False)
    filename        = Column(String, nullable=False)
    created_at      = Column(DateTime, default=datetime.utcnow)
    total_rows      = Column(Integer)
    total_cols      = Column(Integer)
    issues_found    = Column(Integer)
    auto_fixed      = Column(Integer)
    flagged         = Column(Integer)
    download_cleaned = Column(String)
    download_report  = Column(String)
    download_pdf     = Column(String)
    summary_json    = Column(Text)   # full summary as JSON string


def init_db():
    Base.metadata.create_all(engine)


def save_job(job_id: str, filename: str, summary: dict,
             download_cleaned: str, download_report: str, download_pdf: str):
    with Session(engine) as session:
        record = JobRecord(
            job_id=job_id,
            filename=filename,
            total_rows=summary.get("total_rows_input", 0),
            total_cols=summary.get("total_columns", 0),
            issues_found=summary.get("total_issues_found", 0),
            auto_fixed=summary.get("auto_fixed", 0),
            flagged=summary.get("flagged_for_review", 0),
            download_cleaned=download_cleaned,
            download_report=download_report,
            download_pdf=download_pdf,
            summary_json=json.dumps(summary),
        )
        session.add(record)
        session.commit()


def get_all_jobs(limit: int = 50) -> list[dict]:
    with Session(engine) as session:
        rows = session.query(JobRecord).order_by(JobRecord.created_at.desc()).limit(limit).all()
        return [
            {
                "id":              r.id,
                "job_id":          r.job_id,
                "filename":        r.filename,
                "created_at":      r.created_at.strftime("%d/%m/%Y %H:%M") if r.created_at else "",
                "total_rows":      r.total_rows,
                "issues_found":    r.issues_found,
                "auto_fixed":      r.auto_fixed,
                "flagged":         r.flagged,
                "download_cleaned": r.download_cleaned,
                "download_report":  r.download_report,
                "download_pdf":     r.download_pdf,
            }
            for r in rows
        ]


def delete_job(job_id: str):
    with Session(engine) as session:
        session.query(JobRecord).filter(JobRecord.job_id == job_id).delete()
        session.commit()
