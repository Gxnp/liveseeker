from fastapi import FastAPI, HTTPException, Depends, Security, status
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, Field
from typing import List
from datetime import datetime
import uuid
import json
import os
from dotenv import load_dotenv

# -----------------------------
# CONFIG
# -----------------------------
load_dotenv()
API_KEY = os.getenv("SCRAPER_API_KEY")
API_KEY_NAME = "X-API-Key"
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
JOBS_PENDING_DIR = os.path.join(BASE_DIR, "jobs", "pending")

os.makedirs(JOBS_PENDING_DIR, exist_ok=True)

# -----------------------------
# APP
# -----------------------------
app = FastAPI(title="Scraper Job API")

api_key_header = APIKeyHeader(
    name=API_KEY_NAME,
    auto_error=False
)

def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key"
        )

# -----------------------------
# SCHEMA
# -----------------------------
class JobRequest(BaseModel):
    sites: List[str] = Field(..., min_items=1, description="List of URLs to scrape")
    visits_per_site: int = Field(1, ge=1, le=20, description="Number of visits per site")
    max_workers: int = Field(1, ge=1, le=10, description="Maximum number of parallel workers")
    run_at: datetime = Field(
        ...,
        description="Scheduled time to run the job (format: YYYY-MM-DD HH:MM, e.g., 2025-12-30 14:25)",
        json_schema_extra={
            "example": "2025-12-30 14:25"
        }
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "sites": ["https://example.com"],
                "visits_per_site": 1,
                "max_workers": 1,
                "run_at": "2025-12-30 14:25"
            }
        }

# -----------------------------
# ENDPOINT
# -----------------------------
@app.post("/jobs")
def create_job(job: JobRequest, _: str = Depends(verify_api_key)):
    now = datetime.now()
    
    # ทำให้ run_at เป็น naive datetime (เอา timezone ออกถ้ามี)
    run_at = job.run_at
    if run_at.tzinfo is not None:
        # ถ้ามี timezone ให้แปลงเป็น local time แล้วเอา timezone ออก
        run_at = run_at.astimezone().replace(tzinfo=None)

    if run_at <= now:
        raise HTTPException(status_code=400, detail="run_at must be in the future")

    job_id = f"job_{uuid.uuid4().hex}"

    job_data = {
        "job_id": job_id,
        "run_at": run_at.strftime("%Y-%m-%d %H:%M"),  # ใช้ format เดียวกับ scheduler
        "sites": job.sites,
        "visits_per_site": job.visits_per_site,
        "max_workers": job.max_workers,
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S")
    }

    job_path = os.path.join(JOBS_PENDING_DIR, f"{job_id}.json")

    with open(job_path, "w", encoding="utf-8") as f:
        json.dump(job_data, f, ensure_ascii=False, indent=2)

    return {
        "status": "ok",
        "job_id": job_id,
        "path": job_path
    }
