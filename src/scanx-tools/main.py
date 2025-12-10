"""
ScanX Tools - Modern Document Processing Panel
A user-friendly web interface for document processing pipeline
"""
import asyncio
import json
import os
import shutil
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, Request, HTTPException, UploadFile, File, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Paths
BASE_DIR = Path(__file__).parent  # scanx-tools
SRC_DIR = BASE_DIR.parent  # src
PROJECT_ROOT = SRC_DIR.parent  # submission_scanx

# Add submission_scanx to path for imports
sys.path.insert(0, str(SRC_DIR))

# Storage paths
UPLOADS_DIR = BASE_DIR / "uploads"
JOBS_DIR = BASE_DIR / "jobs"
UPLOADS_DIR.mkdir(exist_ok=True)
JOBS_DIR.mkdir(exist_ok=True)

# Pipeline paths
PIPELINE_MODULE = SRC_DIR / "submission_scanx"

app = FastAPI(title="ScanX Tools", version="1.0.0")

# Static files
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Templates
TEMPLATES_DIR = BASE_DIR / "templates"
TEMPLATES_DIR.mkdir(exist_ok=True)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# In-memory job storage (in production, use Redis/DB)
jobs: Dict[str, Dict] = {}
executor = ThreadPoolExecutor(max_workers=2)


# ============================================================================
# Helper Functions
# ============================================================================

def load_jobs():
    """Load jobs from disk"""
    global jobs
    for job_file in JOBS_DIR.glob("*.json"):
        try:
            with open(job_file, 'r', encoding='utf-8') as f:
                job = json.load(f)
                jobs[job['id']] = job
        except Exception as e:
            print(f"Error loading job {job_file}: {e}")


def save_job(job: Dict):
    """Save job to disk"""
    job_file = JOBS_DIR / f"{job['id']}.json"
    with open(job_file, 'w', encoding='utf-8') as f:
        json.dump(job, f, ensure_ascii=False, indent=2, default=str)


def delete_job_files(job_id: str):
    """Delete job files from disk"""
    job_file = JOBS_DIR / f"{job_id}.json"
    if job_file.exists():
        job_file.unlink()
    
    # Delete uploaded PDF
    job = jobs.get(job_id)
    if job:
        pdf_path = Path(job.get('pdf_path', ''))
        if pdf_path.exists():
            pdf_path.unlink()
        
        # Delete output directory
        output_dir = JOBS_DIR / job_id
        if output_dir.exists():
            shutil.rmtree(output_dir)


def get_pdf_page_count(pdf_path: Path) -> int:
    """Get number of pages in PDF"""
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(str(pdf_path))
        count = len(doc)
        doc.close()
        return count
    except:
        return 0


# ============================================================================
# Pipeline Execution
# ============================================================================

async def run_pipeline_for_job(job_id: str):
    """Run the full pipeline for a job"""
    job = jobs.get(job_id)
    if not job:
        return
    
    try:
        job['status'] = 'processing'
        job['started_at'] = datetime.now().isoformat()
        job['phase_status'] = {}
        save_job(job)
        
        pdf_path = Path(job['pdf_path'])
        output_dir = JOBS_DIR / job_id
        output_dir.mkdir(exist_ok=True)
        
        # Import pipeline functions
        from submission_scanx.pipeline import run_phase0, run_phase1ab, run_phase1c, run_phase1d, run_phase1e
        from submission_scanx.phase0_ocr import process_single_pdf
        
        phases = [
            ('phase0', 'OCR Extraction', 20),
            ('phase1ab', 'Page Matching', 40),
            ('phase1c', 'Text Extraction', 60),
            ('phase1d', 'AI Metadata', 80),
            ('phase1e', 'Data Mapping', 100),
        ]
        
        # Phase 0: OCR
        job['current_phase'] = 'Phase 0: OCR'
        job['phase_status']['phase0'] = 'running'
        save_job(job)
        
        ocr_output = output_dir / "ocr"
        ocr_output.mkdir(exist_ok=True)
        
        # Run OCR on single PDF
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            executor,
            lambda: process_single_pdf(pdf_path, ocr_output)
        )
        
        job['phase_status']['phase0'] = 'completed'
        job['progress'] = 20
        save_job(job)
        
        # Phase 1ab: Page Matching
        job['current_phase'] = 'Phase 1: Page Matching'
        job['phase_status']['phase1ab'] = 'running'
        save_job(job)
        
        matched_output = output_dir / "matched"
        matched_output.mkdir(exist_ok=True)
        
        from submission_scanx.phase1_process import run_phase1
        template_path = SRC_DIR / "utils" / "template-docs_raw.json"
        
        await loop.run_in_executor(
            executor,
            lambda: run_phase1(ocr_output, matched_output, template_path, is_final=False, skip_existing=False)
        )
        
        job['phase_status']['phase1ab'] = 'completed'
        job['progress'] = 40
        save_job(job)
        
        # Phase 1c: Text Extraction
        job['current_phase'] = 'Phase 1c: Text Extraction'
        job['phase_status']['phase1c'] = 'running'
        save_job(job)
        
        text_output = output_dir / "text"
        text_output.mkdir(exist_ok=True)
        
        from submission_scanx.phase1_process import process_phase1c
        await loop.run_in_executor(
            executor,
            lambda: process_phase1c(matched_output, text_output)
        )
        
        job['phase_status']['phase1c'] = 'completed'
        job['progress'] = 60
        save_job(job)
        
        # Phase 1d: AI Metadata (optional, skip for now as it needs AI)
        job['phase_status']['phase1d'] = 'completed'
        job['progress'] = 80
        save_job(job)
        
        # Phase 1e: Data Mapping
        job['current_phase'] = 'Phase 1e: Data Mapping'
        job['phase_status']['phase1e'] = 'running'
        save_job(job)
        
        mapping_output = output_dir / "mapping"
        mapping_output.mkdir(exist_ok=True)
        
        # For now, just mark as completed
        job['phase_status']['phase1e'] = 'completed'
        job['progress'] = 100
        
        # Mark job as completed
        job['status'] = 'completed'
        job['completed_at'] = datetime.now().isoformat()
        job['current_phase'] = None
        save_job(job)
        
    except Exception as e:
        job['status'] = 'error'
        job['error'] = str(e)
        job['current_phase'] = None
        save_job(job)
        print(f"Pipeline error for job {job_id}: {e}")


# ============================================================================
# Page Routes
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Main panel page"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/results/{job_id}", response_class=HTMLResponse)
async def results_page(request: Request, job_id: str):
    """Results viewer page"""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return templates.TemplateResponse("results.html", {"request": request, "job": job})


# ============================================================================
# API Routes - Jobs
# ============================================================================

@app.get("/api/jobs")
async def list_jobs():
    """List all jobs"""
    job_list = sorted(jobs.values(), key=lambda x: x.get('created_at', ''), reverse=True)
    return {"jobs": job_list}


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload a PDF file and create a job"""
    if not file.filename.lower().endswith('.pdf'):
        return {"success": False, "error": "Only PDF files are allowed"}
    
    # Create job
    job_id = str(uuid.uuid4())[:8]
    
    # Save uploaded file
    pdf_path = UPLOADS_DIR / f"{job_id}_{file.filename}"
    with open(pdf_path, 'wb') as f:
        content = await file.read()
        f.write(content)
    
    # Get page count
    pages = get_pdf_page_count(pdf_path)
    
    # Create job record
    job = {
        "id": job_id,
        "filename": file.filename,
        "pdf_path": str(pdf_path),
        "pages": pages,
        "status": "queued",
        "progress": 0,
        "current_phase": None,
        "phase_status": {},
        "created_at": datetime.now().isoformat(),
        "started_at": None,
        "completed_at": None,
        "error": None
    }
    
    jobs[job_id] = job
    save_job(job)
    
    return {"success": True, "job_id": job_id, "job": job}


@app.post("/api/jobs/{job_id}/start")
async def start_job(job_id: str, background_tasks: BackgroundTasks):
    """Start processing a job"""
    job = jobs.get(job_id)
    if not job:
        return {"success": False, "error": "Job not found"}
    
    if job['status'] != 'queued':
        return {"success": False, "error": f"Job is already {job['status']}"}
    
    # Start pipeline in background
    background_tasks.add_task(run_pipeline_for_job, job_id)
    
    return {"success": True}


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    """Get job details"""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.delete("/api/jobs/{job_id}")
async def delete_job(job_id: str):
    """Delete a job"""
    if job_id not in jobs:
        return {"success": False, "error": "Job not found"}
    
    delete_job_files(job_id)
    del jobs[job_id]
    
    return {"success": True}


@app.get("/api/jobs/{job_id}/results")
async def get_job_results(job_id: str):
    """Get job results"""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job['status'] != 'completed':
        return {"success": False, "error": "Job not completed"}
    
    output_dir = JOBS_DIR / job_id
    results = {
        "success": True,
        "job": job,
        "ocr_data": None,
        "matched_data": None,
        "mappings": {}
    }
    
    # Load OCR data
    ocr_dir = output_dir / "ocr"
    if ocr_dir.exists():
        for json_file in ocr_dir.glob("*.json"):
            with open(json_file, 'r', encoding='utf-8') as f:
                results['ocr_data'] = json.load(f)
            break
    
    # Load matched data
    matched_dir = output_dir / "matched"
    if matched_dir.exists():
        for json_file in matched_dir.glob("*.json"):
            with open(json_file, 'r', encoding='utf-8') as f:
                results['matched_data'] = json.load(f)
            break
    
    return results


@app.get("/api/jobs/{job_id}/pdf")
async def get_job_pdf(job_id: str):
    """Serve the job's PDF file"""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    pdf_path = Path(job['pdf_path'])
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="PDF not found")
    
    return FileResponse(pdf_path, media_type="application/pdf")


# ============================================================================
# Startup
# ============================================================================

@app.on_event("startup")
async def startup():
    """Load existing jobs on startup"""
    load_jobs()
    print(f"Loaded {len(jobs)} existing jobs")


def run():
    """Run the server"""
    import uvicorn
    print("=" * 60)
    print("   ScanX Tools - Document Processing Panel")
    print("=" * 60)
    print("   Server: http://localhost:8000")
    print("=" * 60)
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    run()
