"""
ScanX Tools - Document Processing Panel
A clean web interface for the ScanX extraction pipeline
"""
import asyncio
import json
import shutil
import sys
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, Request, HTTPException, UploadFile, File, BackgroundTasks
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Paths
BASE_DIR = Path(__file__).parent
SRC_DIR = BASE_DIR.parent
PROJECT_ROOT = SRC_DIR.parent

# Add src to path for imports
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Storage paths
UPLOADS_DIR = BASE_DIR / "uploads"
JOBS_DIR = BASE_DIR / "jobs"
UPLOADS_DIR.mkdir(exist_ok=True)
JOBS_DIR.mkdir(exist_ok=True)

# In-memory job storage
jobs: Dict[str, Dict] = {}
executor = ThreadPoolExecutor(max_workers=2)


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

    job = jobs.get(job_id)
    if job:
        pdf_path = Path(job.get('pdf_path', ''))
        if pdf_path.exists():
            pdf_path.unlink()

        output_dir = JOBS_DIR / job_id
        if output_dir.exists():
            shutil.rmtree(output_dir)


def get_pdf_page_count(pdf_path: Path) -> int:
    """Get number of pages in PDF"""
    try:
        from PyPDF2 import PdfReader
        reader = PdfReader(str(pdf_path))
        return len(reader.pages)
    except Exception:
        return 0


async def run_pipeline_for_job(job_id: str):
    """Run the full pipeline for a job"""
    job = jobs.get(job_id)
    if not job:
        return

    try:
        job['status'] = 'processing'
        job['started_at'] = datetime.now().isoformat()
        job['phase_status'] = {}
        job['logs'] = []
        save_job(job)

        pdf_path = Path(job['pdf_path'])
        output_dir = JOBS_DIR / job_id
        output_dir.mkdir(exist_ok=True)

        # Create subdirectories
        ocr_output = output_dir / "ocr"
        matched_output = output_dir / "matched"
        text_output = output_dir / "text"
        metadata_output = output_dir / "metadata"
        mapping_output = output_dir / "mapping"

        for d in [ocr_output, matched_output, text_output, metadata_output, mapping_output]:
            d.mkdir(exist_ok=True)

        loop = asyncio.get_event_loop()

        def log(msg):
            job['logs'].append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
            save_job(job)

        # Phase 0: OCR
        job['current_phase'] = 'OCR Extraction'
        job['phase_status']['phase0'] = 'running'
        job['progress'] = 5
        save_job(job)
        log(f"Starting OCR for {pdf_path.name}")

        try:
            from submission_scanx.phase0_ocr import process_single_pdf
            result = await loop.run_in_executor(
                executor,
                lambda: process_single_pdf(pdf_path, ocr_output, skip_existing=False)
            )
            if result.get('status') == 'error':
                raise Exception(result.get('error', 'OCR failed'))
            log(f"OCR completed: {result.get('pages', 0)} pages extracted")
        except Exception as e:
            log(f"OCR error: {str(e)}")
            raise

        job['phase_status']['phase0'] = 'completed'
        job['progress'] = 20
        save_job(job)

        # Phase 1ab: Page Matching
        job['current_phase'] = 'Page Matching'
        job['phase_status']['phase1ab'] = 'running'
        job['progress'] = 25
        save_job(job)
        log("Starting page type identification...")

        try:
            from submission_scanx.phase1_process import run_phase1
            template_path = SRC_DIR / "utils" / "template-docs_raw.json"
            await loop.run_in_executor(
                executor,
                lambda: run_phase1(ocr_output, matched_output, template_path, is_final=False, skip_existing=False)
            )
            log("Page matching completed")
        except Exception as e:
            log(f"Page matching error: {str(e)}")
            raise

        job['phase_status']['phase1ab'] = 'completed'
        job['progress'] = 40
        save_job(job)

        # Phase 1c: Text Extraction
        job['current_phase'] = 'Text Extraction'
        job['phase_status']['phase1c'] = 'running'
        job['progress'] = 45
        save_job(job)
        log("Extracting text from pages...")

        try:
            from submission_scanx.phase1_process import process_phase1c
            await loop.run_in_executor(
                executor,
                lambda: process_phase1c(matched_output, text_output, clean=False, skip_existing=False)
            )
            log("Text extraction completed")
        except Exception as e:
            log(f"Text extraction error: {str(e)}")
            raise

        job['phase_status']['phase1c'] = 'completed'
        job['progress'] = 60
        save_job(job)

        # Phase 1d: AI Metadata (optional - may fail without Azure OpenAI)
        job['current_phase'] = 'AI Metadata'
        job['phase_status']['phase1d'] = 'running'
        job['progress'] = 65
        save_job(job)
        log("Extracting metadata with AI...")

        try:
            from submission_scanx.phase1d_metadata import process_phase1d
            await loop.run_in_executor(
                executor,
                lambda: process_phase1d(matched_output, metadata_output, skip_existing=False, clean=False)
            )
            log("AI metadata extraction completed")
            job['phase_status']['phase1d'] = 'completed'
        except Exception as e:
            log(f"AI metadata skipped: {str(e)}")
            job['phase_status']['phase1d'] = 'skipped'

        job['progress'] = 80
        save_job(job)

        # Phase 1e: Data Mapping
        job['current_phase'] = 'Data Mapping'
        job['phase_status']['phase1e'] = 'running'
        job['progress'] = 85
        save_job(job)
        log("Mapping extracted data to CSV...")

        try:
            from submission_scanx.phase1e_mapping import run_phase1e

            # Try to find CSV dir from training data as reference
            csv_dir = SRC_DIR / "training" / "train input"
            if not csv_dir.exists():
                csv_dir = matched_output

            await loop.run_in_executor(
                executor,
                lambda: run_phase1e(matched_output, mapping_output, csv_dir=csv_dir, skip_existing=False, clean=False)
            )
            log("Data mapping completed")
            job['phase_status']['phase1e'] = 'completed'
        except Exception as e:
            log(f"Data mapping error: {str(e)}")
            job['phase_status']['phase1e'] = 'error'

        job['progress'] = 100

        # Mark job as completed
        job['status'] = 'completed'
        job['completed_at'] = datetime.now().isoformat()
        job['current_phase'] = None
        log("Pipeline completed successfully!")
        save_job(job)

    except Exception as e:
        job['status'] = 'error'
        job['error'] = str(e)
        job['current_phase'] = None
        if 'logs' in job:
            job['logs'].append(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: {str(e)}")
        save_job(job)
        print(f"Pipeline error for job {job_id}: {e}")


# Lifespan handler (replaces deprecated on_event)
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    load_jobs()
    print(f"Loaded {len(jobs)} existing jobs")
    yield
    # Shutdown
    executor.shutdown(wait=False)


# Create app with lifespan
app = FastAPI(title="ScanX Tools", version="2.0.0", lifespan=lifespan)

# Static files
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Templates
TEMPLATES_DIR = BASE_DIR / "templates"
TEMPLATES_DIR.mkdir(exist_ok=True)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


# Page Routes
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


# API Routes - Jobs
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

    job_id = str(uuid.uuid4())[:8]

    pdf_path = UPLOADS_DIR / f"{job_id}_{file.filename}"
    with open(pdf_path, 'wb') as f:
        content = await file.read()
        f.write(content)

    pages = get_pdf_page_count(pdf_path)

    job = {
        "id": job_id,
        "filename": file.filename,
        "pdf_path": str(pdf_path),
        "pages": pages,
        "status": "queued",
        "progress": 0,
        "current_phase": None,
        "phase_status": {},
        "logs": [],
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

    output_dir = JOBS_DIR / job_id
    results = {
        "success": True,
        "job": job,
        "ocr_data": None,
        "matched_data": None,
        "text_data": None,
        "csv_files": []
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

    # Load text data
    text_dir = output_dir / "text"
    if text_dir.exists():
        for json_file in text_dir.glob("*_combined.json"):
            with open(json_file, 'r', encoding='utf-8') as f:
                results['text_data'] = json.load(f)
            break

    # List CSV files
    mapping_dir = output_dir / "mapping"
    if mapping_dir.exists():
        results['csv_files'] = [f.name for f in mapping_dir.glob("*.csv")]

    return results


@app.get("/api/jobs/{job_id}/csv/{filename}")
async def get_job_csv(job_id: str, filename: str):
    """Get a CSV file from job results"""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    csv_path = JOBS_DIR / job_id / "mapping" / filename
    if not csv_path.exists():
        raise HTTPException(status_code=404, detail="CSV file not found")

    return FileResponse(csv_path, media_type="text/csv", filename=filename)


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


@app.get("/api/jobs/{job_id}/logs")
async def get_job_logs(job_id: str):
    """Get job processing logs"""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {"logs": job.get('logs', [])}


def run():
    """Run the server"""
    import uvicorn
    print("=" * 50)
    print("  ScanX Tools - Document Processing")
    print("=" * 50)
    print("  Server: http://localhost:8000")
    print("=" * 50)
    uvicorn.run("scanx_tools.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    run()
