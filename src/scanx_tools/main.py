"""
ScanX Tools - Single File Document Processing
A clean web interface for processing Thai financial disclosure documents
"""
import asyncio
import json
import shutil
import sys
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, Request, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
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
WORK_DIR = BASE_DIR / "workspace"
WORK_DIR.mkdir(exist_ok=True)
JOBS_DIR = BASE_DIR / "jobs"  # Store completed job results
JOBS_DIR.mkdir(exist_ok=True)
HISTORY_FILE = BASE_DIR / "history.json"

# Current processing state
current_state: Dict = {
    "status": "idle",  # idle, uploading, processing, completed, error
    "job_id": None,
    "filename": None,
    "pdf_path": None,
    "pages": 0,
    "progress": 0,
    "current_phase": None,
    "phase_status": {},
    "logs": [],
    "error": None,
    "started_at": None,
    "completed_at": None
}

# Processing history
history: List[Dict] = []

executor = ThreadPoolExecutor(max_workers=1)


def load_history():
    """Load history from file"""
    global history
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except Exception:
            history = []


def save_history():
    """Save history to file"""
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history[-50:], f, ensure_ascii=False, indent=2)  # Keep last 50


def generate_job_id() -> str:
    """Generate a unique job ID"""
    return datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]


def save_job_results(job_id: str, state: Dict):
    """Save job results to a dedicated folder"""
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(exist_ok=True)

    # Copy workspace results to job folder
    for subdir in ["ocr", "matched", "text", "metadata", "mapping"]:
        src_dir = WORK_DIR / subdir
        if src_dir.exists():
            dst_dir = job_dir / subdir
            if dst_dir.exists():
                shutil.rmtree(dst_dir)
            shutil.copytree(src_dir, dst_dir)

    # Copy PDF file
    if state.get('pdf_path') and Path(state['pdf_path']).exists():
        pdf_src = Path(state['pdf_path'])
        pdf_dst = job_dir / pdf_src.name
        shutil.copy2(pdf_src, pdf_dst)

    # Save job state
    job_state = {
        "job_id": job_id,
        "filename": state['filename'],
        "pages": state['pages'],
        "status": state['status'],
        "started_at": state['started_at'],
        "completed_at": state['completed_at'],
        "error": state.get('error'),
        "phase_status": state.get('phase_status', {}),
        "logs": state.get('logs', [])
    }
    with open(job_dir / "job_state.json", 'w', encoding='utf-8') as f:
        json.dump(job_state, f, ensure_ascii=False, indent=2)


def delete_job(job_id: str) -> bool:
    """Delete a job and its data"""
    job_dir = JOBS_DIR / job_id
    if job_dir.exists():
        shutil.rmtree(job_dir)
        return True
    return False


def add_to_history(state: Dict):
    """Add completed processing to history"""
    history.append({
        "job_id": state.get('job_id'),
        "filename": state['filename'],
        "pages": state['pages'],
        "status": state['status'],
        "started_at": state['started_at'],
        "completed_at": state['completed_at'],
        "error": state.get('error')
    })
    save_history()


def reset_state():
    """Reset the current state"""
    global current_state
    current_state = {
        "status": "idle",
        "job_id": None,
        "filename": None,
        "pdf_path": None,
        "pages": 0,
        "progress": 0,
        "current_phase": None,
        "phase_status": {},
        "logs": [],
        "error": None,
        "started_at": None,
        "completed_at": None
    }


def clear_workspace():
    """Clear workspace directory"""
    if WORK_DIR.exists():
        for item in WORK_DIR.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()


def get_pdf_page_count(pdf_path: Path) -> int:
    """Get number of pages in PDF"""
    try:
        from PyPDF2 import PdfReader
        reader = PdfReader(str(pdf_path))
        return len(reader.pages)
    except Exception:
        return 0


def log_message(msg: str):
    """Add a log message"""
    current_state['logs'].append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


async def run_pipeline():
    """Run the full pipeline for the current file"""
    global current_state

    if not current_state['pdf_path']:
        return

    try:
        current_state['status'] = 'processing'
        current_state['started_at'] = datetime.now().isoformat()
        current_state['phase_status'] = {}
        current_state['logs'] = []

        pdf_path = Path(current_state['pdf_path'])

        # Create output directories
        ocr_output = WORK_DIR / "ocr"
        matched_output = WORK_DIR / "matched"
        text_output = WORK_DIR / "text"
        metadata_output = WORK_DIR / "metadata"
        mapping_output = WORK_DIR / "mapping"

        for d in [ocr_output, matched_output, text_output, metadata_output, mapping_output]:
            d.mkdir(exist_ok=True)

        loop = asyncio.get_event_loop()

        # Phase 0: OCR
        current_state['current_phase'] = 'OCR Extraction'
        current_state['phase_status']['phase0'] = 'running'
        current_state['progress'] = 5
        log_message(f"Starting OCR for {pdf_path.name}")

        try:
            from submission_scanx.phase0_ocr import process_single_pdf
            result = await loop.run_in_executor(
                executor,
                lambda: process_single_pdf(pdf_path, ocr_output, skip_existing=False)
            )
            if result.get('status') == 'error':
                raise Exception(result.get('error', 'OCR failed'))
            log_message(f"OCR completed: {result.get('pages', 0)} pages extracted")
        except Exception as e:
            log_message(f"OCR error: {str(e)}")
            raise

        current_state['phase_status']['phase0'] = 'completed'
        current_state['progress'] = 20

        # Phase 1ab: Page Matching
        current_state['current_phase'] = 'Page Matching'
        current_state['phase_status']['phase1ab'] = 'running'
        current_state['progress'] = 25
        log_message("Starting page type identification...")

        try:
            from submission_scanx.phase1_process import run_phase1
            template_path = SRC_DIR / "utils" / "template-docs_raw.json"
            await loop.run_in_executor(
                executor,
                lambda: run_phase1(ocr_output, matched_output, template_path, is_final=False, skip_existing=False)
            )
            log_message("Page matching completed")
        except Exception as e:
            log_message(f"Page matching error: {str(e)}")
            raise

        current_state['phase_status']['phase1ab'] = 'completed'
        current_state['progress'] = 40

        # Phase 1c: Text Extraction
        current_state['current_phase'] = 'Text Extraction'
        current_state['phase_status']['phase1c'] = 'running'
        current_state['progress'] = 45
        log_message("Extracting text from pages...")

        try:
            from submission_scanx.phase1_process import process_phase1c
            await loop.run_in_executor(
                executor,
                lambda: process_phase1c(matched_output, text_output, clean=False, skip_existing=False)
            )
            log_message("Text extraction completed")
        except Exception as e:
            log_message(f"Text extraction error: {str(e)}")
            raise

        current_state['phase_status']['phase1c'] = 'completed'
        current_state['progress'] = 60

        # Phase 1d: AI Metadata (optional)
        current_state['current_phase'] = 'AI Metadata'
        current_state['phase_status']['phase1d'] = 'running'
        current_state['progress'] = 65
        log_message("Extracting metadata with AI...")

        try:
            from submission_scanx.phase1d_metadata import process_phase1d
            await loop.run_in_executor(
                executor,
                lambda: process_phase1d(matched_output, metadata_output, skip_existing=False, clean=False)
            )
            log_message("AI metadata extraction completed")
            current_state['phase_status']['phase1d'] = 'completed'
        except Exception as e:
            log_message(f"AI metadata skipped: {str(e)}")
            current_state['phase_status']['phase1d'] = 'skipped'

        current_state['progress'] = 80

        # Phase 1e: Data Mapping
        current_state['current_phase'] = 'Data Mapping'
        current_state['phase_status']['phase1e'] = 'running'
        current_state['progress'] = 85
        log_message("Mapping extracted data to CSV...")

        try:
            from submission_scanx.phase1e_mapping import run_phase1e

            csv_dir = SRC_DIR / "training" / "train input"
            if not csv_dir.exists():
                csv_dir = matched_output

            await loop.run_in_executor(
                executor,
                lambda: run_phase1e(matched_output, mapping_output, csv_dir=csv_dir, skip_existing=False, clean=False)
            )
            log_message("Data mapping completed")
            current_state['phase_status']['phase1e'] = 'completed'
        except Exception as e:
            log_message(f"Data mapping error: {str(e)}")
            current_state['phase_status']['phase1e'] = 'error'

        current_state['progress'] = 100
        current_state['status'] = 'completed'
        current_state['completed_at'] = datetime.now().isoformat()
        current_state['current_phase'] = None
        log_message("Processing completed!")

        # Save job results to permanent storage
        if current_state.get('job_id'):
            save_job_results(current_state['job_id'], current_state)
            log_message(f"Results saved to job: {current_state['job_id']}")

        # Add to history
        add_to_history(current_state)

    except Exception as e:
        current_state['status'] = 'error'
        current_state['error'] = str(e)
        current_state['current_phase'] = None
        log_message(f"ERROR: {str(e)}")
        print(f"Pipeline error: {e}")

        # Add to history even on error
        current_state['completed_at'] = datetime.now().isoformat()
        add_to_history(current_state)


# Lifespan handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    load_history()
    yield
    executor.shutdown(wait=False)


# Create app
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
    """Main page"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/results", response_class=HTMLResponse)
async def results_page(request: Request, job_id: Optional[str] = None):
    """Results viewer page"""
    # If job_id is provided, load that job's state
    if job_id:
        job_dir = JOBS_DIR / job_id
        state_file = job_dir / "job_state.json"
        if state_file.exists():
            with open(state_file, 'r', encoding='utf-8') as f:
                job_state = json.load(f)
            return templates.TemplateResponse("results.html", {
                "request": request,
                "state": job_state,
                "job_id": job_id
            })
        else:
            return HTMLResponse(content="<script>alert('Job not found');window.location.href='/'</script>")

    # Otherwise use current state
    if current_state['status'] != 'completed':
        return HTMLResponse(content="<script>window.location.href='/'</script>")
    return templates.TemplateResponse("results.html", {
        "request": request,
        "state": current_state,
        "job_id": current_state.get('job_id')
    })


# API Routes
@app.get("/api/status")
async def get_status():
    """Get current processing status"""
    return current_state


@app.get("/api/history")
async def get_history():
    """Get processing history"""
    return {"history": list(reversed(history))}


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload a PDF file and start processing"""
    global current_state

    if not file.filename.lower().endswith('.pdf'):
        return JSONResponse({"success": False, "error": "Only PDF files are allowed"}, status_code=400)

    if current_state['status'] == 'processing':
        return JSONResponse({"success": False, "error": "Already processing a file"}, status_code=400)

    # Clear previous data
    reset_state()
    clear_workspace()

    # Generate job ID
    job_id = generate_job_id()

    # Save the uploaded file
    pdf_path = WORK_DIR / file.filename
    with open(pdf_path, 'wb') as f:
        content = await file.read()
        f.write(content)

    pages = get_pdf_page_count(pdf_path)

    current_state['status'] = 'uploading'
    current_state['job_id'] = job_id
    current_state['filename'] = file.filename
    current_state['pdf_path'] = str(pdf_path)
    current_state['pages'] = pages

    # Start processing in background
    asyncio.create_task(run_pipeline())

    return {"success": True, "filename": file.filename, "pages": pages, "job_id": job_id}


@app.post("/api/reset")
async def reset():
    """Reset and clear everything"""
    reset_state()
    clear_workspace()
    return {"success": True}


@app.get("/api/results")
async def get_results():
    """Get processing results"""
    if current_state['status'] != 'completed':
        return {"success": False, "error": "Processing not completed"}

    results = {
        "success": True,
        "state": current_state,
        "ocr_data": None,
        "matched_data": None,
        "text_data": None,
        "metadata": None,
        "csv_files": []
    }

    # Load OCR data
    ocr_dir = WORK_DIR / "ocr"
    if ocr_dir.exists():
        for json_file in ocr_dir.glob("*.json"):
            with open(json_file, 'r', encoding='utf-8') as f:
                results['ocr_data'] = json.load(f)
            break

    # Load matched data
    matched_dir = WORK_DIR / "matched"
    if matched_dir.exists():
        for json_file in matched_dir.glob("*.json"):
            with open(json_file, 'r', encoding='utf-8') as f:
                results['matched_data'] = json.load(f)
            break

    # Load text data
    text_dir = WORK_DIR / "text"
    if text_dir.exists():
        for json_file in text_dir.glob("*.json"):
            with open(json_file, 'r', encoding='utf-8') as f:
                results['text_data'] = json.load(f)
            break

    # Load metadata
    metadata_dir = WORK_DIR / "metadata"
    if metadata_dir.exists():
        for json_file in metadata_dir.glob("*.json"):
            with open(json_file, 'r', encoding='utf-8') as f:
                results['metadata'] = json.load(f)
            break

    # List CSV files
    mapping_dir = WORK_DIR / "mapping"
    if mapping_dir.exists():
        results['csv_files'] = [f.name for f in mapping_dir.glob("*.csv")]

    return results


@app.get("/api/csv/{filename}")
async def get_csv(filename: str):
    """Get a CSV file"""
    csv_path = WORK_DIR / "mapping" / filename
    if not csv_path.exists():
        raise HTTPException(status_code=404, detail="CSV file not found")
    return FileResponse(csv_path, media_type="text/csv", filename=filename)


@app.get("/api/pdf")
async def get_pdf():
    """Serve the uploaded PDF file"""
    if not current_state['pdf_path']:
        raise HTTPException(status_code=404, detail="No PDF uploaded")

    pdf_path = Path(current_state['pdf_path'])
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="PDF not found")

    return FileResponse(pdf_path, media_type="application/pdf")


@app.delete("/api/history/{index}")
async def delete_history_item(index: int):
    """Delete a history item by index"""
    global history
    if index < 0 or index >= len(history):
        raise HTTPException(status_code=404, detail="History item not found")

    # History is stored in chronological order, but displayed reversed
    # So we need to delete from the end
    reversed_index = len(history) - 1 - index
    deleted = history.pop(reversed_index)

    # Also delete job data if exists
    if deleted.get('job_id'):
        delete_job(deleted['job_id'])

    save_history()
    return {"success": True, "deleted": deleted}


@app.post("/api/history/clear")
async def clear_history():
    """Clear all history"""
    global history

    # Delete all job data
    for item in history:
        if item.get('job_id'):
            delete_job(item['job_id'])

    history = []
    save_history()
    return {"success": True}


@app.get("/api/jobs/{job_id}")
async def get_job_results(job_id: str):
    """Get results for a specific job"""
    job_dir = JOBS_DIR / job_id

    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found")

    results = {
        "success": True,
        "job_id": job_id,
        "state": None,
        "ocr_data": None,
        "matched_data": None,
        "text_data": None,
        "metadata": None,
        "csv_files": []
    }

    # Load job state
    state_file = job_dir / "job_state.json"
    if state_file.exists():
        with open(state_file, 'r', encoding='utf-8') as f:
            results['state'] = json.load(f)

    # Load OCR data
    ocr_dir = job_dir / "ocr"
    if ocr_dir.exists():
        for json_file in ocr_dir.glob("*.json"):
            with open(json_file, 'r', encoding='utf-8') as f:
                results['ocr_data'] = json.load(f)
            break

    # Load matched data
    matched_dir = job_dir / "matched"
    if matched_dir.exists():
        for json_file in matched_dir.glob("*.json"):
            with open(json_file, 'r', encoding='utf-8') as f:
                results['matched_data'] = json.load(f)
            break

    # Load text data
    text_dir = job_dir / "text"
    if text_dir.exists():
        for json_file in text_dir.glob("*.json"):
            with open(json_file, 'r', encoding='utf-8') as f:
                results['text_data'] = json.load(f)
            break

    # Load metadata
    metadata_dir = job_dir / "metadata"
    if metadata_dir.exists():
        for json_file in metadata_dir.glob("*.json"):
            with open(json_file, 'r', encoding='utf-8') as f:
                results['metadata'] = json.load(f)
            break

    # List CSV files
    mapping_dir = job_dir / "mapping"
    if mapping_dir.exists():
        results['csv_files'] = [f.name for f in mapping_dir.glob("*.csv")]

    return results


@app.get("/api/jobs/{job_id}/pdf")
async def get_job_pdf(job_id: str):
    """Serve PDF for a specific job"""
    job_dir = JOBS_DIR / job_id

    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found")

    # Find PDF file in job directory
    pdf_files = list(job_dir.glob("*.pdf"))
    if not pdf_files:
        raise HTTPException(status_code=404, detail="PDF not found")

    return FileResponse(pdf_files[0], media_type="application/pdf")


@app.get("/api/jobs/{job_id}/csv/{filename}")
async def get_job_csv(job_id: str, filename: str):
    """Get a CSV file from a specific job"""
    csv_path = JOBS_DIR / job_id / "mapping" / filename
    if not csv_path.exists():
        raise HTTPException(status_code=404, detail="CSV file not found")
    return FileResponse(csv_path, media_type="text/csv", filename=filename)


@app.get("/api/csv/{filename}/data")
async def get_csv_data(filename: str):
    """Get CSV file content as JSON"""
    import csv
    csv_path = WORK_DIR / "mapping" / filename
    if not csv_path.exists():
        raise HTTPException(status_code=404, detail="CSV file not found")

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        headers = reader.fieldnames or []

    return {"filename": filename, "headers": headers, "rows": rows}


@app.get("/api/jobs/{job_id}/csv/{filename}/data")
async def get_job_csv_data(job_id: str, filename: str):
    """Get CSV file content as JSON from a specific job"""
    import csv
    csv_path = JOBS_DIR / job_id / "mapping" / filename
    if not csv_path.exists():
        raise HTTPException(status_code=404, detail="CSV file not found")

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        headers = reader.fieldnames or []

    return {"filename": filename, "headers": headers, "rows": rows}


@app.post("/api/csv/{filename}/save")
async def save_csv_data(filename: str, request: Request):
    """Save CSV file from JSON data"""
    import csv
    csv_path = WORK_DIR / "mapping" / filename
    if not csv_path.parent.exists():
        csv_path.parent.mkdir(parents=True, exist_ok=True)

    data = await request.json()
    headers = data.get('headers', [])
    rows = data.get('rows', [])

    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    # Also update job storage if job_id provided
    job_id = data.get('job_id')
    if job_id:
        job_csv_path = JOBS_DIR / job_id / "mapping" / filename
        if job_csv_path.parent.exists():
            with open(job_csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)

    return {"success": True, "filename": filename}


@app.post("/api/jobs/{job_id}/csv/{filename}/save")
async def save_job_csv_data(job_id: str, filename: str, request: Request):
    """Save CSV file from JSON data for a specific job"""
    import csv
    csv_path = JOBS_DIR / job_id / "mapping" / filename
    if not csv_path.parent.exists():
        raise HTTPException(status_code=404, detail="Job not found")

    data = await request.json()
    headers = data.get('headers', [])
    rows = data.get('rows', [])

    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    return {"success": True, "filename": filename, "job_id": job_id}


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
