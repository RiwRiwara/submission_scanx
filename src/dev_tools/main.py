"""
Scanx Dev Tools - FastAPI web application for development
"""
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Paths for submission_scanx
BASE_DIR = Path(__file__).parent  # dev_tools
SRC_DIR = BASE_DIR.parent  # src

# Training mode paths
TRAIN_RESULT_DIR = SRC_DIR / "result" / "from_train"
TRAIN_PROCESSING = TRAIN_RESULT_DIR / "processing_input"
TRAIN_OUTPUT = TRAIN_RESULT_DIR / "mapping_output"
TRAIN_INPUT_DIR = SRC_DIR / "training" / "train input"

# Final test mode paths
FINAL_RESULT_DIR = SRC_DIR / "result" / "final"
FINAL_PROCESSING = FINAL_RESULT_DIR / "processing_input"
FINAL_OUTPUT = FINAL_RESULT_DIR / "mapping_output"
FINAL_INPUT_DIR = SRC_DIR / "test final" / "test final input"

# Raw data directories (for viewer)
JSON_RAW_DIR = TRAIN_PROCESSING / "extract_raw"
JSON_MATCHED_DIR = TRAIN_PROCESSING / "extract_matched"
PDF_TRAINING_DIR = TRAIN_INPUT_DIR / "Train_pdf" / "pdf"  # PDFs are in Train_pdf/pdf/
PAGE_METADATA_DIR = TRAIN_PROCESSING / "page_metadata"

# Expected output for comparison (training ground truth)
TRAIN_EXPECTED_OUTPUT = SRC_DIR / "training" / "train output"
TRAIN_EXPECTED_SUMMARY = SRC_DIR / "training" / "train summary"  # Train_summary.csv

app = FastAPI(title="Scanx Dev Tools", version="0.1.0")

# Mount static files
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Templates
TEMPLATES_DIR = BASE_DIR / "templates"
TEMPLATES_DIR.mkdir(exist_ok=True)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


# ============================================================================
# API Routes - Data
# ============================================================================

def get_mode_paths(mode: str):
    """Get paths based on mode (training or final)"""
    if mode == "final":
        return {
            "json_raw": FINAL_PROCESSING / "extract_raw",
            "json_matched": FINAL_PROCESSING / "extract_matched",
            "pdf_dir": FINAL_INPUT_DIR / "Test final_pdf",  # Final test PDFs
            "text_each_page": FINAL_PROCESSING / "text_each_page",
            "page_metadata": FINAL_PROCESSING / "page_metadata",
            "output": FINAL_OUTPUT,
            "expected": None,  # No expected output for final test
            "expected_summary": None
        }
    else:
        return {
            "json_raw": TRAIN_PROCESSING / "extract_raw",
            "json_matched": TRAIN_PROCESSING / "extract_matched", 
            "pdf_dir": PDF_TRAINING_DIR,  # src/training/train input/Train_pdf/pdf
            "text_each_page": TRAIN_PROCESSING / "text_each_page",
            "page_metadata": TRAIN_PROCESSING / "page_metadata",
            "output": TRAIN_OUTPUT,
            "expected": TRAIN_EXPECTED_OUTPUT,  # train output/
            "expected_summary": TRAIN_EXPECTED_SUMMARY  # train summary/Train_summary.csv
        }


@app.get("/api/documents")
async def list_documents(mode: str = "training"):
    """List all documents available"""
    paths = get_mode_paths(mode)
    json_dir = paths["json_matched"]  # Use matched JSON for viewing
    pdf_dir = paths["pdf_dir"]
    
    documents = []
    if json_dir.exists():
        for json_file in sorted(json_dir.glob("*.json")):
            name = json_file.stem
            pdf_file = pdf_dir / f"{name}.pdf"
            documents.append({
                "name": name,
                "json_path": str(json_file),
                "pdf_path": str(pdf_file) if pdf_file.exists() else None,
                "has_pdf": pdf_file.exists()
            })
    
    return {"documents": documents, "count": len(documents), "mode": mode}


@app.get("/api/document/{doc_name}")
async def get_document(doc_name: str, mode: str = "training"):
    """Get document JSON data (matched JSON with page info)"""
    paths = get_mode_paths(mode)
    json_path = paths["json_matched"] / f"{doc_name}.json"
    
    if not json_path.exists():
        # Fallback to raw JSON
        json_path = paths["json_raw"] / f"{doc_name}.json"
    
    if not json_path.exists():
        raise HTTPException(status_code=404, detail="Document not found")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return data


@app.get("/api/document/{doc_name}/page/{page_num}")
async def get_page_content(doc_name: str, page_num: int, mode: str = "training"):
    """Get specific page content from text_each_page or extract from JSON"""
    paths = get_mode_paths(mode)
    text_dir = paths["text_each_page"]
    
    # Try individual page file first
    page_file = text_dir / doc_name / f"page_{page_num:03d}.json"
    if page_file.exists():
        with open(page_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    
    # Fallback: extract from matched JSON
    json_path = paths["json_matched"] / f"{doc_name}.json"
    if not json_path.exists():
        json_path = paths["json_raw"] / f"{doc_name}.json"
    
    if not json_path.exists():
        raise HTTPException(status_code=404, detail="Document not found")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        doc_data = json.load(f)
    
    pages = doc_data.get('pages', [])
    if page_num < 1 or page_num > len(pages):
        raise HTTPException(status_code=404, detail=f"Page {page_num} not found")
    
    page = pages[page_num - 1]
    lines = page.get('lines', [])
    
    # Build content and lines data
    content_lines = []
    lines_data = []
    for line in lines:
        text = line.get('content', '')
        polygon = line.get('polygon', [])
        content_lines.append(text)
        lines_data.append({
            'text': text,
            'polygon': polygon,
            'x': polygon[0] if len(polygon) >= 2 else 0,
            'y': polygon[1] if len(polygon) >= 2 else 0
        })
    
    return {
        'page_number': page_num,
        'content': '\n'.join(content_lines),
        'lines': lines_data,
        '_page_info': page.get('_page_info', {})
    }


@app.get("/api/document/{doc_name}/combined")
async def get_combined_document(doc_name: str, mode: str = "training"):
    """Get combined document (all pages)"""
    paths = get_mode_paths(mode)
    text_dir = paths["text_each_page"]
    
    # Try combined file first
    combined_file = text_dir / f"{doc_name}.json"
    if combined_file.exists():
        with open(combined_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    
    # Fallback: build from matched JSON
    json_path = paths["json_matched"] / f"{doc_name}.json"
    if not json_path.exists():
        json_path = paths["json_raw"] / f"{doc_name}.json"
    
    if not json_path.exists():
        raise HTTPException(status_code=404, detail="Document not found")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        doc_data = json.load(f)
    
    pages = doc_data.get('pages', [])
    combined_pages = []
    
    for i, page in enumerate(pages):
        lines = page.get('lines', [])
        content = '\n'.join([line.get('content', '') for line in lines])
        combined_pages.append({
            'page_number': page.get('page_number', i + 1),
            'content': content,
            'lines_count': len(lines),
            '_page_info': page.get('_page_info', {})
        })
    
    return {
        'doc_name': doc_name,
        'total_pages': len(pages),
        'pages': combined_pages
    }


@app.get("/api/pdf/{doc_name}")
async def get_pdf(doc_name: str, mode: str = "training"):
    """Serve PDF file"""
    paths = get_mode_paths(mode)
    pdf_path = paths["pdf_dir"] / f"{doc_name}.pdf"
    
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="PDF not found")
    
    return FileResponse(pdf_path, media_type="application/pdf")


# ============================================================================
# API Routes - Pipeline Results
# ============================================================================

@app.get("/api/results/summary")
async def get_summary(mode: str = "training"):
    """Get pipeline output summary"""
    paths = get_mode_paths(mode)
    summary_path = paths["output"] / "summary.csv"
    # Expected summary is in train summary/Train_summary.csv
    expected_path = paths["expected_summary"] / "Train_summary.csv" if paths["expected_summary"] else None
    
    result = {"summary": None, "expected_summary": None}
    
    if summary_path.exists():
        import pandas as pd
        df = pd.read_csv(summary_path)
        result["summary"] = df.to_dict(orient="records")
    
    if expected_path and expected_path.exists():
        import pandas as pd
        df = pd.read_csv(expected_path)
        result["expected_summary"] = df.to_dict(orient="records")
    
    return result


@app.get("/api/results/accuracy")
async def get_accuracy():
    """Get accuracy metrics - compare generated vs expected"""
    # Use scanx CLI instead of old compare_results.py
    try:
        result = subprocess.run(
            ["poetry", "run", "scanx", "--help"],
            capture_output=True,
            text=True,
            cwd=str(SRC_DIR.parent),  # submission_scanx root
            timeout=30
        )
        return {
            "message": "Use 'poetry run scanx --phase 1 --all' to run pipeline",
            "scanx_help": result.stdout,
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/results/csv/{filename}")
async def get_csv_file(filename: str, mode: str = "training", source: str = "generated"):
    """Get any CSV file from mapping_output or expected output
    
    Args:
        filename: CSV filename (e.g., summary.csv, asset.csv)
        mode: training or final
        source: 'generated' for pipeline output, 'expected' for ground truth
    """
    paths = get_mode_paths(mode)
    
    if source == "expected":
        # Expected files have Train_ prefix
        expected_filename = f"Train_{filename}"
        if filename == "summary.csv":
            csv_path = paths["expected_summary"] / "Train_summary.csv" if paths["expected_summary"] else None
        else:
            csv_path = paths["expected"] / expected_filename if paths["expected"] else None
    else:
        csv_path = paths["output"] / filename
    
    if not csv_path or not csv_path.exists():
        raise HTTPException(status_code=404, detail=f"CSV not found: {filename}")
    
    import pandas as pd
    import math
    df = pd.read_csv(csv_path)
    # Replace NaN with None for JSON compatibility
    df = df.where(pd.notnull(df), None)
    # Convert to records and clean any remaining NaN/inf
    records = df.to_dict(orient="records")
    for record in records:
        for key, value in record.items():
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                record[key] = None
    return {"data": records, "columns": list(df.columns)}


@app.post("/api/pipeline/run")
async def run_pipeline(phase: str = "1", is_final: bool = False):
    """Run scanx pipeline"""
    try:
        cmd = ["poetry", "run", "scanx", "--phase", phase]
        if is_final:
            cmd.append("--final")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(SRC_DIR.parent),  # submission_scanx root
            timeout=600
        )
        return {
            "output": result.stdout,
            "error": result.stderr,
            "returncode": result.returncode
        }
    except subprocess.TimeoutExpired:
        return {"error": "Pipeline timeout (10 min)"}
    except Exception as e:
        return {"error": str(e)}


# ============================================================================
# API Routes - CSV Comparison
# ============================================================================

from pydantic import BaseModel
from typing import List

class CompareRequest(BaseModel):
    generated: str
    expected: str
    key_fields: List[str]
    tolerance: float = 0.05
    primary_key: Optional[str] = None  # Primary key for grouping (e.g., submitter_id)


def normalize_value(val) -> str:
    """Normalize value for comparison"""
    if val is None:
        return 'NONE'
    val = str(val).strip()
    if val == '' or val.lower() == 'none' or val.lower() == 'null' or val.lower() == 'nan':
        return 'NONE'
    return val


def parse_number(val) -> Optional[float]:
    """Parse number from string"""
    try:
        return float(str(val).replace(',', ''))
    except (ValueError, AttributeError, TypeError):
        return None


def compare_values(generated, expected, tolerance: float = 0.05):
    """Compare two values and return match type"""
    gen_norm = normalize_value(generated)
    exp_norm = normalize_value(expected)
    
    if gen_norm == 'NONE' and exp_norm == 'NONE':
        return True, 'both_empty'
    if gen_norm == exp_norm:
        return True, 'exact'
    
    gen_num = parse_number(generated)
    exp_num = parse_number(expected)
    
    if gen_num is not None and exp_num is not None:
        if exp_num == 0:
            return (gen_num == 0), 'exact' if gen_num == 0 else 'mismatch'
        relative_error = abs(gen_num - exp_num) / abs(exp_num)
        if relative_error <= tolerance:
            return True, 'numeric_close'
    
    return False, 'mismatch'


@app.post("/api/compare/csv")
async def compare_csv_files(req: CompareRequest, mode: str = "training"):
    """Compare two CSV files and return detailed results"""
    import pandas as pd
    import math
    
    # Resolve paths
    paths = get_mode_paths(mode)
    gen_path = paths["output"] / req.generated
    
    # Expected files have Train_ prefix
    if req.expected == "summary.csv":
        exp_path = paths["expected_summary"] / "Train_summary.csv" if paths["expected_summary"] else None
    else:
        expected_filename = f"Train_{req.expected}"
        exp_path = paths["expected"] / expected_filename if paths["expected"] else None
    
    if not exp_path:
        return {"error": "No expected output available for this mode"}
    
    if not gen_path.exists():
        return {"error": f"Generated file not found: {req.generated}"}
    if not exp_path.exists():
        return {"error": f"Expected file not found: {req.expected}"}
    
    try:
        gen_df = pd.read_csv(gen_path)
        exp_df = pd.read_csv(exp_path)
    except Exception as e:
        return {"error": f"Error reading CSV: {e}"}
    
    # Get all fields
    all_fields = list(exp_df.columns)
    compare_fields = [f for f in all_fields if f not in req.key_fields]
    
    # Determine matching strategy
    primary_key = req.primary_key
    use_primary_matching = primary_key and primary_key in gen_df.columns and primary_key in exp_df.columns
    
    # Build keyed dictionaries with primary key grouping
    def build_keyed_by_primary(df, primary_key, secondary_keys):
        """Group by primary key, then by secondary keys within each group"""
        result = {}
        for _, row in df.iterrows():
            pk_val = str(row.get(primary_key, '')).strip()
            sk_val = '|'.join(str(row.get(k, '')).strip() for k in secondary_keys if k != primary_key)
            full_key = f"{pk_val}|{sk_val}"
            result[full_key] = {'row': row.to_dict(), 'primary': pk_val, 'secondary': sk_val}
        return result
    
    def build_keyed(df, key_fields):
        result = {}
        for _, row in df.iterrows():
            key = '|'.join(str(row.get(k, '')).strip() for k in key_fields)
            result[key] = row.to_dict()
        return result
    
    if use_primary_matching:
        # Group by primary key for smarter matching
        secondary_keys = [k for k in req.key_fields if k != primary_key]
        gen_data = build_keyed_by_primary(gen_df, primary_key, secondary_keys)
        exp_data = build_keyed_by_primary(exp_df, primary_key, secondary_keys)
        
        # Find common primary keys
        gen_primary = {v['primary'] for v in gen_data.values()}
        exp_primary = {v['primary'] for v in exp_data.values()}
        common_primary = gen_primary & exp_primary
        
        # Match within primary key groups
        common_keys = set()
        for pk in common_primary:
            gen_in_group = {k: v for k, v in gen_data.items() if v['primary'] == pk}
            exp_in_group = {k: v for k, v in exp_data.items() if v['primary'] == pk}
            
            # Exact secondary key matches
            for full_key in gen_in_group:
                if full_key in exp_in_group:
                    common_keys.add(full_key)
        
        # Convert to simple dict format for comparison
        gen_data_simple = {k: v['row'] for k, v in gen_data.items()}
        exp_data_simple = {k: v['row'] for k, v in exp_data.items()}
        gen_keys = set(gen_data_simple.keys())
        exp_keys = set(exp_data_simple.keys())
        gen_data = gen_data_simple
        exp_data = exp_data_simple
    else:
        gen_data = build_keyed(gen_df, req.key_fields)
        exp_data = build_keyed(exp_df, req.key_fields)
        gen_keys = set(gen_data.keys())
        exp_keys = set(exp_data.keys())
        common_keys = gen_keys & exp_keys
    
    # Field accuracy tracking
    field_accuracy = {f: {'total': 0, 'exact': 0, 'close': 0, 'empty': 0, 'mismatch': 0} for f in compare_fields}
    
    # Compare rows
    rows = []
    total_exact = 0
    total_close = 0
    total_comparisons = 0
    
    for key in common_keys:
        gen_row = gen_data[key]
        exp_row = exp_data[key]
        
        # Build key values dict
        key_values = {k: gen_row.get(k, '') for k in req.key_fields}
        
        comparisons = {}
        has_mismatch = False
        
        for field in compare_fields:
            gen_val = gen_row.get(field)
            exp_val = exp_row.get(field)
            
            # Clean NaN
            if isinstance(gen_val, float) and math.isnan(gen_val):
                gen_val = None
            if isinstance(exp_val, float) and math.isnan(exp_val):
                exp_val = None
            
            is_match, match_type = compare_values(gen_val, exp_val, req.tolerance)
            
            comparisons[field] = {
                'generated': gen_val,
                'expected': exp_val,
                'match_type': match_type
            }
            
            # Track field accuracy
            field_accuracy[field]['total'] += 1
            if match_type == 'exact':
                field_accuracy[field]['exact'] += 1
                total_exact += 1
            elif match_type == 'numeric_close':
                field_accuracy[field]['close'] += 1
                total_close += 1
            elif match_type == 'both_empty':
                field_accuracy[field]['empty'] += 1
            else:
                field_accuracy[field]['mismatch'] += 1
                has_mismatch = True
            
            total_comparisons += 1
        
        rows.append({
            'key': key,
            'key_values': key_values,
            'comparisons': comparisons,
            'has_mismatch': has_mismatch
        })
    
    # Calculate overall accuracy
    accuracy = (total_exact + total_close) / total_comparisons * 100 if total_comparisons > 0 else 0
    
    return {
        'stats': {
            'accuracy': accuracy,
            'exact_matches': total_exact,
            'close_matches': total_close,
            'total_comparisons': total_comparisons,
            'common_rows': len(common_keys),
            'total_generated': len(gen_keys),
            'total_expected': len(exp_keys),
            'only_generated': len(gen_keys - exp_keys),
            'only_expected': len(exp_keys - gen_keys)
        },
        'field_accuracy': field_accuracy,
        'fields': all_fields,
        'key_fields': req.key_fields,
        'rows': rows
    }


# ============================================================================
# Page Routes
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Home page"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/viewer", response_class=HTMLResponse)
async def viewer(request: Request):
    """PDF/JSON Viewer page"""
    return templates.TemplateResponse("viewer.html", {"request": request})


@app.get("/content", response_class=HTMLResponse)
async def content_browser(request: Request):
    """Content browser page"""
    return templates.TemplateResponse("content.html", {"request": request})


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Dashboard page"""
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/search", response_class=HTMLResponse)
async def search_page(request: Request):
    """Search by submitter_id page"""
    return templates.TemplateResponse("search.html", {"request": request})


@app.get("/pages", response_class=HTMLResponse)
async def pages_viewer(request: Request):
    """Page metadata viewer - view pages by step"""
    return templates.TemplateResponse("pages.html", {"request": request})


# ============================================================================
# API Routes - Page Metadata
# ============================================================================

@app.get("/api/page-metadata")
async def get_page_metadata_index(mode: str = "training"):
    """Get page metadata index - all documents with their step mappings"""
    paths = get_mode_paths(mode)
    index_path = paths["page_metadata"] / "index.json"
    if not index_path.exists():
        return {"error": "Page metadata index not found", "documents": []}
    
    with open(index_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return data


@app.get("/api/page-metadata/{doc_name}")
async def get_document_page_metadata(doc_name: str, mode: str = "training"):
    """Get detailed page metadata for a specific document"""
    paths = get_mode_paths(mode)
    metadata_path = paths["page_metadata"] / f"{doc_name}_metadata.json"
    if not metadata_path.exists():
        raise HTTPException(status_code=404, detail="Document metadata not found")
    
    with open(metadata_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return data


# Step descriptions for display
STEP_DESCRIPTIONS = {
    "step_1": "ตำแหน่งผู้ยื่น (Submitter Position)",
    "step_2": "ชื่อเดิมผู้ยื่น (Submitter Old Name)",
    "step_3_1": "ข้อมูลคู่สมรส (Spouse Info)",
    "step_3_2": "ชื่อเดิมคู่สมรส (Spouse Old Name)",
    "step_3_3": "ตำแหน่งคู่สมรส (Spouse Position)",
    "step_4": "ข้อมูลบุตร/ญาติ (Relatives)",
    "step_5": "สรุปทรัพย์สิน (Statement Summary)",
    "step_6": "ทรัพย์สินทั่วไป (Assets)",
    "step_7": "ที่ดิน (Land)",
    "step_8": "สิ่งปลูกสร้าง (Buildings)",
    "step_9": "ยานพาหนะ (Vehicles)",
    "step_10": "ทรัพย์สินอื่น (Other Assets)",
}


@app.get("/api/steps")
async def get_steps():
    """Get all step definitions"""
    return {"steps": STEP_DESCRIPTIONS}


# ============================================================================
# API Routes - Page Text with Polygon Data (alternate endpoint)
# ============================================================================

@app.get("/api/document/{doc_name}/page-detail/{page_num}")
async def get_page_text_data(doc_name: str, page_num: int, mode: str = "training"):
    """Get text content and polygon data for a specific page (detailed)"""
    paths = get_mode_paths(mode)
    text_dir = paths["text_each_page"]
    json_dir = paths["json_matched"]
    
    # Load combined text file for document
    combined_path = text_dir / f"{doc_name}.json"
    if not combined_path.exists():
        raise HTTPException(status_code=404, detail=f"Text data not found for {doc_name}")
    
    try:
        with open(combined_path, 'r', encoding='utf-8') as f:
            combined_data = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading text data: {e}")
    
    # Find page in combined data
    pages = combined_data.get('pages', [])
    page_data = None
    for p in pages:
        if p.get('page_number') == page_num:
            page_data = p
            break
    
    if not page_data:
        raise HTTPException(status_code=404, detail=f"Page {page_num} not found")
    
    # Try to load polygon data from individual page file or json_extract
    lines = []
    page_width = None
    page_height = None
    page_unit = 'inch'
    
    # First try: individual page file in text_each_page/<doc_name>/page_XXX.json
    page_file = text_dir / doc_name / f"page_{page_num:03d}.json"
    if page_file.exists():
        try:
            with open(page_file, 'r', encoding='utf-8') as f:
                page_json = json.load(f)
            lines = page_json.get('lines', [])
            page_width = page_json.get('width')
            page_height = page_json.get('height')
            page_unit = page_json.get('unit', 'inch')
        except:
            pass
    
    # If no lines from page file, try json_extract
    if not lines:
        json_path = json_dir / f"{doc_name}.json"
        if json_path.exists():
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                
                # json_extract format: direct pages[] or analyzeResult.pages[]
                json_pages = json_data.get('pages', [])
                if not json_pages:
                    analyze_result = json_data.get('analyzeResult', {})
                    json_pages = analyze_result.get('pages', [])
                
                for jp in json_pages:
                    # Support both page_number and pageNumber formats
                    pn = jp.get('page_number', jp.get('pageNumber'))
                    if pn == page_num:
                        lines = jp.get('lines', [])
                        page_width = jp.get('width')
                        page_height = jp.get('height')
                        page_unit = jp.get('unit', 'inch')
                        # Transform to our format if needed
                        transformed_lines = []
                        for line in lines:
                            transformed_lines.append({
                                'text': line.get('content', line.get('text', '')),
                                'polygon': line.get('polygon', []),
                                'spans': line.get('spans', [])
                            })
                        lines = transformed_lines
                        break
            except Exception as e:
                print(f"Error loading json_extract: {e}")
    
    return {
        "doc_name": doc_name,
        "page_number": page_num,
        "content": page_data.get('content', ''),
        "lines_count": page_data.get('lines_count', len(lines)),
        "lines": lines,
        "width": page_width,
        "height": page_height,
        "unit": page_unit
    }


@app.get("/api/document/{doc_name}/text")
async def get_document_text(doc_name: str, mode: str = "training"):
    """Get all text content for a document"""
    text_dir, _ = get_mode_dirs(mode)
    
    combined_path = text_dir / f"{doc_name}.json"
    if not combined_path.exists():
        raise HTTPException(status_code=404, detail=f"Text data not found for {doc_name}")
    
    try:
        with open(combined_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading text data: {e}")


# ============================================================================
# API Routes - Search by Submitter ID
# ============================================================================

@app.get("/api/search/submitter/{submitter_id}")
async def search_by_submitter(submitter_id: str, mode: str = "training"):
    """Search all data for a submitter_id across all CSV files"""
    import pandas as pd
    import math
    
    paths = get_mode_paths(mode)
    output_dir = paths["output"]
    
    results = {}
    csv_files = [
        "summary.csv",
        "statement.csv",
        "statement_detail.csv",
        "relative_info.csv",
        "spouse_info.csv",
        "spouse_old_name.csv",
        "spouse_position.csv",
        "submitter_old_name.csv",
        "submitter_position.csv",
        "asset.csv",
        "asset_land_info.csv",
        "asset_building_info.csv",
        "asset_vehicle_info.csv",
        "asset_other_asset_info.csv",
    ]
    
    for filename in csv_files:
        csv_path = output_dir / filename
        if not csv_path.exists():
            continue
        
        try:
            df = pd.read_csv(csv_path)
            # Find submitter_id column (could be submitter_id or id)
            id_col = None
            if 'submitter_id' in df.columns:
                id_col = 'submitter_id'
            elif 'id' in df.columns and filename == 'summary.csv':
                # For summary, use nacc_id or submitter_id
                if 'submitter_id' in df.columns:
                    id_col = 'submitter_id'
            
            if id_col:
                # Filter by submitter_id
                filtered = df[df[id_col].astype(str) == str(submitter_id)]
                if len(filtered) > 0:
                    filtered = filtered.where(pd.notnull(filtered), None)
                    records = filtered.to_dict(orient="records")
                    # Clean NaN/inf values
                    for record in records:
                        for key, value in record.items():
                            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                                record[key] = None
                    results[filename.replace('.csv', '')] = {
                        "count": len(records),
                        "data": records,
                        "columns": list(filtered.columns)
                    }
        except Exception as e:
            results[filename.replace('.csv', '')] = {"error": str(e)}
    
    return {"submitter_id": submitter_id, "results": results}


@app.get("/api/search/submitters")
async def list_submitters(mode: str = "training"):
    """List all submitter_ids from summary.csv"""
    import pandas as pd
    
    paths = get_mode_paths(mode)
    summary_path = paths["output"] / "summary.csv"
    if not summary_path.exists():
        return {"submitters": []}
    
    try:
        df = pd.read_csv(summary_path)
        submitters = []
        for _, row in df.iterrows():
            submitters.append({
                "submitter_id": str(row.get('submitter_id', '')),
                "nacc_id": str(row.get('id', row.get('nacc_id', ''))),
                "title": row.get('submitter_title', ''),
                "first_name": row.get('submitter_first_name', ''),
                "last_name": row.get('submitter_last_name', ''),
                "position": row.get('nd_position', '')
            })
        return {"submitters": submitters, "count": len(submitters)}
    except Exception as e:
        return {"error": str(e), "submitters": []}


# ============================================================================
# API Routes - Human-in-the-Loop (Page Ignore)
# ============================================================================

# Paths for human_loop config
TRAIN_HUMAN_LOOP = SRC_DIR / "training" / "human_loop"
FINAL_HUMAN_LOOP = SRC_DIR / "test final" / "human_loop"


def get_human_loop_paths(mode: str):
    """Get human_loop paths based on mode"""
    if mode == "final":
        return {
            "config": FINAL_HUMAN_LOOP / "pre_pdf.json",
            "pdf_dir": FINAL_INPUT_DIR / "Test final_pdf",
            "doc_info": FINAL_INPUT_DIR / "Test final_doc_info.csv"
        }
    else:
        return {
            "config": TRAIN_HUMAN_LOOP / "pre_pdf.json",
            "pdf_dir": TRAIN_INPUT_DIR / "Train_pdf" / "pdf",
            "doc_info": TRAIN_INPUT_DIR / "Train_doc_info.csv"
        }


@app.get("/human-loop", response_class=HTMLResponse)
async def human_loop_page(request: Request):
    """Human-in-the-loop page ignore configuration"""
    return templates.TemplateResponse("human_loop.html", {"request": request})


@app.get("/api/human-loop/config")
async def get_human_loop_config(mode: str = "training"):
    """Get human loop configuration"""
    paths = get_human_loop_paths(mode)
    config_path = paths["config"]
    
    if not config_path.exists():
        return {"documents": [], "error": "Config file not found"}
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        return {"documents": [], "error": str(e)}


@app.get("/api/human-loop/pdfs")
async def list_pdfs_for_human_loop(mode: str = "training"):
    """List all PDFs with their page counts and ignore status"""
    paths = get_human_loop_paths(mode)
    config_path = paths["config"]
    pdf_dir = paths["pdf_dir"]
    
    # Load existing config
    config = {}
    if config_path.exists():
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                config = {doc.get("pdf_name", ""): doc for doc in data.get("documents", [])}
        except:
            pass
    
    # List PDFs
    pdfs = []
    if pdf_dir.exists():
        for pdf_file in sorted(pdf_dir.glob("*.pdf")):
            name = pdf_file.stem
            doc_config = config.get(name, {})
            pdfs.append({
                "name": name,
                "total_pages": doc_config.get("total_pages"),
                "ignore_pages": doc_config.get("ignore_pages", []),
                "notes": doc_config.get("notes", ""),
                "doc_id": doc_config.get("doc_id", ""),
                "has_config": name in config
            })
    
    return {"pdfs": pdfs, "count": len(pdfs), "mode": mode}


@app.get("/api/human-loop/pdf/{pdf_name}/pages")
async def get_pdf_page_count(pdf_name: str, mode: str = "training"):
    """Get page count for a specific PDF"""
    paths = get_human_loop_paths(mode)
    pdf_path = paths["pdf_dir"] / f"{pdf_name}.pdf"
    
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="PDF not found")
    
    try:
        from PyPDF2 import PdfReader
        reader = PdfReader(str(pdf_path))
        total_pages = len(reader.pages)
        return {"pdf_name": pdf_name, "total_pages": total_pages}
    except ImportError:
        return {"pdf_name": pdf_name, "total_pages": None, "error": "PyPDF2 not installed"}
    except Exception as e:
        return {"pdf_name": pdf_name, "total_pages": None, "error": str(e)}


class UpdateIgnorePagesRequest(BaseModel):
    pdf_name: str
    ignore_pages: List[int]
    total_pages: Optional[int] = None
    notes: Optional[str] = None


@app.post("/api/human-loop/update")
async def update_ignore_pages(req: UpdateIgnorePagesRequest, mode: str = "training"):
    """Update ignore pages for a specific PDF"""
    paths = get_human_loop_paths(mode)
    config_path = paths["config"]
    
    # Ensure directory exists
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Load existing config
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        data = {
            "_description": "Human-in-the-loop configuration for Phase 0 OCR.",
            "_usage": "Set USE_HUNMAN_IN_LOOP=TRUE in .env to enable.",
            "documents": []
        }
    
    # Find or create document entry
    documents = data.get("documents", [])
    doc_found = False
    for doc in documents:
        if doc.get("pdf_name") == req.pdf_name:
            doc["ignore_pages"] = sorted(req.ignore_pages)
            if req.total_pages:
                doc["total_pages"] = req.total_pages
            if req.notes is not None:
                doc["notes"] = req.notes
            doc_found = True
            break
    
    if not doc_found:
        documents.append({
            "pdf_name": req.pdf_name,
            "total_pages": req.total_pages,
            "ignore_pages": sorted(req.ignore_pages),
            "notes": req.notes or ""
        })
    
    data["documents"] = documents
    
    # Save config
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return {"success": True, "pdf_name": req.pdf_name, "ignore_pages": sorted(req.ignore_pages)}


@app.post("/api/human-loop/batch-update")
async def batch_update_ignore_pages(updates: List[UpdateIgnorePagesRequest], mode: str = "training"):
    """Batch update ignore pages for multiple PDFs"""
    paths = get_human_loop_paths(mode)
    config_path = paths["config"]
    
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        data = {
            "_description": "Human-in-the-loop configuration for Phase 0 OCR.",
            "_usage": "Set USE_HUNMAN_IN_LOOP=TRUE in .env to enable.",
            "documents": []
        }
    
    # Convert to dict for easier updates
    doc_dict = {doc.get("pdf_name", ""): doc for doc in data.get("documents", [])}
    
    for req in updates:
        if req.pdf_name in doc_dict:
            doc_dict[req.pdf_name]["ignore_pages"] = sorted(req.ignore_pages)
            if req.total_pages:
                doc_dict[req.pdf_name]["total_pages"] = req.total_pages
            if req.notes is not None:
                doc_dict[req.pdf_name]["notes"] = req.notes
        else:
            doc_dict[req.pdf_name] = {
                "pdf_name": req.pdf_name,
                "total_pages": req.total_pages,
                "ignore_pages": sorted(req.ignore_pages),
                "notes": req.notes or ""
            }
    
    data["documents"] = list(doc_dict.values())
    
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return {"success": True, "updated_count": len(updates)}


def run():
    """Run the server"""
    import uvicorn
    print("=" * 50)
    print("Scanx Dev Tools")
    print("=" * 50)
    print("Server: http://localhost:8888")
    print("=" * 50)
    uvicorn.run("main:app", host="0.0.0.0", port=8888, reload=True)


if __name__ == "__main__":
    run()
