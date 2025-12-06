"""
Pipeline Dev Tools - FastAPI web application for development
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

# Paths
BASE_DIR = Path(__file__).parent
PIPELINE_DIR = BASE_DIR.parent
PIPELINE_INPUT = PIPELINE_DIR / "pipeline_input"
PIPELINE_OUTPUT = PIPELINE_DIR / "pipeline_output"
FINAL_TEST_DIR = PIPELINE_DIR / "final_test"

# Raw data directories (for viewer - original sources)
JSON_RAW_DIR = PIPELINE_INPUT / "json_extract_raw"
PDF_TRAINING_DIR = PIPELINE_DIR / "pdf training"
PAGE_METADATA_DIR = PIPELINE_INPUT / "page_metadata"

app = FastAPI(title="Pipeline Dev Tools", version="0.1.0")

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

@app.get("/api/documents")
async def list_documents(mode: str = "training"):
    """List all documents available"""
    if mode == "final":
        json_dir = FINAL_TEST_DIR / "final_json_match"
        pdf_dir = FINAL_TEST_DIR / "final_pdf_aligner"
    else:
        # Use raw JSON and original training PDFs for viewer
        json_dir = JSON_RAW_DIR
        pdf_dir = PDF_TRAINING_DIR
    
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
    """Get document JSON data (raw OCR JSON for viewer)"""
    if mode == "final":
        json_path = FINAL_TEST_DIR / "final_json_match" / f"{doc_name}.json"
    else:
        # Use raw JSON for viewer
        json_path = JSON_RAW_DIR / f"{doc_name}.json"
    
    if not json_path.exists():
        raise HTTPException(status_code=404, detail="Document not found")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return data


@app.get("/api/document/{doc_name}/page/{page_num}")
async def get_page_content(doc_name: str, page_num: int, mode: str = "training"):
    """Get specific page content - extract from raw JSON"""
    if mode == "final":
        text_dir = FINAL_TEST_DIR / "text_each_page"
        page_file = text_dir / doc_name / f"page_{page_num:03d}.json"
        if not page_file.exists():
            raise HTTPException(status_code=404, detail=f"Page {page_num} not found")
        with open(page_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    else:
        # Extract from raw JSON for training mode
        json_path = JSON_RAW_DIR / f"{doc_name}.json"
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
            'lines': lines_data
        }


@app.get("/api/document/{doc_name}/combined")
async def get_combined_document(doc_name: str, mode: str = "training"):
    """Get combined document (all pages) - extract from raw JSON"""
    if mode == "final":
        text_dir = FINAL_TEST_DIR / "text_each_page"
        combined_file = text_dir / f"{doc_name}.json"
        if not combined_file.exists():
            raise HTTPException(status_code=404, detail="Combined document not found")
        with open(combined_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    else:
        # Build combined from raw JSON for training mode
        json_path = JSON_RAW_DIR / f"{doc_name}.json"
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
                'page_number': i + 1,
                'content': content,
                'lines_count': len(lines)
            })
        
        return {
            'doc_name': doc_name,
            'total_pages': len(pages),
            'pages': combined_pages
        }


@app.get("/api/pdf/{doc_name}")
async def get_pdf(doc_name: str, mode: str = "training"):
    """Serve PDF file (original training PDFs for viewer)"""
    if mode == "final":
        pdf_path = FINAL_TEST_DIR / "final_pdf_aligner" / f"{doc_name}.pdf"
    else:
        # Use original training PDFs
        pdf_path = PDF_TRAINING_DIR / f"{doc_name}.pdf"
    
    if not pdf_path.exists():
        raise HTTPException(status_code=404, detail="PDF not found")
    
    return FileResponse(pdf_path, media_type="application/pdf")


# ============================================================================
# API Routes - Pipeline Results
# ============================================================================

@app.get("/api/results/summary")
async def get_summary():
    """Get pipeline output summary"""
    summary_path = PIPELINE_OUTPUT / "summary.csv"
    test_summary_path = PIPELINE_OUTPUT / "test_result" / "test_summary.csv"
    
    result = {"summary": None, "test_summary": None}
    
    if summary_path.exists():
        import pandas as pd
        df = pd.read_csv(summary_path)
        result["summary"] = df.to_dict(orient="records")
    
    if test_summary_path.exists():
        import pandas as pd
        df = pd.read_csv(test_summary_path)
        result["test_summary"] = df.to_dict(orient="records")
    
    return result


@app.get("/api/results/accuracy")
async def get_accuracy():
    """Get accuracy metrics from compare_results"""
    try:
        # Run compare_results.py and capture output
        compare_script = PIPELINE_DIR / "step" / "compare_results.py"
        if not compare_script.exists():
            return {"error": f"compare_results.py not found at {compare_script}"}
        
        result = subprocess.run(
            [sys.executable, str(compare_script)],
            capture_output=True,
            text=True,
            cwd=str(PIPELINE_DIR),
            timeout=60
        )
        return {
            "output": result.stdout,
            "error": result.stderr,
            "returncode": result.returncode
        }
    except subprocess.TimeoutExpired:
        return {"error": "Timeout running compare_results.py"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/results/csv/{filename}")
async def get_csv_file(filename: str):
    """Get any CSV file from pipeline_output"""
    csv_path = PIPELINE_OUTPUT / filename
    if not csv_path.exists():
        csv_path = PIPELINE_OUTPUT / "test_result" / filename
    
    if not csv_path.exists():
        raise HTTPException(status_code=404, detail="CSV not found")
    
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
async def run_pipeline(step: Optional[str] = None):
    """Run pipeline (or specific step)"""
    try:
        if step:
            script = PIPELINE_DIR / "step" / f"{step}.py"
            if not script.exists():
                raise HTTPException(status_code=404, detail=f"Step {step} not found")
            cmd = [sys.executable, str(script)]
        else:
            # Run main.py in step folder
            cmd = [sys.executable, str(PIPELINE_DIR / "step" / "main.py")]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(PIPELINE_DIR / "step"),
            timeout=300
        )
        return {
            "output": result.stdout,
            "error": result.stderr,
            "returncode": result.returncode
        }
    except subprocess.TimeoutExpired:
        return {"error": "Pipeline timeout (5 min)"}
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
async def compare_csv_files(req: CompareRequest):
    """Compare two CSV files and return detailed results"""
    import pandas as pd
    import math
    
    # Resolve paths
    gen_path = PIPELINE_OUTPUT / req.generated
    exp_path = PIPELINE_OUTPUT / "test_result" / req.expected
    
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
async def get_page_metadata_index():
    """Get page metadata index - all documents with their step mappings"""
    index_path = PAGE_METADATA_DIR / "index.json"
    if not index_path.exists():
        return {"error": "Page metadata index not found", "documents": []}
    
    with open(index_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return data


@app.get("/api/page-metadata/{doc_name}")
async def get_document_page_metadata(doc_name: str):
    """Get detailed page metadata for a specific document"""
    metadata_path = PAGE_METADATA_DIR / f"{doc_name}_metadata.json"
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
# API Routes - Page Text with Polygon Data
# ============================================================================

# Text each page directories
TEXT_EACH_PAGE_TRAINING = PIPELINE_INPUT / "text_each_page"
TEXT_EACH_PAGE_FINAL = FINAL_TEST_DIR / "text_each_page"

# JSON extract directories (for polygon data)
JSON_EXTRACT_TRAINING = PIPELINE_INPUT / "json_extract"
JSON_EXTRACT_FINAL = FINAL_TEST_DIR / "final_json_match"


def get_mode_dirs(mode: str):
    """Get appropriate directories based on mode"""
    if mode == "final":
        return TEXT_EACH_PAGE_FINAL, JSON_EXTRACT_FINAL
    return TEXT_EACH_PAGE_TRAINING, JSON_EXTRACT_TRAINING


@app.get("/api/document/{doc_name}/page/{page_num}")
async def get_page_text_data(doc_name: str, page_num: int, mode: str = "training"):
    """Get text content and polygon data for a specific page"""
    text_dir, json_dir = get_mode_dirs(mode)
    
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
async def search_by_submitter(submitter_id: str):
    """Search all data for a submitter_id across all CSV files"""
    import pandas as pd
    import math
    
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
        csv_path = PIPELINE_OUTPUT / filename
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
async def list_submitters():
    """List all submitter_ids from summary.csv"""
    import pandas as pd
    
    summary_path = PIPELINE_OUTPUT / "summary.csv"
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


def run():
    """Run the server"""
    import uvicorn
    print("=" * 50)
    print("Pipeline Dev Tools")
    print("=" * 50)
    print("Server: http://localhost:8888")
    print("=" * 50)
    uvicorn.run("main:app", host="0.0.0.0", port=8888, reload=True)


if __name__ == "__main__":
    run()
