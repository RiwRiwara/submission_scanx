"""
Phase 2b: LLM-based Statement Data Correction

This module uses Azure OpenAI to fix OCR errors in statement.csv and statement_detail.csv.
It uses raw text from text_each_page to verify and correct numeric values.

Usage:
    poetry run scanx --phase 2b
    poetry run scanx --phase 2b --final
"""

import os
import csv
import json
import time
import re
import threading
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Any, TYPE_CHECKING
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Pricing per 1K tokens (USD)
PRICE_INPUT_PER_1K = 0.05
PRICE_OUTPUT_PER_1K = 0.4

# Load environment variables
load_dotenv(Path(__file__).parent.parent / ".env")

# Azure OpenAI configuration
AZURE_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "")
AZURE_API_KEY = os.getenv("AZURE_OPENAI_API_KEY", "")
AZURE_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-5-nano")


def get_llm_client():
    """Initialize Azure OpenAI client."""
    try:
        from openai import AzureOpenAI

        # Fix endpoint if needed (remove /openai/v1/ suffix)
        endpoint = AZURE_ENDPOINT.rstrip("/")
        if endpoint.endswith("/openai/v1"):
            endpoint = endpoint[:-10]

        client = AzureOpenAI(
            api_key=AZURE_API_KEY,
            api_version="2024-02-15-preview",
            azure_endpoint=endpoint
        )
        return client
    except Exception as e:
        print(f"  Error initializing Azure OpenAI client: {e}")
        return None


class TokenTracker:
    """Track token usage for cost calculation (thread-safe)."""
    
    def __init__(self):
        self.records = []
        self.total_prompt_tokens = 0
        self.total_completion_tokens = 0
        self.total_tokens = 0
        self._lock = threading.Lock()
    
    def add_usage(self, response, phase: str, row_index: int, csv_file: str = "", 
                  submitter_id: str = "", nacc_id: str = ""):
        """Add token usage from a response (thread-safe)."""
        usage = getattr(response, 'usage', None)
        if usage:
            prompt_tokens = getattr(usage, 'prompt_tokens', 0)
            completion_tokens = getattr(usage, 'completion_tokens', 0)
            total = prompt_tokens + completion_tokens
            
            with self._lock:
                self.total_prompt_tokens += prompt_tokens
                self.total_completion_tokens += completion_tokens
                self.total_tokens += total
                
                self.records.append({
                    'phase': phase,
                    'csv_file': csv_file,
                    'row_index': row_index,
                    'submitter_id': submitter_id,
                    'nacc_id': nacc_id,
                    'prompt_tokens': prompt_tokens,
                    'completion_tokens': completion_tokens,
                    'total_tokens': total,
                    'timestamp': datetime.now().isoformat()
                })
    
    def save_to_csv(self, output_path: Path):
        """Save token usage records to CSV."""
        if not self.records:
            return
        
        fieldnames = ['phase', 'csv_file', 'row_index', 'submitter_id', 'nacc_id', 
                      'prompt_tokens', 'completion_tokens', 'total_tokens', 'timestamp']
        
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.records)
    
    def get_summary(self) -> dict:
        """Get token usage summary with cost calculation."""
        input_cost = (self.total_prompt_tokens / 1000) * PRICE_INPUT_PER_1K
        output_cost = (self.total_completion_tokens / 1000) * PRICE_OUTPUT_PER_1K
        total_cost = input_cost + output_cost
        
        return {
            'total_calls': len(self.records),
            'total_prompt_tokens': self.total_prompt_tokens,
            'total_completion_tokens': self.total_completion_tokens,
            'total_tokens': self.total_tokens,
            'input_cost_usd': round(input_cost, 6),
            'output_cost_usd': round(output_cost, 6),
            'total_cost_usd': round(total_cost, 6)
        }


def load_text_each_page(text_dir: Path, doc_name: str) -> Dict[int, str]:
    """Load raw text for each page of a document."""
    page_texts = {}

    # Try to load from JSON file
    json_file = text_dir / f"{doc_name}.json"
    if json_file.exists():
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for page in data.get('pages', []):
                    page_num = page.get('page_number', 0)
                    content = page.get('content', '')
                    page_texts[page_num] = content
        except Exception as e:
            print(f"    Warning: Could not load {json_file}: {e}")

    return page_texts


def load_page_metadata(metadata_dir: Path, doc_name: str) -> Dict[str, List[int]]:
    """Load page metadata to find which pages contain statement data."""
    step_pages = {}

    # Load from index.json which has all documents' step mappings
    index_file = metadata_dir / "index.json"
    if index_file.exists():
        try:
            with open(index_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for doc in data.get('documents', []):
                    if doc.get('doc_name') == doc_name:
                        steps = doc.get('steps', {})
                        for step_name, page_numbers in steps.items():
                            step_pages[step_name] = page_numbers
                        break
        except Exception as e:
            print(f"    Warning: Could not load {index_file}: {e}")

    return step_pages


def extract_numbers_from_text(text: str) -> List[str]:
    """Extract all number patterns from text."""
    # Match various number formats: 1,234,567.89 or 1234567.89 or 1,234,567
    patterns = [
        r'\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?',  # With commas: 1,234,567.89
        r'\d+(?:\.\d{1,2})?',  # Without commas: 1234567.89
    ]

    numbers = []
    for pattern in patterns:
        matches = re.findall(pattern, text)
        numbers.extend(matches)

    return list(set(numbers))


def get_statement_context(
    nacc_id: str,
    doc_info_map: Dict[str, str],
    text_dir: Path,
    metadata_dir: Path
) -> str:
    """Get relevant text context for a statement record."""
    doc_name = doc_info_map.get(nacc_id, '')
    if not doc_name:
        return ""

    # Load text file directly - it has page_type info
    json_file = text_dir / f"{doc_name}.json"
    if not json_file.exists():
        return ""
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception:
        return ""
    
    pages = data.get('pages', [])
    
    # Get pages relevant to statement data (summary, first pages with financial info)
    # Include summary page and a few content pages
    context_parts = []
    page_count = 0
    
    for page in pages:
        page_num = page.get('page_number', 0)
        page_type = page.get('page_type', '')
        content = page.get('content', '')
        
        # Include summary, first few pages, or pages with financial content
        if page_type in ('summary', 'personal_info', 'tax_info') or page_num <= 3:
            if content:
                context_parts.append(f"[Page {page_num} - {page_type}]\n{content[:1500]}")
                page_count += 1
                if page_count >= 4:  # Limit context size
                    break

    return "\n\n".join(context_parts)


def fix_statement_row(
    client,
    row: dict,
    context: str,
    row_index: int,
    csv_type: str,
    token_tracker: Optional['TokenTracker'] = None
) -> tuple[dict, dict]:
    """
    Use LLM to fix OCR errors in a statement row.

    Returns:
        Tuple of (fixed_row, change_report)
    """
    # Fields with numeric values to check
    if csv_type == "statement":
        numeric_fields = [
            "valuation_submitter",
            "valuation_spouse",
            "valuation_child"
        ]
    else:  # statement_detail
        numeric_fields = [
            "valuation_submitter",
            "valuation_spouse",
            "valuation_child"
        ]

    # Extract current values
    row_data = {k: v for k, v in row.items() if k in numeric_fields and v}

    if not row_data:
        return row, {}

    # Build prompt
    prompt = f"""You are an OCR error correction specialist for Thai financial documents.

The following numeric values were extracted from a document but may contain OCR errors.
I will provide the raw text context from the source document to help verify/correct the values.

Current extracted values:
{json.dumps(row_data, ensure_ascii=False, indent=2)}

Raw text context from source document:
---
{context[:3000] if context else "No context available"}
---

Common OCR errors in numbers:
- 0 misread as O or o
- 1 misread as l or I
- 5 misread as S
- 8 misread as B
- Decimal points missing or misplaced
- Commas in wrong positions
- Extra/missing digits

Please analyze the raw text to find the correct values. Look for patterns like:
- Numbers followed by ".00" or ".-"
- Numbers in table columns
- Thai number formats with commas

Return ONLY a JSON object with corrected numeric fields. Only include fields that need correction.
If all values appear correct or you cannot determine corrections, return empty object {{}}.

Example output format:
{{"valuation_submitter": "1362720.00", "valuation_spouse": "1800000.00"}}
"""

    try:
        response = client.chat.completions.create(
            model=AZURE_DEPLOYMENT,
            messages=[
                {"role": "system", "content": "You are a numeric OCR error correction assistant. Return only valid JSON with corrected numeric values."},
                {"role": "user", "content": prompt}
            ],
            max_completion_tokens=500
        )

        # Track token usage
        if token_tracker:
            token_tracker.add_usage(
                response,
                phase="phase2b_statement",
                row_index=row_index,
                csv_file=f"{csv_type}.csv",
                submitter_id=row.get("submitter_id", ""),
                nacc_id=row.get("nacc_id", "")
            )

        result_text = response.choices[0].message.content
        if result_text is None:
            return row, {}

        result_text = result_text.strip()

        if not result_text:
            return row, {}

        # Handle markdown code blocks
        if result_text.startswith("```"):
            lines = result_text.split("\n")
            result_text = "\n".join(lines[1:-1])

        if not result_text or result_text in ["{}", "{ }", ""]:
            return row, {}

        corrections = json.loads(result_text)

        # Build change report
        changes = {}
        fixed_row = row.copy()

        for field, new_value in corrections.items():
            if field in row:
                # Normalize both values for comparison
                old_normalized = str(row[field]).replace(",", "").strip()
                new_normalized = str(new_value).replace(",", "").strip()

                if old_normalized != new_normalized:
                    changes[field] = {
                        "original": row[field],
                        "corrected": new_value
                    }
                    fixed_row[field] = new_value

        return fixed_row, changes

    except json.JSONDecodeError as e:
        return row, {"error": f"JSON parse error: {e}"}
    except Exception as e:
        return row, {"error": str(e)}


def build_doc_info_map(mapping_output_dir: Path, text_dir: Path) -> Dict[str, str]:
    """
    Build a mapping from nacc_id to doc_name.
    
    Uses summary.csv (name info) and text_each_page/index.json (doc names)
    to match records to documents for text context loading.
    """
    doc_map = {}
    
    # Load summary.csv to get submitter names per nacc_id
    summary_csv = mapping_output_dir / "summary.csv"
    nacc_names = {}  # nacc_id -> (first_name, last_name)
    
    if summary_csv.exists():
        try:
            with open(summary_csv, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    nacc_id = row.get('id', '')
                    first_name = row.get('submitter_first_name', '')
                    last_name = row.get('submitter_last_name', '')
                    if nacc_id and first_name:
                        nacc_names[nacc_id] = (first_name, last_name)
        except Exception as e:
            print(f"  Warning: Could not load summary.csv: {e}")
    
    # Load text_each_page/index.json to get doc names (matches text files)
    index_file = text_dir / "index.json"
    doc_names = []
    
    if index_file.exists():
        try:
            with open(index_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for doc in data.get('documents', []):
                    # Use 'name' field from text_each_page index
                    doc_name = doc.get('name', '') or doc.get('doc_name', '')
                    if doc_name:
                        doc_names.append(doc_name)
        except Exception as e:
            print(f"  Warning: Could not load index.json: {e}")
    
    # Match nacc_id to doc_name by name matching
    for nacc_id, (first_name, last_name) in nacc_names.items():
        for doc_name in doc_names:
            # Check if both first and last name appear in doc_name
            if first_name in doc_name and last_name in doc_name:
                doc_map[nacc_id] = doc_name
                break
    
    return doc_map


def run_phase2b(
    statement_csv: Path,
    statement_detail_csv: Path,
    text_dir: Path,
    metadata_dir: Path,
    csv_dir: Path,
    report_dir: Path,
    batch_size: int = 5,
    delay_between_batches: float = 1.0,
    max_workers: int = 5
) -> dict:
    """
    Run Phase 2b: Fix statement CSVs using LLM with page context and concurrency.

    Args:
        statement_csv: Path to statement.csv
        statement_detail_csv: Path to statement_detail.csv
        text_dir: Directory with text_each_page data
        metadata_dir: Directory with page_metadata
        csv_dir: Directory with doc_info.csv
        report_dir: Directory to save reports
        batch_size: Rows per batch
        delay_between_batches: Delay between API calls
        max_workers: Maximum concurrent threads for LLM calls

    Returns:
        Statistics dictionary
    """
    start_time = time.time()

    print("=" * 60)
    print("Phase 2b: LLM Statement Data Correction")
    print("=" * 60)
    print(f"Statement CSV: {statement_csv}")
    print(f"Statement Detail CSV: {statement_detail_csv}")
    print(f"Text Dir: {text_dir}")
    print(f"Metadata Dir: {metadata_dir}")
    print(f"Report: {report_dir}")

    # Create report directory
    report_dir.mkdir(parents=True, exist_ok=True)

    # Initialize LLM client
    print("\n[Init] Connecting to Azure OpenAI...")
    client = get_llm_client()
    if client is None:
        print("  ERROR: Could not initialize LLM client")
        return {"error": "LLM client initialization failed"}
    print("  Connected successfully")

    # Build doc_info map (mapping nacc_id to doc_name for text loading)
    print("\n[Init] Loading document info...")
    mapping_output_dir = statement_csv.parent  # e.g., mapping_output/
    doc_info_map = build_doc_info_map(mapping_output_dir, text_dir)
    print(f"  Loaded {len(doc_info_map)} document mappings")

    # Initialize token tracker
    token_tracker = TokenTracker()

    all_changes = []
    total_stats = {
        "statement": {"total": 0, "changed": 0, "errors": 0},
        "statement_detail": {"total": 0, "changed": 0, "errors": 0}
    }

    # Process statement.csv
    if statement_csv.exists():
        print(f"\n[Process] Processing statement.csv (workers={max_workers})...")
        rows = []
        with open(statement_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                rows.append(row)

        total_stats["statement"]["total"] = len(rows)
        print(f"  Loaded {len(rows)} rows")

        # Concurrent processing
        results = [None] * len(rows)
        completed = 0
        completed_lock = threading.Lock()

        def process_statement_row(idx: int, row: dict):
            nacc_id = row.get('nacc_id', '')
            context = get_statement_context(nacc_id, doc_info_map, text_dir, metadata_dir)
            return idx, fix_statement_row(client, row, context, idx + 1, "statement", token_tracker), nacc_id

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_statement_row, i, row): i for i, row in enumerate(rows)}
            
            for future in as_completed(futures):
                idx, (fixed_row, changes), nacc_id = future.result()
                results[idx] = (fixed_row, changes, nacc_id)
                
                with completed_lock:
                    completed += 1
                    if completed % 20 == 0 or completed == len(rows):
                        print(f"  Completed {completed}/{len(rows)} rows...")

        # Collect results
        fixed_rows = []
        for i, (fixed_row, changes, nacc_id) in enumerate(results):
            fixed_rows.append(fixed_row)
            if changes:
                if "error" in changes:
                    total_stats["statement"]["errors"] += 1
                else:
                    total_stats["statement"]["changed"] += 1
                    all_changes.append({
                        "csv_file": "statement.csv",
                        "row_index": i + 1,
                        "nacc_id": nacc_id,
                        "changes": changes
                    })

        # Write back
        with open(statement_csv, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(fixed_rows)
        print(f"  Saved statement.csv")

    # Process statement_detail.csv
    if statement_detail_csv.exists():
        print(f"\n[Process] Processing statement_detail.csv (workers={max_workers})...")
        rows = []
        with open(statement_detail_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                rows.append(row)

        total_stats["statement_detail"]["total"] = len(rows)
        print(f"  Loaded {len(rows)} rows")

        # Concurrent processing
        results = [None] * len(rows)
        completed = 0
        completed_lock = threading.Lock()

        def process_detail_row(idx: int, row: dict):
            nacc_id = row.get('nacc_id', '')
            context = get_statement_context(nacc_id, doc_info_map, text_dir, metadata_dir)
            return idx, fix_statement_row(client, row, context, idx + 1, "statement_detail", token_tracker), nacc_id

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_detail_row, i, row): i for i, row in enumerate(rows)}
            
            for future in as_completed(futures):
                idx, (fixed_row, changes), nacc_id = future.result()
                results[idx] = (fixed_row, changes, nacc_id)
                
                with completed_lock:
                    completed += 1
                    if completed % 50 == 0 or completed == len(rows):
                        print(f"  Completed {completed}/{len(rows)} rows...")

        # Collect results
        fixed_rows = []
        for i, (fixed_row, changes, nacc_id) in enumerate(results):
            fixed_rows.append(fixed_row)
            if changes:
                if "error" in changes:
                    total_stats["statement_detail"]["errors"] += 1
                else:
                    total_stats["statement_detail"]["changed"] += 1
                    all_changes.append({
                        "csv_file": "statement_detail.csv",
                        "row_index": i + 1,
                        "nacc_id": nacc_id,
                        "changes": changes
                    })

        # Write back
        with open(statement_detail_csv, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(fixed_rows)
        print(f"  Saved statement_detail.csv")

    # Write report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = report_dir / f"phase2b_statement_report_{timestamp}.json"
    token_csv_file = report_dir / f"phase2b_token_usage_{timestamp}.csv"

    # Get token usage summary
    token_summary = token_tracker.get_summary()

    report = {
        "timestamp": timestamp,
        "input_files": {
            "statement_csv": str(statement_csv),
            "statement_detail_csv": str(statement_detail_csv)
        },
        "statistics": total_stats,
        "token_usage": token_summary,
        "changes": all_changes
    }

    print("\n[Report] Saving correction report...")
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"  Saved to: {report_file}")

    # Save token usage CSV
    print("\n[Token Usage] Saving token usage CSV...")
    token_tracker.save_to_csv(token_csv_file)
    print(f"  Saved to: {token_csv_file}")
    print(f"  Total tokens used: {token_summary['total_tokens']:,}")
    print(f"  Estimated cost: ${token_summary['total_cost_usd']:.4f} USD")

    elapsed = time.time() - start_time

    print("\n" + "=" * 60)
    print("Phase 2b Complete!")
    print(f"  Statement: {total_stats['statement']['total']} rows, {total_stats['statement']['changed']} corrected, {total_stats['statement']['errors']} errors")
    print(f"  Statement Detail: {total_stats['statement_detail']['total']} rows, {total_stats['statement_detail']['changed']} corrected, {total_stats['statement_detail']['errors']} errors")
    print(f"  Total tokens: {token_summary['total_tokens']:,}")
    print(f"  Cost: ${token_summary['total_cost_usd']:.4f} USD (input: ${token_summary['input_cost_usd']:.4f}, output: ${token_summary['output_cost_usd']:.4f})")
    print(f"  Time elapsed: {elapsed:.2f} seconds")
    print("=" * 60)

    return {
        "statistics": total_stats,
        "elapsed_time": elapsed,
        "report_file": str(report_file),
        "token_csv_file": str(token_csv_file),
        "token_usage": token_summary
    }


def main():
    """CLI entry point for Phase 2b."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Phase 2b: LLM-based Statement Data Correction'
    )
    parser.add_argument('--final', action='store_true',
                        help='Process final test data instead of training')
    parser.add_argument('--batch-size', type=int, default=5,
                        help='Batch size for processing')

    args = parser.parse_args()

    # Determine paths
    src_dir = Path(__file__).parent.parent.parent

    if args.final:
        base = src_dir / "result" / "final"
        csv_dir = src_dir / "test final" / "test final input"
    else:
        base = src_dir / "result" / "from_train"
        csv_dir = src_dir / "training" / "train input"

    statement_csv = base / "mapping_output" / "statement.csv"
    statement_detail_csv = base / "mapping_output" / "statement_detail.csv"
    text_dir = base / "processing_input" / "text_each_page"
    metadata_dir = base / "processing_input" / "page_metadata"
    report_dir = base / "output_phase_2" / "report"

    run_phase2b(
        statement_csv=statement_csv,
        statement_detail_csv=statement_detail_csv,
        text_dir=text_dir,
        metadata_dir=metadata_dir,
        csv_dir=csv_dir,
        report_dir=report_dir,
        batch_size=args.batch_size
    )


if __name__ == '__main__':
    main()
