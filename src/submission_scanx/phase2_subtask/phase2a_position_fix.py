"""
Phase 2a: LLM-based Position Data Correction

This module uses Azure OpenAI to fix OCR errors in submitter_position.csv.
It processes each row and corrects common OCR mistakes in Thai text fields.

Usage:
    poetry run scanx --phase 2a
    poetry run scanx --phase 2a --final
"""

import os
import csv
import json
import time
import re
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
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
    
    def add_usage(self, response, phase: str, row_index: int, submitter_id: str = "", nacc_id: str = ""):
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
        
        fieldnames = ['phase', 'row_index', 'submitter_id', 'nacc_id', 
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


def fix_position_row(client, row: dict, row_index: int, token_tracker: Optional['TokenTracker'] = None) -> tuple[dict, dict]:
    """
    Use LLM to fix OCR errors in a position row.

    Returns:
        Tuple of (fixed_row, change_report)
    """
    # Fields that may contain OCR errors
    fields_to_fix = [
        "position",
        "workplace",
        "workplace_location",
        "note"
    ]

    # Build context for LLM
    row_data = {k: v for k, v in row.items() if k in fields_to_fix and v}

    if not row_data:
        return row, {}

    prompt = f"""You are an OCR error correction specialist for Thai government position documents.

Fix any OCR errors in this Thai text data. Common errors include:
- Garbled Thai characters (e.g., "สำำ" -> "สำ", "ผู็" -> "ผู้")
- Mixed English/Thai (e.g., "ON MW" in Thai context)
- Incorrect word boundaries
- Missing or extra characters
- Abbreviations that should be expanded

Input data:
{json.dumps(row_data, ensure_ascii=False, indent=2)}

Return ONLY a JSON object with the corrected fields. Only include fields that were changed.
If no changes needed, return empty object {{}}.

Example output format:
{{"position": "สมาชิกสภาผู้แทนราษฎร", "workplace": "รัฐสภา"}}
"""

    try:
        response = client.chat.completions.create(
            model=AZURE_DEPLOYMENT,
            messages=[
                {"role": "system", "content": "You are an OCR error correction assistant. Return only valid JSON."},
                {"role": "user", "content": prompt}
            ],
            max_completion_tokens=500
        )

        # Track token usage
        if token_tracker:
            token_tracker.add_usage(
                response, 
                phase="phase2a_position",
                row_index=row_index,
                submitter_id=row.get("submitter_id", ""),
                nacc_id=row.get("nacc_id", "")
            )

        result_text = response.choices[0].message.content
        if result_text is None:
            # Model returned empty response - treat as no changes needed
            return row, {}

        result_text = result_text.strip()

        # Handle empty response
        if not result_text:
            return row, {}

        # Parse JSON response
        # Handle markdown code blocks
        if result_text.startswith("```"):
            lines = result_text.split("\n")
            result_text = "\n".join(lines[1:-1])

        # Handle empty JSON or just whitespace
        if not result_text or result_text in ["{}", "{ }", ""]:
            return row, {}

        corrections = json.loads(result_text)

        # Build change report
        changes = {}
        fixed_row = row.copy()

        for field, new_value in corrections.items():
            if field in row and row[field] != new_value:
                changes[field] = {
                    "original": row[field],
                    "corrected": new_value
                }
                fixed_row[field] = new_value

        return fixed_row, changes

    except json.JSONDecodeError as e:
        print(f"    Row {row_index}: JSON parse error - {e}")
        return row, {"error": str(e)}
    except Exception as e:
        print(f"    Row {row_index}: LLM error - {e}")
        return row, {"error": str(e)}


def run_phase2a(
    input_csv: Path,
    output_csv: Path,
    report_dir: Path,
    batch_size: int = 10,
    delay_between_batches: float = 1.0,
    max_workers: int = 5
) -> dict:
    """
    Run Phase 2a: Fix submitter_position.csv using LLM with concurrent processing.

    Args:
        input_csv: Path to input submitter_position.csv
        output_csv: Path to output (can be same as input to overwrite)
        report_dir: Directory to save LLM correction reports
        batch_size: Number of rows to process before saving progress
        delay_between_batches: Delay in seconds between API calls
        max_workers: Maximum concurrent threads for LLM calls

    Returns:
        Statistics dictionary
    """
    start_time = time.time()

    print("=" * 60)
    print("Phase 2a: LLM Position Data Correction")
    print("=" * 60)
    print(f"Input: {input_csv}")
    print(f"Output: {output_csv}")
    print(f"Report: {report_dir}")

    # Check input file exists
    if not input_csv.exists():
        print(f"  ERROR: Input file not found: {input_csv}")
        return {"error": "Input file not found"}

    # Create report directory
    report_dir.mkdir(parents=True, exist_ok=True)

    # Initialize LLM client
    print("\n[Init] Connecting to Azure OpenAI...")
    client = get_llm_client()
    if client is None:
        print("  ERROR: Could not initialize LLM client")
        return {"error": "LLM client initialization failed"}
    print("  Connected successfully")

    # Read input CSV
    print("\n[Read] Loading input CSV...")
    rows = []
    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            rows.append(row)

    total_rows = len(rows)
    print(f"  Loaded {total_rows} rows")

    # Initialize token tracker
    token_tracker = TokenTracker()

    # Process rows concurrently
    print(f"\n[Process] Correcting OCR errors (workers={max_workers})...")
    results = [None] * total_rows  # Pre-allocate for ordered results
    all_changes = []
    rows_changed = 0
    rows_with_errors = 0
    completed = 0
    completed_lock = threading.Lock()

    def process_row(idx: int, row: dict):
        """Process single row in thread."""
        return idx, fix_position_row(client, row, idx + 1, token_tracker)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_row, i, row): i for i, row in enumerate(rows)}
        
        for future in as_completed(futures):
            idx, (fixed_row, changes) = future.result()
            results[idx] = (fixed_row, changes)
            
            with completed_lock:
                completed += 1
                if completed % 20 == 0 or completed == total_rows:
                    print(f"  Completed {completed}/{total_rows} rows...")

    # Collect results in order
    fixed_rows = []
    for i, (fixed_row, changes) in enumerate(results):
        fixed_rows.append(fixed_row)
        if changes:
            if "error" in changes:
                rows_with_errors += 1
            else:
                rows_changed += 1
                all_changes.append({
                    "row_index": i + 1,
                    "submitter_id": rows[i].get("submitter_id", ""),
                    "nacc_id": rows[i].get("nacc_id", ""),
                    "changes": changes
                })

    # Write output CSV
    print("\n[Write] Saving corrected CSV...")
    with open(output_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(fixed_rows)
    print(f"  Saved to: {output_csv}")

    # Write report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = report_dir / f"phase2a_position_report_{timestamp}.json"
    token_csv_file = report_dir / f"phase2a_token_usage_{timestamp}.csv"

    # Get token usage summary
    token_summary = token_tracker.get_summary()

    report = {
        "timestamp": timestamp,
        "input_file": str(input_csv),
        "output_file": str(output_csv),
        "statistics": {
            "total_rows": total_rows,
            "rows_changed": rows_changed,
            "rows_with_errors": rows_with_errors,
            "rows_unchanged": total_rows - rows_changed - rows_with_errors
        },
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
    print("Phase 2a Complete!")
    print(f"  Total rows: {total_rows}")
    print(f"  Rows corrected: {rows_changed}")
    print(f"  Rows with errors: {rows_with_errors}")
    print(f"  Total tokens: {token_summary['total_tokens']:,}")
    print(f"  Cost: ${token_summary['total_cost_usd']:.4f} USD (input: ${token_summary['input_cost_usd']:.4f}, output: ${token_summary['output_cost_usd']:.4f})")
    print(f"  Time elapsed: {elapsed:.2f} seconds")
    print("=" * 60)

    return {
        "total_rows": total_rows,
        "rows_changed": rows_changed,
        "rows_with_errors": rows_with_errors,
        "elapsed_time": elapsed,
        "report_file": str(report_file),
        "token_csv_file": str(token_csv_file),
        "token_usage": token_summary
    }


def main():
    """CLI entry point for Phase 2a."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Phase 2a: LLM-based Position Data Correction'
    )
    parser.add_argument('--final', action='store_true',
                        help='Process final test data instead of training')
    parser.add_argument('--input', '-i', type=str, default=None,
                        help='Input CSV file path')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='Output CSV file path (default: overwrite input)')
    parser.add_argument('--batch-size', type=int, default=10,
                        help='Batch size for processing')

    args = parser.parse_args()

    # Determine paths
    src_dir = Path(__file__).parent.parent.parent

    if args.final:
        base = src_dir / "result" / "final"
    else:
        base = src_dir / "result" / "from_train"

    input_csv = Path(args.input) if args.input else base / "mapping_output" / "submitter_position.csv"
    output_csv = Path(args.output) if args.output else input_csv  # Overwrite by default
    report_dir = base / "output_phase_2" / "report"

    run_phase2a(
        input_csv=input_csv,
        output_csv=output_csv,
        report_dir=report_dir,
        batch_size=args.batch_size
    )


if __name__ == '__main__':
    main()
