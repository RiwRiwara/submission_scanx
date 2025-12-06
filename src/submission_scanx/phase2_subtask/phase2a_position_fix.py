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
from pathlib import Path
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

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


def fix_position_row(client, row: dict, row_index: int) -> tuple[dict, dict]:
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
    delay_between_batches: float = 1.0
) -> dict:
    """
    Run Phase 2a: Fix submitter_position.csv using LLM.

    Args:
        input_csv: Path to input submitter_position.csv
        output_csv: Path to output (can be same as input to overwrite)
        report_dir: Directory to save LLM correction reports
        batch_size: Number of rows to process before saving progress
        delay_between_batches: Delay in seconds between API calls

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

    # Process rows
    print("\n[Process] Correcting OCR errors...")
    fixed_rows = []
    all_changes = []
    rows_changed = 0
    rows_with_errors = 0

    for i, row in enumerate(rows):
        # Progress indicator
        if (i + 1) % 10 == 0 or i == 0:
            print(f"  Processing row {i + 1}/{total_rows}...")

        fixed_row, changes = fix_position_row(client, row, i + 1)
        fixed_rows.append(fixed_row)

        if changes:
            if "error" in changes:
                rows_with_errors += 1
            else:
                rows_changed += 1
                all_changes.append({
                    "row_index": i + 1,
                    "submitter_id": row.get("submitter_id", ""),
                    "nacc_id": row.get("nacc_id", ""),
                    "changes": changes
                })

        # Rate limiting
        if (i + 1) % batch_size == 0:
            time.sleep(delay_between_batches)

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
        "changes": all_changes
    }

    print("\n[Report] Saving correction report...")
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"  Saved to: {report_file}")

    elapsed = time.time() - start_time

    print("\n" + "=" * 60)
    print("Phase 2a Complete!")
    print(f"  Total rows: {total_rows}")
    print(f"  Rows corrected: {rows_changed}")
    print(f"  Rows with errors: {rows_with_errors}")
    print(f"  Time elapsed: {elapsed:.2f} seconds")
    print("=" * 60)

    return {
        "total_rows": total_rows,
        "rows_changed": rows_changed,
        "rows_with_errors": rows_with_errors,
        "elapsed_time": elapsed,
        "report_file": str(report_file)
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
