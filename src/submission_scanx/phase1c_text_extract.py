"""
Phase 1c: Extract text content from each page of matched JSON files.

This module creates:
1. Individual JSON files for each page (in text_each_page/<doc_name>/)
2. Combined document JSON with all pages (in text_each_page/<doc_name>.json)

The output is optimized for LLM processing - text-only without polygon coordinates.

Usage:
    python -m submission_scanx.phase1c_text_extract
    python -m submission_scanx.phase1c_text_extract --final
"""

import argparse
import json
import shutil
from pathlib import Path
from typing import Dict, List, Tuple, Any


def get_polygon_center(polygon: List[float]) -> Tuple[float, float]:
    """Get center point of polygon (x, y)."""
    if not polygon or len(polygon) < 8:
        return (0.0, 0.0)
    x_coords = [polygon[i] for i in range(0, 8, 2)]
    y_coords = [polygon[i] for i in range(1, 8, 2)]
    return (sum(x_coords) / 4, sum(y_coords) / 4)


def extract_page_content(page: Dict) -> Dict:
    """
    Extract text content from a single page.

    Returns:
        Dict with 'content' (full text) and 'lines' (structured line data)
    """
    lines = page.get('lines', [])

    # Sort lines by y position (top to bottom), then x (left to right)
    sorted_lines = []
    for line in lines:
        content = line.get('content', '').strip()
        polygon = line.get('polygon', [0] * 8)
        cx, cy = get_polygon_center(polygon)

        if content:  # Only include non-empty lines
            sorted_lines.append({
                'text': content,
                'y': round(cy, 3),
                'x': round(cx, 3)
            })

    # Sort by y, then x
    sorted_lines.sort(key=lambda l: (l['y'], l['x']))

    # Group lines by similar y-position (within 0.15 tolerance)
    grouped_lines = []
    current_group = []
    current_y = None

    for line in sorted_lines:
        if current_y is None:
            current_y = line['y']
            current_group = [line]
        elif abs(line['y'] - current_y) <= 0.15:
            current_group.append(line)
        else:
            # Sort current group by x and add to grouped lines
            current_group.sort(key=lambda l: l['x'])
            grouped_lines.append(current_group)
            current_group = [line]
            current_y = line['y']

    if current_group:
        current_group.sort(key=lambda l: l['x'])
        grouped_lines.append(current_group)

    # Build full content text
    content_parts = []
    for group in grouped_lines:
        line_text = ' '.join(l['text'] for l in group)
        content_parts.append(line_text)

    full_content = '\n'.join(content_parts)

    return {
        'content': full_content,
        'lines': sorted_lines
    }


def process_json_file(json_path: Path, output_dir: Path) -> int:
    """
    Process a single JSON file and extract page content.

    Creates:
    1. Individual page files in output_dir/<doc_name>/page_XXX.json
    2. Combined document file at output_dir/<doc_name>.json

    Args:
        json_path: Path to the JSON file
        output_dir: Directory to save output files

    Returns:
        Number of pages processed
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"  Error reading {json_path.name}: {e}")
        return 0

    pages = data.get('pages', [])
    if not pages:
        print(f"  No pages found in {json_path.name}")
        return 0

    doc_name = json_path.stem  # filename without extension

    # Create subdirectory for individual page files
    doc_output_dir = output_dir / doc_name
    doc_output_dir.mkdir(parents=True, exist_ok=True)

    total_pages = len(pages)

    # Collect data for combined document
    combined_pages = []
    all_content_parts = []

    for page_idx, page in enumerate(pages):
        page_num = page_idx + 1

        # Extract page content
        page_data = extract_page_content(page)

        # Get page_info metadata if available
        page_info = page.get('_page_info', {})
        page_type = page_info.get('page_type', 'unknown')
        is_continuation = page_info.get('is_continuation', False)
        is_extra_page = page_info.get('is_extra_page', False)
        matched_template_page = page_info.get('matched_template_page')

        # Build output structure for individual page
        output = {
            'doc_name': json_path.name,
            'page_number': page_num,
            'page_index': page_idx,
            'total_pages': total_pages,
            'page_type': page_type,
            'is_continuation': is_continuation,
            'is_extra_page': is_extra_page,
            'matched_template_page': matched_template_page,
            'content': page_data['content'],
            'lines': page_data['lines']
        }

        # Save individual page file
        output_file = doc_output_dir / f'page_{page_num:03d}.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        # Build page type label for combined content
        type_label = page_type
        if is_continuation:
            type_label += " (ต่อ)"

        # Collect for combined document (without detailed lines)
        combined_pages.append({
            'page_number': page_num,
            'page_type': page_type,
            'is_continuation': is_continuation,
            'matched_template_page': matched_template_page,
            'content': page_data['content'],
            'lines_count': len(page_data['lines']),
            'page_file': f'{doc_name}/page_{page_num:03d}.json'
        })

        # Add to full content with page separator and type
        all_content_parts.append(f"=== หน้า {page_num} [{type_label}] ===\n{page_data['content']}")

    # Create combined document JSON
    combined_doc = {
        'doc_name': json_path.name,
        'source_file': str(json_path),
        'total_pages': total_pages,
        'pages': combined_pages,
        'full_content': '\n\n'.join(all_content_parts)
    }

    # Save combined document file
    combined_file = output_dir / f'{doc_name}.json'
    with open(combined_file, 'w', encoding='utf-8') as f:
        json.dump(combined_doc, f, ensure_ascii=False, indent=2)

    return total_pages


def create_index(output_dir: Path, processed_docs: List[Dict]) -> None:
    """Create an index file listing all processed documents."""
    index = {
        'total_documents': len(processed_docs),
        'total_pages': sum(d['pages'] for d in processed_docs),
        'documents': []
    }

    for doc in processed_docs:
        index['documents'].append({
            'name': doc['name'],
            'file': doc['file'],
            'pages': doc['pages'],
            'combined_file': f"{doc['name']}.json",
            'pages_folder': f"{doc['name']}/"
        })

    index_file = output_dir / 'index.json'
    with open(index_file, 'w', encoding='utf-8') as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    print(f"Created index at {index_file}")


def process_phase1c(
    input_dir: Path,
    output_dir: Path,
    clean: bool = True,
    skip_existing: bool = False
) -> Dict[str, Any]:
    """
    Process all matched JSON files and extract text content.

    Args:
        input_dir: Directory containing matched JSON files (extract_matched/)
        output_dir: Directory to save text_each_page output
        clean: Remove existing output before processing
        skip_existing: Skip if output already exists

    Returns:
        Dict with processing statistics
    """
    # Clean output directory if requested
    if clean and output_dir.exists():
        print(f"Removing existing output: {output_dir}")
        shutil.rmtree(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Phase 1c: Extract Text Each Page")
    print("=" * 60)
    print(f"Input:  {input_dir}")
    print(f"Output: {output_dir}")
    print()

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    # Find all JSON files
    json_files = list(input_dir.glob('*.json'))

    if not json_files:
        print("No JSON files found in input directory.")
        return {'total': 0, 'processed': 0, 'errors': []}

    print(f"Found {len(json_files)} JSON files to process")
    print("-" * 60)

    stats = {
        'total': len(json_files),
        'processed': 0,
        'skipped': 0,
        'total_pages': 0,
        'errors': []
    }

    processed_docs = []

    for i, json_path in enumerate(sorted(json_files), 1):
        doc_name = json_path.stem

        # Check if already processed
        if skip_existing:
            combined_file = output_dir / f'{doc_name}.json'
            if combined_file.exists():
                print(f"[{i}/{len(json_files)}] SKIP (exists): {json_path.name[:50]}...")
                stats['skipped'] += 1
                continue

        print(f"[{i}/{len(json_files)}] Processing: {json_path.name[:50]}...", end=" ")

        try:
            pages = process_json_file(json_path, output_dir)

            if pages > 0:
                processed_docs.append({
                    'name': doc_name,
                    'file': json_path.name,
                    'pages': pages
                })
                stats['total_pages'] += pages
                stats['processed'] += 1
                print(f"OK ({pages} pages)")
            else:
                print("WARN (no pages)")

        except Exception as e:
            print(f"ERROR: {str(e)[:50]}")
            stats['errors'].append((json_path.name, str(e)))

    # Create index
    if processed_docs:
        create_index(output_dir, processed_docs)

    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Documents processed: {stats['processed']}/{stats['total']}")
    print(f"Documents skipped: {stats['skipped']}")
    print(f"Total pages extracted: {stats['total_pages']}")
    print(f"Errors: {len(stats['errors'])}")

    if stats['errors']:
        print("\nErrors:")
        for fname, err in stats['errors'][:5]:
            print(f"  - {fname[:40]}: {err[:50]}")
        if len(stats['errors']) > 5:
            print(f"  ... and {len(stats['errors']) - 5} more")

    print("=" * 60)

    return stats


def main():
    """Main entry point for Phase 1c."""
    parser = argparse.ArgumentParser(
        description="Phase 1c: Extract text content from each page"
    )
    parser.add_argument(
        "--final",
        action="store_true",
        help="Process test final data instead of training data"
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Don't clean output directory before processing"
    )
    parser.add_argument(
        "--skip",
        action="store_true",
        help="Skip existing files"
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default=None,
        help="Override input directory"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Override output directory"
    )

    args = parser.parse_args()

    # Determine base paths
    src_dir = Path(__file__).parent.parent

    if args.input_dir:
        input_dir = Path(args.input_dir)
    elif args.final:
        input_dir = src_dir / "result" / "final" / "processing_input" / "extract_matched"
    else:
        input_dir = src_dir / "result" / "from_train" / "processing_input" / "extract_matched"

    if args.output_dir:
        output_dir = Path(args.output_dir)
    elif args.final:
        output_dir = src_dir / "result" / "final" / "processing_input" / "text_each_page"
    else:
        output_dir = src_dir / "result" / "from_train" / "processing_input" / "text_each_page"

    process_phase1c(
        input_dir=input_dir,
        output_dir=output_dir,
        clean=not args.no_clean,
        skip_existing=args.skip
    )


if __name__ == "__main__":
    main()
