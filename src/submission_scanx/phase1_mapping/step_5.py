"""
Step 5: Extract statement and statement_detail information from JSON extract files

This step extracts:
1. statement.csv - Statement summary (รายได้, รายจ่าย, ภาษี, ทรัพย์สิน, หนี้สิน)
2. statement_detail.csv - Detailed statement items

Statement data is found using keyword detection:
- Income pages: ข้อมูลรายได้ต่อปีและรายจ่ายต่อปี, รายได้ต่อปี, รวมรายได้ต่อปี (โดยประมาณ)
- Expense pages: รายจ่ายต่อปี, รายจ่ายตอบ (OCR variant), รวมรายจ่ายต่อปี
- Tax pages: ข้อมูลการเสียภาษี, ภาษีเงินได้, มาตรา 40
- Asset/Liability summary: ข้อมูลรายการทรัพย์สินและหนี้สิน, รวมทรัพย์สิน, รวมหนี้สิน

Typical page locations (not 100%):
- Page 5-6 (หน้า 5-6): รายได้ต่อปี and รายจ่ายต่อปี (Income and Expense)
- Page 6-7 (หน้า 6-7): ภาษี (Tax) - may be combined with expense page
- Page 7-8 (หน้า 7-8): ทรัพย์สิน/หนี้สิน summary (Assets and Liabilities)

statement_type_id:
1 = รายได้ (Income)
2 = รายจ่าย (Expense)
3 = ภาษี (Tax)
4 = ทรัพย์สิน (Assets)
5 = หนี้สิน (Liabilities)

statement_detail_type_id:
1 = รายได้ประจำ
2 = รายได้จากทรัพย์สิน
3 = รายได้จากการรับให้
4 = รายได้จากการทำเกษตร
5 = รายได้อื่นๆ
6 = รายจ่ายประจำ
7 = รายจ่ายอื่นๆ
8 = เงินสด
9 = เงินฝาก
10 = เงินลงทุน
11 = เงินให้กู้ยืม
12 = ที่ดิน
13 = โรงเรือนและสิ่งปลูกสร้าง
14 = ยานพาหนะ
15 = สิทธิและสัมปทาน
16 = ทรัพย์สินอื่น
17 = เงินเบิกเกินบัญชี
18 = เงินกู้จากธนาคารและสถาบันการเงินอื่น
19 = หนี้สินที่มีหลักฐานเป็นหนังสือ
20 = หนี้สินอื่น
"""

import os
import re
from typing import List, Dict, Tuple

# Import shared utilities from utils package
import sys
from pathlib import Path
_src_dir = str(Path(__file__).parent.parent.parent)
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from utils.common import (
    get_polygon_center,
    clean_number_with_decimal_fragment,
    format_disclosure_date,
)
from utils.data_loader import PipelineDataLoader, CSVWriter


# X-coordinate boundaries for columns
# Based on actual OCR text_each_page data analysis:
# - Submitter values (ผู้ยื่นบัญชี): x ~ 2.8-3.8
# - Spouse values (คู่สมรส): x ~ 4.8-5.8
# - Child values (บุตร): x ~ 6.8-7.8
# Note: These boundaries are more precise based on text_each_page x coordinates
SUBMITTER_X_MIN = 2.5
SUBMITTER_X_MAX = 4.2
SPOUSE_X_MIN = 4.5
SPOUSE_X_MAX = 6.2
CHILD_X_MIN = 6.5
CHILD_X_MAX = 8.0


def clean_output_value(value):
    """
    Clean float values for CSV output.
    Removes unnecessary .0 suffix (e.g., 102000.0 -> 102000)
    """
    if value is None:
        return None
    if isinstance(value, float):
        # If the value is a whole number, convert to int
        if value == int(value):
            return int(value)
    return value


def clean_detail_text(text: str) -> str:
    """
    Clean statement detail text by removing OCR artifacts and normalizing text.

    Handles:
    - Special characters: •, *, ...
    - Trailing dots and spaces
    - OCR errors mapping to correct Thai text
    - Normalizing spacing
    """
    if not text:
        return ''

    # Remove leading/trailing whitespace
    text = text.strip()

    # Remove common OCR artifacts and special characters
    # Remove bullets, asterisks, dots at start/end
    text = re.sub(r'^[•\*\.\s:]+', '', text)
    text = re.sub(r'[•\*]+', '', text)
    text = re.sub(r'\.{2,}$', '', text)  # Remove trailing multiple dots
    text = re.sub(r'\s*\.\s*$', '', text)  # Remove trailing single dot with spaces
    text = re.sub(r'\s+', ' ', text)  # Normalize multiple spaces to single

    # Remove trailing dots and periods first
    text = text.rstrip('.')
    text = text.rstrip()

    # Exact match corrections (highest priority)
    exact_corrections = {
        # Standard asset/liability names - exact matches
        'โรงเรือนและสิ่งปลูกสร้าง': 'โรงเรือนและสิ่งปลูกสร้าง',
        'โรงเรือน': 'โรงเรือนและสิ่งปลูกสร้าง',
        'เงินกู้จากธนาคาร': 'เงินกู้จากธนาคารและสถาบันการเงินอื่น',
        'เงินกู้จากธนาคารและสถาบันการเงินอื่น': 'เงินกู้จากธนาคารและสถาบันการเงินอื่น',
        'หนี้สินที่มีหลักฐาน': 'หนี้สินที่มีหลักฐานเป็นหนังสือ',
        'หนี้สินที่มีหลักฐานเป็นหนังสือ': 'หนี้สินที่มีหลักฐานเป็นหนังสือ',

        # Fix missing ่ and other tone marks
        'คาเขาอาคาร': 'ค่าเช่าอาคาร',
        'คาเชาอาคาร': 'ค่าเช่าอาคาร',
        'คาเช่าอาคาร': 'ค่าเช่าอาคาร',
        'คาผอนรถ': 'ค่าผ่อนรถ',
        'ค่าผอนรถ': 'ค่าผ่อนรถ',
        'คาผ่อนรถ': 'ค่าผ่อนรถ',

        # Fix common misspellings
        'เบียประชุม': 'ค่าเบี้ยประชุม',
        'เบี้ยประชุม': 'ค่าเบี้ยประชุม',
        'ประกันชีวิตแบบบำนาญ': 'เบี้ยประกันชีวิตแบบบำนาญ',

        # Normalize spacing variations
        'ค่าอุปโภค บริโภค': 'ค่าอุปโภคบริโภค',
        'ค่า อุปโภคบริโภค': 'ค่าอุปโภคบริโภค',

        # Fix parentheses format variations
        'มาตรา 40 (1) - (8)': 'มาตรา 40(1)-(8)',
        'มาตรา40(1)-(8)': 'มาตรา 40(1)-(8)',

        # Add ค่า prefix where needed
        'ผ่อนที่อยู่อาศัย': 'ค่าผ่อนที่อยู่อาศัย',
        'ค่าผ่อนรถยนต์': 'ผ่อนรถยนต์',
        'อุปการะบิดามารดา': 'ค่าอุปการะบิดามารดา',
        'อุปการะมารดา': 'ค่าอุปการะมารดา',
        'ดอกเบี้ยเงินฝาก': 'ดอกเบี้ยเงินฝากธนาคาร',
        'ค่าอุปโภคบริโภค': 'ค่าอุปโภค บริโภค',
        'ค่าผ่อนที่อยู่อาศัย': 'ผ่อนที่อยู่อาศัย',

        # OCR errors with missing/wrong characters
        'เงินปันผลหน': 'เงินปันผลหุ้น',
        'เงินปันผลหุน': 'เงินปันผลหุ้น',
        'ดอกเบี้ยเงินฝากธนาคาร': 'ดอกเบี้ยเงินฝาก',
        'ดอกเบียเงินฝาก': 'ดอกเบี้ยเงินฝาก',

        # Generic asset names should be more specific
        'ที่ดิน': 'ทรัพย์สิน',  # Keep as is
        'ทรัพย์สิน': 'ทรัพย์สินอื่น',  # Generic to specific
    }

    # Apply exact corrections first
    if text in exact_corrections:
        return exact_corrections[text]

    # Pattern-based corrections for year format
    # Fix (ปี.XX) → (ปี XX)
    text = re.sub(r'\(ปี\.(\d+)\)', r'(ปี \1)', text)

    # Remove trailing dots again after all processing
    text = text.rstrip('.')
    text = text.rstrip()

    # Filter out garbage values (must contain Thai characters to be valid)
    # Skip values that are only numbers, symbols, or Latin characters
    if not re.search(r'[ก-๙]', text):
        return ''

    # Filter out values that are too short (likely OCR noise)
    if len(text) < 3:
        return ''

    return text


def find_pages_by_keyword(pages: List[Dict], keyword: str) -> List[Tuple[int, Dict]]:
    """Find all pages containing the keyword"""
    result = []
    for i, page in enumerate(pages):
        for line in page.get('lines', []):
            if keyword in line.get('content', ''):
                result.append((i, page))
                break
    return result


def find_statement_pages(pages: List[Dict]) -> Dict[str, List[int]]:
    """
    Find page indices for different statement sections by content.

    Returns dict with keys:
    - 'income_expense': Pages with รายได้ต่อปี / รายจ่ายต่อปี (หน้า 5-6 in form)
    - 'tax': Pages with ข้อมูลการเสียภาษี (หน้า 6 in form)
    - 'asset_liability': Pages with รายการทรัพย์สิน / หนี้สิน (หน้า 7 in form)
    """
    result = {
        'income_expense': [],
        'tax': [],
        'asset_liability': []
    }

    for idx, page in enumerate(pages):
        lines = page.get('lines', [])
        if not lines:
            continue

        # Get page text
        page_text = ' '.join(l.get('content', '') for l in lines)

        # Check for income/expense page (หน้า 5 - รายได้/รายจ่าย)
        if 'รายได้ต่อปี' in page_text or 'รายจ่ายต่อปี' in page_text:
            result['income_expense'].append(idx)

        # Check for tax page (หน้า 6 - ภาษี)
        if 'ข้อมูลการเสียภาษี' in page_text or 'เงินได้พึงประเมิน' in page_text:
            result['tax'].append(idx)

        # Check for asset/liability summary page (หน้า 7 - ทรัพย์สิน/หนี้สิน)
        if 'ข้อมูลรายการทรัพย์สินและหนี้สิน' in page_text:
            result['asset_liability'].append(idx)
        # Also check for summary page with รวมทรัพย์สิน and รวมหนี้สิน
        elif 'รวมทรัพย์สิน' in page_text and 'รวมหนี้สิน' in page_text:
            if idx not in result['asset_liability']:
                result['asset_liability'].append(idx)

    return result


def extract_values_from_row(lines: List[Dict], y_target: float, y_tolerance: float = 0.25) -> Dict:
    """Extract submitter, spouse, child values from a row at specific y position.

    Handles OCR-split numbers where decimal parts appear as separate text elements.
    """
    values = {'submitter': None, 'spouse': None, 'child': None}

    # Collect all relevant lines at this y position
    row_lines = []
    for line in lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [0]*8)
        cx, cy = get_polygon_center(polygon)

        if abs(cy - y_target) > y_tolerance:
            continue
        row_lines.append({'content': content, 'x': cx, 'y': cy})

    # Sort by x position for left-to-right processing
    row_lines.sort(key=lambda l: l.get('x', 0))

    # Process each line, looking for decimal fragments to merge
    i = 0
    while i < len(row_lines):
        item = row_lines[i]
        content = item.get('content', '')
        x = item.get('x', 0)

        # Determine which column this belongs to
        column = None
        if SUBMITTER_X_MIN <= x <= SUBMITTER_X_MAX:
            column = 'submitter'
        elif SPOUSE_X_MIN <= x <= SPOUSE_X_MAX:
            column = 'spouse'
        elif CHILD_X_MIN <= x <= CHILD_X_MAX:
            column = 'child'

        if column is None:
            i += 1
            continue

        # Look for potential decimal fragment in next element (within same column range)
        decimal_fragment = None
        if i + 1 < len(row_lines):
            next_item = row_lines[i + 1]
            next_x = next_item.get('x', 0)
            next_content = next_item.get('content', '')

            # Check if next element is close enough and looks like decimal fragment
            x_distance = next_x - x
            if 0 < x_distance < 1.2:
                # Check if it looks like a decimal fragment (-52, H52, .52, 00, etc.)
                if re.match(r'^[Hh\-.]?\d{1,2}$', next_content.strip()):
                    decimal_fragment = next_content
                    i += 1  # Skip the decimal fragment in next iteration

        # Try to extract value, merging decimal fragment if found
        value = clean_number_with_decimal_fragment(content, decimal_fragment)
        if value is not None and values[column] is None:
            values[column] = value

        i += 1

    return values


def extract_income_expense_from_pages(pages: List[Dict]) -> Tuple[List[Dict], Dict, List[Dict], Dict]:
    """Extract income and expense details from pages 8-9"""
    income_details = []
    income_totals = {'submitter': None, 'spouse': None, 'child': None}
    expense_details = []
    expense_totals = {'submitter': None, 'spouse': None, 'child': None}

    # Find pages with income and expense data
    for page_idx, page in enumerate(pages):
        lines = page.get('lines', [])
        sorted_lines = sorted(lines, key=lambda x: get_polygon_center(x.get('polygon', [0]*8))[1])

        page_text = ' '.join([l.get('content', '') for l in lines])

        # Check if this is income page
        # Must have income section AND income totals to be a proper income page
        # OCR variants: "รายได้ต่อปี", "รายได้ต่อย", "ขอมสิรายได้", "รายได้ประจำ"
        # Full header: "ข้อมูลรายได้ต่อปีและรายจ่ายต่อปี (โดยประมาณ)"
        has_income_section = any(kw in page_text for kw in [
            'ข้อมูลรายได้ต่อปีและรายจ่ายต่อปี',  # Full header
            'รายได้ต่อปี (โดยประมาณ)',  # Income header with approximate
            'รายได้ต่อปี', 'รายได้ต่อย', 'ขอมสิรายได้', '1. รายได้ประจำ',
            'รายได้ประจำ', 'รายได้จากทรัพย์สิน', 'รายได้จากการรับให้',
        ])
        has_income_totals = any(kw in page_text for kw in ['รวมรายได้', 'ร่วมรายได้', 'รวม รายได้'])
        # Skip cover pages (page 0-1) which may have summary labels
        if has_income_section and has_income_totals and page_idx >= 2:
            # Income section mappings
            income_sections = [
                ('1. รายได้ประจำ', 'รายได้ประจำ', 1),
                ('2. รายได้จากทรัพย์สิน', 'รายได้จากทรัพย์สิน', 2),
                ('3. รายได้จากการรับให้', 'รายได้จากการรับให้', 3),
                ('4. รายได้จากการทำเกษตร', 'รายได้จากการทำเกษตร', 4),
                ('5. รายได้อื่น', 'รายได้อื่น', 5),
            ]

            # Find section positions
            section_y_positions = []
            for line in sorted_lines:
                content = line.get('content', '')
                polygon = line.get('polygon', [0]*8)
                cx, cy = get_polygon_center(polygon)

                for section_key1, section_key2, type_id in income_sections:
                    if section_key1 in content or section_key2 in content:
                        section_y_positions.append((type_id, cy))
                        break

            # Remove duplicates and sort
            section_y_positions = sorted(list(set(section_y_positions)), key=lambda x: x[1])

            # Extract items for each section
            for i, (type_id, section_y_start) in enumerate(section_y_positions):
                section_y_end = section_y_positions[i+1][1] if i+1 < len(section_y_positions) else 15.0

                items = []
                for line in sorted_lines:
                    content = line.get('content', '')
                    polygon = line.get('polygon', [0]*8)
                    cx, cy = get_polygon_center(polygon)

                    # Skip if outside section
                    if cy < section_y_start + 0.15 or cy >= section_y_end:
                        continue

                    # Skip headers
                    skip_words = ['รายได้', 'ได้แก่', 'ผู้ยื่น', 'คู่สมรส', 'บุตร', 'หมายเหตุ', 'รวม', 'หน้า']
                    if any(skip in content for skip in skip_words):
                        continue

                    # Look for item name in left column (x < 2.8)
                    if 0.7 <= cx <= 2.8:
                        # Remove number prefix
                        detail = re.sub(r'^\(\d+\)\s*', '', content).strip()
                        detail = re.sub(r'^[.\s:*]+', '', detail).strip()

                        if detail and len(detail) > 2 and not re.match(r'^[\d\.\-\s]+$', detail):
                            items.append({'y': cy, 'detail': detail})

                # Get values for each item
                for idx, item in enumerate(items):
                    values = extract_values_from_row(sorted_lines, item['y'])
                    if values['submitter'] or values['spouse'] or values['child']:
                        income_details.append({
                            'statement_detail_type_id': type_id,
                            'index': idx + 1,
                            'detail': item['detail'],
                            'valuation_submitter': values['submitter'],
                            'valuation_spouse': values['spouse'],
                            'valuation_child': values['child'],
                            'note': ''
                        })

            # Find income totals
            # Note: OCR may produce "ร่วมรายได้" instead of "รวมรายได้"
            for line in sorted_lines:
                content = line.get('content', '')
                polygon = line.get('polygon', [0]*8)
                _, cy = get_polygon_center(polygon)

                if any(kw in content for kw in ['รวมรายได้ต่อปี', 'รวมรายได้', 'ร่วมรายได้ต่อปี', 'ร่วมรายได้']):
                    income_totals = extract_values_from_row(sorted_lines, cy, 0.15)
                    break

            # If no explicit total found, sum up income details
            if not income_totals['submitter'] and not income_totals['spouse'] and income_details:
                submitter_sum = 0
                spouse_sum = 0
                child_sum = 0
                for detail in income_details:
                    if detail.get('valuation_submitter'):
                        submitter_sum += detail['valuation_submitter']
                    if detail.get('valuation_spouse'):
                        spouse_sum += detail['valuation_spouse']
                    if detail.get('valuation_child'):
                        child_sum += detail['valuation_child']
                if submitter_sum > 0:
                    income_totals['submitter'] = submitter_sum
                if spouse_sum > 0:
                    income_totals['spouse'] = spouse_sum
                if child_sum > 0:
                    income_totals['child'] = child_sum


        # Check if this is expense page
        # Must have expense section AND expense totals
        # OCR variants: "รายจ่ายต่อปี", "รายจ่ายตอบ", "รายจ่ายต่อบ"
        # Full header: "รายจ่ายต่อปี (โดยประมาณ)"
        has_expense_section = any(kw in page_text for kw in [
            'ข้อมูลรายได้ต่อปีและรายจ่ายต่อปี',  # Combined header
            'รายจ่ายต่อปี (โดยประมาณ)',  # Expense header with approximate
            'รายจ่ายต่อปี', 'รายจ่ายตอบ', 'รายจ่ายต่อบ',  # OCR variants
            '1. รายจ่ายประจำ', 'รายจ่ายประจำ', 'รายจ่ายอื่น',
        ])
        has_expense_totals = any(kw in page_text for kw in [
            'รวมรายจ่ายต่อปี', 'รวมรายจ่าย', 'ร่วมรายจ่าย', 'รวม รายจ่าย'
        ])
        # Skip cover pages (page 0-1)
        if has_expense_section and has_expense_totals and page_idx >= 2:
            # Expense section mappings
            expense_sections = [
                ('1. รายจ่ายประจำ', 'รายจ่ายประจำ', 6),
                ('2. รายจ่ายอื่น', 'รายจ่ายอื่น', 7),
            ]

            # Find section positions
            section_y_positions = []
            for line in sorted_lines:
                content = line.get('content', '')
                polygon = line.get('polygon', [0]*8)
                cx, cy = get_polygon_center(polygon)

                for section_key1, section_key2, type_id in expense_sections:
                    if section_key1 in content or section_key2 in content:
                        section_y_positions.append((type_id, cy))
                        break

            # Remove duplicates and sort
            section_y_positions = sorted(list(set(section_y_positions)), key=lambda x: x[1])

            # Extract items for each section
            for i, (type_id, section_y_start) in enumerate(section_y_positions):
                section_y_end = section_y_positions[i+1][1] if i+1 < len(section_y_positions) else 15.0

                items = []
                for line in sorted_lines:
                    content = line.get('content', '')
                    polygon = line.get('polygon', [0]*8)
                    cx, cy = get_polygon_center(polygon)

                    # Skip if outside section
                    if cy < section_y_start + 0.15 or cy >= section_y_end:
                        continue

                    # Skip headers
                    skip_words = ['รายจ่าย', 'ได้แก่', 'ผู้ยื่น', 'คู่สมรส', 'บุตร', 'หมายเหตุ', 'รวม', 'หน้า']
                    if any(skip in content for skip in skip_words):
                        continue

                    # Look for item name in left column
                    if 0.7 <= cx <= 2.8:
                        detail = re.sub(r'^\(\d+\)\s*', '', content).strip()
                        detail = re.sub(r'^[.\s:*•]+', '', detail).strip()

                        if detail and len(detail) > 2 and not re.match(r'^[\d\.\-\s]+$', detail):
                            items.append({'y': cy, 'detail': detail})

                # Get values for each item
                for idx, item in enumerate(items):
                    values = extract_values_from_row(sorted_lines, item['y'])
                    if values['submitter'] or values['spouse'] or values['child']:
                        expense_details.append({
                            'statement_detail_type_id': type_id,
                            'index': idx + 1,
                            'detail': item['detail'],
                            'valuation_submitter': values['submitter'],
                            'valuation_spouse': values['spouse'],
                            'valuation_child': values['child'],
                            'note': ''
                        })

            # Find expense totals
            for line in sorted_lines:
                content = line.get('content', '')
                polygon = line.get('polygon', [0]*8)
                _, cy = get_polygon_center(polygon)

                if 'รวมรายจ่ายต่อปี' in content or 'รวมรายจ่าย' in content:
                    expense_totals = extract_values_from_row(sorted_lines, cy, 0.15)
                    break

            # If no explicit total found, sum up expense details
            if not expense_totals['submitter'] and not expense_totals['spouse'] and expense_details:
                submitter_sum = 0
                spouse_sum = 0
                child_sum = 0
                for detail in expense_details:
                    if detail.get('valuation_submitter'):
                        submitter_sum += detail['valuation_submitter']
                    if detail.get('valuation_spouse'):
                        spouse_sum += detail['valuation_spouse']
                    if detail.get('valuation_child'):
                        child_sum += detail['valuation_child']
                if submitter_sum > 0:
                    expense_totals['submitter'] = submitter_sum
                if spouse_sum > 0:
                    expense_totals['spouse'] = spouse_sum
                if child_sum > 0:
                    expense_totals['child'] = child_sum

    return income_details, income_totals, expense_details, expense_totals


def extract_tax_info(pages: List[Dict]) -> Dict:
    """Extract tax information (ภาษี)"""
    totals = {'submitter': None, 'spouse': None, 'child': None}

    for page_idx, page in enumerate(pages):
        lines = page.get('lines', [])
        page_text = ' '.join([l.get('content', '') for l in lines])

        # Skip cover page
        if page_idx < 2:
            continue

        # Check for tax page indicators
        # Tax info can be on expense page (page 6-7) or separate
        has_tax_indicators = any(kw in page_text for kw in [
            'เงินได้พึงประเมิน', 'ข้อมูลการเสียภาษี', 'มาตรา 40', 'ภาษีเงินได้',
            'ภาษีเงินได้บุคคลธรรมดา', 'ภาษี (ปีก่อน)', 'รายจ่ายในการชำระภาษี',
            'ภาษีปีก่อน', 'ภาษี ปีก่อน',
        ])
        if not has_tax_indicators:
            continue

        sorted_lines = sorted(lines, key=lambda x: get_polygon_center(x.get('polygon', [0]*8))[1])

        for line in sorted_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [0]*8)
            _, cy = get_polygon_center(polygon)

            # Look for tax value row
            if 'เงินได้พึงประเมิน' in content or 'มาตรา 40' in content:
                totals = extract_values_from_row(sorted_lines, cy, 0.3)
                if totals['submitter'] or totals['spouse'] or totals['child']:
                    return totals

    return totals


def extract_asset_liability_summary(pages: List[Dict]) -> Tuple[List[Dict], Dict, Dict]:
    """Extract asset and liability summary from the summary page"""
    asset_details = []
    asset_totals = {'submitter': None, 'spouse': None, 'child': None}
    liability_totals = {'submitter': None, 'spouse': None, 'child': None}

    # Keywords for detecting asset/liability summary pages
    # Full header: "ข้อมูลรายการทรัพย์สินและหนี้สิน"
    summary_page_keywords = [
        'ข้อมูลรายการทรัพย์สินและหนี้สิน',  # Full header
        'รายการทรัพย์สินและหนี้สิน',  # Asset and liability list
        'ทรัพย์สินและหนี้สิน',  # Assets and liabilities
        'สรุปรายการทรัพย์สิน',  # Asset summary
    ]

    # Find the page with รวมทรัพย์สิน and รวมหนี้สิน
    for page_idx, page in enumerate(pages):
        lines = page.get('lines', [])
        page_text = ' '.join([l.get('content', '') for l in lines])

        # Skip cover page (page 0) - it has summary labels but no values
        if page_idx < 2:
            continue

        # Check for summary page header keywords OR both totals
        has_summary_header = any(kw in page_text for kw in summary_page_keywords)
        has_asset_total = 'รวมทรัพย์สิน' in page_text
        has_liability_total = 'รวมหนี้สิน' in page_text

        # Must have header keywords OR both totals
        if not has_summary_header and not (has_asset_total and has_liability_total):
            continue

        sorted_lines = sorted(lines, key=lambda x: get_polygon_center(x.get('polygon', [0]*8))[1])

        # Asset items and their type IDs
        asset_items = [
            ('เงินสด', 8),
            ('เงินฝาก', 9),
            ('เงินลงทุน', 10),
            ('เงินให้กู้ยืม', 11),
            ('ที่ดิน', 12),
            ('โรงเรือนและสิ่งปลูกสร้าง', 13),
            ('โรงเรือน', 13),
            ('ยานพาหนะ', 14),
            ('สิทธิและสัมปทาน', 15),
            ('ทรัพย์สินอื่น', 16),
        ]

        # Liability items
        liability_items = [
            ('เงินเบิกเกินบัญชี', 17),
            ('เงินกู้จากธนาคาร', 18),
            ('หนี้สินที่มีหลักฐาน', 19),
            ('หนี้สินอื่น', 20),
        ]

        processed_types = set()

        # Extract asset details
        for line in sorted_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [0]*8)
            _, cy = get_polygon_center(polygon)

            # Check for asset items
            for item_name, type_id in asset_items:
                if item_name in content and type_id not in processed_types:
                    values = extract_values_from_row(sorted_lines, cy, 0.25)
                    if values['submitter'] or values['spouse'] or values['child']:
                        asset_details.append({
                            'statement_detail_type_id': type_id,
                            'index': 1,
                            'detail': item_name,
                            'valuation_submitter': values['submitter'],
                            'valuation_spouse': values['spouse'],
                            'valuation_child': values['child'],
                            'note': ''
                        })
                        processed_types.add(type_id)
                    break

            # Check for liability items
            for item_name, type_id in liability_items:
                if item_name in content and type_id not in processed_types:
                    values = extract_values_from_row(sorted_lines, cy, 0.3)
                    if values['submitter'] or values['spouse'] or values['child']:
                        asset_details.append({
                            'statement_detail_type_id': type_id,
                            'index': 1,
                            'detail': item_name,
                            'valuation_submitter': values['submitter'],
                            'valuation_spouse': values['spouse'],
                            'valuation_child': values['child'],
                            'note': ''
                        })
                        processed_types.add(type_id)
                    break

            # Check for totals
            if 'รวมทรัพย์สิน' in content and 'ทั้งสิ้น' not in content:
                asset_totals = extract_values_from_row(sorted_lines, cy, 0.2)

            if 'รวมหนี้สิน' in content and 'ทั้งสิ้น' not in content:
                liability_totals = extract_values_from_row(sorted_lines, cy, 0.2)

        # If we found data, return it
        if asset_totals['submitter'] or asset_totals['spouse'] or liability_totals['submitter'] or liability_totals['spouse']:
            return asset_details, asset_totals, liability_totals

    return asset_details, asset_totals, liability_totals


def extract_statement_data(
    json_content: Dict,
    nacc_detail: Dict,
    submitter_info: Dict
) -> Tuple[List[Dict], List[Dict]]:
    """Extract statement and statement_detail from JSON content"""

    statements = []
    statement_details = []

    nacc_id = nacc_detail.get('nacc_id')
    submitter_id = submitter_info.get('submitter_id')

    # Parse disclosure date
    disclosure_date = nacc_detail.get('disclosure_announcement_date', '')
    latest_submitted_date = format_disclosure_date(disclosure_date)

    pages = json_content.get('pages', [])
    if not pages:
        return statements, statement_details

    # Extract income and expense
    income_details, income_totals, expense_details, expense_totals = extract_income_expense_from_pages(pages)

    # Add income details
    for detail in income_details:
        detail['nacc_id'] = nacc_id
        detail['submitter_id'] = submitter_id
        detail['latest_submitted_date'] = latest_submitted_date
        statement_details.append(detail)

    # Add income statement
    if income_totals['submitter'] or income_totals['spouse'] or income_totals['child']:
        statements.append({
            'nacc_id': nacc_id,
            'statement_type_id': 1,
            'valuation_submitter': income_totals['submitter'],
            'submitter_id': submitter_id,
            'valuation_spouse': income_totals['spouse'],
            'valuation_child': income_totals['child'],
            'latest_submitted_date': latest_submitted_date
        })

    # Add expense details
    for detail in expense_details:
        detail['nacc_id'] = nacc_id
        detail['submitter_id'] = submitter_id
        detail['latest_submitted_date'] = latest_submitted_date
        statement_details.append(detail)

    # Add expense statement
    if expense_totals['submitter'] or expense_totals['spouse'] or expense_totals['child']:
        statements.append({
            'nacc_id': nacc_id,
            'statement_type_id': 2,
            'valuation_submitter': expense_totals['submitter'],
            'submitter_id': submitter_id,
            'valuation_spouse': expense_totals['spouse'],
            'valuation_child': expense_totals['child'],
            'latest_submitted_date': latest_submitted_date
        })

    # Extract tax
    tax_totals = extract_tax_info(pages)
    if tax_totals['submitter'] or tax_totals['spouse'] or tax_totals['child']:
        statements.append({
            'nacc_id': nacc_id,
            'statement_type_id': 3,
            'valuation_submitter': tax_totals['submitter'],
            'submitter_id': submitter_id,
            'valuation_spouse': tax_totals['spouse'],
            'valuation_child': tax_totals['child'],
            'latest_submitted_date': latest_submitted_date
        })

    # Extract assets and liabilities
    asset_details, asset_totals, liability_totals = extract_asset_liability_summary(pages)

    for detail in asset_details:
        detail['nacc_id'] = nacc_id
        detail['submitter_id'] = submitter_id
        detail['latest_submitted_date'] = latest_submitted_date
        statement_details.append(detail)

    # Add asset statement
    if asset_totals['submitter'] or asset_totals['spouse'] or asset_totals['child']:
        statements.append({
            'nacc_id': nacc_id,
            'statement_type_id': 4,
            'valuation_submitter': asset_totals['submitter'],
            'submitter_id': submitter_id,
            'valuation_spouse': asset_totals['spouse'],
            'valuation_child': asset_totals['child'],
            'latest_submitted_date': latest_submitted_date
        })

    # Add liability statement
    if liability_totals['submitter'] or liability_totals['spouse'] or liability_totals['child']:
        statements.append({
            'nacc_id': nacc_id,
            'statement_type_id': 5,
            'valuation_submitter': liability_totals['submitter'],
            'submitter_id': submitter_id,
            'valuation_spouse': liability_totals['spouse'],
            'valuation_child': liability_totals['child'],
            'latest_submitted_date': latest_submitted_date
        })

    return statements, statement_details


def clean_statement_values(statements: List[Dict]) -> List[Dict]:
    """
    Clean float values in statement records (remove .0 suffix).

    Note: The extraction functions already place values in the correct columns
    based on X coordinates. No swapping needed.
    """
    for stmt in statements:
        # Clean float values (remove .0 suffix)
        for key in ['valuation_submitter', 'valuation_spouse', 'valuation_child']:
            if key in stmt:
                stmt[key] = clean_output_value(stmt[key])
    return statements


def clean_statement_detail_values(details: List[Dict]) -> List[Dict]:
    """Clean float values and detail text in statement_detail records.

    Also filters out records with empty or invalid detail text.
    """
    cleaned_details = []

    for detail in details:
        # Clean float values (remove .0 suffix)
        for key in ['valuation_submitter', 'valuation_spouse', 'valuation_child']:
            if key in detail:
                detail[key] = clean_output_value(detail[key])

        # Clean detail text (remove OCR artifacts, normalize text)
        if 'detail' in detail:
            detail['detail'] = clean_detail_text(detail['detail'])

            # Skip records with empty or invalid detail text
            if not detail['detail'] or detail['detail'] == 'detail':
                continue

        cleaned_details.append(detail)

    return cleaned_details


def run_step_5(input_dir: str, output_dir: str, data_loader: PipelineDataLoader = None):
    """
    Run step 5 to extract statement and statement_detail.

    Args:
        input_dir: Input directory path
        output_dir: Output directory path
        data_loader: Optional shared PipelineDataLoader instance for caching
    """
    # Use shared data loader if provided, otherwise create new one
    loader = data_loader or PipelineDataLoader(input_dir)

    all_statements = []
    all_statement_details = []

    for context in loader.iterate_documents():
        json_content = context['json_content']
        nacc_detail = context['nacc_detail']
        submitter_info = context['submitter_info']

        # Standard extraction from json_content
        statements, statement_details = extract_statement_data(
            json_content, nacc_detail, submitter_info
        )

        all_statements.extend(statements)
        all_statement_details.extend(statement_details)

    # Clean float values (remove .0 suffix)
    all_statements = clean_statement_values(all_statements)
    all_statement_details = clean_statement_detail_values(all_statement_details)

    # Write outputs using CSVWriter
    statement_fields = [
        'nacc_id', 'statement_type_id', 'valuation_submitter', 'submitter_id',
        'valuation_spouse', 'valuation_child', 'latest_submitted_date'
    ]
    writer1 = CSVWriter(output_dir, 'statement.csv', statement_fields)
    count1 = writer1.write_rows(all_statements)
    print(f"Extracted {count1} statements to {writer1.output_path}")

    statement_detail_fields = [
        'nacc_id', 'submitter_id', 'statement_detail_type_id', 'index', 'detail',
        'valuation_submitter', 'valuation_spouse', 'valuation_child', 'note',
        'latest_submitted_date'
    ]
    writer2 = CSVWriter(output_dir, 'statement_detail.csv', statement_detail_fields)
    count2 = writer2.write_rows(all_statement_details)
    print(f"Extracted {count2} statement details to {writer2.output_path}")

    return all_statements, all_statement_details


if __name__ == '__main__':
    input_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_input')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_output')

    run_step_5(input_dir, output_dir)
