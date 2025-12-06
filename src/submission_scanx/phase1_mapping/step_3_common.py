"""
Step 3 Common: Shared utilities for spouse extraction

This module contains common functions used across step_3_1, step_3_2, and step_3_3.
"""

from typing import List, Dict, Optional

import sys
from pathlib import Path
_src_dir = str(Path(__file__).parent.parent.parent)
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from utils.common import get_polygon_center


def find_spouse_page(pages: List[Dict]) -> Optional[Dict]:
    """
    Find the spouse page by searching for 'คู่สมรส' content.

    The spouse page is identified by having "คู่สมรส" as a standalone header
    near the top of the page, NOT just mentioning spouse in other contexts.

    Also handles combined pages where both submitter and spouse data are on the same page.

    Returns the page dict if found, None otherwise.
    """
    spouse_header_variants = [
        'คู่สมรส', 'ข้อมูลคู่สมรส', 'คสมรส', 'คู่สมร', 'กู่สมรส',
        'ดูสมรส', 'คสมรส.', 'คู่สมรส.', 'คมรส', 'กู่สมร', 'คคสมรส'
    ]

    # Keywords that indicate this is NOT a spouse page (debt/asset pages)
    debt_page_indicators = [
        'ผู้ให้กู้', 'เจ้าหนี้', 'หนี้สิน', 'เงินให้กู้ยืม', 'ยกไปกรอกในบัญชี',
        'รายละเอียดประกอบรายการหนี้สินอื่น', 'จำนวนเงินรวม', 'บัญชีฯ หน้า',
        'ทรัพย์สินอื่น', 'รายการทรัพย์สิน', 'สิทธิและสัมปทาน'
    ]

    def is_debt_or_asset_page(page_lines: List[Dict]) -> bool:
        """Check if this page is a debt/asset declaration page."""
        debt_indicator_count = 0
        has_debt_table_header = False

        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])

            for indicator in debt_page_indicators:
                if indicator in content:
                    debt_indicator_count += 1

            if len(polygon) >= 8:
                if 'ผย.' in content and 'คส.' in content:
                    has_debt_table_header = True
                elif 'หมายเหตุ ผย' in content or 'ผย. = ผู้ยื่นบัญชี' in content:
                    has_debt_table_header = True

        return debt_indicator_count >= 2 or has_debt_table_header

    # First pass: Look for dedicated spouse pages
    for page in pages:
        page_lines = page.get('lines', [])

        if is_debt_or_asset_page(page_lines):
            continue

        # Check if submitter page
        is_submitter_page = False
        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])
            if len(polygon) >= 8:
                _, cy = get_polygon_center(polygon)
                if ('ผู้ยื่นบัญชี' in content or content.strip() in ['ผู้ยืน', 'ผู้ยื่น']) and cy < 3.0:
                    is_submitter_page = True
                    break

        if is_submitter_page:
            continue

        # Look for standalone "คู่สมรส" header
        for line in page_lines:
            content = line.get('content', '').strip()
            polygon = line.get('polygon', [])

            content_clean = content.rstrip('.').rstrip()

            is_spouse_header = (
                content_clean in spouse_header_variants or
                any(content_clean.startswith(v) and len(content_clean) < 15 for v in spouse_header_variants) or
                content_clean == 'คสมรส' or content_clean.startswith('คสมรส')
            )

            if is_spouse_header:
                if len(polygon) >= 8:
                    cx, cy = get_polygon_center(polygon)
                    if cy < 2.0 and cx < 3.0:
                        return page

    # Second pass: Look for combined pages
    for page in pages:
        page_lines = page.get('lines', [])

        if is_debt_or_asset_page(page_lines):
            continue

        spouse_header_found = False
        spouse_header_y = None

        for line in page_lines:
            content = line.get('content', '').strip()
            polygon = line.get('polygon', [])

            content_clean = content.rstrip('.').rstrip()

            is_spouse_header = (
                content_clean in spouse_header_variants or
                any(content_clean.startswith(v) and len(content_clean) < 15 for v in spouse_header_variants)
            )

            if is_spouse_header:
                if len(polygon) >= 8:
                    cx, cy = get_polygon_center(polygon)
                    if cx < 3.0 and cy > 4.0:
                        spouse_header_found = True
                        spouse_header_y = cy
                        break

        if spouse_header_found and spouse_header_y:
            for line in page_lines:
                content = line.get('content', '')
                polygon = line.get('polygon', [])
                if len(polygon) >= 8:
                    _, cy = get_polygon_center(polygon)
                    if 'ชื่อและชื่อสกุล' in content and abs(cy - spouse_header_y) < 1.0:
                        return page

    # Fallback: look for spouse page with typical structure
    for page in pages:
        page_lines = page.get('lines', [])

        if is_debt_or_asset_page(page_lines):
            continue

        is_submitter_page = False
        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])
            if len(polygon) >= 8:
                _, cy = get_polygon_center(polygon)
                if 'ผู้ยื่นบัญชี' in content and cy < 2.5:
                    is_submitter_page = True
                    break

        if is_submitter_page:
            continue

        has_spouse_header_in_context = False
        has_marriage_status = False
        has_name_field = False

        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])

            if len(polygon) < 8:
                continue

            cx, cy = get_polygon_center(polygon)

            if 'คู่สมรส' in content and cy < 2.0:
                if content.startswith('คู่สมรส') and len(content) < 30:
                    has_spouse_header_in_context = True

            if 'จดทะเบียนสมรส' in content and 2.5 < cy < 4.5:
                has_marriage_status = True

            if any(t in content for t in ['นาย', 'นาง', 'นางสาว']):
                if 1.0 <= cy <= 2.5 and 2.0 <= cx <= 5.0:
                    has_name_field = True

        if has_spouse_header_in_context and has_marriage_status and has_name_field:
            return page

    # Second fallback
    for page in pages:
        page_lines = page.get('lines', [])

        if is_debt_or_asset_page(page_lines):
            continue

        is_submitter_page = False
        is_child_page = False
        is_relative_page = False

        has_name_label_in_top = False
        has_marriage_status = False
        has_name_with_title = False
        has_foreign_spouse_note = False

        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])

            if len(polygon) < 8:
                continue

            cx, cy = get_polygon_center(polygon)

            if 'ผู้ยื่นบัญชี' in content and cy < 2.5:
                is_submitter_page = True
            if 'บุตรที่ยังไม่บรรลุนิติภาวะ' in content and cy < 2.0:
                is_child_page = True
            if 'ญาติ' in content and cy < 2.0:
                is_relative_page = True

            if 'ชื่อและชื่อสกุล' in content and 1.0 <= cy <= 2.0:
                has_name_label_in_top = True

            if 'จดทะเบียนสมรส' in content:
                has_marriage_status = True

            if 'กรณีคู่สมรสเป็นคนต่างด้าว' in content:
                has_foreign_spouse_note = True

            if any(t in content for t in ['นาย', 'นาง', 'นางสาว']):
                if 1.0 <= cy <= 2.0 and 2.0 <= cx <= 5.0:
                    has_name_with_title = True

        if is_submitter_page or is_child_page or is_relative_page:
            continue

        if has_name_label_in_top and has_name_with_title and (has_marriage_status or has_foreign_spouse_note):
            return page

    return None
