"""
Phase 1d: Map document pages to extraction steps using hybrid regex + AI.

Uses regex patterns with priorities for initial detection, then Azure OpenAI
for uncertain cases.

Template Standard Layout (37 pages):
- Page 1-3: Cover and instructions
- Page 4: หน้า 1 - Personal info (submitter) -> step_1, step_2, step_4
- Page 5: หน้า 2 - Spouse info (คู่สมรส) -> step_3_1, step_3_2, step_3_3, step_4
- Page 6: หน้า 3 - Children (บุตร) -> step_4
- Page 7: หน้า 4 - Siblings (พี่น้อง) -> step_4
- Page 8-9: หน้า 5-6 - Income/Expense -> step_5
- Page 10: หน้า 7 - Assets/Liabilities summary -> step_5
- Page 11-12: Instructions and certification
- Page 13-14: หน้า 9 - Cash details -> step_6
- Page 15-16: หน้า 10 - Deposits -> step_6
- Page 17-18: หน้า 11 - Investments -> step_6
- Page 19-20: หน้า 12 - Loans given -> step_6
- Page 21-22: หน้า 13 - Land -> step_7 + step_6
- Page 23-24: หน้า 14 - Buildings -> step_8 + step_6
- Page 25-26: หน้า 15 - Vehicles -> step_9 + step_6
- Page 27-28: หน้า 16 - Concessions -> step_6
- Page 29-30: หน้า 17 - Other assets -> step_10 + step_6
- Page 31-32: หน้า 18 - Overdraft -> step_6
- Page 33-34: หน้า 19 - Bank loans -> step_6
- Page 35: หน้า 20 - Written debts -> step_6
- Page 36-37: Document list

Step-to-Output Mapping:
- step_1: submitter_position.csv (ตำแหน่งผู้ยื่นบัญชี)
- step_2: submitter_old_name.csv (ชื่อเดิมผู้ยื่นบัญชี)
- step_3_1: spouse_info.csv (ข้อมูลคู่สมรส)
- step_3_2: spouse_old_name.csv (ชื่อเดิมคู่สมรส)
- step_3_3: spouse_position.csv (ตำแหน่งคู่สมรส)
- step_4: relative_info.csv (ญาติ: บิดา, มารดา, บุตร, พี่น้อง)
- step_5: statement.csv, statement_detail.csv (รายได้/รายจ่าย/ภาษี/สรุป)
- step_6: asset.csv (รายการทรัพย์สินและหนี้สิน)
- step_7: asset_land_info.csv (รายละเอียดที่ดิน)
- step_8: asset_building_info.csv (รายละเอียดโรงเรือน)
- step_9: asset_vehicle_info.csv (รายละเอียดยานพาหนะ)
- step_10: asset_other_asset_info.csv (รายละเอียดทรัพย์สินอื่น)
- step_11: summary.csv (สรุปรวม)

Usage:
    poetry run scanx-meta
    poetry run scanx-meta --final
    poetry run scanx-meta --clean
"""

import argparse
import json
import os
import re
import shutil
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Tuple, Any
from dotenv import load_dotenv

# Load environment variables from package .env
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)


@dataclass
class StepMapping:
    """Mapping of a step to its pages."""
    step_name: str
    step_description: str
    output_csv: str
    page_numbers: List[int]
    page_types: List[str]
    confidence: float = 1.0


@dataclass
class DocumentMetadata:
    """Metadata for a single document."""
    doc_name: str
    total_pages: int
    step_mappings: List[StepMapping]
    unmapped_pages: List[int]


# Page type patterns for detection with priorities and negative patterns
# Uses ข้? to handle OCR variations (ข้อ vs ขอ - missing tone marks)
PAGE_PATTERNS = {
    'personal_info': {
        'patterns': [
            r'หน้า\s*1\b',
            r'ข้?อมูลส่วนบุคคล',  # Handle OCR variation ขอ vs ข้อ
            r'เลขประจำตัวประชาชน.*ผู้ยื่นบัญชี',
            r'ตำแหน่งปัจจุบันในหน่วยงานราชการ',
            r'ประวัติการทำงานย้อนหลัง\s*5\s*ปี',
            r'ผู้ยื่นบัญชี\s*:',
            r'สถานภาพ\s+โสด\s+สมรส',
            r'คู่สมรสเสียชีวิต',
            r'หย่า\s+เมื่อวันที่',
        ],
        'negative_patterns': [
            r'หน้า\s*2\b',
            r'ที่อยู่เดียวกันกับผู้ยื่นบัญชี',
            r'กรณีมีคู่สมรสมากกว่าหนึ่งคน',
            r'กรณีคู่สมรสเป็นคนต่างด้าว',
            r'สถานภาพการสมรส',  # Spouse-only field (not on personal_info page)
        ],
        'step': 'step_1',
        'additional_steps': ['step_2', 'step_4'],
        'output_csv': 'submitter_position.csv + submitter_old_name.csv + relative_info.csv',
        'description': 'ข้อมูลผู้ยื่นบัญชี + ตำแหน่ง + ชื่อเดิม + บิดามารดา',
        'can_have_extra': True,
        'priority': 5,
    },
    'personal_info_continuation': {
        'patterns': [
            r'ประวัติการทำงานย้อนหลัง\s*5\s*ปี\s*\(ต่อ\)',
            r'\(ต่อ\)',
            r'ระยะเวลาดำรงตำแหน่ง',
        ],
        'negative_patterns': [
            r'หน้า\s*2\b',
            r'หน้า\s*3\b',
            r'หน้า\s*4\b',
            r'รายละเอียดประกอบ',
        ],
        'step': 'step_1',
        'output_csv': 'submitter_position.csv',
        'description': 'ตำแหน่งผู้ยื่นบัญชี (ต่อ)',
        'can_have_extra': True,
        'priority': 4,
    },
    'spouse_info': {
        'patterns': [
            r'หน้า\s*2\b',
            r'ที่อยู่เดียวกันกับผู้ยื่นบัญชี',
            r'กรณีมีคู่สมรสมากกว่าหนึ่งคน',
            r'กรณีคู่สมรสเป็นคนต่างด้าว',
            r'สถานภาพการสมรส',
            r'จดทะเบียนสมรส\s+เมื่อวันที่',
            r'อยู่กินกันฉันสามีภริยา\s+ตามที่คณะกรรมการ',
            r'บิดา\s*:\s*ชื่อและชื่อสกุล',
            r'มารดา\s*:\s*ชื่อและชื่อสกุล',
        ],
        'negative_patterns': [
            r'หน้า\s*1\b',
            r'ข้?อมูลส่วนบุคคล',  # Handle OCR variation ขอ vs ข้อ
            r'สถานภาพ\s+โสด\s+สมรส',  # This is submitter's marital status checkbox
        ],
        'step': 'step_3_1',
        'additional_steps': ['step_3_2', 'step_3_3', 'step_4'],
        'output_csv': 'spouse_info.csv + spouse_old_name.csv + spouse_position.csv + relative_info.csv',
        'description': 'ข้อมูลคู่สมรส + ตำแหน่ง + ชื่อเดิม + บิดามารดาคู่สมรส',
        'can_have_extra': True,
        'priority': 10,
    },
    'children': {
        'patterns': [
            r'หน้า\s*3\b',
            r'บุตร',
            r'บุตรโดยชอบด้วยกฎหมาย',
            r'บุตรบุญธรรม',
            r'เด็กชาย',
            r'เด็กหญิง',
            r'ด\.ช\.',
            r'ด\.ญ\.',
        ],
        'step': 'step_4',
        'output_csv': 'relative_info.csv',
        'description': 'ข้อมูลบุตร (relationship_id=4)',
        'can_have_extra': True,
    },
    'siblings': {
        'patterns': [
            r'หน้า\s*4\b',
            r'พี่น้อง',
            r'พี่น้องร่วมบิดามารดา',
            r'พี่น้องร่วมบิดา',
            r'พี่น้องร่วมมารดา',
        ],
        'step': 'step_4',
        'output_csv': 'relative_info.csv',
        'description': 'ข้อมูลพี่น้อง (relationship_id=3)',
        'can_have_extra': True,
    },
    'income_expense': {
        'patterns': [
            r'หน้า\s*5\b',
            r'หน้า\s*6\b',
            r'ข้อมูลรายได้ต่อปีและรายจ่ายต่อปี',
            r'รายได้ต่อปี.*โดยประมาณ',
            r'รายจ่ายต่อปี.*โดยประมาณ',
            r'รวมรายได้ต่อปี',
            r'รวมรายจ่ายต่อปี',
            r'รายได้ต่อปี',
            r'รายจ่ายต่อปี',
            r'รายจ่ายตอบ',
            r'รายได้ประจำ',
            r'รายจ่ายประจำ',
            r'รายได้จากทรัพย์สิน',
            r'รายได้จากการรับให้',
            r'รายได้จากการทำเกษตร',
            r'รายได้อื่น',
            r'รายจ่ายอื่น',
            r'ค่าอุปโภคบริโภค',
            r'ค่าใช้จ่ายในครัวเรือน',
        ],
        'step': 'step_5',
        'output_csv': 'statement.csv + statement_detail.csv',
        'description': 'รายได้/รายจ่าย (statement_type_id=1,2)',
        'can_have_extra': True,
    },
    'tax_info': {
        'patterns': [
            r'หน้า\s*6\b',
            r'หน้า\s*7\b',
            r'ข้อมูลการเสียภาษี',
            r'ภาษีเงินได้บุคคลธรรมดา',
            r'ภาษีเงินได้',
            r'เงินได้พึงประเมิน',
            r'มาตรา\s*40',
            r'ภาษี.*ปีก่อน',
            r'รายจ่ายในการชำระภาษี',
        ],
        'step': 'step_5',
        'output_csv': 'statement.csv',
        'description': 'ข้อมูลภาษี (statement_type_id=3)',
        'can_have_extra': True,
    },
    'assets_summary': {
        'patterns': [
            r'หน้า\s*7\b',
            r'หน้า\s*8\b',
            r'ข้อมูลรายการทรัพย์สินและหนี้สิน',
            r'ข้อมูลรายการทรัพย์สิน',
            r'รายการทรัพย์สินและหนี้สิน',
            r'ทรัพย์สินและหนี้สิน',
            r'สรุปรายการทรัพย์สิน',
            r'สรุปทรัพย์สิน',
            r'รวมทรัพย์สิน',
            r'รวมหนี้สิน',
            r'เงินสด.*เงินฝาก.*เงินลงทุน',
            r'โรงเรือน.*ยานพาหนะ',
            r'ที่ดิน.*สิ่งปลูกสร้าง',
        ],
        'step': 'step_5',
        'output_csv': 'statement.csv',
        'description': 'สรุปทรัพย์สิน/หนี้สิน (statement_type_id=4,5)',
        'can_have_extra': True,
    },
    'cash': {
        'patterns': [
            r'หน้า\s*9\b',
            r'รายละเอียดประกอบรายการเงินสด',
            r'เงินสด\s*\(ทั้งเงินบาท',
        ],
        'negative_patterns': [
            r'ข้อมูลส่วนบุคคล',
            r'หน้า\s*[1-4]\b',
        ],
        'step': 'step_6',
        'output_csv': 'asset.csv',
        'description': 'เงินสด',
        'can_have_extra': True,
    },
    'deposits': {
        'patterns': [
            r'หน้า\s*10\b',
            r'รายละเอียดประกอบรายการเงินฝาก',
            r'ประเภทบัญชี',
        ],
        'negative_patterns': [
            r'ข้อมูลส่วนบุคคล',
            r'หน้า\s*[1-4]\b',
        ],
        'step': 'step_6',
        'output_csv': 'asset.csv',
        'description': 'เงินฝาก',
        'can_have_extra': True,
    },
    'investments': {
        'patterns': [
            r'หน้า\s*11\b',
            r'รายละเอียดประกอบรายการเงินลงทุน',
            r'หุ้น.*หลักทรัพย์',
        ],
        'negative_patterns': [
            r'ข้อมูลส่วนบุคคล',
            r'หน้า\s*[1-4]\b',
        ],
        'step': 'step_6',
        'output_csv': 'asset.csv',
        'description': 'เงินลงทุน',
        'can_have_extra': True,
    },
    'loans_given': {
        'patterns': [
            r'หน้า\s*12\b',
            r'รายละเอียดประกอบรายการเงินให้กู้ยืม',
            r'ผู้กู้',
        ],
        'negative_patterns': [
            r'ข้อมูลส่วนบุคคล',
            r'หน้า\s*[1-4]\b',
        ],
        'step': 'step_6',
        'output_csv': 'asset.csv',
        'description': 'เงินให้กู้ยืม',
        'can_have_extra': True,
    },
    'land': {
        'patterns': [
            r'หน้า\s*13\b',
            r'รายละเอียดประกอบรายการที่ดิน',
            r'โฉนด',
            r'โฉนดที่ดิน',
            r'ส\.ป\.ก',
            r'ส\.ป\.ก\s*4-01',
            r'น\.ส\.3',
            r'น\.ส\.4',
            r'ไร่.*งาน.*ตร\.ว',
        ],
        'negative_patterns': [
            r'ข้อมูลส่วนบุคคล',
            r'หน้า\s*[1-4]\b',
        ],
        'step': 'step_7',
        'additional_steps': ['step_6'],
        'output_csv': 'asset_land_info.csv + asset.csv',
        'description': 'รายละเอียดที่ดิน',
        'can_have_extra': True,
    },
    'buildings': {
        'patterns': [
            r'หน้า\s*14\b',
            r'รายละเอียดประกอบรายการโรงเรือน',
            r'รายละเอียดประกอบรายการโรงเรือนและสิ่งปลูกสร้าง',
            r'สิ่งปลูกสร้าง',
            r'บ้านพักอาศัย',
        ],
        'negative_patterns': [
            r'หน้า\s*1\b',
            r'หน้า\s*2\b',
            r'หน้า\s*3\b',
            r'หน้า\s*4\b',
            r'ข้อมูลส่วนบุคคล',
            r'ที่อยู่ที่ติดต่อได้',
            r'ที่อยู่เดียวกันกับผู้ยื่นบัญชี',
        ],
        'required_patterns': [
            r'รายละเอียดประกอบรายการ',
        ],
        'step': 'step_8',
        'additional_steps': ['step_6'],
        'output_csv': 'asset_building_info.csv + asset.csv',
        'description': 'รายละเอียดโรงเรือน/สิ่งปลูกสร้าง',
        'can_have_extra': True,
    },
    'vehicles': {
        'patterns': [
            r'หน้า\s*15\b',
            r'รายละเอียดประกอบรายการยานพาหนะ',
            r'ทะเบียน.*รถ',
            r'รถยนต์',
            r'รถจักรยานยนต์',
            r'รถบรรทุก',
            r'รถตู้',
            r'รถกระบะ',
        ],
        'negative_patterns': [
            r'ข้อมูลส่วนบุคคล',
            r'หน้า\s*[1-4]\b',
        ],
        'step': 'step_9',
        'additional_steps': ['step_6'],
        'output_csv': 'asset_vehicle_info.csv + asset.csv',
        'description': 'รายละเอียดยานพาหนะ',
        'can_have_extra': True,
    },
    'concessions': {
        'patterns': [
            r'หน้า\s*16\b',
            r'รายละเอียดประกอบรายการสิทธิและสัมปทาน',
            r'สิทธิ.*สัมปทาน',
            r'กรมธรรม์',
            r'กองทุน',
        ],
        'negative_patterns': [
            r'ข้อมูลส่วนบุคคล',
            r'หน้า\s*[1-4]\b',
        ],
        'step': 'step_6',
        'output_csv': 'asset.csv',
        'description': 'สิทธิและสัมปทาน',
        'can_have_extra': True,
    },
    'other_assets': {
        'patterns': [
            r'หน้า\s*17\b',
            r'รายละเอียดประกอบรายการทรัพย์สินอื่น',
            r'นาฬิกา',
            r'เครื่องประดับ',
            r'ทองคำ',
            r'กระเป๋า',
            r'พระ',
            r'แหวน',
            r'สร้อย',
        ],
        'negative_patterns': [
            r'ข้อมูลส่วนบุคคล',
            r'หน้า\s*[1-4]\b',
        ],
        'step': 'step_10',
        'additional_steps': ['step_6'],
        'output_csv': 'asset_other_asset_info.csv + asset.csv',
        'description': 'รายละเอียดทรัพย์สินอื่น',
        'can_have_extra': True,
    },
    'overdraft': {
        'patterns': [
            r'หน้า\s*18\b',
            r'รายละเอียดประกอบรายการเงินเบิกเกินบัญชี',
        ],
        'negative_patterns': [
            r'ข้อมูลส่วนบุคคล',
            r'หน้า\s*[1-4]\b',
        ],
        'step': 'step_6',
        'output_csv': 'asset.csv',
        'description': 'เงินเบิกเกินบัญชี (หนี้สิน)',
        'can_have_extra': True,
    },
    'bank_loans': {
        'patterns': [
            r'หน้า\s*19\b',
            r'รายละเอียดประกอบรายการเงินกู้จากธนาคาร',
            r'เงินกู้.*สถาบันการเงิน',
        ],
        'negative_patterns': [
            r'ข้อมูลส่วนบุคคล',
            r'หน้า\s*[1-4]\b',
        ],
        'step': 'step_6',
        'output_csv': 'asset.csv',
        'description': 'เงินกู้จากธนาคาร (หนี้สิน)',
        'can_have_extra': True,
    },
    'written_debts': {
        'patterns': [
            r'หน้า\s*20\b',
            r'รายละเอียดประกอบรายการหนี้สินที่มีหลักฐาน',
            r'รายละเอียดประกอบรายการหนี้สินอื่น',
        ],
        'negative_patterns': [
            r'ข้อมูลส่วนบุคคล',
            r'หน้า\s*[1-4]\b',
        ],
        'step': 'step_6',
        'output_csv': 'asset.csv',
        'description': 'หนี้สินที่มีหลักฐาน',
        'can_have_extra': True,
    },
    'documents_list': {
        'patterns': [r'รายละเอียดของเอกสารประกอบ'],
        'step': None,
        'output_csv': None,
        'description': 'รายการเอกสารประกอบ',
    },
    'cover': {
        'patterns': [r'บัญชีทรัพย์สินและหนี้สิน', r'กรณีที่ยื่น', r'แบบ\s*ผย\.'],
        'step': None,
        'output_csv': None,
        'description': 'หน้าปก',
    },
    'instructions': {
        'patterns': [r'คำอธิบาย', r'ตัวอย่างเอกสารประกอบ'],
        'step': None,
        'output_csv': None,
        'description': 'คำอธิบาย/คำแนะนำ',
    },
}


def get_page_text(page: Dict, num_lines: int = 50) -> str:
    """Extract text from a page's first N lines."""
    lines = page.get('lines', [])[:num_lines]
    return ' '.join([line.get('content', '') for line in lines])


def get_full_page_text(page: Dict) -> str:
    """Extract ALL text from a page for comprehensive pattern matching."""
    lines = page.get('lines', [])
    return ' '.join([line.get('content', '') for line in lines])


def detect_page_type_with_regex(page: Dict) -> Tuple[str, float]:
    """
    Detect the type of a page based on content patterns.
    Supports negative patterns, required patterns, and priorities.

    Returns:
        Tuple of (page_type, confidence)
    """
    page_text = get_full_page_text(page)

    best_match = None
    best_score = 0
    best_priority = 0

    for page_type, config in PAGE_PATTERNS.items():
        patterns = config['patterns']
        negative_patterns = config.get('negative_patterns', [])
        required_patterns = config.get('required_patterns', [])
        priority = config.get('priority', 0)

        # Check negative patterns first - if any match, skip this page type
        has_negative = any(re.search(p, page_text, re.IGNORECASE) for p in negative_patterns)
        if has_negative:
            continue

        # Check required patterns - ALL must be present
        has_all_required = all(re.search(p, page_text, re.IGNORECASE) for p in required_patterns)
        if not has_all_required:
            continue

        # Count positive pattern matches
        matches = sum(1 for p in patterns if re.search(p, page_text, re.IGNORECASE))

        if matches > 0:
            score = matches / len(patterns)
            # Use priority for tie-breaking (higher priority wins)
            if score > best_score or (score == best_score and priority > best_priority):
                best_score = score
                best_match = page_type
                best_priority = priority

    return (best_match or 'unknown', best_score if best_match else 0)


def detect_all_page_types(page: Dict, threshold: float = 0.2) -> List[Tuple[str, float]]:
    """
    Detect ALL matching page types above threshold.
    This allows same page to map to multiple steps.

    Returns:
        List of (page_type, confidence) tuples, sorted by priority then score
    """
    page_text = get_full_page_text(page)
    matches = []

    for page_type, config in PAGE_PATTERNS.items():
        patterns = config['patterns']
        negative_patterns = config.get('negative_patterns', [])
        required_patterns = config.get('required_patterns', [])
        priority = config.get('priority', 0)

        # Check negative patterns
        has_negative = any(re.search(p, page_text, re.IGNORECASE) for p in negative_patterns)
        if has_negative:
            continue

        # Check required patterns
        has_all_required = all(re.search(p, page_text, re.IGNORECASE) for p in required_patterns)
        if not has_all_required:
            continue

        # Count positive pattern matches
        match_count = sum(1 for p in patterns if re.search(p, page_text, re.IGNORECASE))

        if match_count > 0:
            score = match_count / len(patterns)
            if score >= threshold:
                matches.append((page_type, score, priority))

    # Sort by priority (descending) then score (descending)
    matches.sort(key=lambda x: (-x[2], -x[1]))

    # Return only page_type and score
    return [(m[0], m[1]) for m in matches]


def detect_continuation_page(page: Dict, prev_page_type: str) -> bool:
    """Check if page is a continuation of the previous page type."""
    page_text = get_page_text(page, 10)

    # Continuation markers
    if re.search(r'\(ต่อ\)', page_text):
        return True

    # Check if it has same type-specific content
    if prev_page_type and prev_page_type in PAGE_PATTERNS:
        config = PAGE_PATTERNS[prev_page_type]
        if config.get('can_have_extra'):
            # Check for data patterns without header
            for pattern in config['patterns'][1:]:
                if re.search(pattern, page_text):
                    return True

    return False


def get_azure_openai_client():
    """Initialize Azure OpenAI client."""
    from openai import AzureOpenAI

    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-5-nano")

    if not endpoint or not api_key:
        raise ValueError(
            "Missing Azure OpenAI credentials. "
            "Please set AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_API_KEY in .env file."
        )

    # Clean endpoint - remove trailing slash and /openai/v1/ if present
    endpoint = endpoint.strip().rstrip('/')
    if endpoint.endswith('/openai/v1'):
        endpoint = endpoint[:-10]

    client = AzureOpenAI(
        azure_endpoint=endpoint,
        api_key=api_key,
        api_version="2024-02-15-preview"
    )

    return client, deployment


def detect_page_type_with_llm(
    client,
    deployment: str,
    page_text: str
) -> Tuple[str, float]:
    """Use Azure OpenAI to detect page type for uncertain cases."""
    prompt = f"""Analyze this Thai asset declaration document (NACC form) page and classify it.

CRITICAL DISTINCTION - personal_info vs spouse_info:

1. personal_info (step_1): Page for SUBMITTER (ผู้ยื่นบัญชี). Contains:
   * หน้า 1 (Page 1) in document
   * ข้อมูลส่วนบุคคล header
   * เลขประจำตัวประชาชน ผู้ยื่นบัญชี (ID of SUBMITTER)
   * ตำแหน่งปัจจุบันในหน่วยงานราชการ
   * ประวัติการทำงานย้อนหลัง 5 ปี
   * MUST NOT have "คู่สมรส" prominently

2. spouse_info (step_3): Page for SPOUSE (คู่สมรส). Contains:
   * หน้า 2 (Page 2) in document
   * คู่สมรส label prominently (REQUIRED)
   * สถานภาพการสมรส (marriage status)
   * ที่อยู่เดียวกันกับผู้ยื่นบัญชี

Page types:
- personal_info: Submitter page (หน้า 1, ข้อมูลส่วนบุคคล, ผู้ยื่นบัญชี)
- spouse_info: Spouse page (หน้า 2, คู่สมรส)
- children: Children list (หน้า 3, บุตร)
- siblings: Siblings list (หน้า 4, พี่น้อง)
- income_expense: Income/Expense (รายได้ต่อปี, รายจ่ายต่อปี)
- tax_info: Tax info (ภาษีเงินได้, ข้อมูลการเสียภาษี)
- assets_summary: Asset/Liability summary (ข้อมูลรายการทรัพย์สินและหนี้สิน, รวมทรัพย์สิน)
- cash: Cash details (รายละเอียดประกอบรายการเงินสด)
- deposits: Bank deposits (รายละเอียดประกอบรายการเงินฝาก)
- investments: Investments (รายละเอียดประกอบรายการเงินลงทุน)
- loans_given: Loans given (รายละเอียดประกอบรายการเงินให้กู้ยืม)
- land: Land details (รายละเอียดประกอบรายการที่ดิน, โฉนด)
- buildings: Buildings (รายละเอียดประกอบรายการโรงเรือน, สิ่งปลูกสร้าง)
- vehicles: Vehicles (รายละเอียดประกอบรายการยานพาหนะ, รถยนต์)
- concessions: Concessions (รายละเอียดประกอบรายการสิทธิและสัมปทาน)
- other_assets: Other assets (รายละเอียดประกอบรายการทรัพย์สินอื่น, นาฬิกา, เครื่องประดับ)
- overdraft: Overdraft liability (รายละเอียดประกอบรายการเงินเบิกเกินบัญชี)
- bank_loans: Bank loans liability (รายละเอียดประกอบรายการเงินกู้จากธนาคาร)
- written_debts: Written debts liability (รายละเอียดประกอบรายการหนี้สินที่มีหลักฐาน)
- cover: Cover page (บัญชีทรัพย์สินและหนี้สิน, กรณีที่ยื่น)
- instructions: Instructions (คำอธิบาย)
- documents_list: Document list (รายละเอียดของเอกสารประกอบ)
- unknown: Cannot determine

RULE: If "คู่สมรส" appears prominently -> spouse_info. If "ผู้ยื่นบัญชี" without คู่สมรส -> personal_info.

Page text (first 2500 chars):
{page_text[:2500]}

Respond ONLY with JSON: {{"page_type": "<type>", "confidence": <0.0-1.0>}}"""

    try:
        response = client.chat.completions.create(
            model=deployment,
            messages=[{"role": "user", "content": prompt}],
            max_completion_tokens=100,
        )

        result_text = response.choices[0].message.content.strip()

        # Try to extract JSON from response
        json_match = re.search(r'\{[^}]+\}', result_text)
        if json_match:
            result = json.loads(json_match.group())
            return (result.get('page_type', 'unknown'), result.get('confidence', 0.5))

        return ('unknown', 0)

    except Exception as e:
        print(f"  LLM error: {e}")
        return ('unknown', 0)


def map_document_pages_hybrid(
    json_path: Path,
    client=None,
    deployment: str = None,
    use_llm: bool = True
) -> DocumentMetadata:
    """
    Map pages using hybrid regex + LLM approach.

    Uses regex patterns with priorities first, then LLM for uncertain cases.
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    pages = data.get('pages', [])
    total_pages = len(pages)

    # Detect page types
    page_types = []
    prev_type = None

    for i, page in enumerate(pages):
        page_num = i + 1
        page_text = get_full_page_text(page)

        # First try regex pattern matching
        regex_type, regex_conf = detect_page_type_with_regex(page)

        # Use LLM if regex confidence is low and LLM is available
        if regex_conf < 0.4 and use_llm and client:
            llm_type, llm_conf = detect_page_type_with_llm(client, deployment, page_text)
            if llm_conf > regex_conf:
                page_type, confidence = llm_type, llm_conf
            else:
                page_type, confidence = regex_type, regex_conf
        else:
            page_type, confidence = regex_type, regex_conf

        # Check for continuation
        if page_type == 'unknown' and prev_type:
            if detect_continuation_page(page, prev_type):
                page_type = prev_type
                confidence = 0.7

        page_types.append({
            'page_number': page_num,
            'page_type': page_type,
            'confidence': confidence
        })

        if page_type != 'unknown':
            prev_type = page_type

        # Rate limiting for LLM
        if use_llm and client and regex_conf < 0.4:
            time.sleep(0.2)

    # Group by step
    step_pages: Dict[str, Dict] = {}
    unmapped = []

    for pt in page_types:
        primary_type = pt['page_type']
        page_num = pt['page_number']

        if primary_type == 'unknown':
            unmapped.append(page_num)
            continue

        # Get all matching page types for this page
        page_idx = page_num - 1
        if page_idx < len(pages):
            all_types = detect_all_page_types(pages[page_idx])
            # If detected type not in pattern matches, add it
            if primary_type not in [t[0] for t in all_types]:
                all_types.append((primary_type, pt.get('confidence', 0.8)))
        else:
            all_types = [(primary_type, pt.get('confidence', 0.8))]

        # Add page to ALL matching steps (including additional_steps)
        for page_type, _ in all_types:
            config = PAGE_PATTERNS.get(page_type, {})
            step = config.get('step')
            additional_steps = config.get('additional_steps', [])
            output_csv = config.get('output_csv', '')
            description = config.get('description', '')

            # Collect all steps to add this page to
            all_steps = []
            if step:
                all_steps.append((step, output_csv, description))
            for add_step in additional_steps:
                # Get output_csv for additional step
                for _, pt_config in PAGE_PATTERNS.items():
                    if pt_config.get('step') == add_step:
                        all_steps.append((add_step, pt_config.get('output_csv', ''), pt_config.get('description', '')))
                        break

            for current_step, csv_output, desc in all_steps:
                if current_step not in step_pages:
                    step_pages[current_step] = {
                        'output_csv': csv_output,
                        'description': desc,
                        'pages': [],
                        'types': []
                    }
                if page_num not in step_pages[current_step]['pages']:
                    step_pages[current_step]['pages'].append(page_num)
                if page_type not in step_pages[current_step]['types']:
                    step_pages[current_step]['types'].append(page_type)

    # Build step mappings
    step_mappings = []
    for step_name, info in step_pages.items():
        mapping = StepMapping(
            step_name=step_name,
            step_description=info['description'],
            output_csv=info['output_csv'],
            page_numbers=sorted(info['pages']),
            page_types=list(set(info['types']))
        )
        step_mappings.append(mapping)

    # Sort by step name
    step_mappings.sort(key=lambda x: x.step_name)

    return DocumentMetadata(
        doc_name=json_path.stem,
        total_pages=total_pages,
        step_mappings=step_mappings,
        unmapped_pages=sorted(unmapped)
    )


def save_metadata(metadata: DocumentMetadata, output_path: Path) -> None:
    """Save metadata to JSON file."""
    output_data = {
        'doc_name': metadata.doc_name,
        'total_pages': metadata.total_pages,
        'step_mappings': [asdict(m) for m in metadata.step_mappings],
        'unmapped_pages': metadata.unmapped_pages
    }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)


def process_phase1d(
    input_dir: Path,
    output_dir: Path,
    skip_existing: bool = False,
    clean: bool = False,
    use_llm: bool = True
) -> Dict[str, Any]:
    """
    Process all matched JSON files and create metadata mappings.

    Args:
        input_dir: Directory containing matched JSON files
        output_dir: Directory to save metadata output
        skip_existing: Skip if output already exists
        clean: Clean output directory before processing
        use_llm: Use LLM for uncertain page detection

    Returns:
        Dict with processing statistics
    """
    # Clean output directory if requested
    if clean and output_dir.exists():
        print(f"Cleaning output directory: {output_dir}")
        shutil.rmtree(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Phase 1d: Page Metadata Mapping (Hybrid Regex + AI)")
    print("=" * 60)
    print(f"Input:  {input_dir}")
    print(f"Output: {output_dir}")
    print(f"Use LLM: {use_llm}")
    print()

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    # Initialize Azure OpenAI client if using LLM
    client = None
    deployment = None
    if use_llm:
        try:
            client, deployment = get_azure_openai_client()
            print(f"Using Azure OpenAI deployment: {deployment}")
        except Exception as e:
            print(f"Warning: Could not initialize Azure OpenAI: {e}")
            print("Falling back to regex-only mode")
            use_llm = False

    json_files = list(input_dir.glob('*.json'))

    stats = {
        'total': len(json_files),
        'processed': 0,
        'skipped': 0,
        'errors': []
    }

    print(f"Found {len(json_files)} documents to process")
    print("-" * 60)

    all_metadata = []

    for i, json_file in enumerate(sorted(json_files), 1):
        output_file = output_dir / f"{json_file.stem}_metadata.json"

        if skip_existing and output_file.exists():
            print(f"[{i}/{len(json_files)}] SKIP: {json_file.name[:50]}...")
            stats['skipped'] += 1
            continue

        print(f"[{i}/{len(json_files)}] {json_file.name[:50]}...", end=" ", flush=True)

        try:
            metadata = map_document_pages_hybrid(
                json_file,
                client=client,
                deployment=deployment,
                use_llm=use_llm
            )

            # Save individual metadata
            save_metadata(metadata, output_file)

            all_metadata.append(metadata)
            stats['processed'] += 1

            # Print summary
            steps_found = len(metadata.step_mappings)
            unmapped = len(metadata.unmapped_pages)
            print(f"OK ({metadata.total_pages} pages, {steps_found} steps, {unmapped} unmapped)")

        except Exception as e:
            print(f"ERROR: {str(e)[:50]}")
            stats['errors'].append((json_file.name, str(e)))

    # Save combined index
    if all_metadata:
        index_data = {
            'total_documents': len(all_metadata),
            'documents': []
        }

        for meta in all_metadata:
            doc_info = {
                'doc_name': meta.doc_name,
                'total_pages': meta.total_pages,
                'steps': {m.step_name: m.page_numbers for m in meta.step_mappings},
                'unmapped_pages': meta.unmapped_pages
            }
            index_data['documents'].append(doc_info)

        index_file = output_dir / 'index.json'
        with open(index_file, 'w', encoding='utf-8') as f:
            json.dump(index_data, f, ensure_ascii=False, indent=2)

        print(f"\nCreated index at {index_file}")

    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Processed: {stats['processed']}/{stats['total']}")
    print(f"Skipped: {stats['skipped']}")
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
    """Main entry point for Phase 1d."""
    parser = argparse.ArgumentParser(
        description="Phase 1d: Map document pages to extraction steps using hybrid regex + AI"
    )
    parser.add_argument(
        "--final",
        action="store_true",
        help="Process test final data instead of training data"
    )
    parser.add_argument(
        "--skip",
        action="store_true",
        help="Skip existing files"
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean output directory before processing"
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Disable LLM, use regex-only mode"
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
        output_dir = src_dir / "result" / "final" / "processing_input" / "page_metadata"
    else:
        output_dir = src_dir / "result" / "from_train" / "processing_input" / "page_metadata"

    process_phase1d(
        input_dir=input_dir,
        output_dir=output_dir,
        skip_existing=args.skip,
        clean=args.clean,
        use_llm=not args.no_llm
    )


if __name__ == "__main__":
    main()
