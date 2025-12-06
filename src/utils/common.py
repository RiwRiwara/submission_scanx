"""
Common utility functions shared across all pipeline steps.

This module consolidates duplicated code from step_1.py through step_11.py including:
- Thai month mapping and date parsing
- OCR text cleaning and validation
- Polygon-based spatial utilities
- Name extraction utilities
"""

import re
from typing import List, Dict, Optional, Tuple, Any


# =============================================================================
# CONSTANTS
# =============================================================================

# Thai month mapping for date parsing
THAI_MONTHS = {
    'มกราคม': 1, 'ม.ค.': 1, 'ม.ค': 1, 'มค': 1,
    'กุมภาพันธ์': 2, 'ก.พ.': 2, 'ก.พ': 2, 'กพ': 2,
    'มีนาคม': 3, 'มี.ค.': 3, 'มี.ค': 3, 'มีค': 3,
    'เมษายน': 4, 'เม.ย.': 4, 'เม.ย': 4, 'เมย': 4,
    'พฤษภาคม': 5, 'พ.ค.': 5, 'พ.ค': 5, 'พค': 5,
    'มิถุนายน': 6, 'มิ.ย.': 6, 'มิ.ย': 6, 'มิย': 6,
    'กรกฎาคม': 7, 'ก.ค.': 7, 'ก.ค': 7, 'กค': 7,
    'สิงหาคม': 8, 'ส.ค.': 8, 'ส.ค': 8, 'สค': 8,
    'กันยายน': 9, 'ก.ย.': 9, 'ก.ย': 9, 'กย': 9,
    'ตุลาคม': 10, 'ต.ค.': 10, 'ต.ค': 10, 'ตค': 10,
    'พฤศจิกายน': 11, 'พ.ย.': 11, 'พ.ย': 11, 'พย': 11,
    'ธันวาคม': 12, 'ธ.ค.': 12, 'ธ.ค': 12, 'ธค': 12,
}

# Thai titles for name parsing (ordered by length - longer first)
# This comprehensive list covers military, police, religious, and civilian titles
THAI_TITLES = [
    # ===== POLICE (ตำรวจ) - with หญิง suffix first =====
    'พลตำรวจเอกหญิง', 'พลตำรวจโทหญิง', 'พลตำรวจตรีหญิง',
    'พันตำรวจเอกหญิง', 'พันตำรวจโทหญิง', 'พันตำรวจตรีหญิง',
    'ร้อยตำรวจเอกหญิง', 'ร้อยตำรวจโทหญิง', 'ร้อยตำรวจตรีหญิง',
    'นายดาบตำรวจหญิง', 'จ่าสิบตำรวจหญิง',
    'สิบตำรวจเอกหญิง', 'สิบตำรวจโทหญิง', 'สิบตำรวจตรีหญิง',
    # Police - full titles
    'พลตำรวจเอก', 'พลตำรวจโท', 'พลตำรวจตรี',
    'พันตำรวจเอก', 'พันตำรวจโท', 'พันตำรวจตรี',
    'ร้อยตำรวจเอก', 'ร้อยตำรวจโท', 'ร้อยตำรวจตรี',
    'นายดาบตำรวจ', 'จ่าสิบตำรวจ',
    'สิบตำรวจเอก', 'สิบตำรวจโท', 'สิบตำรวจตรี',
    'พลตำรวจ',
    # Police - abbreviated with หญิง
    'พล.ต.อ.หญิง', 'พล.ต.ท.หญิง', 'พล.ต.ต.หญิง',
    'พ.ต.อ.หญิง', 'พ.ต.ท.หญิง', 'พ.ต.ต.หญิง',
    'ร.ต.อ.หญิง', 'ร.ต.ท.หญิง', 'ร.ต.ต.หญิง',
    'ด.ต.หญิง', 'จ.ส.ต.หญิง',
    'ส.ต.อ.หญิง', 'ส.ต.ท.หญิง', 'ส.ต.ต.หญิง',
    # Police - abbreviated
    'พล.ต.อ.', 'พล.ต.ท.', 'พล.ต.ต.',
    'พ.ต.อ.', 'พ.ต.ท.', 'พ.ต.ต.',
    'ร.ต.อ.', 'ร.ต.ท.', 'ร.ต.ต.',
    'ด.ต.', 'จ.ส.ต.',
    'ส.ต.อ.', 'ส.ต.ท.', 'ส.ต.ต.',

    # ===== ARMY (ทหารบก) - with หญิง suffix first =====
    'พลเอกหญิง', 'พลโทหญิง', 'พลตรีหญิง',
    'พันเอกหญิง', 'พันโทหญิง', 'พันตรีหญิง',
    'ร้อยเอกหญิง', 'ร้อยโทหญิง', 'ร้อยตรีหญิง',
    'จ่าสิบเอกหญิง', 'จ่าสิบโทหญิง', 'จ่าสิบตรีหญิง',
    'สิบเอกหญิง', 'สิบโทหญิง', 'สิบตรีหญิง',
    # Army - full titles
    'พลเอก', 'พลโท', 'พลตรี',
    'พันเอก', 'พันโท', 'พันตรี',
    'ร้อยเอก', 'ร้อยโท', 'ร้อยตรี',
    'จ่าสิบเอก', 'จ่าสิบโท', 'จ่าสิบตรี',
    'สิบเอก', 'สิบโท', 'สิบตรี',
    'พลทหาร',
    # Army - abbreviated with หญิง
    'พล.อ.หญิง', 'พล.ท.หญิง', 'พล.ต.หญิง',
    'พ.อ.หญิง', 'พ.ท.หญิง', 'พ.ต.หญิง',
    'ร.อ.หญิง', 'ร.ท.หญิง', 'ร.ต.หญิง',
    'จ.ส.อ.หญิง', 'จ.ส.ท.หญิง', 'จ.ส.ต.หญิง',
    'ส.อ.หญิง', 'ส.ท.หญิง', 'ส.ต.หญิง',
    # Army - abbreviated
    'พล.อ.', 'พล.ท.', 'พล.ต.',
    'พ.อ.', 'พ.ท.', 'พ.ต.',
    'ร.อ.', 'ร.ท.', 'ร.ต.',
    'จ.ส.อ.', 'จ.ส.ท.', 'จ.ส.ต.',
    'ส.อ.', 'ส.ท.', 'ส.ต.',
    'พลฯ',
    # Army - with space variants
    'พล. อ.', 'พล. ท.', 'พล. ต.',

    # ===== NAVY (ทหารเรือ) - with หญิง suffix first =====
    'พลเรือเอกหญิง', 'พลเรือโทหญิง', 'พลเรือตรีหญิง',
    'นาวาเอกหญิง', 'นาวาโทหญิง', 'นาวาตรีหญิง',
    'เรือเอกหญิง', 'เรือโทหญิง', 'เรือตรีหญิง',
    'พันจ่าเอกหญิง', 'พันจ่าโทหญิง', 'พันจ่าตรีหญิง',
    'จ่าเอกหญิง', 'จ่าโทหญิง', 'จ่าตรีหญิง',
    # Navy - full titles
    'พลเรือเอก', 'พลเรือโท', 'พลเรือตรี',
    'นาวาเอก', 'นาวาโท', 'นาวาตรี',
    'เรือเอก', 'เรือโท', 'เรือตรี',
    'พันจ่าเอก', 'พันจ่าโท', 'พันจ่าตรี',
    'จ่าเอก', 'จ่าโท', 'จ่าตรี',
    # Navy - abbreviated with หญิง
    'พล.ร.อ.หญิง', 'พล.ร.ท.หญิง', 'พล.ร.ต.หญิง',
    'น.อ.หญิง', 'น.ท.หญิง', 'น.ต.หญิง',
    'ร.อ.หญิง', 'ร.ท.หญิง', 'ร.ต.หญิง',
    'พ.จ.อ.หญิง', 'พ.จ.ท.หญิง', 'พ.จ.ต.หญิง',
    'จ.อ.หญิง', 'จ.ท.หญิง', 'จ.ต.หญิง',
    # Navy - abbreviated
    'พล.ร.อ.', 'พล.ร.ท.', 'พล.ร.ต.',
    'น.อ.', 'น.ท.', 'น.ต.',
    'พ.จ.อ.', 'พ.จ.ท.', 'พ.จ.ต.',
    'จ.อ.', 'จ.ท.', 'จ.ต.',

    # ===== AIR FORCE (ทหารอากาศ) - with หญิง suffix first =====
    'พลอากาศเอกหญิง', 'พลอากาศโทหญิง', 'พลอากาศตรีหญิง',
    'นาวาอากาศเอกหญิง', 'นาวาอากาศโทหญิง', 'นาวาอากาศตรีหญิง',
    'เรืออากาศเอกหญิง', 'เรืออากาศโทหญิง', 'เรืออากาศตรีหญิง',
    'พันจ่าอากาศเอกหญิง', 'พันจ่าอากาศโทหญิง', 'พันจ่าอากาศตรีหญิง',
    'จ่าอากาศเอกหญิง', 'จ่าอากาศโทหญิง', 'จ่าอากาศตรีหญิง',
    # Air Force - full titles
    'พลอากาศเอก', 'พลอากาศโท', 'พลอากาศตรี',
    'นาวาอากาศเอก', 'นาวาอากาศโท', 'นาวาอากาศตรี',
    'เรืออากาศเอก', 'เรืออากาศโท', 'เรืออากาศตรี',
    'พันจ่าอากาศเอก', 'พันจ่าอากาศโท', 'พันจ่าอากาศตรี',
    'จ่าอากาศเอก', 'จ่าอากาศโท', 'จ่าอากาศตรี',
    # Air Force - abbreviated with หญิง
    'พล.อ.อ.หญิง', 'พล.อ.ท.หญิง', 'พล.อ.ต.หญิง',
    'น.อ.หญิง', 'น.ท.หญิง', 'น.ต.หญิง',
    'ร.อ.หญิง', 'ร.ท.หญิง', 'ร.ต.หญิง',
    'พ.อ.อ.หญิง', 'พ.อ.ท.หญิง', 'พ.อ.ต.หญิง',
    'จ.อ.หญิง', 'จ.ท.หญิง', 'จ.ต.หญิง',
    # Air Force - abbreviated
    'พล.อ.อ.', 'พล.อ.ท.', 'พล.อ.ต.',
    'พ.อ.อ.', 'พ.อ.ท.', 'พ.อ.ต.',

    # ===== RELIGIOUS (พระภิกษุ) =====
    'พระครูธรรมธร', 'พระครูวินัยธร', 'พระครูปลัด', 'พระครูสมุห์', 'พระครูใบฎีกา',
    'พระอธิการ', 'เจ้าอธิการ', 'พระปลัด', 'พระสมุห์', 'พระใบฎีกา',
    'พระมหา', 'พระครู', 'พระ',
    'สามเณร', 'บาทหลวง',

    # ===== ROYAL (ราชวงศ์) =====
    'หม่อมราชวงศ์', 'หม่อมหลวง',
    'ม.ร.ว.', 'ม.ล.',

    # ===== ACADEMIC (วิชาการ) =====
    'ศาสตราจารย์เกียรติคุณ', 'ศาสตราจารย์พิเศษ',
    'รองศาสตราจารย์', 'ผู้ช่วยศาสตราจารย์',
    'ศาสตราจารย์', 'อาจารย์',
    'ศ.ดร.', 'รศ.ดร.', 'ผศ.ดร.',
    'ศ.', 'รศ.', 'ผศ.', 'ดร.',

    # ===== COMMON TITLES (ทั่วไป) =====
    'นางสาว', 'เด็กชาย', 'เด็กหญิง',
    'นาง', 'นาย',
    # Common OCR variants / abbreviations
    'น.ส.', 'นส.', 'นาส.', 'น.ส',  # OCR variants for นางสาว
    'ด.ช.', 'ด.ญ.',  # abbreviations for เด็กชาย, เด็กหญิง
]

# OCR title variants mapping - map OCR errors to correct titles
TITLE_OCR_VARIANTS = {
    'น.ส.': 'นางสาว',
    'นส.': 'นางสาว',
    'นาส.': 'นางสาว',
    'น.ส': 'นางสาว',
    'ด.ช.': 'เด็กชาย',
    'ด.ญ.': 'เด็กหญิง',
}

# Common labels to skip when validating Thai names
# Note: Labels like 'ประเทศ' (country) removed as they can be part of names (e.g., อประเทศ)
NAME_SKIP_LABELS = [
    'ชื่อเดิม', 'ชื่อสกุลเดิม', 'นามสกุลเดิม', 'สถานภาพ', 'โสด',
    'สมรส', 'ที่อยู่', 'โปรดระบุ', 'คำนำหน้า', 'ชื่อและชื่อสกุล',
    'กรณีคู่สมรส', 'หนังสือเดินทาง', 'สัญชาติ',
    'อาชีพ', 'สถานที่ทำงาน', 'อายุ', 'ลำดับ', 'บิดา', 'มารดา',
    'บุตร', 'พี่น้อง', 'สถานศึกษา', 'ที่ทำงาน', 'วันเดือนปี', 'ตาย',
    'ถึงแก่กรรม', 'ชื่อ-ชื่อสกุล'
]

# Patterns that indicate OCR noise (not valid names) - use regex to match exactly
OCR_NOISE_PATTERNS = [
    r'^\d+\s*ปี\s*$',              # "66 ปี", "42 ปี" - age patterns
    r'^ปี\s*$',                     # Just "ปี"
    r'^[ๆ\s]+$',                    # Just Thai repetition mark
    r'^\d+$',                        # Just numbers
    r'^[-.\s:]+$',                   # Just punctuation
    r'^น?วันเดือนปี.*$',              # วันเดือนปี... or นวันเดือนปี...
    r'^น?เดือนปี.*$',                 # เดือนปี... or นเดือนปี...
    r'^\d+\s*ปี.*เกิด.*$',           # "66 ปี เกิด :"
]

# Polygon detection parameters
Y_TOLERANCE = 0.3


# =============================================================================
# TEXT CLEANING FUNCTIONS
# =============================================================================

def clean_text(text: str) -> str:
    """Clean OCR text by removing extra whitespace."""
    if not text:
        return ''
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def clean_ocr_text(text: str) -> str:
    """
    Clean OCR artifacts and fix common spacing issues in Thai text.
    
    This is the primary text cleaning function used across all steps.
    """
    if not text:
        return ''

    # Common OCR spacing fixes for Thai text
    replacements = [
        ('สมาชิกสภาผู้แทนราษฎร แบบบัญชีรายชื่อ', 'สมาชิกสภาผู้แทนราษฎรแบบบัญชีรายชื่อ'),
        ('ในพระ บรมราชูปถัมภ์', 'ในพระบรมราชูปถัมภ์'),
        ('ในพระ บนมราชูปถัมภ์', 'ในพระบรมราชูปถัมภ์'),
        ('สวัสดิการ สังคม', 'สวัสดิการสังคม'),
        ('จำกัด (มหาชน)', 'จำกัด (มหาชน)'),
    ]

    result = text
    for old, new in replacements:
        result = result.replace(old, new)

    # Remove extra spaces
    result = re.sub(r'\s+', ' ', result)
    return result.strip()


def clean_position_text(text: str) -> str:
    """
    Clean position/job title text by removing OCR artifacts and noise.

    This function removes common OCR errors found in position fields like:
    - Leading garbage text (มีเชียงชาต, ไต๋วันหนึ่ง, etc.)
    - "ตาแหน่ง" OCR error of "ตำแหน่ง"
    - Random prefixes before actual position text
    """
    if not text:
        return ''

    text = text.strip()

    # Common OCR noise prefixes to remove from position text
    noise_prefixes = [
        r'^มีเชียงชาต\s*',           # OCR garbage
        r'^ไต๋วันหนึ่ง\s*',           # OCR garbage (should be ปีที่หนึ่ง or similar)
        r'^ตาแหน่ง\s*',              # OCR error of "ตำแหน่ง"
        r'^ตำแหน่ง\s*:?\s*',          # "ตำแหน่ง:" label
        r'^\d+\.\s*',                # Numbered list prefix "1. "
        r'^[-\s]+',                  # Leading dashes/spaces
    ]

    result = text
    for pattern in noise_prefixes:
        result = re.sub(pattern, '', result, flags=re.IGNORECASE)

    # Remove trailing garbage patterns
    noise_suffixes = [
        r'\s*ปี\s*$',                # Trailing "ปี"
        r'\s*[-:]\s*$',              # Trailing punctuation
    ]

    for pattern in noise_suffixes:
        result = re.sub(pattern, '', result)

    # Clean up spacing
    result = re.sub(r'\s+', ' ', result)
    return result.strip()


def clean_number(text: str) -> Optional[float]:
    """Extract and clean numeric value from text.

    Handles various number formats including:
    - Standard format: 1,234.56
    - European format: 1.234,56 (dot as thousands, comma as decimal)
    - OCR error format: 1,234,56 (comma as decimal instead of period)
    - Dash as decimal separator: 553983-43 → 553983.43
    - OCR split numbers with H/h prefix: 46778 and H52 → 46778.52
    - Spaced numbers: 2 300 0 0 0 → 2300000, 15 0000 00 → 150000.00
    - Standard decimal: 3500000.00
    """
    if not text:
        return None

    text = text.strip()

    # Skip if just dashes, dots, empty or markers
    if re.match(r'^[\-\s./*:]+$', text) or text in ['-', '', '1', 'l']:
        return None

    # First, handle spaced numbers like "2 300 0 0 0" or "15 0000 00" or "1 600 000 00"
    # These are OCR errors where spaces appear within numbers
    # Pattern: digits with spaces, possibly ending with 00 (decimal)
    if ' ' in text:
        # Remove all spaces first
        no_space = text.replace(' ', '')
        # Check if it ends with 00 (likely .00 decimal)
        # But only if the result looks like a valid number with decimal
        if re.match(r'^\d+00$', no_space) and len(no_space) >= 5:
            # Check if the last 2 digits are decimal (00)
            # Heuristic: if removing 00 gives a round number, treat as .00
            integer_part = no_space[:-2]
            text = f"{integer_part}.00"
        else:
            text = no_space

    # Handle standard decimal format like "3500000.00" first
    standard_decimal_match = re.match(r'^(\d+)\.(\d{2})$', text)
    if standard_decimal_match:
        # Already in correct format, proceed to extraction
        pass
    # Handle dash as decimal separator (e.g., "553983-43" → "553983.43")
    # Pattern: digits-digits where second part is 1-2 digits (decimal part)
    elif re.search(r'^(\d+)-(\d{1,2})$', text):
        dash_decimal_match = re.search(r'^(\d+)-(\d{1,2})$', text)
        integer_part = dash_decimal_match.group(1)
        decimal_part = dash_decimal_match.group(2)
        text = f"{integer_part}.{decimal_part}"
    # Handle dash with 00 at end (e.g., "27825000-00" → "27825000.00")
    elif re.search(r'^(\d+)-(00)$', text):
        dash_double_zero_match = re.search(r'^(\d+)-(00)$', text)
        integer_part = dash_double_zero_match.group(1)
        decimal_part = dash_double_zero_match.group(2)
        text = f"{integer_part}.{decimal_part}"
    # Detect and handle European number format (e.g., "102.000,00" = 102000.00)
    elif re.search(r'([\d]+(?:\.[\d]{3})+),(\d{2})$', text):
        european_match = re.search(r'([\d]+(?:\.[\d]{3})+),(\d{2})$', text)
        integer_part = european_match.group(1).replace('.', '')
        decimal_part = european_match.group(2)
        text = f"{integer_part}.{decimal_part}"
    # Also handle partial European format like "2.000.00" -> "2000.00"
    elif re.search(r'^([\d]+(?:\.[\d]{3})+)\.(\d{2})$', text):
        partial_euro_match = re.search(r'^([\d]+(?:\.[\d]{3})+)\.(\d{2})$', text)
        integer_part = partial_euro_match.group(1).replace('.', '')
        decimal_part = partial_euro_match.group(2)
        text = f"{integer_part}.{decimal_part}"
    # Handle OCR comma-decimal format (e.g., "70344,89" → "70344.89")
    elif re.search(r'^(\d+),(\d{2})$', text):
        comma_decimal_match = re.search(r'^(\d+),(\d{2})$', text)
        integer_part = comma_decimal_match.group(1)
        decimal_part = comma_decimal_match.group(2)
        text = f"{integer_part}.{decimal_part}"
    # Handle OCR error where decimal point is read as comma with thousands
    elif re.search(r'^([\d,]+),(\d{2})$', text):
        ocr_comma_decimal_match = re.search(r'^([\d,]+),(\d{2})$', text)
        integer_part = ocr_comma_decimal_match.group(1).replace(',', '')
        decimal_part = ocr_comma_decimal_match.group(2)
        text = f"{integer_part}.{decimal_part}"
    else:
        # Standard format: remove commas and spaces
        text = text.replace(',', '').replace(' ', '')

    # Fix OCR errors like : instead of .
    text = text.replace(':', '.')

    # Extract number pattern
    match = re.search(r'[\-]?[\d]+(?:\.\d+)?', text)
    if match:
        num_str = match.group()
        try:
            val = float(num_str)
            # Filter out small numbers (page numbers, indices) but keep values >= 100
            if val >= 100:
                return val
        except ValueError:
            return None

    return None


def clean_number_with_decimal_fragment(main_text: str, decimal_fragment: str = None) -> Optional[float]:
    """
    Extract and clean numeric value, optionally merging a decimal fragment.

    This handles OCR splitting numbers like "46778" and "52" or "-52" into separate text elements.

    Args:
        main_text: The main number text (e.g., "46778", "3020470")
        decimal_fragment: Optional decimal fragment (e.g., "52", "-52", ".52", "H52")

    Returns:
        Float value or None
    """
    if not main_text:
        return None

    main_text = main_text.strip()

    # First try clean_number on main_text (handles dash-decimal like "553983-43")
    result = clean_number(main_text)

    # If no decimal_fragment or main_text already has decimal, return result
    if not decimal_fragment or '.' in main_text or '-' in main_text[1:]:
        return result

    # Try to merge decimal fragment
    decimal_fragment = decimal_fragment.strip()

    # Skip empty or invalid fragments
    if not decimal_fragment or decimal_fragment in ['-', '.', '.-', '-.']:
        return result

    # Extract digits from decimal fragment (handles "H52", "-52", ".52", "00")
    # OCR sometimes reads decimal points as 'H', 'h', '-', etc.
    frag_match = re.search(r'[Hh\-.]?(\d{1,2})$', decimal_fragment)
    if frag_match:
        decimal_digits = frag_match.group(1)

        # Get integer part from main_text
        int_match = re.search(r'(\d+)', main_text)
        if int_match:
            integer_part = int_match.group(1)
            combined = f"{integer_part}.{decimal_digits}"
            try:
                val = float(combined)
                if val >= 100:
                    return val
            except ValueError:
                pass

    return result


# =============================================================================
# NAME VALIDATION AND EXTRACTION
# =============================================================================

def is_valid_thai_name(text: str, additional_skip_labels: List[str] = None) -> bool:
    """
    Check if text looks like a valid Thai name (not a label or empty).

    Args:
        text: The text to validate
        additional_skip_labels: Additional labels to skip besides the default ones
    """
    if not text:
        return False

    text_stripped = text.strip()
    # Also strip common quotes and punctuation that OCR may add
    text_for_pattern = text_stripped.strip('"\'`.,;:!?()[]{}')

    # Remove common non-name characters
    cleaned = re.sub(r'[.\s:]+', '', text_stripped)
    if len(cleaned) < 2:
        return False

    # Must contain Thai characters
    if not re.search(r'[ก-๙]', cleaned):
        return False

    # Check against OCR noise patterns (using cleaned text without quotes)
    for pattern in OCR_NOISE_PATTERNS:
        if re.match(pattern, text_for_pattern):
            return False

    # Build skip labels list
    skip_labels = NAME_SKIP_LABELS.copy()
    if additional_skip_labels:
        skip_labels.extend(additional_skip_labels)

    # Skip if it's a label
    for label in skip_labels:
        if label in text or text_stripped == label:
            return False

    return True


def extract_title_and_name(full_name: str) -> Tuple[str, str, str]:
    """
    Extract title, first name, last name from Thai full name.

    Args:
        full_name: Full Thai name including title

    Returns:
        Tuple of (title, first_name, last_name)
    """
    if not full_name:
        return '', '', ''

    full_name = clean_ocr_text(full_name)

    # Strip trailing dots and common punctuation
    full_name = full_name.rstrip('.').rstrip()

    title = ''
    name_part = full_name

    for t in THAI_TITLES:
        if full_name.startswith(t):
            title = t
            name_part = full_name[len(t):].strip()
            break

    # Normalize OCR title variants to canonical forms
    if title in TITLE_OCR_VARIANTS:
        title = TITLE_OCR_VARIANTS[title]

    # Strip any remaining dots from name part
    name_part = name_part.rstrip('.').strip()

    # Split remaining into first and last name
    parts = name_part.split()
    if len(parts) >= 2:
        first_name = parts[0].rstrip('.')
        last_name = ' '.join(parts[1:]).rstrip('.')
    elif len(parts) == 1:
        first_name = parts[0].rstrip('.')
        last_name = ''
    else:
        first_name = ''
        last_name = ''

    return title, first_name, last_name


# =============================================================================
# DATE PARSING FUNCTIONS
# =============================================================================

def parse_thai_date(date_str: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """
    Parse Thai date string and extract day, month, year.

    Handles formats like:
    - "5 มิ.ย.2562" (Thai month name)
    - "23/4/2562" (DD/MM/YYYY numeric)
    - "2559" (year only)

    Args:
        date_str: Thai date string

    Returns:
        Tuple of (day, month, year) - any can be None
    """
    if not date_str or date_str.strip() in ['-', '']:
        return None, None, None

    date_str = date_str.strip()

    # Pattern 1: "5 มิ.ย.2562" (Thai month name)
    pattern1 = r'(\d{1,2})\s*([ก-๙\.]+\.?)\s*(\d{4})'
    match = re.search(pattern1, date_str)
    if match:
        day = int(match.group(1))
        month_str = match.group(2).strip().rstrip('.')
        month = THAI_MONTHS.get(month_str) or THAI_MONTHS.get(month_str + '.')
        year = int(match.group(3))
        if year > 2500:
            year = year - 543
        return day, month, year

    # Pattern 2: "23/4/2562" or "23/04/2562" (DD/MM/YYYY numeric format)
    pattern2 = r'(\d{1,2})\s*/\s*(\d{1,2})\s*/\s*(\d{4})'
    match = re.search(pattern2, date_str)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year = int(match.group(3))
        if year > 2500:
            year = year - 543
        return day, month, year

    # Pattern 3: just year "2559"
    pattern3 = r'^(\d{4})$'
    match = re.match(pattern3, date_str.strip())
    if match:
        year = int(match.group(1))
        if year > 2500:
            year = year - 543
        return None, None, year

    return None, None, None


def parse_date_range(date_range_str: str) -> Tuple[Tuple[Optional[int], Optional[int], Optional[int]], 
                                                    Tuple[Optional[int], Optional[int], Optional[int]]]:
    """
    Parse date range string like '5 มิ.ย.2562-16 ส.ค.2565'.
    
    Returns:
        Tuple of ((start_day, start_month, start_year), (end_day, end_month, end_year))
    """
    if not date_range_str or date_range_str.strip() == '-':
        return (None, None, None), (None, None, None)

    parts = re.split(r'\s*[-–]\s*', date_range_str.strip())

    if len(parts) >= 2:
        start = parse_thai_date(parts[0])
        end = parse_thai_date(parts[-1])
        return start, end
    elif len(parts) == 1:
        start = parse_thai_date(parts[0])
        return start, (None, None, None)

    return (None, None, None), (None, None, None)


def parse_marriage_date(text: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """
    Parse marriage date from various text formats.

    Handles patterns like:
    - 'จดทะเบียนสมรส เมื่อวันที่ 30 / เมษายน / 2565'
    - 'เมื่อวันที่ 18 พฤษภาคม 2556'
    - '31พค.' (day + abbreviated month)
    - '11มกราคม 2534'
    - 'วันที่ 22 มิถุนายน / 2527'
    - '12/ก.ย. /2531' (abbreviated with dots)
    """
    if not text:
        return None, None, None

    # Skip empty date patterns (only dots, slashes, dashes)
    cleaned_for_check = re.sub(r'เมื่อวันที่\s*', '', text)
    if re.match(r'^[\s./\-]+$', cleaned_for_check.strip()):
        return None, None, None

    # Try numeric date format first: DD/MM/YYYY or D/MM/YYYY (e.g., "3/08/2525")
    pattern_numeric = r'(\d{1,2})\s*/\s*(\d{1,2})\s*/\s*(\d{4})'
    match = re.search(pattern_numeric, text)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year = int(match.group(3))
        if 1 <= month <= 12:  # Valid month
            return day, month, year

    # Try pattern with abbreviated month like "12/ก.ย. /2531" first (before cleaning)
    # This handles formats like: 12/ก.ย./2531, 25/ก.พ./2525, 30/พ.ค./2563, 15/มิ.ย./2545
    # Pattern for abbreviations with dots: DD/X.X./YYYY or DD/XX.X./YYYY
    pattern_abbrev = r'(\d{1,2})\s*/\s*([ก-๙]{1,2})\s*\.?\s*([ก-๙])\s*\.?\s*/\s*(\d{4})'
    match = re.search(pattern_abbrev, text)
    if match:
        day = int(match.group(1))
        month_part1 = match.group(2)  # First part of abbreviated month (1-2 chars)
        month_part2 = match.group(3)  # Second part (1 char)
        year = int(match.group(4))

        # Build possible abbreviation variants
        abbrev_variants = [
            month_part1 + '.' + month_part2 + '.',  # ก.ย. or มี.ค.
            month_part1 + '.' + month_part2,         # ก.ย
            month_part1 + month_part2,               # กย
        ]

        # Match abbreviated Thai months
        month = None
        for abbrev in abbrev_variants:
            if abbrev in THAI_MONTHS:
                month = THAI_MONTHS[abbrev]
                break

        if not month:
            # Try matching by comparing cleaned versions
            cleaned_abbrev = (month_part1 + month_part2).lower()
            for m_name, m_num in THAI_MONTHS.items():
                m_clean = m_name.replace('.', '').lower()
                if m_clean == cleaned_abbrev or m_clean.startswith(cleaned_abbrev):
                    month = m_num
                    break

        if month:
            return day, month, year

    # Clean the text for other patterns
    text_clean = text.replace('.', ' ').replace('/', ' ').strip()
    text_clean = re.sub(r'\s+', ' ', text_clean)

    # Pattern 1: "DD month YYYY" or "DD month / YYYY" - full Thai month name
    # e.g., "18 พฤษภาคม 2556", "22 มิถุนายน 2527"
    pattern1 = r'(\d{1,2})\s*([ก-๙]+)\s*(\d{4})'
    match = re.search(pattern1, text_clean)
    if match:
        day = int(match.group(1))
        month_str = match.group(2).strip()
        year = int(match.group(3))

        # Try to match month
        month = THAI_MONTHS.get(month_str)
        if not month:
            # Try with period suffix
            month = THAI_MONTHS.get(month_str + '.')
        if not month:
            # Try stripping common suffixes
            for m_name, m_num in THAI_MONTHS.items():
                if month_str.startswith(m_name.rstrip('.')):
                    month = m_num
                    break

        return day, month, year

    # Pattern 2: "DDmonth" abbreviated without space - e.g., "31พค", "15มิย"
    pattern2 = r'(\d{1,2})([ก-๙]{2,4})\.?(?:\s|$)'
    match = re.search(pattern2, text_clean)
    if match:
        day = int(match.group(1))
        month_str = match.group(2).strip()

        # Try matching abbreviated month
        month = None
        for m_name, m_num in THAI_MONTHS.items():
            # Match by first 2-3 characters
            if m_name.startswith(month_str) or month_str.startswith(m_name.rstrip('.')):
                month = m_num
                break

        # Look for year after month
        year = None
        year_match = re.search(r'(\d{4})', text_clean)
        if year_match:
            year = int(year_match.group(1))

        return day, month, year

    # Pattern 3: Just year "2534" or similar
    year_match = re.search(r'(\d{4})', text_clean)
    if year_match:
        year = int(year_match.group(1))
        # Look for day and month separately
        day = None
        month = None

        day_match = re.search(r'(\d{1,2})\s*(?=[ก-๙]|$)', text_clean)
        if day_match:
            day = int(day_match.group(1))

        for m_name, m_num in THAI_MONTHS.items():
            if m_name.rstrip('.') in text_clean:
                month = m_num
                break

        return day, month, year

    return None, None, None


def is_empty_date_field(text: str) -> bool:
    """
    Check if text represents an empty date field (dots, slashes only).

    Returns True for patterns like:
    - '........ / .......................'
    - '//'
    - '....... / ................ /.'
    """
    if not text:
        return True

    # Remove "เมื่อวันที่" prefix
    cleaned = re.sub(r'เมื่อวันที่\s*', '', text)
    cleaned = cleaned.strip()

    # Check if only contains dots, slashes, spaces
    if re.match(r'^[.\s/\-_]+$', cleaned):
        return True

    # Check for pattern like "/ /" or ".. / .."
    if re.match(r'^[.\s]*/?[.\s]*/?\s*$', cleaned):
        return True

    return False


def parse_age(text: str) -> Optional[int]:
    """Extract age from text like 'อายุ 41 ปี'"""
    if not text:
        return None

    match = re.search(r'อายุ[.\s]*(\d+)', text)
    if match:
        return int(match.group(1))

    # Also try just number followed by ปี
    match = re.search(r'(\d+)\s*ปี', text)
    if match:
        return int(match.group(1))

    # Just a number
    match = re.search(r'^(\d{1,3})$', text.strip())
    if match:
        age = int(match.group(1))
        if 0 < age < 150:
            return age

    return None


def format_disclosure_date(disclosure_date: str) -> str:
    """
    Convert disclosure date from DD/MM/YYYY to YYYY-MM-DD format.
    
    Args:
        disclosure_date: Date string in DD/MM/YYYY format
        
    Returns:
        Date string in YYYY-MM-DD format or empty string
    """
    if not disclosure_date:
        return ''
    
    parts = disclosure_date.split('/')
    if len(parts) == 3:
        return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
    return ''


# =============================================================================
# POLYGON UTILITIES
# =============================================================================

def get_polygon_center(polygon: List[float]) -> Tuple[float, float]:
    """
    Get the center point of a polygon.
    
    Args:
        polygon: List of 8 values: x1,y1,x2,y2,x3,y3,x4,y4
        
    Returns:
        Tuple of (center_x, center_y)
    """
    if len(polygon) < 8:
        return (0, 0)
    x_coords = [polygon[0], polygon[2], polygon[4], polygon[6]]
    y_coords = [polygon[1], polygon[3], polygon[5], polygon[7]]
    return (sum(x_coords) / 4, sum(y_coords) / 4)


def get_polygon_bounds(polygon: List[float]) -> Tuple[float, float, float, float]:
    """
    Get bounding box (x_min, y_min, x_max, y_max) from polygon.
    
    Args:
        polygon: List of 8 values: x1,y1,x2,y2,x3,y3,x4,y4
        
    Returns:
        Tuple of (x_min, y_min, x_max, y_max)
    """
    if len(polygon) < 8:
        return (0, 0, 0, 0)
    x_coords = [polygon[0], polygon[2], polygon[4], polygon[6]]
    y_coords = [polygon[1], polygon[3], polygon[5], polygon[7]]
    return (min(x_coords), min(y_coords), max(x_coords), max(y_coords))


def is_in_x_range(polygon: List[float], x_min: float, x_max: float) -> bool:
    """Check if polygon center x is within a range."""
    cx, _ = get_polygon_center(polygon)
    return x_min <= cx <= x_max


def is_y_close(polygon: List[float], target_y: float, tolerance: float = Y_TOLERANCE) -> bool:
    """Check if polygon center y is close to target y."""
    _, cy = get_polygon_center(polygon)
    return abs(cy - target_y) <= tolerance


def get_line_y(line: Dict) -> float:
    """Get Y coordinate of a line."""
    polygon = line.get('polygon', [])
    if len(polygon) < 2:
        return 0.0
    return polygon[1]


def group_lines_by_row(lines: List[Dict], y_tolerance: float = 0.3) -> List[List[Dict]]:
    """
    Group lines that are on the same row based on Y coordinate.
    
    Args:
        lines: List of line dictionaries with 'polygon' key
        y_tolerance: Maximum Y difference to consider same row
        
    Returns:
        List of row groups, each containing lines at similar Y positions
    """
    if not lines:
        return []

    # Sort by Y coordinate
    sorted_lines = sorted(lines, key=lambda l: get_line_y(l))

    rows = []
    current_row = [sorted_lines[0]]
    current_y = get_line_y(sorted_lines[0])

    for line in sorted_lines[1:]:
        line_y = get_line_y(line)
        if abs(line_y - current_y) <= y_tolerance:
            current_row.append(line)
        else:
            rows.append(current_row)
            current_row = [line]
            current_y = line_y

    if current_row:
        rows.append(current_row)

    return rows


def extract_values_from_row(
    lines: List[Dict], 
    y_target: float, 
    y_tolerance: float = 0.25,
    submitter_x_range: Tuple[float, float] = (2.8, 4.8),
    spouse_x_range: Tuple[float, float] = (4.8, 6.8),
    child_x_range: Tuple[float, float] = (6.8, 8.5)
) -> Dict[str, Optional[float]]:
    """
    Extract submitter, spouse, child values from a row at specific y position.
    
    Args:
        lines: List of line dictionaries
        y_target: Target Y coordinate
        y_tolerance: Y tolerance for matching
        submitter_x_range: X range for submitter column
        spouse_x_range: X range for spouse column  
        child_x_range: X range for child column
        
    Returns:
        Dict with 'submitter', 'spouse', 'child' keys
    """
    values = {'submitter': None, 'spouse': None, 'child': None}

    for line in lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [0]*8)
        cx, cy = get_polygon_center(polygon)

        if abs(cy - y_target) > y_tolerance:
            continue

        value = clean_number(content)
        if value is not None:
            if submitter_x_range[0] <= cx <= submitter_x_range[1]:
                values['submitter'] = value
            elif spouse_x_range[0] <= cx <= spouse_x_range[1]:
                values['spouse'] = value
            elif child_x_range[0] <= cx <= child_x_range[1]:
                values['child'] = value

    return values


# =============================================================================
# POSITION DETECTION UTILITIES
# =============================================================================

def detect_position_category(position_name: str) -> Any:
    """
    Detect position category type based on position name.
    
    Position Category Types:
    - 2: นายกรัฐมนตรี
    - 3: รัฐมนตรี
    - 4: สมาชิกสภาผู้แทนราษฎร
    - 5: สมาชิกวุฒิสภา
    """
    if 'สมาชิกสภาผู้แทนราษฎร' in position_name or 'ส.ส.' in position_name:
        return 4
    if 'สมาชิกวุฒิสภา' in position_name or 'ส.ว.' in position_name:
        return 5
    if 'รัฐมนตรี' in position_name:
        return 3
    if 'นายกรัฐมนตรี' in position_name:
        return 2
    return ''


def is_date_range(text: str) -> bool:
    """Check if text looks like a date range."""
    date_pattern = r'\d{4}\s*[-–]\s*(\d{1,2}\s*[ก-๙\.]+\s*)?\d{4}|\d{1,2}\s*[ก-๙\.]+\s*\d{4}\s*[-–]'
    return bool(re.search(date_pattern, text))


def is_header_line(text: str) -> bool:
    """Check if text is a table header."""
    headers = [
        'ระยะเวลาดำรงตำแหน่ง', '(ปี พ.ศ.)', 'ตำแหน่ง', 'หน่วยงาน', 'ที่ตั้ง',
        'หมายเหตุ', 'ประวัติการทำงาน', 'วันที่เข้ารับตำแหน่ง'
    ]
    return any(h in text for h in headers)


def is_skip_line(text: str) -> bool:
    """Check if this line should be skipped in processing."""
    skip_patterns = [
        '- ลับ -', 'ลงชื่อ', 'ผู้ยื่นบัญชี', 'หน้า ', '1.00.00',
        'เลขประจำตัวประชาชน', 'วันเดือนปี เกิด', 'ชื่อและชื่อสกุล',
        'ชื่อเดิม', 'สถานภาพการสมรส', 'คู่สมรส', 'พี่น้อง',
        'ที่อยู่ที่ติดต่อ', 'โทรศัพท์', 'บิดา :', 'มารดา :',
        'ตำแหน่งปัจจุบันในหน่วยงาน'
    ]
    return any(p in text for p in skip_patterns)


# =============================================================================
# PAGE UTILITIES
# =============================================================================

def find_pages_by_keyword(pages: List[Dict], keyword: str) -> List[Tuple[int, Dict]]:
    """
    Find all pages containing the keyword.
    
    Args:
        pages: List of page dictionaries
        keyword: Keyword to search for
        
    Returns:
        List of (page_index, page_dict) tuples
    """
    result = []
    for i, page in enumerate(pages):
        for line in page.get('lines', []):
            if keyword in line.get('content', ''):
                result.append((i, page))
                break
    return result


def get_page_text(page: Dict) -> str:
    """Get all text content from a page joined by spaces."""
    lines = page.get('lines', [])
    return ' '.join([l.get('content', '') for l in lines])
