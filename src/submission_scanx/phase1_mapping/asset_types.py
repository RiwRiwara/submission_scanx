"""
Asset Type Detection Module

Provides functions to detect asset types from OCR text using regex patterns.
Used by step_6.py for asset classification.

Asset type categories (from asset_type.csv):
- Land (1-9, 36): โฉนด, ส.ป.ก, น.ส.3, etc.
- Building (10-17, 37): บ้าน, อาคาร, ห้องชุด, คอนโด, etc.
- Vehicle (18-21, 38): รถยนต์, จักรยานยนต์, เรือยนต์, เครื่องบิน
- Rights (22-27, 39): กรมธรรม์, สัญญา, สมาชิก, กองทุน, เงินสงเคราะห์, ป้ายประมูล
- Other (28-35): กระเป๋า, อาวุธปืน, นาฬิกา, เครื่องประดับ, วัตถุมงคล, ทองคำ, งานศิลปะ, ของสะสมอื่น
"""

import re
from typing import Tuple

# Land type patterns (asset_type_id: 1-9, 36)
LAND_PATTERNS = {
    1: [r'โฉนด(?:ที่ดิน)?', r'น\.?ส\.?4'],
    2: [r'ส\.?ป\.?ก\.?(?![4\s])', r'สปก(?![4\s])'],  # ส.ป.ก not followed by 4
    3: [r'ส\.?ป\.?ก\.?\s*4-?01', r'สปก\s*4-?01'],
    4: [r'น\.?ส\.?3(?![กข])'],
    5: [r'น\.?ส\.?3\s*ก'],
    6: [r'ภ\.?บ\.?ท\.?\s*5'],
    7: [r'อ\.?ช\.?\s*2', r'ห้องชุด.*อ\.?ช\.?\s*2'],
    8: [r'สัญญา(?:ซื้อขาย|เช่า)', r'หนังสือสัญญา'],
    9: [r'น\.?ค\.?3'],
    36: [],  # Other land - catch-all
}

# Building type patterns (asset_type_id: 10-17, 37)
BUILDING_PATTERNS = {
    10: [r'บ้าน(?:เดี่ยว|พักอาศัย|ไม้)?', r'ที่พักอาศัย'],
    11: [r'อาคาร(?:พาณิชย์)?'],
    12: [r'ตึก(?:แถว)?'],
    13: [r'ห้องชุด(?!.*อ\.?ช)', r'เพนท์?เฮ้าส์?'],  # ห้องชุด but not อ.ช.2
    14: [r'คอนโด(?:มิเนียม)?'],
    15: [r'หอพัก'],
    16: [r'ลานจอด', r'ที่จอดรถ'],
    17: [r'โรงงาน', r'โรงเรือน', r'โกดัง', r'สิ่งปลูกสร้าง'],
    37: [r'ทาว(?:น์)?เฮ(?:าส์|้าส์)?', r'บ้านแถว', r'อพาร์ท?เม้?น?ท์?'],  # Other buildings
}

# Vehicle type patterns (asset_type_id: 18-21, 38)
VEHICLE_PATTERNS = {
    18: [r'รถยนต์', r'รถเก๋ง', r'รถกระบะ', r'รถตู้', r'รถบรรทุก', r'รถ\s*SUV', r'รถนั่ง'],
    19: [r'รถจักรยานยนต์', r'จักรยานยนต์', r'มอเตอร์ไซค์', r'มอเตอร์ไซด์'],
    20: [r'เรือยนต์', r'เรือ(?!น)', r'เรือยอร์ช', r'เรือสปีด'],  # เรือ but not เรือน (building)
    21: [r'เครื่องบิน', r'อากาศยาน'],
    38: [r'ยานพาหนะอื่น', r'รถอื่น', r'รถพ่วง', r'รถไถ', r'รถแทรกเตอร์'],
}

# Rights/concession type patterns (asset_type_id: 22-27, 39) - based on asset_type.csv
RIGHTS_PATTERNS = {
    22: [r'กรมธรรม์', r'ประกัน(?:ภัย|ชีวิต)', r'สิทธิ์?(?:ใน)?กรมธรรม์'],  # กรมธรรม์/insurance
    23: [r'สัญญา(?!ซื้อขาย)', r'ข้อตกลง'],  # สัญญา (contract) but not land contract
    24: [r'สมาชิก', r'สิทธิ์?(?:ใน)?สมาชิก'],  # สมาชิก (membership)
    25: [r'กองทุน', r'หุ้น', r'พันธบัตร', r'ตราสาร'],  # กองทุน (fund)
    26: [r'เงินสงเคราะห์', r'สงเคราะห์'],  # เงินสงเคราะห์ (compensation)
    27: [r'ป้ายประมูล', r'ป้ายทะเบียน'],  # ป้ายประมูล (auction sign)
    39: [r'สัมปทาน', r'ใบอนุญาต', r'ลิขสิทธิ์', r'สิทธิบัตร', r'สิทธิการเช่า'],  # Other rights
}

# Other asset type patterns (asset_type_id: 28-35) - based on asset_type.csv
OTHER_PATTERNS = {
    28: [r'กระเป๋า'],  # กระเป๋า (bag)
    29: [r'อาวุธปืน', r'ปืน'],  # อาวุธปืน (gun)
    30: [r'นาฬิกา'],  # นาฬิกา (watch)
    31: [r'เครื่องประดับ', r'อัญมณี', r'เพชร(?!ร)', r'แหวน'],  # เครื่องประดับ (jewelry)
    32: [r'วัตถุมงคล', r'พระเครื่อง', r'พระพุทธ', r'พระ(?!มหา)'],  # วัตถุมงคล (amulet)
    33: [r'ทองคำ', r'ทอง(?:แท่ง|รูปพรรณ)'],  # ทองคำ (gold)
    34: [r'งานศิลปะ', r'โบราณวัตถุ', r'ภาพ(?:วาด|เขียน)'],  # งานศิลปะ (art)
    35: [r'ของสะสม'],  # ของสะสมอื่น (other collectibles) - catch-all
}

# Invalid content patterns - should never be in asset_type_other or asset_name
INVALID_PATTERNS = [
    r'^ตำบล', r'^อำเภอ', r'^จังหวัด', r'^ถนน', r'^ถ\.', r'^ซอย', r'^ซ\.',
    r'^หมู่', r'^เลขที่\s*\d', r'^\d+\s*ก[ก-ฮ]', r'^กรุงเทพ',
    r'^\d{1,2}[-/]\d', r'^\(.*\)$',  # Dates or parentheses only
]


def is_invalid_asset_content(text: str) -> bool:
    """Check if text looks like address, registration number, or other invalid content."""
    text = text.strip()
    for pattern in INVALID_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    # Also check for vehicle registration patterns (shouldn't be in type_other)
    if re.match(r'^\d*[ก-ฮ]{1,3}\s*\d{1,4}$', text):
        return True
    return False


def extract_valid_type_other(text: str, default_name: str = '') -> str:
    """
    Extract only valid asset type description from text.
    Returns empty string if text is invalid (address, registration, etc.)
    """
    text = text.strip()
    if is_invalid_asset_content(text):
        return ''
    # Check if it's a meaningful type description (not just numbers or addresses)
    if len(text) < 3 or re.match(r'^[\d\s\.\-\/]+$', text):
        return ''
    return text[:50] if len(text) > 50 else text


def detect_land_type(text: str) -> Tuple[int, str]:
    """Detect land type from text. Returns (asset_type_id, asset_type_other)."""
    text = text.strip()
    for type_id, patterns in LAND_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return (type_id, '')
    # Don't store invalid content (addresses, etc.) as type_other
    return (36, '')


def detect_building_type(text: str) -> Tuple[int, str]:
    """Detect building type from text. Returns (asset_type_id, asset_type_other)."""
    text = text.strip()
    for type_id, patterns in BUILDING_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                # For type 37 (other), return the description if valid
                if type_id == 37:
                    return (type_id, extract_valid_type_other(text))
                return (type_id, '')
    # Don't store invalid content as type_other
    return (37, '')


def detect_vehicle_type(text: str) -> Tuple[int, str]:
    """Detect vehicle type from text. Returns (asset_type_id, asset_type_other)."""
    text = text.strip()
    for type_id, patterns in VEHICLE_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return (type_id, '')
    # Don't store registration numbers as type_other
    return (38, '')


def detect_rights_type(text: str) -> Tuple[int, str]:
    """Detect rights/concession type from text. Returns (asset_type_id, asset_type_other)."""
    text = text.strip()
    for type_id, patterns in RIGHTS_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return (type_id, '')
    # Check if it looks like rights content (has siti/สิทธิ keywords)
    if re.search(r'สิทธิ|กรมธรรม์|ประกัน|กองทุน|สงเคราะห์|สมาชิก', text):
        return (39, extract_valid_type_other(text))
    return (39, '')


def detect_other_type(text: str) -> Tuple[int, str]:
    """Detect other asset type from text. Returns (asset_type_id, asset_type_other)."""
    text = text.strip()
    for type_id, patterns in OTHER_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return (type_id, '')
    return (35, '')


def detect_asset_type(text: str, category: str) -> Tuple[int, str]:
    """
    Main detection function - routes to appropriate category detector.

    Args:
        text: Asset description text
        category: 'land', 'building', 'vehicle', 'rights', 'other'

    Returns:
        Tuple of (asset_type_id, asset_type_other)
    """
    detectors = {
        'land': detect_land_type,
        'building': detect_building_type,
        'vehicle': detect_vehicle_type,
        'rights': detect_rights_type,
        'other': detect_other_type,
    }

    detector = detectors.get(category, detect_other_type)
    return detector(text)
