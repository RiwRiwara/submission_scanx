"""
Asset Type Detection Module

Provides functions to detect asset types from OCR text using regex patterns.
Used by step_6.py for asset classification.

Asset type categories:
- Land (1-9, 36): โฉนด, ส.ป.ก, น.ส.3, etc.
- Building (10-17, 37): บ้านพักอาศัย, อาคาร, ห้องชุด, etc.
- Vehicle (18-21, 38): รถยนต์, รถจักรยานยนต์, เรือ, etc.
- Rights (22-27, 39): สิทธิ, สัมปทาน, หุ้น, ทรัพย์สินทางปัญญา, etc.
- Other (28-35): เงินสด, เงินฝาก, เครื่องประดับ, etc.
"""

import re
from typing import Tuple

# Land type patterns (asset_type_id: 1-9, 36)
LAND_PATTERNS = {
    1: [r'โฉนด(?:ที่ดิน)?', r'น\.?ส\.?4'],
    2: [r'ส\.?ป\.?ก\.?(?:4-01)?', r'สปก'],
    3: [r'ส\.?ป\.?ก\.?\s*4-01'],
    4: [r'น\.?ส\.?3(?![กข])'],
    5: [r'น\.?ส\.?3\s*ก'],
    6: [r'ภ\.?บ\.?ท\.?\s*5'],
    7: [r'อ\.?ช\.?\s*2'],
    8: [r'สัญญา(?:ซื้อขาย|เช่า)', r'หนังสือสัญญา'],
    9: [r'น\.?ค\.?3'],
    36: [r'ห้องชุด', r'คอนโด(?:มิเนียม)?', r'อพาร์ท?เม้?น?ท์?'],
}

# Building type patterns (asset_type_id: 10-17, 37)
BUILDING_PATTERNS = {
    10: [r'บ้าน(?:พักอาศัย)?', r'ที่พักอาศัย', r'บ้านเดี่ยว'],
    11: [r'อาคาร(?:พาณิชย์)?', r'ตึก(?:แถว)?'],
    12: [r'ห้องชุด(?!.*ที่ดิน)', r'คอนโด(?:มิเนียม)?'],
    13: [r'ทาว(?:น์)?เฮ(?:าส์|้าส์)?', r'บ้านแถว'],
    14: [r'โรงเรือน', r'โกดัง', r'สิ่งปลูกสร้าง'],
    15: [r'สำนักงาน', r'ร้านค้า'],
    16: [r'โรงงาน', r'อาคารอุตสาหกรรม'],
    17: [r'หอพัก', r'อพาร์ท?เม้?น?ท์?'],
    37: [r'อื่น\s*ๆ', r'สิ่งปลูกสร้างอื่น'],
}

# Vehicle type patterns (asset_type_id: 18-21, 38)
VEHICLE_PATTERNS = {
    18: [r'รถยนต์', r'รถเก๋ง', r'รถกระบะ', r'รถตู้', r'รถบรรทุก', r'รถ SUV'],
    19: [r'รถจักรยานยนต์', r'มอเตอร์ไซค์', r'จักรยานยนต์'],
    20: [r'เรือ', r'เรือยอร์ช', r'เรือสปีดโบ๊ท'],
    21: [r'เครื่องบิน', r'อากาศยาน'],
    38: [r'ยานพาหนะอื่น', r'รถอื่น\s*ๆ', r'รถพ่วง', r'รถไถ'],
}

# Rights/concession type patterns (asset_type_id: 22-27, 39)
RIGHTS_PATTERNS = {
    22: [r'หุ้น(?:สามัญ)?', r'หุ้น(?:บุริมสิทธิ)?'],
    23: [r'พันธบัตร', r'ตราสารหนี้'],
    24: [r'สิทธิเรียกร้อง', r'ลูกหนี้'],
    25: [r'ทรัพย์สินทางปัญญา', r'สิทธิบัตร', r'ลิขสิทธิ์'],
    26: [r'สัมปทาน', r'ใบอนุญาต'],
    27: [r'สิทธิการเช่า', r'สิทธิเช่า'],
    39: [r'สิทธิอื่น\s*ๆ', r'สิทธิและสัมปทานอื่น'],
}

# Other asset type patterns (asset_type_id: 28-35)
OTHER_PATTERNS = {
    28: [r'เงินสด'],
    29: [r'เงินฝาก', r'เงินออม'],
    30: [r'เงินลงทุน', r'กองทุน'],
    31: [r'เงินให้กู้(?:ยืม)?', r'เงินให้(?:ยืม)?'],
    32: [r'เครื่องประดับ', r'อัญมณี', r'เพชร', r'ทอง'],
    33: [r'ของสะสม', r'พระเครื่อง', r'งานศิลปะ'],
    34: [r'สัตว์เลี้ยง', r'ปศุสัตว์'],
    35: [r'ทรัพย์สินอื่น\s*ๆ?', r'อื่น\s*ๆ'],
}


def detect_land_type(text: str) -> Tuple[int, str]:
    """Detect land type from text. Returns (asset_type_id, asset_type_other)."""
    text = text.strip()
    for type_id, patterns in LAND_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return (type_id, '')
    return (36, text)  # Default to "other land" category


def detect_building_type(text: str) -> Tuple[int, str]:
    """Detect building type from text. Returns (asset_type_id, asset_type_other)."""
    text = text.strip()
    for type_id, patterns in BUILDING_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return (type_id, '')
    return (37, text)  # Default to "other building" category


def detect_vehicle_type(text: str) -> Tuple[int, str]:
    """Detect vehicle type from text. Returns (asset_type_id, asset_type_other)."""
    text = text.strip()
    for type_id, patterns in VEHICLE_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return (type_id, '')
    return (38, text)  # Default to "other vehicle" category


def detect_rights_type(text: str) -> Tuple[int, str]:
    """Detect rights/concession type from text. Returns (asset_type_id, asset_type_other)."""
    text = text.strip()
    for type_id, patterns in RIGHTS_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return (type_id, '')
    return (39, text)  # Default to "other rights" category


def detect_other_type(text: str) -> Tuple[int, str]:
    """Detect other asset type from text. Returns (asset_type_id, asset_type_other)."""
    text = text.strip()
    for type_id, patterns in OTHER_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return (type_id, '')
    return (35, text)  # Default to "other" category


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
