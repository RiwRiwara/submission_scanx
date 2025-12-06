"""
Thai Location Lookup Helper

This module provides lookup functions to auto-fill missing location data.
Given a sub_district, it can find the corresponding district, province, and post_code.

Data structure:
- provinces.json: id, name_th, name_en
- districts.json: id, name_th, name_en, province_id
- sub_districts.json: id, name_th, name_en, district_id, zip_code
"""

import json
import os
import re
from typing import Dict, Optional, Tuple, List

# Module-level cache for location data
_location_data = None


def _load_location_data() -> Dict:
    """Load and cache the Thai province data."""
    global _location_data

    if _location_data is not None:
        return _location_data

    data_dir = os.path.join(os.path.dirname(__file__), 'thai-province-data')

    # Load provinces
    with open(os.path.join(data_dir, 'provinces.json'), 'r', encoding='utf-8') as f:
        provinces_list = json.load(f)

    # Load districts
    with open(os.path.join(data_dir, 'districts.json'), 'r', encoding='utf-8') as f:
        districts_list = json.load(f)

    # Load sub_districts
    with open(os.path.join(data_dir, 'sub_districts.json'), 'r', encoding='utf-8') as f:
        sub_districts_list = json.load(f)

    # Create lookup dictionaries
    provinces_by_id = {p['id']: p for p in provinces_list}
    provinces_by_name = {p['name_th']: p for p in provinces_list}

    districts_by_id = {d['id']: d for d in districts_list}
    districts_by_name = {}  # name -> list of districts (names can repeat across provinces)
    for d in districts_list:
        name = d['name_th']
        # Remove "เขต" prefix for Bangkok districts
        clean_name = re.sub(r'^เขต', '', name).strip()
        if name not in districts_by_name:
            districts_by_name[name] = []
        districts_by_name[name].append(d)
        if clean_name != name:
            if clean_name not in districts_by_name:
                districts_by_name[clean_name] = []
            districts_by_name[clean_name].append(d)

    sub_districts_by_id = {s['id']: s for s in sub_districts_list}
    sub_districts_by_name = {}  # name -> list of sub_districts
    for s in sub_districts_list:
        name = s['name_th']
        if name not in sub_districts_by_name:
            sub_districts_by_name[name] = []
        sub_districts_by_name[name].append(s)

    _location_data = {
        'provinces_by_id': provinces_by_id,
        'provinces_by_name': provinces_by_name,
        'districts_by_id': districts_by_id,
        'districts_by_name': districts_by_name,
        'sub_districts_by_id': sub_districts_by_id,
        'sub_districts_by_name': sub_districts_by_name,
    }

    return _location_data


def normalize_location_name(name: str) -> str:
    """Normalize a location name for matching."""
    if not name:
        return ''

    # Remove common prefixes
    name = re.sub(r'^(ตำบล|แขวง|อำเภอ|เขต|จังหวัด)\s*', '', name.strip())
    # Remove trailing dots
    name = name.rstrip('.')
    return name.strip()


def _similar_thai_chars(char1: str, char2: str) -> bool:
    """Check if two Thai characters are commonly confused in OCR."""
    similar_groups = [
        set('นก'),  # น and ก look similar
        set('หม'),  # ห and ม can be confused
        set('ลน'),  # ล and น
        set('บป'),  # บ and ป
        set('พภ'),  # พ and ภ
        set('คด'),  # ค and ด
        set('ญฐ'),  # ญ and ฐ
        set('อย'),  # อ and ย at end
        set('กภ'),  # ก and ภ
        set('วน'),  # ว and น
    ]
    for group in similar_groups:
        if char1 in group and char2 in group:
            return True
    return False


def _fuzzy_match_name(name1: str, name2: str, max_diff: int = 2) -> bool:
    """Check if two Thai names are similar (allowing for OCR errors)."""
    if not name1 or not name2:
        return False

    # Exact match
    if name1 == name2:
        return True

    # Length difference too big
    if abs(len(name1) - len(name2)) > max_diff:
        return False

    # Count differences
    differences = 0
    min_len = min(len(name1), len(name2))

    for i in range(min_len):
        if name1[i] != name2[i]:
            if not _similar_thai_chars(name1[i], name2[i]):
                differences += 1
            else:
                differences += 0.5  # Similar chars count as half difference

    # Add length difference
    differences += abs(len(name1) - len(name2))

    return differences <= max_diff


def lookup_sub_district(sub_district_name: str, district_hint: str = '',
                        province_hint: str = '') -> Optional[Dict]:
    """
    Look up a sub_district by name and return full location info.

    Args:
        sub_district_name: Name of the sub_district (ตำบล/แขวง)
        district_hint: Optional district name to narrow down search
        province_hint: Optional province name to narrow down search

    Returns:
        Dict with sub_district, district, province, post_code or None if not found
    """
    data = _load_location_data()

    sub_district_name = normalize_location_name(sub_district_name)
    district_hint = normalize_location_name(district_hint)
    province_hint = normalize_location_name(province_hint)

    if not sub_district_name:
        return None

    # Skip invalid sub_district names
    invalid_patterns = ['หมู่ที่', 'ซอย', 'ถนน', 'เลขที่', 'บ้านเลขที่']
    for invalid in invalid_patterns:
        if invalid in sub_district_name:
            return None

    # Find matching sub_districts
    matches = data['sub_districts_by_name'].get(sub_district_name, [])

    if not matches:
        # Try partial matching
        for name, subs in data['sub_districts_by_name'].items():
            if sub_district_name in name or name in sub_district_name:
                matches.extend(subs)

    # Fuzzy matching disabled for now - causes false positives
    # TODO: Enable with stricter matching if needed

    if not matches:
        return None

    # If only one match, use it
    if len(matches) == 1:
        return _build_location_result(matches[0], data)

    # Filter by district hint
    if district_hint:
        filtered = []
        for sub in matches:
            district = data['districts_by_id'].get(sub['district_id'])
            if district:
                dist_name = normalize_location_name(district['name_th'])
                if district_hint in dist_name or dist_name in district_hint:
                    filtered.append(sub)
        if filtered:
            matches = filtered

    # Filter by province hint
    if province_hint:
        filtered = []
        for sub in matches:
            district = data['districts_by_id'].get(sub['district_id'])
            if district:
                province = data['provinces_by_id'].get(district['province_id'])
                if province:
                    prov_name = normalize_location_name(province['name_th'])
                    if province_hint in prov_name or prov_name in province_hint:
                        filtered.append(sub)
        if filtered:
            matches = filtered

    # Return first match
    if matches:
        return _build_location_result(matches[0], data)

    return None


def lookup_district(district_name: str, province_hint: str = '') -> Optional[Dict]:
    """
    Look up a district by name and return province info.

    Args:
        district_name: Name of the district (อำเภอ/เขต)
        province_hint: Optional province name to narrow down search

    Returns:
        Dict with district, province or None if not found
    """
    data = _load_location_data()

    district_name = normalize_location_name(district_name)
    province_hint = normalize_location_name(province_hint)

    if not district_name:
        return None

    # Find matching districts
    matches = data['districts_by_name'].get(district_name, [])

    if not matches:
        # Try with "เมือง" prefix for provincial capitals
        if not district_name.startswith('เมือง'):
            matches = data['districts_by_name'].get('เมือง' + district_name, [])

    if not matches:
        return None

    # If only one match, use it
    if len(matches) == 1:
        district = matches[0]
        province = data['provinces_by_id'].get(district['province_id'])
        if province:
            return {
                'district': normalize_location_name(district['name_th']),
                'province': province['name_th']
            }

    # Filter by province hint
    if province_hint:
        for district in matches:
            province = data['provinces_by_id'].get(district['province_id'])
            if province:
                prov_name = normalize_location_name(province['name_th'])
                if province_hint in prov_name or prov_name in province_hint:
                    return {
                        'district': normalize_location_name(district['name_th']),
                        'province': province['name_th']
                    }

    # Return first match
    if matches:
        district = matches[0]
        province = data['provinces_by_id'].get(district['province_id'])
        if province:
            return {
                'district': normalize_location_name(district['name_th']),
                'province': province['name_th']
            }

    return None


def lookup_province(province_name: str) -> Optional[str]:
    """
    Validate and normalize a province name.

    Args:
        province_name: Name of the province

    Returns:
        Normalized province name or None if not found
    """
    data = _load_location_data()

    province_name = normalize_location_name(province_name)

    if not province_name:
        return None

    # Direct match
    if province_name in data['provinces_by_name']:
        return data['provinces_by_name'][province_name]['name_th']

    # Partial match
    for name, prov in data['provinces_by_name'].items():
        if province_name in name or name in province_name:
            return prov['name_th']

    # Handle special cases
    special_cases = {
        'กทม': 'กรุงเทพมหานคร',
        'กรุงเทพ': 'กรุงเทพมหานคร',
        'กรุงเทพฯ': 'กรุงเทพมหานคร',
    }

    if province_name in special_cases:
        return special_cases[province_name]

    return None


def lookup_postcode(sub_district_name: str, district_name: str = '',
                    province_name: str = '') -> Optional[str]:
    """
    Look up postal code for a location.

    Args:
        sub_district_name: Name of the sub_district
        district_name: Optional district name
        province_name: Optional province name

    Returns:
        Post code string or None if not found
    """
    result = lookup_sub_district(sub_district_name, district_name, province_name)
    if result:
        return result.get('post_code')
    return None


def _build_location_result(sub_district: Dict, data: Dict) -> Dict:
    """Build a full location result from a sub_district record."""
    district = data['districts_by_id'].get(sub_district['district_id'])
    province = None
    if district:
        province = data['provinces_by_id'].get(district['province_id'])

    return {
        'sub_district': sub_district['name_th'],
        'district': normalize_location_name(district['name_th']) if district else '',
        'province': province['name_th'] if province else '',
        'post_code': str(sub_district.get('zip_code', '')) if sub_district.get('zip_code') else ''
    }


def fill_missing_location(sub_district: str, district: str, province: str,
                          post_code: str) -> Dict[str, str]:
    """
    Fill in missing location fields based on available data.

    Uses the Thai province data to auto-fill missing fields:
    - If sub_district is known, can fill district, province, and post_code
    - If district is known, can fill province
    - Validates and normalizes province names

    Args:
        sub_district: Sub-district name (ตำบล/แขวง)
        district: District name (อำเภอ/เขต)
        province: Province name (จังหวัด)
        post_code: Postal code

    Returns:
        Dict with all location fields filled where possible
    """
    result = {
        'sub_district': sub_district.strip() if sub_district else '',
        'district': district.strip() if district else '',
        'province': province.strip() if province else '',
        'post_code': post_code.strip() if post_code else ''
    }

    # Try to fill from sub_district first (most specific)
    if result['sub_district']:
        lookup = lookup_sub_district(
            result['sub_district'],
            result['district'],
            result['province']
        )
        if lookup:
            if not result['district']:
                result['district'] = lookup.get('district', '')
            if not result['province']:
                result['province'] = lookup.get('province', '')
            if not result['post_code']:
                result['post_code'] = lookup.get('post_code', '')

    # Try to fill province from district
    if result['district'] and not result['province']:
        lookup = lookup_district(result['district'], result['province'])
        if lookup:
            result['province'] = lookup.get('province', '')

    # Validate and normalize province
    if result['province']:
        normalized = lookup_province(result['province'])
        if normalized:
            result['province'] = normalized

    return result


def get_all_provinces() -> List[str]:
    """Get list of all province names."""
    data = _load_location_data()
    return sorted(data['provinces_by_name'].keys())


def get_districts_for_province(province_name: str) -> List[str]:
    """Get list of district names for a province."""
    data = _load_location_data()

    province_name = normalize_location_name(province_name)
    province = data['provinces_by_name'].get(province_name)

    if not province:
        return []

    districts = []
    for d in data['districts_by_id'].values():
        if d['province_id'] == province['id']:
            districts.append(normalize_location_name(d['name_th']))

    return sorted(set(districts))


def is_valid_sub_district(name: str) -> bool:
    """Check if a sub_district name exists in the Thai location database."""
    if not name or len(name) < 2:
        return False

    data = _load_location_data()
    name = normalize_location_name(name)

    # Direct match
    if name in data['sub_districts_by_name']:
        return True

    return False


def is_valid_district(name: str) -> bool:
    """Check if a district name exists in the Thai location database."""
    if not name or len(name) < 2:
        return False

    data = _load_location_data()
    name = normalize_location_name(name)

    # Direct match
    if name in data['districts_by_name']:
        return True

    # Try with เมือง prefix
    if 'เมือง' + name in data['districts_by_name']:
        return True

    return False


def is_valid_province(name: str) -> bool:
    """Check if a province name exists in the Thai location database."""
    if not name or len(name) < 2:
        return False

    data = _load_location_data()
    name = normalize_location_name(name)

    # Direct match
    if name in data['provinces_by_name']:
        return True

    # Special cases
    special_cases = ['กทม', 'กรุงเทพ', 'กรุงเทพฯ']
    if name in special_cases:
        return True

    return False


def clean_invalid_location(sub_district: str, district: str, province: str,
                            post_code: str) -> Dict[str, str]:
    """
    Clean location fields by removing invalid values that don't exist
    in the Thai location database.

    This is useful for removing OCR garbage that got picked up as location values.

    Args:
        sub_district: Sub-district name to validate
        district: District name to validate
        province: Province name to validate
        post_code: Postal code (validated by format)

    Returns:
        Dict with cleaned location fields (invalid values cleared)
    """
    result = {
        'sub_district': '',
        'district': '',
        'province': '',
        'post_code': ''
    }

    # Validate sub_district
    if sub_district:
        sub_district = sub_district.strip()
        if is_valid_sub_district(sub_district):
            result['sub_district'] = sub_district

    # Validate district
    if district:
        district = district.strip()
        if is_valid_district(district):
            result['district'] = district

    # Validate province
    if province:
        province = province.strip()
        if is_valid_province(province):
            # Normalize province name
            normalized = lookup_province(province)
            result['province'] = normalized if normalized else province

    # Validate post_code (5 digits for Thailand)
    if post_code:
        post_code = post_code.strip()
        if re.match(r'^\d{5}$', post_code):
            result['post_code'] = post_code

    return result
