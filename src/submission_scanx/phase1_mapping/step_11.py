"""
Step 11: Generate summary.csv from all pipeline output files

This step implements the logic from validation_query.sql in Python,
aggregating data from all output CSVs into a summary table.

Output columns:
- nacc_detail fields (id, doc_id, title, name, position, dates, agency)
- submitter_info fields (personal info, address, contact)
- spouse_info fields (if married)
- statement totals (valuation by submitter/spouse/child)
- statement_detail aggregates (count, has_note flag)
- asset counts (total, land, building, vehicle, other)
- asset valuations (by type and by owner)
- relative aggregates (count, has_death flag)
"""

import os
import csv
from typing import Dict, List, Any, Optional
from collections import defaultdict


def load_csv(file_path: str) -> List[Dict]:
    """Load CSV file into list of dicts"""
    if not os.path.exists(file_path):
        return []
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        return list(reader)


def load_csv_dict(file_path: str, key_field: str) -> Dict[str, Dict]:
    """Load CSV file into dict keyed by specified field"""
    if not os.path.exists(file_path):
        return {}
    result = {}
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            result[row[key_field]] = row
    return result


def safe_float(val: Any) -> float:
    """Safely convert value to float"""
    if val is None or val == '' or val == 'NONE':
        return 0.0
    try:
        return float(str(val).replace(',', ''))
    except (ValueError, TypeError):
        return 0.0


def safe_int(val: Any) -> int:
    """Safely convert value to int"""
    if val is None or val == '' or val == 'NONE':
        return 0
    try:
        return int(float(str(val).replace(',', '')))
    except (ValueError, TypeError):
        return 0


def format_date(date_str: str) -> str:
    """Convert DD/MM/YYYY to YYYY-MM-DD format"""
    if not date_str or date_str == 'NONE':
        return 'NONE'
    parts = date_str.split('/')
    if len(parts) == 3:
        return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
    return date_str


def none_if_empty(val: Any) -> str:
    """Return 'NONE' if value is empty"""
    if val is None or str(val).strip() == '':
        return 'NONE'
    return str(val)


def format_numeric_value(val: Any, is_asset_other_count: bool = False) -> str:
    """
    Format numeric value:
    - NULL/empty -> 'NONE'
    - For asset_other_count: 0 or 0.0 -> 'NONE'
    - Whole numbers: remove .0 (e.g., 200000.0 -> 200000)
    - Decimals: keep 2 decimal places (e.g., 123.456 -> 123.46)
    """
    # Handle NULL/empty
    if val is None or str(val).strip() == '' or val == 'NONE':
        return 'NONE'

    try:
        num = float(str(val).replace(',', ''))

        # Special case for asset_other_count: 0 becomes NONE
        if is_asset_other_count and num == 0:
            return 'NONE'

        # Check if it's a whole number
        if num == int(num):
            return str(int(num))
        else:
            # Keep 2 decimal places for actual decimals
            return f"{num:.2f}"
    except (ValueError, TypeError):
        return str(val) if val else 'NONE'


def run_step_11(input_dir: str, output_dir: str, data_loader=None):
    """
    Run step 11 to generate summary.csv.

    Args:
        input_dir: Input directory path
        output_dir: Output directory path
        data_loader: Optional PipelineDataLoader instance for mode detection
    """
    from utils.data_loader import get_input_paths

    # Determine is_final from data_loader if available
    is_final = data_loader.is_final_test if data_loader else None
    paths = get_input_paths(input_dir, is_final=is_final)

    # Load input data
    nacc_detail = load_csv(paths['nacc_detail'])
    submitter_info = load_csv_dict(paths['submitter_info'], 'submitter_id')

    # Load doc_info to get doc_id mapping (nacc_id -> doc_id)
    doc_info = load_csv(paths['doc_info'])
    nacc_id_to_doc_id = {row['nacc_id']: row['doc_id'] for row in doc_info}

    # Load asset_type reference
    enum_dir = os.path.join(os.path.dirname(input_dir), 'enum_type')
    asset_type_list = load_csv(os.path.join(enum_dir, 'asset_type.csv'))
    asset_type_map = {row['asset_type_id']: row['asset_type_main_type_name'] for row in asset_type_list}
    
    # Build type ID to main category mapping for faster lookups
    # Land: 1-9, 36
    # Building: 10-17, 37
    # Vehicle: 18-21, 38
    # Rights/Other: 22-35, 39
    land_type_ids = set(str(i) for i in list(range(1, 10)) + [36])
    building_type_ids = set(str(i) for i in list(range(10, 18)) + [37])
    vehicle_type_ids = set(str(i) for i in list(range(18, 22)) + [38])

    # Load pipeline output data
    spouse_info = load_csv(os.path.join(output_dir, 'spouse_info.csv'))
    statement = load_csv(os.path.join(output_dir, 'statement.csv'))
    statement_detail = load_csv(os.path.join(output_dir, 'statement_detail.csv'))
    asset = load_csv(os.path.join(output_dir, 'asset.csv'))
    asset_land_info = load_csv(os.path.join(output_dir, 'asset_land_info.csv'))
    asset_building_info = load_csv(os.path.join(output_dir, 'asset_building_info.csv'))
    asset_vehicle_info = load_csv(os.path.join(output_dir, 'asset_vehicle_info.csv'))
    asset_other_info = load_csv(os.path.join(output_dir, 'asset_other_asset_info.csv'))
    relative_info = load_csv(os.path.join(output_dir, 'relative_info.csv'))

    # Index spouse_info by nacc_id
    spouse_by_nacc = {}
    for sp in spouse_info:
        spouse_by_nacc[sp['nacc_id']] = sp

    # Aggregate statement totals by nacc_id
    statement_totals = defaultdict(lambda: {'submitter': 0.0, 'spouse': 0.0, 'child': 0.0})
    for st in statement:
        nacc_id = st['nacc_id']
        statement_totals[nacc_id]['submitter'] += safe_float(st.get('valuation_submitter', 0))
        statement_totals[nacc_id]['spouse'] += safe_float(st.get('valuation_spouse', 0))
        statement_totals[nacc_id]['child'] += safe_float(st.get('valuation_child', 0))

    # Aggregate statement_detail by nacc_id
    detail_totals = defaultdict(lambda: {'count': 0, 'has_note': 0})
    for dt in statement_detail:
        nacc_id = dt['nacc_id']
        detail_totals[nacc_id]['count'] += 1
        note = dt.get('note', '')
        if note and note.strip():
            detail_totals[nacc_id]['has_note'] = 1

    # Count assets by nacc_id and by type (from asset.csv using asset_type_id)
    asset_counts = defaultdict(int)
    land_counts = defaultdict(int)
    building_counts = defaultdict(int)
    vehicle_counts = defaultdict(int)

    for a in asset:
        nacc_id = a['nacc_id']
        asset_type_id = a.get('asset_type_id', '')

        asset_counts[nacc_id] += 1

        # Count by asset type ID
        if asset_type_id in land_type_ids:
            land_counts[nacc_id] += 1
        elif asset_type_id in building_type_ids:
            building_counts[nacc_id] += 1
        elif asset_type_id in vehicle_type_ids:
            vehicle_counts[nacc_id] += 1

    # Sum asset_other_asset_info count by nacc_id (using SUM(count) as per validation_query.sql)
    other_counts = defaultdict(int)
    for a in asset_other_info:
        other_counts[a['nacc_id']] += safe_int(a.get('count', 1))

    # Calculate asset valuations by nacc_id
    asset_valuations = defaultdict(lambda: {
        'total': 0.0,
        'land': 0.0,
        'building': 0.0,
        'vehicle': 0.0,
        'other': 0.0,
        'submitter': 0.0,
        'spouse': 0.0,
        'child': 0.0
    })

    for a in asset:
        nacc_id = a['nacc_id']
        valuation = safe_float(a.get('valuation', 0))
        asset_type_id = a.get('asset_type_id', '')
        main_type = asset_type_map.get(asset_type_id, '')

        asset_valuations[nacc_id]['total'] += valuation

        # By asset type - use type ID for reliable categorization
        if asset_type_id in land_type_ids:
            asset_valuations[nacc_id]['land'] += valuation
        elif asset_type_id in building_type_ids:
            asset_valuations[nacc_id]['building'] += valuation
        elif asset_type_id in vehicle_type_ids:
            asset_valuations[nacc_id]['vehicle'] += valuation
        else:
            asset_valuations[nacc_id]['other'] += valuation

        # By owner
        if str(a.get('owner_by_submitter', '')).lower() in ['true', '1', 'yes']:
            asset_valuations[nacc_id]['submitter'] += valuation
        if str(a.get('owner_by_spouse', '')).lower() in ['true', '1', 'yes']:
            asset_valuations[nacc_id]['spouse'] += valuation
        if str(a.get('owner_by_child', '')).lower() in ['true', '1', 'yes']:
            asset_valuations[nacc_id]['child'] += valuation

    # Aggregate relatives by nacc_id
    relative_agg = defaultdict(lambda: {'count': 0, 'has_death': 0})
    for r in relative_info:
        nacc_id = r['nacc_id']
        relative_agg[nacc_id]['count'] += 1
        is_death = r.get('is_death', '')
        if str(is_death).lower() in ['true', '1', 'yes']:
            relative_agg[nacc_id]['has_death'] = 1

    # Build summary rows
    summary_rows = []

    for nd in nacc_detail:
        nacc_id = nd['nacc_id']
        submitter_id = nd.get('submitter_id', '')
        submitter = submitter_info.get(submitter_id, {})
        spouse = spouse_by_nacc.get(nacc_id, {})

        row = {
            'id': nacc_id,
            'doc_id': nacc_id_to_doc_id.get(nacc_id, nacc_id),  # Use doc_id from Train_doc_info.csv

            # nacc_detail fields
            'nd_title': none_if_empty(nd.get('title', '')),
            'nd_first_name': none_if_empty(nd.get('first_name', '')),
            'nd_last_name': none_if_empty(nd.get('last_name', '')),
            'nd_position': none_if_empty(nd.get('position', '')),
            'submitted_date': format_date(nd.get('submitted_date', '')),
            'disclosure_announcement_date': format_date(nd.get('disclosure_announcement_date', '')),
            'disclosure_start_date': format_date(nd.get('disclosure_start_date', '')),
            'disclosure_end_date': format_date(nd.get('disclosure_end_date', '')),
            'date_by_submitted_case': format_date(nd.get('date_by_submitted_case', '')),
            'royal_start_date': format_date(nd.get('royal_start_date', '')),
            'agency': none_if_empty(nd.get('agency', '')),

            # submitter_info fields
            'submitter_id': none_if_empty(submitter_id),
            'submitter_title': none_if_empty(submitter.get('title', '')),
            'submitter_first_name': none_if_empty(submitter.get('first_name', '')),
            'submitter_last_name': none_if_empty(submitter.get('last_name', '')),
            'submitter_age': none_if_empty(submitter.get('age', '')),
            'submitter_marital_status': none_if_empty(submitter.get('status', '')),
            'submitter_status_date': none_if_empty(submitter.get('status_date', '')),
            'submitter_status_month': none_if_empty(submitter.get('status_month', '')),
            'submitter_status_year': none_if_empty(submitter.get('status_year', '')),
            'submitter_sub_district': none_if_empty(submitter.get('sub_district', '')),
            'submitter_district': none_if_empty(submitter.get('district', '')),
            'submitter_province': none_if_empty(submitter.get('province', '')),
            'submitter_post_code': none_if_empty(submitter.get('post_code', '')),
            'submitter_phone_number': none_if_empty(submitter.get('phone_number', '')),
            'submitter_mobile_number': none_if_empty(submitter.get('mobile_number', '')),
            'submitter_email': none_if_empty(submitter.get('email', '')),

            # spouse_info fields
            'spouse_id': none_if_empty(spouse.get('spouse_id', '')),
            'spouse_title': none_if_empty(spouse.get('title', '')),
            'spouse_first_name': none_if_empty(spouse.get('first_name', '')),
            'spouse_last_name': none_if_empty(spouse.get('last_name', '')),
            'spouse_age': none_if_empty(spouse.get('age', '')),
            'spouse_status': none_if_empty(spouse.get('status', '')),
            'spouse_status_date': none_if_empty(spouse.get('status_date', '')),
            'spouse_status_month': none_if_empty(spouse.get('status_month', '')),
            'spouse_status_year': none_if_empty(spouse.get('status_year', '')),

            # statement totals
            'statement_valuation_submitter_total': format_numeric_value(statement_totals[nacc_id]['submitter']),
            'statement_valuation_spouse_total': format_numeric_value(statement_totals[nacc_id]['spouse']),
            'statement_valuation_child_total': format_numeric_value(statement_totals[nacc_id]['child']),

            # statement_detail aggregates
            'statement_detail_count': format_numeric_value(detail_totals[nacc_id]['count']),
            'has_statement_detail_note': format_numeric_value(detail_totals[nacc_id]['has_note']),

            # asset counts
            'asset_count': format_numeric_value(asset_counts[nacc_id]),
            'asset_land_count': format_numeric_value(land_counts[nacc_id]),
            'asset_building_count': format_numeric_value(building_counts[nacc_id]),
            'asset_vehicle_count': format_numeric_value(vehicle_counts[nacc_id]),
            'asset_other_count': format_numeric_value(other_counts[nacc_id], is_asset_other_count=True),

            # asset valuations
            'asset_total_valuation_amount': format_numeric_value(asset_valuations[nacc_id]['total']),
            'asset_land_valuation_amount': format_numeric_value(asset_valuations[nacc_id]['land']),
            'asset_building_valuation_amount': format_numeric_value(asset_valuations[nacc_id]['building']),
            'asset_vehicle_valuation_amount': format_numeric_value(asset_valuations[nacc_id]['vehicle']),
            'asset_other_asset_valuation_amount': format_numeric_value(asset_valuations[nacc_id]['other']),
            'asset_valuation_submitter_amount': format_numeric_value(asset_valuations[nacc_id]['submitter']),
            'asset_valuation_spouse_amount': format_numeric_value(asset_valuations[nacc_id]['spouse']),
            'asset_valuation_child_amount': format_numeric_value(asset_valuations[nacc_id]['child']),

            # relative aggregates
            'relative_count': format_numeric_value(relative_agg[nacc_id]['count']),
            'relative_has_death_flag': format_numeric_value(relative_agg[nacc_id]['has_death'])
        }

        summary_rows.append(row)

    # Sort by nacc_id
    summary_rows.sort(key=lambda x: int(x['id']))

    # Write output
    os.makedirs(output_dir, exist_ok=True)

    summary_path = os.path.join(output_dir, 'summary.csv')
    fieldnames = [
        'id', 'doc_id', 'nd_title', 'nd_first_name', 'nd_last_name', 'nd_position',
        'submitted_date', 'disclosure_announcement_date', 'disclosure_start_date',
        'disclosure_end_date', 'date_by_submitted_case', 'royal_start_date', 'agency',
        'submitter_id', 'submitter_title', 'submitter_first_name', 'submitter_last_name',
        'submitter_age', 'submitter_marital_status', 'submitter_status_date',
        'submitter_status_month', 'submitter_status_year', 'submitter_sub_district',
        'submitter_district', 'submitter_province', 'submitter_post_code',
        'submitter_phone_number', 'submitter_mobile_number', 'submitter_email',
        'spouse_id', 'spouse_title', 'spouse_first_name', 'spouse_last_name',
        'spouse_age', 'spouse_status', 'spouse_status_date', 'spouse_status_month',
        'spouse_status_year',
        'statement_valuation_submitter_total', 'statement_valuation_spouse_total',
        'statement_valuation_child_total',
        'statement_detail_count', 'has_statement_detail_note',
        'asset_count', 'asset_land_count', 'asset_building_count', 'asset_vehicle_count',
        'asset_other_count',
        'asset_total_valuation_amount', 'asset_land_valuation_amount',
        'asset_building_valuation_amount', 'asset_vehicle_valuation_amount',
        'asset_other_asset_valuation_amount', 'asset_valuation_submitter_amount',
        'asset_valuation_spouse_amount', 'asset_valuation_child_amount',
        'relative_count', 'relative_has_death_flag'
    ]

    with open(summary_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in summary_rows:
            writer.writerow(row)

    print(f"Generated summary with {len(summary_rows)} rows to {summary_path}")


if __name__ == '__main__':
    input_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_input')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_output')

    run_step_11(input_dir, output_dir)
