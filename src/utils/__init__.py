"""
Utils package for Submission ScanX pipeline.

Contains shared utilities for:
- Data loading and caching (data_loader.py)
- Common text cleaning and parsing (common.py)
- Layout detection and normalization (layout_detector.py, layout_normalizer.py)
- Thai location lookup (thai_location_lookup.py)
"""

from .common import (
    THAI_MONTHS,
    THAI_TITLES,
    Y_TOLERANCE,
    OCR_NOISE_PATTERNS,
    clean_text,
    clean_ocr_text,
    clean_position_text,
    clean_number,
    clean_number_with_decimal_fragment,
    is_valid_thai_name,
    extract_title_and_name,
    parse_thai_date,
    parse_date_range,
    parse_marriage_date,
    is_empty_date_field,
    parse_age,
    get_polygon_center,
    get_polygon_bounds,
    is_in_x_range,
    is_y_close,
    group_lines_by_row,
    format_disclosure_date,
    detect_position_category,
    is_date_range,
    is_header_line,
    is_skip_line,
    get_line_y,
    find_pages_by_keyword,
    get_page_text,
)

from .data_loader import (
    PipelineDataLoader,
    CSVWriter,
    get_input_paths,
)

from .layout_normalizer import (
    LayoutNormalizer,
    TemplateAligner,
    get_normalizer,
    normalize_json_content,
)

from .layout_detector import (
    LayoutDetector,
    AdaptiveExtractor,
    detect_layout_for_page,
    get_detector,
)

__all__ = [
    # common
    'THAI_MONTHS',
    'THAI_TITLES',
    'clean_text',
    'clean_ocr_text',
    'clean_number',
    'is_valid_thai_name',
    'extract_title_and_name',
    'parse_thai_date',
    'parse_date_range',
    'parse_marriage_date',
    'get_polygon_center',
    'get_polygon_bounds',
    'group_lines_by_row',
    'format_disclosure_date',
    'detect_position_category',
    'is_date_range',
    'is_header_line',
    'is_skip_line',
    'get_line_y',
    'find_pages_by_keyword',
    'get_page_text',
    # data_loader
    'PipelineDataLoader',
    'CSVWriter',
    'get_input_paths',
    # layout_normalizer
    'LayoutNormalizer',
    'TemplateAligner',
    'get_normalizer',
    'normalize_json_content',
    # layout_detector
    'LayoutDetector',
    'AdaptiveExtractor',
    'detect_layout_for_page',
    'get_detector',
]
