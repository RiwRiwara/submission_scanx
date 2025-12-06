"""
Layout Detector Module

Auto-detects column positions and thresholds for each page/document.
This allows flexible extraction regardless of document layout variations.

Key features:
- Detects row number column position
- Detects data column boundaries
- Handles different document margins and layouts
- Caches detected layouts for performance
"""

import re
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
from .common import get_polygon_center


class LayoutDetector:
    """
    Detects and calibrates layout parameters for asset extraction pages.
    """

    def __init__(self):
        # Cache detected layouts by page characteristics
        self._layout_cache = {}

    def detect_page_layout(self, lines: List[Dict], page_type: str = 'asset') -> Dict:
        """
        Detect layout parameters for a page.

        Args:
            lines: List of line dictionaries with 'content' and 'polygon'
            page_type: Type of page ('land', 'building', 'vehicle', 'rights', 'other')

        Returns:
            Dict with detected layout parameters:
            - row_num_x_max: Maximum x for row number column
            - type_col_x_range: (min, max) for asset type column
            - date_col_x_range: (min, max) for date column
            - value_col_x_range: (min, max) for valuation column
            - owner_col_x_range: (min, max) for ownership checkmarks
            - header_y_min: Minimum y (skip above this)
            - footer_y_max: Maximum y (skip below this)
        """
        if not lines:
            return self._get_default_layout(page_type)

        # Analyze line positions
        x_positions = []
        y_positions = []
        row_number_candidates = []

        for line in lines:
            content = line.get('content', '').strip()
            polygon = line.get('polygon', [0] * 8)
            cx, cy = get_polygon_center(polygon)

            x_positions.append(cx)
            y_positions.append(cy)

            # Detect potential row numbers (1-99)
            if re.match(r'^\d{1,2}$', content):
                num = int(content)
                if 1 <= num <= 99:
                    row_number_candidates.append((cx, cy, num))

        if not x_positions:
            return self._get_default_layout(page_type)

        # Detect row number column
        row_num_x_max = self._detect_row_number_column(row_number_candidates)

        # Detect header/footer boundaries
        header_y_min, footer_y_max = self._detect_vertical_boundaries(lines)

        # Detect column boundaries based on content distribution
        columns = self._detect_columns(lines, row_num_x_max, header_y_min, footer_y_max)

        layout = {
            'row_num_x_max': row_num_x_max,
            'type_col_x_range': columns.get('type', (row_num_x_max, row_num_x_max + 2.0)),
            'date_col_x_range': columns.get('date', (4.0, 5.5)),
            'value_col_x_range': columns.get('value', (5.5, 7.0)),
            'owner_col_x_range': columns.get('owner', (6.8, 8.1)),
            'header_y_min': header_y_min,
            'footer_y_max': footer_y_max,
            'detected': True
        }

        return layout

    def _detect_row_number_column(self, candidates: List[Tuple[float, float, int]]) -> float:
        """
        Detect the x boundary for row number column.

        Logic:
        - Row numbers are typically in the leftmost column
        - They form a vertical sequence (1, 2, 3, ...)
        - Find the rightmost x of these numbers + small margin
        """
        if not candidates:
            return 1.0  # Default

        # Group by similar x positions
        x_groups = defaultdict(list)
        for cx, cy, num in candidates:
            # Round to 0.2 precision for grouping
            x_key = round(cx * 5) / 5
            x_groups[x_key].append((cx, cy, num))

        # Find the group that looks most like row numbers
        # (should have sequential numbers, leftmost position)
        best_group = None
        best_score = -1

        for x_key, group in x_groups.items():
            # Score based on:
            # 1. Being leftmost (lower x is better)
            # 2. Having sequential numbers
            # 3. Having multiple numbers

            if len(group) < 2:
                continue

            numbers = sorted([num for _, _, num in group])

            # Check for sequential pattern
            sequential_count = 0
            for i in range(len(numbers) - 1):
                if numbers[i + 1] - numbers[i] == 1:
                    sequential_count += 1

            # Score: more sequential = better, lower x = better
            score = sequential_count * 10 - x_key

            if score > best_score:
                best_score = score
                best_group = group

        if best_group:
            # Get max x from the group + margin
            max_x = max(cx for cx, _, _ in best_group)
            return max_x + 0.3

        return 1.0

    def _detect_vertical_boundaries(self, lines: List[Dict]) -> Tuple[float, float]:
        """
        Detect header (top) and footer (bottom) boundaries.

        Returns:
            (header_y_min, footer_y_max)
        """
        # Header keywords should be exact or start-of-content matches
        # to avoid matching data like "ไม่พบรายละเอียด"
        header_keywords = [
            'รายละเอียดประกอบ', 'ลำดับ', 'ประเภท', 'วัน / เดือน / ปี',
            'มูลค่าปัจจุบัน', 'ผู้ยื่นบัญชี', 'คู่สมรส', 'เจ้าของ',
            'ที่ได้มา', 'รายการ', 'จำนวน', 'หน่วย'
        ]
        # Keywords to exclude from header detection
        header_exclude = ['ไม่พบ', 'ไม่ทราบ', 'ไม่ระบุ']

        footer_keywords = [
            'หมายเหตุ', 'ลงชื่อ', '- ลับ -'
        ]
        # Don't treat owner column headers as footer
        footer_exclude = ['ผย.', 'คส.', 'บ.']

        header_y = 0.8  # Default - lower to catch headers at top of page
        footer_y = 10.5  # Default

        for line in lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [0] * 8)
            _, cy = get_polygon_center(polygon)

            # Skip if content contains excluded phrases
            if any(excl in content for excl in header_exclude):
                continue

            # Check for header content - only in top 2.0 of page
            if any(kw in content for kw in header_keywords):
                if cy < 2.0:  # Only consider very top portion for headers
                    header_y = max(header_y, cy + 0.3)

            # Check for footer content
            if any(kw in content for kw in footer_keywords):
                if any(excl in content for excl in footer_exclude):
                    continue
                if cy > 8.0:  # Only consider bottom portion
                    footer_y = min(footer_y, cy - 0.2)

        return header_y, footer_y

    def _detect_columns(self, lines: List[Dict], row_num_x_max: float,
                        header_y_min: float, footer_y_max: float) -> Dict:
        """
        Detect column boundaries based on content analysis.
        """
        columns = {}

        # Collect x positions of different content types
        date_x_positions = []
        value_x_positions = []
        checkmark_x_positions = []

        date_pattern = re.compile(r'\d{1,2}/\d{1,2}/\d{4}|\d{1,2}\s*[ก-๙\.]+\s*\d{4}')
        value_pattern = re.compile(r'[\d,]+\.\d{2}')
        checkmark_chars = ['/', '✓', 'V', 'v', '1', 'I', 'l', '|', '✔']

        for line in lines:
            content = line.get('content', '').strip()
            polygon = line.get('polygon', [0] * 8)
            cx, cy = get_polygon_center(polygon)

            # Skip header/footer
            if cy < header_y_min or cy > footer_y_max:
                continue

            # Skip row number column
            if cx < row_num_x_max:
                continue

            # Detect date column
            if date_pattern.search(content):
                date_x_positions.append(cx)

            # Detect value column
            if value_pattern.search(content):
                value_x_positions.append(cx)

            # Detect checkmark column
            if content in checkmark_chars:
                checkmark_x_positions.append(cx)

        # Calculate column ranges
        if date_x_positions:
            date_min = min(date_x_positions) - 0.3
            date_max = max(date_x_positions) + 0.3
            columns['date'] = (date_min, date_max)

        if value_x_positions:
            value_min = min(value_x_positions) - 0.3
            value_max = max(value_x_positions) + 0.3
            columns['value'] = (value_min, value_max)

        if checkmark_x_positions:
            owner_min = min(checkmark_x_positions) - 0.2
            owner_max = max(checkmark_x_positions) + 0.2
            columns['owner'] = (owner_min, owner_max)

        # Type column is between row_num and date/value
        type_x_min = row_num_x_max
        type_x_max = min(
            columns.get('date', (5.0, 5.0))[0],
            columns.get('value', (6.0, 6.0))[0]
        ) - 0.2
        columns['type'] = (type_x_min, max(type_x_max, type_x_min + 1.0))

        return columns

    def _get_default_layout(self, page_type: str) -> Dict:
        """Get default layout parameters for a page type."""
        defaults = {
            'land': {
                'row_num_x_max': 1.0,
                'type_col_x_range': (0.9, 1.9),
                'date_col_x_range': (4.5, 5.5),
                'value_col_x_range': (6.0, 7.0),
                'owner_col_x_range': (6.9, 8.1),
                'header_y_min': 2.5,
                'footer_y_max': 10.5,
                'detected': False
            },
            'building': {
                'row_num_x_max': 1.0,
                'type_col_x_range': (0.9, 2.5),
                'date_col_x_range': (4.2, 5.5),
                'value_col_x_range': (5.5, 7.0),
                'owner_col_x_range': (6.8, 8.1),
                'header_y_min': 2.0,
                'footer_y_max': 10.5,
                'detected': False
            },
            'vehicle': {
                'row_num_x_max': 1.0,
                'type_col_x_range': (0.9, 2.5),
                'date_col_x_range': (4.2, 5.5),
                'value_col_x_range': (5.5, 7.0),
                'owner_col_x_range': (6.8, 8.1),
                'header_y_min': 2.0,
                'footer_y_max': 10.5,
                'detected': False
            },
            'rights': {
                'row_num_x_max': 1.0,
                'type_col_x_range': (1.0, 3.8),
                'date_col_x_range': (3.8, 5.6),
                'value_col_x_range': (6.0, 7.0),
                'owner_col_x_range': (6.9, 8.1),
                'header_y_min': 0.8,
                'footer_y_max': 10.5,
                'detected': False
            },
            'other': {
                'row_num_x_max': 1.0,
                'type_col_x_range': (1.0, 4.0),
                'date_col_x_range': (4.3, 5.3),
                'value_col_x_range': (5.8, 7.0),
                'owner_col_x_range': (6.8, 8.1),
                'header_y_min': 0.8,
                'footer_y_max': 10.5,
                'detected': False
            }
        }
        return defaults.get(page_type, defaults['land'])


class AdaptiveExtractor:
    """
    Adaptive asset extractor that uses detected layouts.
    """

    def __init__(self):
        self.detector = LayoutDetector()

    def extract_rows(self, lines: List[Dict], page_type: str) -> List[Dict]:
        """
        Extract asset rows using auto-detected layout.

        Args:
            lines: Page lines
            page_type: 'land', 'building', 'vehicle', 'rights', 'other'

        Returns:
            List of extracted asset dictionaries
        """
        # Detect layout
        layout = self.detector.detect_page_layout(lines, page_type)

        # Sort lines by position
        sorted_lines = sorted(
            lines,
            key=lambda x: (
                get_polygon_center(x.get('polygon', [0] * 8))[1],
                get_polygon_center(x.get('polygon', [0] * 8))[0]
            )
        )

        assets = []
        current_asset = None

        for line in sorted_lines:
            content = line.get('content', '').strip()
            polygon = line.get('polygon', [0] * 8)
            cx, cy = get_polygon_center(polygon)

            # Skip header/footer
            if cy < layout['header_y_min'] or cy > layout['footer_y_max']:
                continue

            # Detect row number
            if cx < layout['row_num_x_max'] and re.match(r'^\d{1,2}$', content):
                # Save previous asset
                if current_asset and current_asset.get('valuation'):
                    assets.append(current_asset)

                row_num = int(content)
                current_asset = {
                    'index': row_num,
                    'y_pos': cy,
                    'layout': layout,
                    'content_parts': []
                }

            if not current_asset:
                continue

            # Check y tolerance
            if abs(cy - current_asset['y_pos']) > 1.5:
                continue

            # Collect content with position info
            current_asset['content_parts'].append({
                'content': content,
                'cx': cx,
                'cy': cy
            })

        # Save last asset
        if current_asset and current_asset.get('content_parts'):
            assets.append(current_asset)

        return assets


def detect_layout_for_page(lines: List[Dict], page_type: str = 'asset') -> Dict:
    """
    Convenience function to detect layout for a page.

    Args:
        lines: List of line dictionaries
        page_type: Type of asset page

    Returns:
        Layout parameters dictionary
    """
    detector = LayoutDetector()
    return detector.detect_page_layout(lines, page_type)


# Singleton detector for reuse
_global_detector = None


def get_detector() -> LayoutDetector:
    """Get or create global layout detector."""
    global _global_detector
    if _global_detector is None:
        _global_detector = LayoutDetector()
    return _global_detector


if __name__ == '__main__':
    # Test with sample data
    print("Layout Detector Module")
    print("=" * 50)
    print("Use detect_layout_for_page(lines, page_type) to detect layout")
    print("Available page types: land, building, vehicle, rights, other")
