"""
Layout Normalizer Module

This module provides functionality to normalize skewed document layouts
by detecting and correcting rotation/skew in OCR polygon coordinates.

The normalization process:
1. Detect skew angle from polygon coordinates
2. Find reference anchor points (like "- ลับ -", "หน้า X", etc.)
3. Calculate transformation matrix to align with template
4. Apply transformation to normalize all polygon coordinates
"""

import math
import json
import os
from typing import Dict, List, Tuple, Optional, Any
from functools import lru_cache


# Reference anchor patterns for alignment
ANCHOR_PATTERNS = [
    '- ลับ -',
    'หน้า ',
    'ข้อมูลส่วนบุคคล',
    'ประวัติการทำงาน',
    'บัญชีทรัพย์สิน',
    'ผู้ยื่นบัญชี',
    'เลขประจำตัวประชาชน',
]

# Template reference positions (y-coordinates for common anchors)
TEMPLATE_ANCHORS = {
    '- ลับ -': {'y_top': 0.23, 'page_types': ['info_page']},
    'หน้า 1': {'y_top': 0.58, 'page_types': ['info_page']},
    'ข้อมูลส่วนบุคคล': {'y_top': 0.97, 'page_types': ['info_page']},
}


class LayoutNormalizer:
    """
    Normalizes document layout by correcting skew and aligning coordinates.

    Usage:
        normalizer = LayoutNormalizer(template_path)
        normalized_json = normalizer.normalize(json_content)
    """

    def __init__(self, template_path: Optional[str] = None):
        """
        Initialize the normalizer.

        Args:
            template_path: Path to template JSON file for reference alignment
        """
        self.template = None
        self.template_pages = {}

        if template_path and os.path.exists(template_path):
            self._load_template(template_path)

    def _load_template(self, template_path: str):
        """Load template reference data."""
        with open(template_path, 'r', encoding='utf-8') as f:
            self.template = json.load(f)

        # Index template pages by page number
        for page in self.template.get('pages', []):
            page_num = page.get('page_number', 0)
            self.template_pages[page_num] = page

    def calculate_skew_angle(self, polygon: List[float]) -> Optional[float]:
        """
        Calculate skew angle from a polygon.

        Polygon format: [x1, y1, x2, y2, x3, y3, x4, y4]
        where points are: top-left, top-right, bottom-right, bottom-left

        Returns angle in degrees (positive = clockwise rotation)
        """
        if len(polygon) != 8:
            return None

        x1, y1, x2, y2, x3, y3, x4, y4 = polygon

        # Calculate angle of top edge
        dx = x2 - x1
        dy = y2 - y1

        if abs(dx) < 0.001:  # Nearly vertical line
            return 90.0 if dy > 0 else -90.0

        angle = math.atan2(dy, dx) * 180 / math.pi
        return angle

    def detect_page_skew(self, page: Dict) -> float:
        """
        Detect average skew angle for a page using multiple lines.

        Args:
            page: Page dictionary with 'lines' containing polygons

        Returns:
            Average skew angle in degrees
        """
        lines = page.get('lines', [])
        if not lines:
            return 0.0

        angles = []
        weights = []

        for line in lines:
            polygon = line.get('polygon', [])
            content = line.get('content', '')

            if len(polygon) != 8:
                continue

            angle = self.calculate_skew_angle(polygon)
            if angle is None or abs(angle) > 10:  # Skip extreme angles
                continue

            # Weight by line width (longer lines are more reliable)
            x1, y1, x2, y2 = polygon[:4]
            width = math.sqrt((x2-x1)**2 + (y2-y1)**2)

            # Higher weight for anchor patterns
            weight = width
            if any(pattern in content for pattern in ANCHOR_PATTERNS):
                weight *= 2.0

            angles.append(angle)
            weights.append(weight)

        if not angles:
            return 0.0

        # Weighted average
        total_weight = sum(weights)
        if total_weight == 0:
            return sum(angles) / len(angles)

        weighted_sum = sum(a * w for a, w in zip(angles, weights))
        return weighted_sum / total_weight

    def rotate_point(self, x: float, y: float, angle: float,
                     cx: float, cy: float) -> Tuple[float, float]:
        """
        Rotate a point around a center point.

        Args:
            x, y: Point coordinates
            angle: Rotation angle in degrees (positive = counter-clockwise)
            cx, cy: Center of rotation

        Returns:
            Rotated (x, y) coordinates
        """
        rad = math.radians(-angle)  # Negative to correct skew
        cos_a = math.cos(rad)
        sin_a = math.sin(rad)

        # Translate to origin
        dx = x - cx
        dy = y - cy

        # Rotate
        new_x = dx * cos_a - dy * sin_a
        new_y = dx * sin_a + dy * cos_a

        # Translate back
        return new_x + cx, new_y + cy

    def normalize_polygon(self, polygon: List[float], angle: float,
                          page_width: float, page_height: float) -> List[float]:
        """
        Normalize a polygon by correcting skew.

        Args:
            polygon: [x1, y1, x2, y2, x3, y3, x4, y4]
            angle: Skew angle to correct
            page_width, page_height: Page dimensions for center calculation

        Returns:
            Normalized polygon coordinates
        """
        if len(polygon) != 8 or abs(angle) < 0.01:
            return polygon

        # Use page center as rotation center
        cx = page_width / 2
        cy = page_height / 2

        normalized = []
        for i in range(0, 8, 2):
            x, y = polygon[i], polygon[i+1]
            new_x, new_y = self.rotate_point(x, y, angle, cx, cy)
            normalized.extend([round(new_x, 4), round(new_y, 4)])

        return normalized

    def normalize_page(self, page: Dict) -> Dict:
        """
        Normalize all polygons in a page.

        Args:
            page: Page dictionary

        Returns:
            Page with normalized polygons
        """
        # Detect skew
        skew_angle = self.detect_page_skew(page)

        if abs(skew_angle) < 0.05:  # Skip if minimal skew
            return page

        page_width = page.get('width', 8.2639)
        page_height = page.get('height', 11.6944)

        # Create normalized page
        normalized_page = page.copy()
        normalized_lines = []

        for line in page.get('lines', []):
            normalized_line = line.copy()
            polygon = line.get('polygon', [])

            if len(polygon) == 8:
                normalized_line['polygon'] = self.normalize_polygon(
                    polygon, skew_angle, page_width, page_height
                )
                # Store original for debugging
                normalized_line['_original_polygon'] = polygon

            normalized_lines.append(normalized_line)

        normalized_page['lines'] = normalized_lines
        normalized_page['_skew_corrected'] = skew_angle

        return normalized_page

    def normalize(self, json_content: Dict, pages: Optional[List[int]] = None) -> Dict:
        """
        Normalize all pages in a document.

        Args:
            json_content: Full JSON document content
            pages: Optional list of page numbers to normalize (None = all)

        Returns:
            Normalized JSON content
        """
        if 'pages' not in json_content:
            return json_content

        normalized = json_content.copy()
        normalized_pages = []

        for page in json_content['pages']:
            page_num = page.get('page_number', 0)

            # Only normalize specified pages or all if not specified
            if pages is None or page_num in pages:
                normalized_page = self.normalize_page(page)
            else:
                normalized_page = page

            normalized_pages.append(normalized_page)

        normalized['pages'] = normalized_pages
        normalized['_normalized'] = True

        return normalized

    def get_normalized_coordinates(self, line: Dict, page: Dict) -> Dict:
        """
        Get normalized coordinates for a single line.

        Returns dict with x, y, width, height, and angle.
        """
        polygon = line.get('polygon', [])

        if len(polygon) != 8:
            return {'x': 0, 'y': 0, 'width': 0, 'height': 0, 'angle': 0}

        x1, y1, x2, y2, x3, y3, x4, y4 = polygon

        # Calculate bounding box
        min_x = min(x1, x2, x3, x4)
        max_x = max(x1, x2, x3, x4)
        min_y = min(y1, y2, y3, y4)
        max_y = max(y1, y2, y3, y4)

        return {
            'x': min_x,
            'y': min_y,
            'width': max_x - min_x,
            'height': max_y - min_y,
            'center_x': (min_x + max_x) / 2,
            'center_y': (min_y + max_y) / 2,
            'angle': self.calculate_skew_angle(polygon) or 0
        }


class TemplateAligner:
    """
    Aligns document content to a template using anchor points.

    This provides more precise alignment by finding common reference
    points between the document and template.
    """

    def __init__(self, template_path: str):
        """
        Initialize with template reference.

        Args:
            template_path: Path to template JSON file
        """
        self.template_path = template_path
        self.template = None
        self.template_anchors = {}

        self._load_template()

    def _load_template(self):
        """Load and analyze template for anchor points."""
        with open(self.template_path, 'r', encoding='utf-8') as f:
            self.template = json.load(f)

        # Extract anchor points from each page
        for page in self.template.get('pages', []):
            page_num = page.get('page_number', 0)
            self.template_anchors[page_num] = self._find_anchors(page)

    def _find_anchors(self, page: Dict) -> List[Dict]:
        """Find anchor points in a page."""
        anchors = []

        for line in page.get('lines', []):
            content = line.get('content', '')
            polygon = line.get('polygon', [])

            # Check if this is an anchor pattern
            for pattern in ANCHOR_PATTERNS:
                if pattern in content and len(polygon) == 8:
                    x1, y1 = polygon[0], polygon[1]
                    anchors.append({
                        'pattern': pattern,
                        'content': content,
                        'x': x1,
                        'y': y1,
                        'polygon': polygon
                    })
                    break

        return anchors

    def find_document_anchors(self, page: Dict) -> List[Dict]:
        """Find anchor points in a document page."""
        return self._find_anchors(page)

    def calculate_alignment_transform(self, doc_page: Dict,
                                       template_page_num: int) -> Optional[Dict]:
        """
        Calculate transformation to align document to template.

        Returns transformation parameters: scale, rotation, translation.
        """
        doc_anchors = self.find_document_anchors(doc_page)
        template_anchors = self.template_anchors.get(template_page_num, [])

        if not doc_anchors or not template_anchors:
            return None

        # Find matching anchors
        matches = []
        for doc_anchor in doc_anchors:
            for tmpl_anchor in template_anchors:
                if doc_anchor['pattern'] == tmpl_anchor['pattern']:
                    matches.append((doc_anchor, tmpl_anchor))
                    break

        if not matches:
            return None

        # Calculate average offset
        dx_sum = 0
        dy_sum = 0

        for doc_anchor, tmpl_anchor in matches:
            dx_sum += tmpl_anchor['x'] - doc_anchor['x']
            dy_sum += tmpl_anchor['y'] - doc_anchor['y']

        n = len(matches)

        return {
            'dx': dx_sum / n,
            'dy': dy_sum / n,
            'scale': 1.0,  # Can be enhanced to detect scale
            'rotation': 0.0,  # Already handled by LayoutNormalizer
            'anchors_matched': n
        }

    def align_page(self, page: Dict, template_page_num: int) -> Dict:
        """
        Align a document page to the template.

        Args:
            page: Document page to align
            template_page_num: Target template page number

        Returns:
            Aligned page with transformed coordinates
        """
        transform = self.calculate_alignment_transform(page, template_page_num)

        if transform is None:
            return page

        dx = transform['dx']
        dy = transform['dy']

        aligned_page = page.copy()
        aligned_lines = []

        for line in page.get('lines', []):
            aligned_line = line.copy()
            polygon = line.get('polygon', [])

            if len(polygon) == 8:
                # Apply translation
                aligned_polygon = [
                    polygon[0] + dx, polygon[1] + dy,
                    polygon[2] + dx, polygon[3] + dy,
                    polygon[4] + dx, polygon[5] + dy,
                    polygon[6] + dx, polygon[7] + dy,
                ]
                aligned_line['polygon'] = [round(v, 4) for v in aligned_polygon]
                aligned_line['_alignment_applied'] = True

            aligned_lines.append(aligned_line)

        aligned_page['lines'] = aligned_lines
        aligned_page['_alignment_transform'] = transform

        return aligned_page


def get_normalizer(template_path: Optional[str] = None) -> LayoutNormalizer:
    """
    Get a LayoutNormalizer instance.

    Args:
        template_path: Optional path to template JSON

    Returns:
        LayoutNormalizer instance
    """
    if template_path is None:
        # Default template path
        base_dir = os.path.dirname(os.path.dirname(__file__))
        template_path = os.path.join(base_dir, 'template-docs_raw.json')

    return LayoutNormalizer(template_path)


def normalize_json_content(json_content: Dict,
                           template_path: Optional[str] = None) -> Dict:
    """
    Convenience function to normalize JSON content.

    Args:
        json_content: Document JSON content
        template_path: Optional template path

    Returns:
        Normalized JSON content
    """
    normalizer = get_normalizer(template_path)
    return normalizer.normalize(json_content)


# Testing utilities
def analyze_document_skew(json_path: str) -> Dict:
    """
    Analyze skew in a document.

    Returns summary of skew angles per page.
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    normalizer = LayoutNormalizer()
    results = {
        'file': os.path.basename(json_path),
        'pages': []
    }

    for page in data.get('pages', []):
        page_num = page.get('page_number', 0)
        skew = normalizer.detect_page_skew(page)

        results['pages'].append({
            'page_number': page_num,
            'skew_angle': round(skew, 3),
            'line_count': len(page.get('lines', []))
        })

    return results


if __name__ == '__main__':
    # Test the normalizer
    import sys

    if len(sys.argv) > 1:
        json_path = sys.argv[1]
        result = analyze_document_skew(json_path)
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print("Usage: python layout_normalizer.py <json_file>")
        print("Example: python layout_normalizer.py ../pipeline_input/json_extract/file.json")
