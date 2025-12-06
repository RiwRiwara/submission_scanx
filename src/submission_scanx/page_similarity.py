"""
Page Layout Similarity - Calculate similarity between pages based on polygon layouts.

Adapted from pipeline_oc for use in submission_scanx.
"""

import math
from typing import List


def polygon_to_box(polygon: List[float]) -> dict:
    """Convert polygon to bounding box."""
    xs = polygon[0::2]  # x0, x1, x2, x3
    ys = polygon[1::2]  # y0, y1, y2, y3

    return {
        "minX": min(xs),
        "minY": min(ys),
        "maxX": max(xs),
        "maxY": max(ys)
    }


def center_of_polygon(poly: List[float]) -> tuple:
    """Get center point of polygon."""
    xs = poly[0::2]
    ys = poly[1::2]
    return (
        sum(xs) / len(xs),
        sum(ys) / len(ys)
    )


def iou_similarity(polyA: List[float], polyB: List[float]) -> float:
    """Calculate IoU (Intersection over Union) similarity between two polygons."""
    a = polygon_to_box(polyA)
    b = polygon_to_box(polyB)

    inter_x = max(0, min(a["maxX"], b["maxX"]) - max(a["minX"], b["minX"]))
    inter_y = max(0, min(a["maxY"], b["maxY"]) - max(a["minY"], b["minY"]))

    intersection = inter_x * inter_y
    areaA = (a["maxX"] - a["minX"]) * (a["maxY"] - a["minY"])
    areaB = (b["maxX"] - b["minX"]) * (b["maxY"] - b["minY"])

    union = areaA + areaB - intersection

    if union == 0:
        return 0

    return intersection / union


def distance_similarity(polyA: List[float], polyB: List[float], max_dist: float = 1.0) -> float:
    """Calculate position similarity based on center distance."""
    c1 = center_of_polygon(polyA)
    c2 = center_of_polygon(polyB)

    dx = c1[0] - c2[0]
    dy = c1[1] - c2[1]
    dist = math.sqrt(dx*dx + dy*dy)

    # Normalize to 0-1
    sim = max(0, 1 - (dist / max_dist))
    return sim


def polygon_similarity(polyA: List[float], polyB: List[float], w_iou: float = 0.5, w_pos: float = 0.5) -> float:
    """Calculate combined similarity between two polygons."""
    iou = iou_similarity(polyA, polyB)
    pos = distance_similarity(polyA, polyB)
    return (iou * w_iou) + (pos * w_pos)


def page_layout_similarity(polygons_a: List[List[float]], polygons_b: List[List[float]]) -> float:
    """
    Calculate layout similarity between two pages based on their polygon layouts.

    Uses a greedy matching approach: for each polygon in page A, find the best
    matching polygon in page B and average the similarities.

    Args:
        polygons_a: list of normalized polygon arrays from page A
        polygons_b: list of normalized polygon arrays from page B

    Returns:
        float between 0 and 1 representing layout similarity
    """
    if not polygons_a or not polygons_b:
        return 0.0

    # For each polygon in A, find best match in B
    total_similarity = 0.0
    matched_b = set()

    for poly_a in polygons_a:
        best_sim = 0.0
        best_idx = -1

        for idx, poly_b in enumerate(polygons_b):
            if idx in matched_b:
                continue

            sim = polygon_similarity(poly_a, poly_b, w_iou=0.6, w_pos=0.4)
            if sim > best_sim:
                best_sim = sim
                best_idx = idx

        if best_idx >= 0:
            matched_b.add(best_idx)

        total_similarity += best_sim

    # Average similarity
    avg_sim = total_similarity / len(polygons_a)

    # Penalize if page sizes are very different
    size_ratio = min(len(polygons_a), len(polygons_b)) / max(len(polygons_a), len(polygons_b))

    # Weight by size similarity
    return avg_sim * (0.7 + 0.3 * size_ratio)
