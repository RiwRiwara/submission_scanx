"""
Phase 1 Mapping Module

Contains step-based extraction pipeline for converting OCR JSON data
into structured CSV output files.

Steps:
- step_1: Submitter positions
- step_2: Submitter old names
- step_3: Spouse information (3_1: info, 3_2: old names, 3_3: positions)
- step_4: Relatives (parents, children, siblings)
- step_5: Statements (income, expense, tax, assets, liabilities)
- step_6: Assets summary
- step_7: Land asset details
- step_8: Building asset details
- step_9: Vehicle asset details
- step_10: Other asset details
- step_11: Final summary aggregation
"""

from .mapping import run_pipeline

__all__ = ['run_pipeline']
