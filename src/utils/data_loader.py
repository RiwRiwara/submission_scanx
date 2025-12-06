"""
Centralized data loading module with caching support.

This module provides efficient data loading across all pipeline steps by:
- Caching loaded CSV files to avoid repeated I/O
- Caching JSON files for document processing
- Providing a unified interface for data access
- Normalizing skewed document layouts before processing
"""

import os
import json
import csv
from typing import Dict, List, Optional, Any
from functools import lru_cache

# Import layout normalizer for skew correction
from .layout_normalizer import LayoutNormalizer, get_normalizer


class PipelineDataLoader:
    """
    Centralized data loader with caching for pipeline processing.

    Usage:
        loader = PipelineDataLoader(input_dir)

        # Get all documents to process
        for doc_info in loader.doc_info_list:
            nacc_id = doc_info['nacc_id']
            nacc_detail = loader.get_nacc_detail(nacc_id)
            submitter_info = loader.get_submitter_info(nacc_detail.get('submitter_id'))
            json_content = loader.load_json(doc_info['doc_location_url'])

    Note: For final_test mode, uses name-based matching from filename to handle
    potential nacc_id mismatches in test CSV files.

    Page Metadata:
        The loader can use page_metadata/index.json to get exact pages for each step.
        Use get_step_pages(doc_name, step_name) to get page numbers for a step.
    """

    def __init__(self, input_dir: str, normalize_layout: bool = True, is_final: bool = None):
        """
        Initialize the data loader.

        Args:
            input_dir: Base input directory (pipeline_input or final_test)
            normalize_layout: Whether to normalize skewed document layouts
            is_final: Explicitly set final test mode. If None, auto-detect from path.
        """
        self.input_dir = input_dir
        self.normalize_layout = normalize_layout
        # Auto-detect final test mode from path if not explicitly set
        if is_final is None:
            input_lower = input_dir.lower()
            self.is_final_test = 'final_test' in input_lower or 'test final' in input_lower or 'final' in input_lower
        else:
            self.is_final_test = is_final
        self._paths = self._get_input_paths()

        # Cached data stores
        self._doc_info_list: Optional[List[Dict]] = None
        self._nacc_detail_dict: Optional[Dict[str, Dict]] = None
        self._submitter_info_dict: Optional[Dict[str, Dict]] = None
        self._nacc_detail_by_name: Optional[Dict[str, Dict]] = None  # For name-based lookup
        self._submitter_by_name: Optional[Dict[str, Dict]] = None  # For name-based lookup
        self._json_cache: Dict[str, Dict] = {}
        self._page_metadata: Optional[Dict] = None  # Page metadata cache
        self._text_each_page_cache: Dict[str, Dict] = {}  # Text each page cache

        # Initialize layout normalizer if enabled
        self._normalizer: Optional[LayoutNormalizer] = None
        if normalize_layout:
            base_dir = os.path.dirname(os.path.dirname(__file__))
            template_path = os.path.join(base_dir, 'template-docs_raw.json')
            if os.path.exists(template_path):
                self._normalizer = get_normalizer(template_path)
    
    def _get_input_paths(self) -> Dict[str, str]:
        """Get input file paths based on the input directory."""
        if self.is_final_test:
            return {
                'doc_info': os.path.join(self.input_dir, 'Test final_doc_info.csv'),
                'nacc_detail': os.path.join(self.input_dir, 'Test final_nacc_detail.csv'),
                'submitter_info': os.path.join(self.input_dir, 'Test final_submitter_info.csv'),
                'json_dir': os.path.join(self.input_dir, 'final_json_match')
            }
        else:
            return {
                'doc_info': os.path.join(self.input_dir, 'Train_doc_info.csv'),
                'nacc_detail': os.path.join(self.input_dir, 'Train_nacc_detail.csv'),
                'submitter_info': os.path.join(self.input_dir, 'Train_submitter_info.csv'),
                'json_dir': os.path.join(self.input_dir, 'json_extract')
            }
    
    @property
    def json_dir(self) -> str:
        """Get the JSON directory path."""
        return self._paths['json_dir']
    
    @property
    def doc_info_list(self) -> List[Dict]:
        """Get document info list (lazy loaded and cached)."""
        if self._doc_info_list is None:
            self._doc_info_list = self._load_csv(self._paths['doc_info'])
        return self._doc_info_list
    
    @property
    def nacc_detail_dict(self) -> Dict[str, Dict]:
        """Get NACC detail dictionary keyed by nacc_id (lazy loaded and cached)."""
        if self._nacc_detail_dict is None:
            rows = self._load_csv(self._paths['nacc_detail'])
            self._nacc_detail_dict = {row['nacc_id']: row for row in rows}
        return self._nacc_detail_dict
    
    @property
    def submitter_info_dict(self) -> Dict[str, Dict]:
        """Get submitter info dictionary keyed by submitter_id (lazy loaded and cached)."""
        if self._submitter_info_dict is None:
            rows = self._load_csv(self._paths['submitter_info'])
            self._submitter_info_dict = {row['submitter_id']: row for row in rows}
        return self._submitter_info_dict
    
    def _load_csv(self, path: str) -> List[Dict]:
        """Load a CSV file and return list of dictionaries."""
        rows = []
        with open(path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        return rows
    
    def get_nacc_detail(self, nacc_id: str) -> Dict:
        """Get NACC detail for a specific nacc_id."""
        return self.nacc_detail_dict.get(nacc_id, {})
    
    def get_submitter_info(self, submitter_id: str) -> Dict:
        """Get submitter info for a specific submitter_id."""
        return self.submitter_info_dict.get(submitter_id, {})
    
    @property
    def nacc_detail_by_name(self) -> Dict[str, Dict]:
        """Get NACC detail dictionary keyed by 'first_name_last_name' for name-based lookup.

        Note: For duplicate names, use nacc_detail_by_name_case for more precise matching.
        """
        if self._nacc_detail_by_name is None:
            self._nacc_detail_by_name = {}
            for row in self._load_csv(self._paths['nacc_detail']):
                first_name = row.get('first_name', '').strip()
                last_name = row.get('last_name', '').strip()
                key = f"{first_name}_{last_name}"
                if key and key != '_':
                    self._nacc_detail_by_name[key] = row
        return self._nacc_detail_by_name

    @property
    def nacc_detail_by_name_case(self) -> Dict[str, Dict]:
        """Get NACC detail dictionary keyed by 'first_name_last_name_submitted_case' for precise matching."""
        if not hasattr(self, '_nacc_detail_by_name_case') or self._nacc_detail_by_name_case is None:
            self._nacc_detail_by_name_case = {}
            for row in self._load_csv(self._paths['nacc_detail']):
                first_name = row.get('first_name', '').strip()
                last_name = row.get('last_name', '').strip()
                submitted_case = row.get('submitted_case', '').strip()
                key = f"{first_name}_{last_name}_{submitted_case}"
                if first_name and last_name:
                    self._nacc_detail_by_name_case[key] = row
        return self._nacc_detail_by_name_case
    
    @property
    def submitter_by_name(self) -> Dict[str, Dict]:
        """Get submitter info dictionary keyed by 'first_name_last_name' for name-based lookup."""
        if self._submitter_by_name is None:
            self._submitter_by_name = {}
            for row in self._load_csv(self._paths['submitter_info']):
                first_name = row.get('first_name', '').strip()
                last_name = row.get('last_name', '').strip()
                key = f"{first_name}_{last_name}"
                if key and key != '_':
                    self._submitter_by_name[key] = row
        return self._submitter_by_name
    
    def extract_name_from_filename(self, filename: str) -> tuple:
        """
        Extract first_name, last_name, and case from document filename.

        Filename format: {first_name}_{last_name}_{position}_{case}_{date}.pdf
        Example: วีรศักดิ์_หวังศุภกิจโกศล_รัฐมนตรีช่วยว่าการกระทรวง_กรณีพ้นจากตำแหน่ง_12_เม.ย._2566.pdf

        Returns:
            tuple: (first_name, last_name, submitted_case) or (None, None, None) if extraction fails
        """
        # Remove file extension
        name = filename.replace('.pdf', '').replace('.json', '')
        parts = name.split('_')

        if len(parts) >= 2:
            first_name = parts[0]
            last_name = parts[1]

            # Extract case type (กรณีเข้ารับตำแหน่ง or กรณีพ้นจากตำแหน่ง)
            submitted_case = None
            for part in parts:
                if 'กรณีเข้ารับตำแหน่ง' in part:
                    submitted_case = 'กรณีเข้ารับตำแหน่ง'
                    break
                elif 'กรณีพ้นจากตำแหน่ง' in part:
                    submitted_case = 'กรณีพ้นจากตำแหน่ง'
                    break

            return first_name, last_name, submitted_case
        return None, None, None
    
    def get_nacc_detail_by_name_case(self, first_name: str, last_name: str, submitted_case: str) -> Dict:
        """Get NACC detail by matching first_name, last_name, and submitted_case."""
        if submitted_case:
            key = f"{first_name}_{last_name}_{submitted_case}"
            result = self.nacc_detail_by_name_case.get(key, {})
            if result:
                return result
        # Fallback to simple name lookup
        return self.get_nacc_detail_by_name(first_name, last_name)

    def get_nacc_detail_by_name(self, first_name: str, last_name: str) -> Dict:
        """Get NACC detail by matching first_name and last_name."""
        key = f"{first_name}_{last_name}"
        return self.nacc_detail_by_name.get(key, {})
    
    def get_submitter_by_name(self, first_name: str, last_name: str) -> Dict:
        """Get submitter info by matching first_name and last_name."""
        key = f"{first_name}_{last_name}"
        return self.submitter_by_name.get(key, {})
    
    def load_json(self, doc_location_url: str, normalize: bool = None) -> Optional[Dict]:
        """
        Load JSON content for a document (with caching and optional normalization).

        Args:
            doc_location_url: Document location URL (PDF filename)
            normalize: Override default normalization setting (None = use default)

        Returns:
            JSON content dictionary or None if not found
        """
        json_filename = doc_location_url.replace('.pdf', '.json')

        # Determine if we should normalize
        should_normalize = normalize if normalize is not None else self.normalize_layout

        # Create cache key that includes normalization state
        cache_key = f"{json_filename}:norm={should_normalize}"

        # Check cache first
        if cache_key in self._json_cache:
            return self._json_cache[cache_key]

        json_path = os.path.join(self._paths['json_dir'], json_filename)

        if not os.path.exists(json_path):
            print(f"Warning: JSON file not found: {json_path}")
            return None

        with open(json_path, 'r', encoding='utf-8') as f:
            content = json.load(f)

        # Apply layout normalization if enabled
        if should_normalize and self._normalizer is not None:
            content = self._normalizer.normalize(content)

        # Cache the result
        self._json_cache[cache_key] = content
        return content
    
    def clear_json_cache(self):
        """Clear the JSON cache to free memory."""
        self._json_cache.clear()

    @property
    def page_metadata(self) -> Dict:
        """Get page metadata index (lazy loaded and cached)."""
        if self._page_metadata is None:
            metadata_path = os.path.join(self.input_dir, 'page_metadata', 'index.json')
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    self._page_metadata = json.load(f)
            else:
                self._page_metadata = {'documents': []}
        return self._page_metadata

    def get_doc_metadata(self, doc_location_url: str) -> Optional[Dict]:
        """
        Get page metadata for a specific document.

        Args:
            doc_location_url: Document location URL (PDF filename)

        Returns:
            Document metadata dict with 'steps' mapping or None if not found
        """
        # Convert PDF filename to doc_name format
        doc_name = doc_location_url.replace('.pdf', '').replace('.json', '')

        for doc in self.page_metadata.get('documents', []):
            if doc.get('doc_name') == doc_name:
                return doc
        return None

    def get_step_pages(self, doc_location_url: str, step_name: str) -> List[int]:
        """
        Get page numbers for a specific step in a document.

        Args:
            doc_location_url: Document location URL (PDF filename)
            step_name: Step name (e.g., 'step_1', 'step_2')

        Returns:
            List of page numbers (1-indexed) for the step
        """
        doc_metadata = self.get_doc_metadata(doc_location_url)
        if doc_metadata:
            return doc_metadata.get('steps', {}).get(step_name, [])
        return []

    def load_page_text(self, doc_location_url: str, page_num: int) -> Optional[Dict]:
        """
        Load text_each_page data for a specific page.

        Args:
            doc_location_url: Document location URL (PDF filename)
            page_num: Page number (1-indexed)

        Returns:
            Page text data with 'content', 'lines' etc. or None if not found
        """
        doc_name = doc_location_url.replace('.pdf', '').replace('.json', '')
        cache_key = f"{doc_name}:{page_num}"

        if cache_key in self._text_each_page_cache:
            return self._text_each_page_cache[cache_key]

        page_path = os.path.join(
            self.input_dir, 'text_each_page', doc_name,
            f'page_{page_num:03d}.json'
        )

        if not os.path.exists(page_path):
            return None

        with open(page_path, 'r', encoding='utf-8') as f:
            page_data = json.load(f)

        self._text_each_page_cache[cache_key] = page_data
        return page_data

    def load_step_pages_text(self, doc_location_url: str, step_name: str) -> List[Dict]:
        """
        Load all page text data for a specific step.

        Args:
            doc_location_url: Document location URL (PDF filename)
            step_name: Step name (e.g., 'step_1', 'step_2')

        Returns:
            List of page text data dictionaries
        """
        page_nums = self.get_step_pages(doc_location_url, step_name)
        pages = []
        for page_num in page_nums:
            page_data = self.load_page_text(doc_location_url, page_num)
            if page_data:
                pages.append(page_data)
        return pages
    
    def get_document_context(self, doc_info: Dict) -> Dict[str, Any]:
        """
        Get full document context including nacc_detail, submitter_info, and json_content.

        Primary: Uses name-based matching from filename with case type for precise matching.
        This handles duplicate names by using the submitted_case (กรณีเข้ารับตำแหน่ง/กรณีพ้นจากตำแหน่ง).
        Fallback: Uses nacc_id from doc_info if name-based matching fails.

        Args:
            doc_info: Document info dictionary

        Returns:
            Dictionary with keys: doc_info, nacc_detail, submitter_info, json_content
        """
        doc_location_url = doc_info.get('doc_location_url', '')
        json_content = self.load_json(doc_location_url)

        # Primary: Use name-based matching from filename with case type
        # This handles duplicate names (same person with different submission cases)
        first_name, last_name, submitted_case = self.extract_name_from_filename(doc_location_url)
        if first_name and last_name:
            # Try precise match with name + case first
            nacc_detail = self.get_nacc_detail_by_name_case(first_name, last_name, submitted_case)
            submitter_info = self.get_submitter_by_name(first_name, last_name)

            if nacc_detail and submitter_info:
                return {
                    'doc_info': doc_info,
                    'nacc_detail': nacc_detail,
                    'submitter_info': submitter_info,
                    'json_content': json_content
                }

        # Fallback: Use nacc_id from doc_info (for training data)
        nacc_id = doc_info.get('nacc_id', '')
        nacc_detail = self.get_nacc_detail(nacc_id)

        if nacc_detail:
            submitter_id = nacc_detail.get('submitter_id', '')
            submitter_info = self.get_submitter_info(submitter_id)

            if submitter_info:
                return {
                    'doc_info': doc_info,
                    'nacc_detail': nacc_detail,
                    'submitter_info': submitter_info,
                    'json_content': json_content
                }

        # Return empty if both methods fail
        return {
            'doc_info': doc_info,
            'nacc_detail': nacc_detail or {},
            'submitter_info': {},
            'json_content': json_content
        }
    
    def iterate_documents(self):
        """
        Generator that yields document context for each document.
        
        Yields:
            Dictionary with doc_info, nacc_detail, submitter_info, json_content
        """
        for doc_info in self.doc_info_list:
            context = self.get_document_context(doc_info)
            if context['json_content'] is not None:
                yield context

    @staticmethod
    def find_pages_by_type(json_content: Dict, page_type: str) -> List[int]:
        """
        Find page indices by page_type from _page_info metadata.
        
        Args:
            json_content: JSON content dictionary
            page_type: Page type to search for (e.g., 'personal_info', 'children', 'siblings')
            
        Returns:
            List of page indices (0-based) matching the page type
        """
        result = []
        pages = json_content.get('pages', [])
        
        for idx, page in enumerate(pages):
            page_info = page.get('_page_info', {})
            if page_info.get('page_type') == page_type:
                result.append(idx)
        
        return result

    @staticmethod
    def find_pages_by_types(json_content: Dict, page_types: List[str]) -> Dict[str, List[int]]:
        """
        Find page indices for multiple page types.
        
        Args:
            json_content: JSON content dictionary
            page_types: List of page types to search for
            
        Returns:
            Dictionary mapping page_type to list of page indices
        """
        result = {pt: [] for pt in page_types}
        pages = json_content.get('pages', [])
        
        for idx, page in enumerate(pages):
            page_info = page.get('_page_info', {})
            pt = page_info.get('page_type', '')
            if pt in result:
                result[pt].append(idx)
        
        return result

    @staticmethod
    def get_page_info(json_content: Dict, page_idx: int) -> Dict:
        """
        Get _page_info metadata for a specific page.
        
        Args:
            json_content: JSON content dictionary
            page_idx: Page index (0-based)
            
        Returns:
            Page info dictionary or empty dict if not found
        """
        pages = json_content.get('pages', [])
        if 0 <= page_idx < len(pages):
            return pages[page_idx].get('_page_info', {})
        return {}

    @staticmethod
    def get_processing_info(json_content: Dict) -> Dict:
        """
        Get _processing_info metadata for the document.
        
        Args:
            json_content: JSON content dictionary
            
        Returns:
            Processing info dictionary or empty dict if not found
        """
        return json_content.get('_processing_info', {})


class CSVWriter:
    """
    Helper class for writing CSV output files.
    
    Usage:
        writer = CSVWriter(output_dir, 'submitter_position.csv', fieldnames)
        writer.write_rows(all_positions)
    """
    
    def __init__(self, output_dir: str, filename: str, fieldnames: List[str]):
        """
        Initialize the CSV writer.
        
        Args:
            output_dir: Output directory path
            filename: Output filename
            fieldnames: List of column names
        """
        self.output_dir = output_dir
        self.filename = filename
        self.fieldnames = fieldnames
        self.output_path = os.path.join(output_dir, filename)
    
    def write_rows(self, rows: List[Dict]) -> int:
        """
        Write rows to the CSV file.
        
        Args:
            rows: List of row dictionaries
            
        Returns:
            Number of rows written
        """
        os.makedirs(self.output_dir, exist_ok=True)
        
        with open(self.output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        
        return len(rows)


# Convenience function for backward compatibility
def get_input_paths(input_dir: str, is_final: bool = None) -> dict:
    """
    Get input file paths based on the input directory.
    
    This function is kept for backward compatibility with existing step files.
    For new code, prefer using PipelineDataLoader.
    
    Args:
        input_dir: Base input directory
        is_final: Explicitly set final test mode. If None, auto-detect from path.
    """
    # Auto-detect final test mode from path if not explicitly set
    if is_final is None:
        input_lower = input_dir.lower()
        is_final = 'final_test' in input_lower or 'test final' in input_lower or 'final' in input_lower
    
    if is_final:
        return {
            'doc_info': os.path.join(input_dir, 'Test final_doc_info.csv'),
            'nacc_detail': os.path.join(input_dir, 'Test final_nacc_detail.csv'),
            'submitter_info': os.path.join(input_dir, 'Test final_submitter_info.csv'),
            'json_dir': os.path.join(input_dir, 'final_json_match')
        }
    else:
        return {
            'doc_info': os.path.join(input_dir, 'Train_doc_info.csv'),
            'nacc_detail': os.path.join(input_dir, 'Train_nacc_detail.csv'),
            'submitter_info': os.path.join(input_dir, 'Train_submitter_info.csv'),
            'json_dir': os.path.join(input_dir, 'json_extract')
        }
