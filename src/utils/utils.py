"""
Utility functions for the pipeline steps
"""

import os


def get_input_paths(input_dir: str) -> dict:
    """
    Get input file paths based on the input directory.
    Automatically detects whether it's training or final test data.

    Args:
        input_dir: Base input directory

    Returns:
        dict with paths for doc_info, nacc_detail, submitter_info, json_dir
    """
    # Check if it's final_test folder
    if 'final_test' in input_dir:
        # Final test mode
        doc_info_path = os.path.join(input_dir, 'Test final_doc_info.csv')
        nacc_detail_path = os.path.join(input_dir, 'Test final_nacc_detail.csv')
        submitter_info_path = os.path.join(input_dir, 'Test final_submitter_info.csv')
        json_dir = os.path.join(input_dir, 'final_json_match')
    else:
        # Training mode (default)
        doc_info_path = os.path.join(input_dir, 'Train_doc_info.csv')
        nacc_detail_path = os.path.join(input_dir, 'Train_nacc_detail.csv')
        submitter_info_path = os.path.join(input_dir, 'Train_submitter_info.csv')
        json_dir = os.path.join(input_dir, 'json_extract')

    return {
        'doc_info': doc_info_path,
        'nacc_detail': nacc_detail_path,
        'submitter_info': submitter_info_path,
        'json_dir': json_dir
    }
