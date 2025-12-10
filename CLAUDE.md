# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Thai Government Financial Disclosure Document Extraction System - extracts structured data from Thai officials' financial disclosure PDFs (บัญชีแสดงรายการทรัพย์สินและหนี้สิน) using Azure Document Intelligence for OCR and Azure OpenAI for NLP processing.

## Common Commands

```bash
# Install dependencies
poetry install

# Run complete pipeline on training data (Phase 1a-1e with clean output)
poetry run scanx --phase 1 --all

# Run complete pipeline on test final data
poetry run scanx --phase 1 --final --all

# Run OCR extraction only (run once, then use --skip)
poetry run scanx --phase 0
poetry run scanx --phase 0 --final

# Run individual phases
poetry run scanx --phase 1e        # Data mapping only
poetry run scanx --phase 1d        # AI metadata only
poetry run scanx --phase 2a        # LLM position correction
poetry run scanx --phase 2b        # LLM statement correction

# Dev tools server (port 8888)
cd src/dev_tools && poetry run python main.py

# ScanX tools web panel (port 8000)
cd src/scanx-tools && poetry run python main.py
```

## Architecture

### Pipeline Flow
```
PDF → Phase 0 (OCR) → Phase 1a+1b (Page Type ID) → Phase 1c (Text) → Phase 1d (AI Metadata) → Phase 1e (CSV) → 13 Output CSVs
                                                                                                    ↓
                                                                              Phase 2a/2b (LLM Corrections)
```

### Key Modules (`src/submission_scanx/`)
- `pipeline.py` - Main CLI orchestrator (`scanx` command)
- `phase0_ocr.py` - Azure Document Intelligence PDF extraction
- `phase1_process.py` - Page type identification using regex with OCR variation handling
- `phase1c_text_extract.py` - Text extraction from matched pages
- `phase1d_metadata.py` - Hybrid regex + Azure OpenAI page metadata mapping
- `phase1e_mapping.py` - CSV data extraction coordinator
- `phase1_mapping/` - 11 step modules (step_1 through step_11) for specific CSV outputs
- `phase2_subtask/` - LLM-based data correction modules

### Data Flow Paths
- Training data: `src/training/train input/` → `src/result/from_train/`
- Test data: `src/test final/test final input/` → `src/result/final/`

### Output Directories (under `result/{from_train|final}/`)
- `processing_input/extract_raw/` - Phase 0 OCR JSON
- `processing_input/extract_matched/` - Phase 1a+1b page type JSON
- `processing_input/text_each_page/` - Phase 1c extracted text
- `processing_input/page_metadata/` - Phase 1d step mappings
- `mapping_output/` - Phase 1e final CSV files (13 files)

### Step-to-CSV Mapping
| Steps | Output CSVs |
|-------|-------------|
| step_1, step_2 | submitter_position.csv, submitter_old_name.csv |
| step_3_1, step_3_2, step_3_3 | spouse_info.csv, spouse_old_name.csv, spouse_position.csv |
| step_4 | relative_info.csv |
| step_5 | statement.csv, statement_detail.csv |
| step_6 | asset.csv |
| step_7-10 | asset_land_info.csv, asset_building_info.csv, asset_vehicle_info.csv, asset_other_asset_info.csv |
| step_11 | summary.csv |

## Environment Configuration

Create `.env` in `src/submission_scanx/`:
```env
AZURE_OPENAI_ENDPOINT="https://your-resource.openai.azure.com/"
AZURE_OPENAI_DEPLOYMENT_NAME="gpt-4o"
AZURE_OPENAI_API_KEY="your-key"
AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT="https://your-resource.cognitiveservices.azure.com/"
AZURE_DOCUMENT_INTELLIGENCE_API_KEY="your-key"
USE_HUNMAN_IN_LOOP="FALSE"  # Set TRUE to use page ignore config
```

## Important Notes

- All text data is Thai language (UTF-8)
- Dates use Thai Buddhist calendar (พ.ศ. = ค.ศ. + 543)
- Page type detection handles OCR variations (e.g., missing tone marks: `ข้อ` vs `ขอ`)
- Human-in-the-loop config: `src/training/human_loop/pre_pdf.json` or `src/test final/human_loop/pre_pdf.json`
