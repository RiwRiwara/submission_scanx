# Submission ScanX

Thai Government Financial Disclosure Document Extraction System

## Overview

This project extracts structured data from Thai government officials' financial disclosure PDF documents (บัญชีแสดงรายการทรัพย์สินและหนี้สิน) submitted to the National Anti-Corruption Commission (NACC / ป.ป.ช.).

The system uses Azure Document Intelligence for PDF parsing and Azure OpenAI for natural language processing to convert unstructured PDF documents into structured CSV data.

## Project Structure

```
submission_scanx/
├── pyproject.toml                 # Poetry project configuration
├── README.md                      # This file
├── src/
│   ├── submission_scanx/          # Main Python package
│   │   ├── __init__.py
│   │   ├── .env                   # Azure credentials configuration
│   │   ├── pipeline.py            # Main pipeline runner
│   │   ├── phase0_ocr.py          # Phase 0: PDF to OCR extraction
│   │   ├── phase1_process.py      # Phase 1: JSON processing & matching
│   │   ├── phase1c_text_extract.py # Phase 1c: Text extraction
│   │   ├── phase1d_metadata.py    # Phase 1d: Page metadata mapping
│   │   └── page_similarity.py     # Page layout similarity utilities
│   ├── training/                  # Training dataset (69 documents)
│   │   ├── train input/           # Input data
│   │   │   ├── Train_doc_info.csv
│   │   │   ├── Train_nacc_detail.csv
│   │   │   ├── Train_submitter_info.csv
│   │   │   └── Train_pdf/pdf/     # 69 PDF files
│   │   ├── train output/          # Ground truth extracted data (13 CSVs)
│   │   └── train summary/         # Aggregated summary data
│   ├── test final/                # Test dataset (23 documents)
│   │   ├── test final input/
│   │   │   ├── Test final_doc_info.csv
│   │   │   ├── Test final_nacc_detail.csv
│   │   │   ├── Test final_submitter_info.csv
│   │   │   └── Test final_pdf/    # 23 PDF files
│   │   └── test final output/     # Output directory for predictions
│   ├── result/                    # Pipeline output
│   │   ├── from_train/            # Training data results
│   │   │   └── processing_input/
│   │   │       ├── extract_raw/       # Phase 0 output
│   │   │       ├── extract_matched/   # Phase 1b output
│   │   │       ├── text_each_page/    # Phase 1c output
│   │   │       └── page_metadata/     # Phase 1d output
│   │   └── final/                 # Test final results
│   │       └── processing_input/
│   │           ├── extract_raw/
│   │           ├── extract_matched/
│   │           ├── text_each_page/
│   │           └── page_metadata/
│   └── utils/                     # Reference data
│       ├── enum_type/             # Thai enumeration types (9 CSVs)
│       ├── thai-province-data/    # Thai geographic data (JSON)
│       └── template-docs_raw.json # Template document for page matching
└── tests/                         # Test modules
```

## Pipeline Phases

### Phase 0: PDF to OCR Extraction

Uses Azure Document Intelligence to extract text and layout from PDF documents.

**Input:** PDF files from `train input/Train_pdf/` or `test final input/Test final_pdf/`
**Output:** JSON files with text content and polygon coordinates in `result/*/processing_input/extract_raw/`

### Phase 1: JSON Processing and Page Matching

Processes raw OCR JSON to identify page types and match to template structure.

**Phase 1a - Page Type Identification:**
- Identifies page types (personal_info, spouse_info, assets, etc.)
- Detects continuation pages
- Adds metadata about page structure

**Phase 1b - Page Matching:**
- Matches document pages to 37-page template structure
- Uses layout similarity (polygon positions) and text similarity
- Aligns pages for consistent downstream processing

**Output:** Matched JSON files in `result/*/processing_input/extract_matched/`

**Phase 1c - Text Extraction:**
- Extracts text content from each page for LLM processing
- Creates individual page JSON files and combined document JSON
- Sorts lines by position (top-to-bottom, left-to-right)

**Output:** Text files in `result/*/processing_input/text_each_page/`

**Phase 1d - Metadata Mapping:**
- Maps pages to extraction steps (step_1 through step_11)
- Creates page-to-CSV output mapping
- Supports downstream data extraction

**Output:** Metadata files in `result/*/processing_input/page_metadata/`

## Usage

### Quick Start

```bash
# Install dependencies
cd submission_scanx
poetry install

# Activate virtual environment
poetry shell

# Or run commands with poetry run
poetry run scanx --help
```

### Running the Pipeline

```bash
# Using CLI commands (after poetry shell)
scanx                      # Run complete pipeline on training data
scanx --phase 0            # Run only Phase 0 (OCR extraction)
scanx --phase 1            # Run only Phase 1 (JSON processing)
scanx --phase 1a           # Run only Phase 1a (page identification)
scanx --phase 1b           # Run only Phase 1b (page matching)
scanx --phase 1c           # Run only Phase 1c (text extraction)
scanx --phase 1d           # Run only Phase 1d (metadata mapping)
scanx --final              # Process test final data
scanx --phase 0 --limit 5  # Process only first 5 PDFs
scanx --no-skip            # Reprocess all files

# Or using poetry run (without activating shell)
poetry run scanx --phase 0
poetry run scanx --final
```

### Running Individual Phases

```bash
# Phase 0 only (OCR extraction)
poetry run scanx-ocr
poetry run scanx-ocr --final
poetry run scanx-ocr --limit 10

# Phase 1 only (JSON processing)
poetry run scanx-process
poetry run scanx-process --final
poetry run scanx-process --phase 1a
poetry run scanx-process --phase 1b

# Phase 1c (text extraction)
poetry run scanx-text
poetry run scanx-text --final

# Phase 1d (metadata mapping)
poetry run scanx-meta
poetry run scanx-meta --final
```

### Alternative: Python Module

```bash
poetry run python -m submission_scanx.pipeline --phase 0
poetry run python -m submission_scanx.phase0_ocr --final
poetry run python -m submission_scanx.phase1_process --phase 1a
poetry run python -m submission_scanx.phase1c_text_extract
poetry run python -m submission_scanx.phase1d_metadata
```

## Data Pipeline

### Input Data

| File | Description |
|------|-------------|
| `*_submitter_info.csv` | Personal information of officials (name, age, address, contact) |
| `*_nacc_detail.csv` | NACC disclosure case details |
| `*_doc_info.csv` | Document mapping (doc_id → PDF location) |
| `*_pdf/` | Original PDF disclosure documents |

### Output Data (Ground Truth)

| File | Rows (Train) | Description |
|------|--------------|-------------|
| `Train_asset.csv` | 368 | Asset records (type, valuation, ownership) |
| `Train_asset_land_info.csv` | 195 | Land details (deed number, area, location) |
| `Train_asset_building_info.csv` | 324 | Building details (type, location) |
| `Train_asset_vehicle_info.csv` | 279 | Vehicle details (registration, model) |
| `Train_asset_other_asset_info.csv` | 329 | Other assets (count, unit) |
| `Train_statement.csv` | 291 | Financial statements (income, expenses, tax) |
| `Train_statement_detail.csv` | 265 | Statement line items |
| `Train_submitter_position.csv` | 213 | Official's positions held |
| `Train_spouse_position.csv` | 64 | Spouse's positions |
| `Train_spouse_info.csv` | 75 | Spouse personal information |
| `Train_relative_info.csv` | 205 | Family member details |
| `Train_submitter_old_name.csv` | 13 | Previous names (submitter) |
| `Train_spouse_old_name.csv` | 27 | Previous names (spouse) |

### Summary Data

| File | Description |
|------|-------------|
| `Train_summary.csv` | Aggregated totals for each disclosure |

## Enumeration Types

Located in `src/utils/enum_type/`:

| File | Description |
|------|-------------|
| `statement_type.csv` | 5 types: รายได้, รายจ่าย, ภาษี, ทรัพย์สิน, หนี้สิน |
| `statement_detail_type.csv` | 20 subtypes for statements |
| `asset_type.csv` | 39 asset types (land, building, vehicle, rights, other) |
| `asset_acquisition_type.csv` | 6 types: ซื้อ, มรดก, ให้, สร้าง, etc. |
| `position_category_type.csv` | 34 government position categories |
| `position_period_type.csv` | 3 types: current, history |
| `date_acquiring_type.csv` | 4 types for acquisition date status |
| `date_ending_type.csv` | 5 types for ending date status |
| `relationship.csv` | 6 family relationships |

## Thai Geographic Data

Located in `src/utils/thai-province-data/`:

| File | Records | Description |
|------|---------|-------------|
| `provinces.json` | 77 | Thai provinces (จังหวัด) |
| `districts.json` | ~1,000 | Districts (อำเภอ/เขต) |
| `sub_districts.json` | ~7,400 | Sub-districts (ตำบล/แขวง) with postal codes |
| `geographies.json` | 6 | Geographic regions (ภาค) |

## Environment Configuration

Create `.env` file in `src/submission_scanx/`:

```env
AZURE_OPENAI_ENDPOINT="<your-endpoint>"
AZURE_OPENAI_DEPLOYMENT_NAME="<your-deployment>"
AZURE_OPENAI_API_KEY="<your-api-key>"
AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT="<your-endpoint>"
DOC_INT_REGION="<your-region>"
AZURE_DOCUMENT_INTELLIGENCE_API_KEY="<your-api-key>"
```

## Installation

```bash
# Install Poetry (if not installed)
curl -sSL https://install.python-poetry.org | python3 -

# Install dependencies
poetry install

# Activate virtual environment
poetry shell
```

## Requirements

- Python >= 3.11
- Poetry >= 2.0.0

### Dependencies

- `python-dotenv` - Environment variable management
- `azure-ai-documentintelligence` - Azure Document Intelligence client
- `azure-core` - Azure SDK core
- `numpy` - Numerical computing
- `scipy` - Scientific computing (Hungarian algorithm for page matching)

## Data Statistics

| Dataset | Documents | Submitters |
|---------|-----------|------------|
| Training | 69 PDFs | 327 records |
| Test Final | 23 PDFs | 23 records |

## Key Relationships

```
Submitter (submitter_id) - IMPORTANT KEY
  └── NACC Disclosure (nacc_id)
        ├── Document (doc_id → PDF)
        ├── Positions (position records)
        ├── Assets (with type-specific details)
        ├── Statements (income/expense/tax/assets/liabilities)
        ├── Spouse → Spouse positions, info, old names
        ├── Relatives (family members)
        └── Summary (aggregated data)
```

## Page Types in Documents

The disclosure documents follow a 37-page template structure:

| Page | Type | Description |
|------|------|-------------|
| 1-3 | cover/work_history | Cover page and work history |
| 4 | personal_info | ข้อมูลส่วนบุคคล (Personal information) |
| 5 | spouse_info | คู่สมรส (Spouse information) |
| 6 | children | บุตร (Children) |
| 7 | siblings | พี่น้อง (Siblings) |
| 8-9 | income_expense | รายได้/รายจ่าย (Income/Expenses) |
| 10 | tax_info | ภาษี (Tax information) |
| 11 | assets_summary | ทรัพย์สินและหนี้สิน (Assets/Liabilities summary) |
| 12 | attachments | คำรับรอง (Certifications) |
| 13-14 | cash | เงินสด (Cash) |
| 15-16 | deposits | เงินฝาก (Deposits) |
| 17 | investments | เงินลงทุน (Investments) |
| 18-20 | loans_given | เงินให้กู้ยืม (Loans given) |
| 21-22 | land | ที่ดิน (Land) |
| 23-24 | buildings | โรงเรือน (Buildings) |
| 25-26 | vehicles | ยานพาหนะ (Vehicles) |
| 27-28 | concessions | สิทธิและสัมปทาน (Rights/Concessions) |
| 29-30 | other_assets | ทรัพย์สินอื่น (Other assets) |
| 31-32 | overdraft | เงินเบิกเกินบัญชี (Overdraft) |
| 33-34 | bank_loans | เงินกู้จากธนาคาร (Bank loans) |
| 35 | written_debts | หนี้สินที่มีหลักฐาน (Written debts) |
| 36-37 | documents_list | รายการเอกสาร (Document list) |

## Notes

- All text data is in Thai language (UTF-8 with BOM)
- Dates may use Thai Buddhist calendar (พ.ศ. = ค.ศ. + 543)
- Missing values are marked as "NONE" or empty strings

## Author

Riwara (awirut2629@gmail.com)
# submission_scanx
