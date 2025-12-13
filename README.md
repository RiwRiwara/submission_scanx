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
│   │   ├── pipeline.py            # Main pipeline runner (CLI: scanx)
│   │   ├── phase0_ocr.py          # Phase 0: PDF to OCR extraction
│   │   ├── phase1_process.py      # Phase 1a+1b: JSON processing & page type ID
│   │   ├── phase1c_text_extract.py # Phase 1c: Text extraction
│   │   ├── phase1d_metadata.py    # Phase 1d: Page metadata mapping (AI)
│   │   ├── phase1e_mapping.py     # Phase 1e: Data extraction to CSV
│   │   ├── phase1_mapping/        # Step-based extraction modules (step_1 to step_11)
│   │   └── page_similarity.py     # Page layout similarity utilities
│   ├── training/                  # Training dataset (69 documents)
│   │   ├── train input/           # Input data
│   │   │   ├── Train_doc_info.csv
│   │   │   ├── Train_nacc_detail.csv
│   │   │   ├── Train_submitter_info.csv
│   │   │   └── Train_pdf/pdf/     # 69 PDF files
│   │   ├── human_loop/            # Human-in-the-loop configuration
│   │   │   ├── pre_pdf.json       # Page ignore config for training
│   │   │   └── generate_template.py # Template generator script
│   │   ├── train output/          # Ground truth extracted data (13 CSVs)
│   │   └── train summary/         # Aggregated summary data
│   ├── test final/                # Test dataset (23 documents)
│   │   ├── test final input/
│   │   │   ├── Test final_doc_info.csv
│   │   │   ├── Test final_nacc_detail.csv
│   │   │   ├── Test final_submitter_info.csv
│   │   │   └── Test final_pdf/    # 23 PDF files
│   │   ├── human_loop/            # Human-in-the-loop configuration
│   │   │   └── pre_pdf.json       # Page ignore config for test final
│   │   └── test final output/     # Output directory for predictions
│   ├── result/                    # Pipeline output
│   │   ├── from_train/            # Training data results
│   │   │   ├── processing_input/
│   │   │   │   ├── extract_raw/       # Phase 0 output (OCR JSON)
│   │   │   │   ├── extract_matched/   # Phase 1a+1b output (page types)
│   │   │   │   ├── text_each_page/    # Phase 1c output (extracted text)
│   │   │   │   └── page_metadata/     # Phase 1d output (step mappings)
│   │   │   └── mapping_output/        # Phase 1e output (13 CSV files)
│   │   └── final/                 # Test final results (same structure)
│   └── utils/                     # Reference data
│       ├── enum_type/             # Thai enumeration types (9 CSVs)
│       ├── thai-province-data/    # Thai geographic data (JSON)
│       └── template-docs_raw.json # Template document for page matching
└── tests/                         # Test modules
```

## Pipeline Phases

```
PDF Documents → Phase 0 (OCR) → Phase 1a+1b (Page ID) → Phase 1c (Text) → Phase 1d (Metadata) → Phase 1e (CSV) → 13 Output CSVs
```

### Phase 0: PDF to OCR Extraction

Uses Azure Document Intelligence to extract text and layout from PDF documents.

- **Input:** PDF files from `train input/Train_pdf/` or `test final input/Test final_pdf/`
- **Output:** JSON files with text content and polygon coordinates in `extract_raw/`

### Phase 1a+1b: Page Type Identification

Processes raw OCR JSON to identify page types using regex patterns with OCR variation handling.

- Identifies page types (personal_info, spouse_info, children, siblings, assets, etc.)
- Uses negative patterns to distinguish similar page types (e.g., personal_info vs spouse_info)
- Handles OCR variations (e.g., `ข้อ` vs `ขอ` missing tone marks)
- Preserves all original pages with page type metadata

- **Output:** Processed JSON files in `extract_matched/`

### Phase 1c: Text Extraction

Extracts text content from each page for LLM processing.

- Creates individual page JSON files and combined document JSON
- Sorts lines by position (top-to-bottom, left-to-right)
- Generates index file for quick lookup

- **Output:** Text files in `text_each_page/`

### Phase 1d: Page Metadata Mapping (AI-assisted)

Maps pages to extraction steps using hybrid regex + Azure OpenAI approach.

- Uses regex patterns with priorities for initial detection
- Falls back to LLM for uncertain cases (confidence < 0.4)
- Creates step-to-page mappings for data extraction

- **Output:** Metadata JSON files in `page_metadata/`

### Phase 1e: Data Extraction to CSV

Extracts structured data from pages and outputs to CSV files.

- Runs 11 extraction steps (step_1 through step_11)
- Uses page metadata to find relevant pages for each step
- Outputs 13 CSV files matching the expected output format

- **Output:** CSV files in `mapping_output/`

## Usage

### Quick Start

```bash
# Install dependencies
cd submission_scanx
poetry install

# first OCR step (run once )
poetry run scanx --phase 0   
poetry run scanx --phase 0  --final

# Run complete pipeline on training data
poetry run scanx --phase 1 --all

# Run complete pipeline on test final data
poetry run scanx --phase 1 --final --all
```

### CLI Commands

The main CLI command is `scanx`:

```bash
# Run complete Phase 1 (clean + 1a-1e) - RECOMMENDED
poetry run scanx --phase 1 --all

# Run complete Phase 1 on test final data
poetry run scanx --phase 1 --final --all

# Run individual phases
poetry run scanx --phase 0            # OCR extraction only
poetry run scanx --phase 1            # Phase 1a+1b+1c only
poetry run scanx --phase 1 --1d       # Phase 1a+1b+1c+1d
poetry run scanx --phase 1c           # Text extraction only
poetry run scanx --phase 1d           # AI metadata only
poetry run scanx --phase 1e           # Data mapping only

# Run complete pipeline (Phase 0 + Phase 1 all)
poetry run scanx

# Options
poetry run scanx --phase 0 --limit 5  # Process only first 5 PDFs
poetry run scanx --skip               # Skip existing files
poetry run scanx --final              # Process test final data
```

### CLI Options

| Option | Description |
|--------|-------------|
| `--phase {0,1,1c,1d,1e,all}` | Phase to run (default: all) |
| `--all` | Run complete Phase 1 (1a-1e) with clean output |
| `--1d` | Include Phase 1d when running `--phase 1` |
| `--final` | Process test final data instead of training |
| `--limit N` | Maximum PDFs to process in Phase 0 |
| `--skip` | Skip existing files (default: regenerate) |

### Phase Combinations

| Command | Phases Run | Use Case |
|---------|------------|----------|
| `scanx --phase 1 --all` | 1a+1b+1c+1d+1e | Full extraction (clean) |
| `scanx --phase 1` | 1a+1b+1c | Quick processing |
| `scanx --phase 1 --1d` | 1a+1b+1c+1d | Processing + AI metadata |
| `scanx --phase 1e` | 1e only | Re-run CSV extraction |
| `scanx` | 0+1a+1b+1c+1d+1e | Complete from PDF |

### Alternative CLI Commands

```bash
# Individual phase commands
poetry run scanx-ocr                  # Phase 0 only
poetry run scanx-ocr --final --limit 5

poetry run scanx-meta                 # Phase 1d only
poetry run scanx-meta --final

poetry run scanx-mapping              # Phase 1e only
poetry run scanx-mapping --final
```

## Output Files

### Phase 1e Output (13 CSV files)

| File | Description |
|------|-------------|
| `submitter_position.csv` | Official's positions held |
| `submitter_old_name.csv` | Previous names (submitter) |
| `spouse_info.csv` | Spouse personal information |
| `spouse_old_name.csv` | Previous names (spouse) |
| `spouse_position.csv` | Spouse's positions |
| `relative_info.csv` | Family member details |
| `statement.csv` | Financial statements summary |
| `statement_detail.csv` | Statement line items |
| `asset.csv` | Asset records |
| `asset_land_info.csv` | Land details |
| `asset_building_info.csv` | Building details |
| `asset_vehicle_info.csv` | Vehicle details |
| `asset_other_asset_info.csv` | Other asset details |
| `summary.csv` | Aggregated totals |

## Step Mapping

| Step | Output CSV | Page Types |
|------|------------|------------|
| step_1 | submitter_position.csv | personal_info |
| step_2 | submitter_old_name.csv | personal_info |
| step_3_1 | spouse_info.csv | spouse_info |
| step_3_2 | spouse_old_name.csv | spouse_info |
| step_3_3 | spouse_position.csv | spouse_info |
| step_4 | relative_info.csv | personal_info, spouse_info, children, siblings |
| step_5 | statement.csv, statement_detail.csv | income_expense, tax_info, assets_summary |
| step_6 | asset.csv | cash, deposits, investments, loans_given, land, buildings, vehicles, concessions, other_assets, overdraft, bank_loans, written_debts |
| step_7 | asset_land_info.csv | land |
| step_8 | asset_building_info.csv | buildings |
| step_9 | asset_vehicle_info.csv | vehicles |
| step_10 | asset_other_asset_info.csv | other_assets |
| step_11 | summary.csv | (aggregation) |

## Environment Configuration

Create `.env` file in `src/submission_scanx/`:

```env
# Azure Document Intelligence (for Phase 0 OCR)
AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT="https://your-resource.cognitiveservices.azure.com/"
DOC_INT_REGION="your-region"
AZURE_DOCUMENT_INTELLIGENCE_API_KEY="your-api-key"

# Human-in-the-loop (optional - for cost reduction)
USE_HUNMAN_IN_LOOP="FALSE"

# Azure OpenAI (Optionall)
AZURE_OPENAI_ENDPOINT="https://your-resource.openai.azure.com/"
AZURE_OPENAI_DEPLOYMENT_NAME="gpt-4o"
AZURE_OPENAI_API_KEY="your-api-key"
```

## Human-in-the-Loop (Page Ignore)

To reduce OCR costs, you can configure specific pages to skip during Phase 0 OCR processing.

### Setup

1. Set `USE_HUNMAN_IN_LOOP="TRUE"` in `.env`
2. Edit the configuration file:
   - Training: `src/training/human_loop/pre_pdf.json`
   - Test Final: `src/test final/human_loop/pre_pdf.json`

### Configuration Format

```json
{
  "documents": [
    {
      "pdf_name": "document_name_without_extension",
      "total_pages": 30,
      "ignore_pages": [2, 5, 10],
      "notes": "Pages 2,5,10 are blank or irrelevant"
    }
  ]
}
```

- **pdf_name**: PDF filename without `.pdf` extension
- **total_pages**: Total number of pages in the PDF (required when ignoring pages)
- **ignore_pages**: Array of page numbers (1-indexed) to skip OCR
- **notes**: Optional notes explaining why pages are ignored

### Generate Template

```bash
# Generate template with all documents from Train_doc_info.csv
poetry run python src/training/human_loop/generate_template.py

# Generate for test final data
poetry run python src/training/human_loop/generate_template.py --final
```

### Output Format

Ignored pages appear in output JSON with empty lines:

```json
{
  "page_number": 2,
  "lines": [],
  "_ignored": true
}
```

### OCR Usage Reports

Phase 0 automatically tracks usage and costs per run:

```bash
# View usage report (training data)
poetry run python -m submission_scanx.phase0_ocr --report

# View usage report (test final data)
poetry run python -m submission_scanx.phase0_ocr --report --final

# Clear/reset report
poetry run python -m submission_scanx.phase0_ocr --report-clear
```

Report includes:
- **Per-run statistics**: PDFs processed, pages OCR'd, pages ignored
- **Cost tracking**: Estimated cost based on Azure S0 Read pricing ($1.50/1000 pages)
- **Run history**: Timestamps for each OCR run
- **All-time summary**: Cumulative totals across all runs

Reports are saved to:
- Training: `src/training/human_loop/ocr_usage_report.json`
- Test Final: `src/test final/human_loop/ocr_usage_report.json`

## Installation

```bash
# Install Poetry (if not installed)
curl -sSL https://install.python-poetry.org | python3 -

# Clone and install
cd submission_scanx
poetry install

# Run pipeline
poetry run scanx --phase 1 --all
```

## Requirements

- Python >= 3.11
- Poetry >= 2.0.0

### Dependencies

- `python-dotenv` - Environment variable management
- `azure-ai-documentintelligence` - Azure Document Intelligence client
- `azure-core` - Azure SDK core
- `openai` - Azure OpenAI client
- `numpy` - Numerical computing
- `scipy` - Scientific computing (Hungarian algorithm)
- `pandas` - Data manipulation
- `pypdf2` - PDF page counting (for human_loop template generator)

## Data Statistics

| Dataset | Documents | Pages (avg) |
|---------|-----------|-------------|
| Training | 69 PDFs | ~25-40 pages |
| Test Final | 23 PDFs | ~25-40 pages |

## Page Types

The system identifies the following page types:

| Type | Thai Name | Description |
|------|-----------|-------------|
| `personal_info` | ข้อมูลส่วนบุคคล | Submitter personal information |
| `spouse_info` | คู่สมรส | Spouse information |
| `children` | บุตร | Children information |
| `siblings` | พี่น้อง | Siblings information |
| `income_expense` | รายได้/รายจ่าย | Income and expenses |
| `tax_info` | ภาษี | Tax information |
| `assets_summary` | ทรัพย์สินและหนี้สิน | Assets/Liabilities summary |
| `cash` | เงินสด | Cash details |
| `deposits` | เงินฝาก | Bank deposits |
| `investments` | เงินลงทุน | Investments |
| `loans_given` | เงินให้กู้ยืม | Loans given |
| `land` | ที่ดิน | Land assets |
| `buildings` | โรงเรือน | Building assets |
| `vehicles` | ยานพาหนะ | Vehicle assets |
| `concessions` | สิทธิและสัมปทาน | Rights/Concessions |
| `other_assets` | ทรัพย์สินอื่น | Other assets |
| `overdraft` | เงินเบิกเกินบัญชี | Overdraft liabilities |
| `bank_loans` | เงินกู้จากธนาคาร | Bank loans |
| `written_debts` | หนี้สินที่มีหลักฐาน | Written debts |

## Notes

- All text data is in Thai language (UTF-8)
- Dates may use Thai Buddhist calendar (พ.ศ. = ค.ศ. + 543)
- Missing values are marked as "NONE" or empty strings
- Page type detection handles OCR variations (missing tone marks)

## Development Tools

A web-based development tool is available for viewing PDFs, JSON data, and pipeline results.

```bash
# Run dev tools server
cd submission_scanx/src/dev_tools
poetry run python main.py
```

Server runs at http://localhost:8888

### Features

- **PDF/JSON Viewer** (`/viewer`) - View PDFs with polygon overlay
- **Content Browser** (`/content`) - Browse extracted text by page
- **Dashboard** (`/dashboard`) - Run pipeline and view accuracy metrics
- **Search** (`/search`) - Search data by submitter_id
- **Pages Viewer** (`/pages`) - View page metadata and step mappings
- **Human Loop** (`/human-loop`) - Configure pages to ignore for OCR cost reduction

See [dev_tools README](src/dev_tools/README.md) for full documentation.

## Author

Riwara (awirut2629@gmail.com)
TaChanseewong (schanseewong@gmail.com)

