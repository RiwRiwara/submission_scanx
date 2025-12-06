# Scanx Dev Tools

เครื่องมือพัฒนาสำหรับ Submission Scanx Pipeline

## Features

### 1. PDF/JSON Viewer (`/viewer`)
- เลือกเอกสารจากรายการ (ไม่ต้อง upload)
- แสดง PDF พร้อม polygon overlay
- ดูข้อความในแต่ละหน้า
- Zoom และ toggle polygon

### 2. Content Browser (`/content`)
- เรียกดูข้อความแยกตามหน้า
- ค้นหาข้อความในหน้า
- ดูรายละเอียด coordinates ของแต่ละบรรทัด
- Copy content

### 3. Dashboard (`/dashboard`)
- ดู accuracy metrics
- รัน scanx pipeline
- ดู CSV output files
- Console output

### 4. Search (`/search`)
- ค้นหาข้อมูลตาม submitter_id
- ดูข้อมูลจากทุก CSV files

### 5. Pages Viewer (`/pages`)
- ดู page metadata จาก phase 1d
- ดู page type และ step mappings

## Installation

```bash
cd src/dev_tools

# Install dependencies (uses main project's poetry)
cd ../..
poetry install

# Or install dev_tools separately
cd src/dev_tools
poetry install
```

## Usage

```bash
# From dev_tools directory
cd src/dev_tools
poetry run python main.py

# Or directly
python main.py
```

Server จะรันที่ http://localhost:8888

## API Endpoints

### Documents
- `GET /api/documents?mode=training|final` - List all documents
- `GET /api/document/{name}?mode=...` - Get document JSON
- `GET /api/document/{name}/page/{num}?mode=...` - Get page content
- `GET /api/document/{name}/combined?mode=...` - Get combined document
- `GET /api/pdf/{name}?mode=...` - Get PDF file

### Results
- `GET /api/results/summary?mode=...` - Get summary CSV
- `GET /api/results/csv/{filename}?mode=...` - Get any CSV file

### Pipeline
- `POST /api/pipeline/run?phase=1&is_final=false` - Run scanx pipeline

### Search
- `GET /api/search/submitters?mode=...` - List all submitter IDs
- `GET /api/search/submitter/{id}?mode=...` - Search by submitter ID

## Directory Structure (submission_scanx)

```
submission_scanx/
├── src/
│   ├── dev_tools/           # This tool
│   │   ├── main.py          # FastAPI application
│   │   ├── templates/       # HTML templates
│   │   └── static/          # Static files
│   ├── result/
│   │   ├── from_train/      # Training results
│   │   │   ├── processing_input/
│   │   │   │   ├── extract_raw/
│   │   │   │   ├── extract_matched/
│   │   │   │   ├── text_each_page/
│   │   │   │   └── page_metadata/
│   │   │   └── mapping_output/  # CSV outputs
│   │   └── final/           # Final test results
│   ├── training/
│   │   └── train input/     # Training CSVs and PDFs
│   └── test final/
│       └── test final input/  # Final test data
```

## Tech Stack

- **Backend**: FastAPI + Uvicorn
- **Frontend**: Tailwind CSS + Vanilla JS
- **PDF**: PDF.js
- **Data**: Pandas
