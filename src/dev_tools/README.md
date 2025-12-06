# Pipeline Dev Tools

เครื่องมือพัฒนาสำหรับ NACC Asset Declaration Pipeline

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
- รัน pipeline และ evaluation
- ดู CSV output files
- Console output

## Installation

```bash
cd dev_tools

# Install with poetry
poetry install

# Or with pip
pip install -r requirements.txt
```

## Usage

```bash
# Run with poetry
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
- `GET /api/results/summary` - Get summary and test_summary
- `GET /api/results/accuracy` - Run compare_results.py
- `GET /api/results/csv/{filename}` - Get CSV file

### Pipeline
- `POST /api/pipeline/run?step=...` - Run pipeline or specific step

## Tech Stack

- **Backend**: FastAPI + Uvicorn
- **Frontend**: Tailwind CSS + Vanilla JS
- **PDF**: PDF.js
- **Data**: Pandas

## Directory Structure

```
dev_tools/
├── main.py              # FastAPI application
├── pyproject.toml       # Poetry config
├── README.md
├── templates/
│   ├── base.html        # Base template
│   ├── index.html       # Home page
│   ├── viewer.html      # PDF/JSON viewer
│   ├── content.html     # Content browser
│   └── dashboard.html   # Dashboard
└── static/              # Static files (auto-created)
```
