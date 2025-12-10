# ScanX Tools

Modern, user-friendly web panel for document processing with the ScanX pipeline.

## Features

- **📤 Upload Documents** - Drag & drop single or multiple PDF files
- **⚙️ Pipeline Processing** - Run the complete extraction pipeline with real-time progress
- **📊 Results Viewer** - View extraction results with PDF-to-data mapping
- **🎨 Modern UI** - Beautiful, responsive interface with glass morphism design

## Quick Start

```bash
cd src/scanx-tools

# Install dependencies
poetry install

# Run the server
poetry run scanx-tools

# Or run directly
poetry run python main.py
```

Server will start at: **http://localhost:8000**

## Architecture

```
src/scanx-tools/
├── main.py              # FastAPI application
├── templates/
│   ├── index.html       # Main panel UI
│   └── results.html     # Results viewer
├── static/              # Static assets
├── uploads/             # Uploaded PDF files
├── jobs/                # Job data storage
├── pyproject.toml       # Project configuration
└── README.md            # This file
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Main panel page |
| GET | `/results/{job_id}` | Results viewer page |
| GET | `/api/jobs` | List all jobs |
| POST | `/api/upload` | Upload PDF file |
| POST | `/api/jobs/{id}/start` | Start processing |
| GET | `/api/jobs/{id}` | Get job details |
| DELETE | `/api/jobs/{id}` | Delete job |
| GET | `/api/jobs/{id}/results` | Get extraction results |
| GET | `/api/jobs/{id}/pdf` | Serve PDF file |

## Pipeline Phases

1. **Phase 0: OCR** - Extract text from PDF using Azure Document Intelligence
2. **Phase 1ab: Page Matching** - Identify page types using templates
3. **Phase 1c: Text Extraction** - Extract structured text from pages
4. **Phase 1d: AI Metadata** - Extract metadata using AI
5. **Phase 1e: Data Mapping** - Map extracted data to structured format

## Requirements

- Python 3.10+
- Poetry
- Azure Document Intelligence API key (for OCR)

## Configuration

Set environment variables in `.env` or system environment:

```bash
AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT=your_endpoint
AZURE_DOCUMENT_INTELLIGENCE_KEY=your_key
```

## Usage

1. **Upload PDFs** - Drag & drop files or click to browse
2. **Start Processing** - Click "Start Processing" on queued jobs
3. **Monitor Progress** - Watch real-time pipeline progress
4. **View Results** - Click "View Results" on completed jobs
5. **Explore Data** - Navigate pages and extracted data in the results viewer

## Tech Stack

- **Backend**: FastAPI, Python 3.10+
- **Frontend**: Vanilla JS, Tailwind CSS
- **PDF Rendering**: PDF.js
- **Pipeline**: ScanX submission_scanx module
