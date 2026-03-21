# Q-CV

A desktop application for converting CVs from PDF, DOCX, or image formats into standardized Quantori Word document templates using Google Gemini AI.

## Features

- **Multi-format input**: PDF, DOCX, PNG, JPG, JPEG
- **Batch processing**: Import and convert multiple CVs at once
- **QA Audit**: AI-powered quality check that compares extracted JSON against the original source
- **AutoFix**: Automatic repair of extraction issues based on QA reports
- **Anonymization**: Removes PII (name, email, phone) and optionally masks company names
- **CV Matcher**: Score candidates against a job description
- **GitHub Miner**: Import GitHub profiles as CVs
- **X-Ray Builder**: Generate Boolean search queries for sourcing
- **Modify CV**: Apply AI-driven edits to existing CVs

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install flet google-genai python-docx docxtpl pypdf pillow
```

A Gemini API key is required. On first launch the app will prompt for it, or create `~/.quantoricv_settings.json` manually:

```json
{
  "api_key": "YOUR_API_KEY"
}
```

## Running

```bash
python3 main_03_49.py
```

## Building (macOS)

```bash
pip install pyinstaller
pyinstaller Q-CV.spec
# Output: dist/Q-CV.app
```

## Architecture

| File | Role |
|------|------|
| `main_03_49.py` | Flet desktop UI — tabs, navigation, callbacks |
| `cv_engine.py` | Core logic: Gemini API, JSON schema, DOCX generation, anonymization |
| `ai_tasks.py` | Batch task orchestration: import, QA, autofix, matching, mining |
| `admin_qcv7.py` | Separate admin panel for settings and prompt management |

## Processing Pipeline

1. **Import** — source file hashed; duplicates skipped automatically
2. **Extract** — Gemini 2.0 Flash converts PDF/DOCX/image to structured JSON
3. **QA Audit** *(optional)* — re-reads original to find data losses and hallucinations
4. **AutoFix** *(optional)* — LLM repairs JSON based on QA report; lossless safety gate prevents data reduction
5. **Anonymize** *(optional)* — strips PII, masks company names via AI mapping
6. **Generate** — renders DOCX using a Quantori template via `docxtpl`

## Workspace

Default workspace: `~/Documents/Quantori_CV_Workplace/`

```
source/      Input CV files
jsons/       Extracted JSON files
docxs/       Generated DOCX output
docxs_a/     Anonymized DOCX output
templates/   DOCX templates
reports/     QA audit reports
```

## Stack

- **UI**: Python, Flet (cross-platform desktop)
- **AI**: Google Gemini 2.0 Flash (`google-genai`)
- **Document processing**: python-docx, docxtpl, pypdf, Pillow
