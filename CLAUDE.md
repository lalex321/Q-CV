# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the Application

```bash
source venv/bin/activate
python3 main_03_55.py
```

## Building the macOS App

```bash
pyinstaller Q-CV.spec
# Output: dist/Q-CV.app
```

## Environment / Configuration

- **API key**: Gemini API key stored in `~/.quantoricv_settings.json` under key `"api_key"`
- **Master prompts**: Override prompts via `~/.master_prompts.json`
- **Workspace**: Default `~/Documents/Quantori_CV_Workplace/` — auto-created on first run; contains `source/`, `jsons/`, `docxs/`, `docxs_a/`, `templates/`, `reports/`
- **Active template**: Set via `cfg["active_template"]` (filename in workspace `templates/`); bundled default is `quantori_classic.docx`

## Architecture

```
main_03_55.py      Flet desktop UI: tabs, file browser, table rendering, task callbacks
ai_tasks.py        Batch task functions: import, QA, autofix, match, modify, X-Ray, GitHub mine
cv_engine.py       Core: Gemini API calls, CV_JSON_SCHEMA, prompts, DOCX generation, anonymization
admin_qcv7.py      Separate Flet app for admin settings and prompt versioning
```

### Processing Pipeline

Each source file goes through: **hash dedup → Gemini extract → sanitize → (QA audit) → (autofix) → (anonymize) → DOCX render**

- `process_file_gemini()` in `cv_engine.py` — main LLM extraction entry point; handles DOCX (text), PDF and images (file upload)
- `_retry_generate(client, model_name, contents)` — retry helper for 429/quota errors; defined in both `cv_engine.py` and `ai_tasks.py`
- `sanitize_json()` — cleans extracted data after every LLM call
- `generate_docx_from_json()` — renders DOCX via `docxtpl`; reads template from workspace `templates/`
- `smart_anonymize_data()` — strips PII, calls LLM to map company names to generic descriptions

### Task Functions (`ai_tasks.py`)

Each function creates its own `genai.Client(api_key=...)` at the start:

| Function | Description |
|----------|-------------|
| `run_import_task` | Main import loop: extract → optional QA/autofix → save JSON + DOCX |
| `run_batch_autofix_task` | Re-run autofix on existing JSONs |
| `run_batch_qa_task` | QA audit with per-candidate scores and aggregated systemic report |
| `run_matcher_task` | Score candidates against a job description |
| `run_modify_task` | Apply user-specified AI edits to existing CVs |
| `run_mine_github_task` | Convert GitHub profiles to CV JSONs |
| `run_xray_task` | Generate Boolean search queries |

### Key Data

- **`CV_JSON_SCHEMA`** (in `cv_engine.py`) — canonical schema passed to LLM on every extraction
- **`DEFAULT_PROMPTS`** (in `cv_engine.py`) — all LLM prompts; user-editable via settings, versioned in `~/.master_prompts.json`
- **Lossless safety gate** — autofix is rejected if the fixed JSON has fewer strings or characters than the original (prevents data loss)
- **File dedup** — source files hashed with MD5; already-imported hashes skipped in `run_import_task`

### LLM Integration

- Model: `gemini-2.0-flash` (`MODEL_NAME` constant in `cv_engine.py`)
- New SDK: `from google import genai` — `genai.Client(api_key=...)` per call
- Images and PDFs uploaded via `client.files.upload()` with `UploadFileConfig(mime_type=...)`; state polled until `ACTIVE`
- Token usage read from `response.usage_metadata.prompt_token_count` / `candidates_token_count`

### PyInstaller Notes

- Entry point: `main_03_55.py` (defined in `Q-CV.spec`)
- The `fix_docx_path_bug()` function in `cv_engine.py` creates missing `docx/parts/` and `docx/templates/` directories inside the frozen bundle at startup
- Resources resolved via `get_resource_path()` which checks `sys._MEIPASS` when frozen
