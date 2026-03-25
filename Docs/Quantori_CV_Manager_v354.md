# Quantori CV Manager

**Version 3.54**

Quantori CV Manager is a desktop application for recruiters and HR professionals built on the Google Gemini 2.0 Flash neural network. The software automates the entire candidate lifecycle: from smart web sourcing to generating perfect Word documents with zero data loss.

A companion **web version** ([webqcv.onrender.com](https://webqcv.onrender.com)) provides single-file CV conversion from any browser and doubles as a **Gemini API proxy** for restricted corporate networks.

---

## Key Features

### Comprehensive AI Sourcing Suite (X-Ray & GitHub Miner)

- **Boolean Search (X-Ray Builder):** The recruiter describes the ideal candidate in plain language, and the AI translates it into 3-5 complex Boolean search formulas for LinkedIn, GitHub, and Google. The "Open in Google" button instantly opens your browser with the pre-filled search query and a ready-to-view list of relevant candidates.
- **GitHub Miner:** Find active developers directly via the GitHub API. Search popular repositories by Tech Stack and Minimum Stars, extract top contributors, and filter them by target location. The tool saves your last-used search parameters between sessions.
- **AI "Code Quality" Analysis:** Import developers from GitHub with one click. Gemini analyzes their raw repository data, infers their job title, evaluates their footprint based on a "Code Quality Over Vanity" principle, and seamlessly converts it into a standard JSON resume with an automatic `Source: GitHub` tag.

### Smart Lossless Parsing (PDF/DOCX/JPG -> JSON)

- **Universal Extraction:** Extracts 100% of data even from poorly formatted or non-standard PDF files (including standard LinkedIn profile exports).
- **LinkedIn Auto-Detector:** The import engine automatically identifies standard LinkedIn profile exports (via file naming conventions like `Profile.pdf` or internal URL scanning) and instantly tags them with `Source: LinkedIn` in the Comments field.
- **Zero Data Loss Architecture:** Thanks to a deeply tuned prompt, the parser carefully extracts work experience, projects, education, certifications, and "pulls out" specific technologies (e.g., boto3, JIRA) and soft skills, even if they are hidden in plain text within a project description.
- **Duplicate Protection:** A built-in "smart cache" remembers file hashes and original names. If you accidentally upload the same resume twice, the script instantly skips it, saving your time and API budget.
- **Flexible Import Modes:** Choose your parsing strategy on the fly using radio buttons:
  - **Fast Import** (Skip QA) -- parse only, no validation
  - **Auto-QA** (Audit Only) -- parse + quality audit
  - **Auto-QA & Auto-Fix** (default) -- parse + audit + automatic repair if score falls below the configurable threshold (default 90/100)
- **(NEW) Structured DOCX Parser:** DOCX files are now parsed with a section-aware extractor that preserves headings, tables, and layout structure before sending to the LLM, resulting in significantly better extraction quality compared to flat paragraph reading.

### Smart Word Template Engine (docxtpl)

- **Native MS Word Templates:** Transforms raw JSON data into beautifully formatted resumes using standard `.docx` templates. All design elements (company logos, custom brand colors, headers, and footers) are handled natively inside Microsoft Word, requiring zero code modifications.
- **Dynamic Template Selector:** Store multiple Word layouts (e.g., classic, minimalistic, developer) in the `templates` folder and switch between them instantly via a dropdown in the Settings tab.
- **Bundle & Extract:** On the very first launch, the app automatically unpacks a default `quantori_classic.docx` template into the user's workspace, ensuring an out-of-the-box working environment.
- **Smart Anonymization (Blind CVs):** Generates anonymized versions of CVs to send to clients. It hides contact details, replaces names with initials, and uses AI to convert real company and organization names into generic industry terms (e.g., Sberbank -> Large Fintech Enterprise).
- **Original Job Titles:** A dedicated setting to preserve and inject original, untruncated job titles directly into the generated DOCX. (Note: Turned off by default for standardized formatting).

### Deep Search, Database & Interactive Grid ("CVs" Tab)

- **Tokenized Full-Text Search (AND-logic):** Instant candidate search using space-separated keywords (e.g., "Yerevan Python AWS"). The system intelligently searches across all fields simultaneously (name, skills, full experience, comments, etc.) regardless of word order.
- **Inline Match Scoring:** Displays the candidate's latest Match Score directly in the main grid with color-coded indicators (Green/Yellow/Red). You can sort your entire database by this score.
- **Customizable Columns:** Easily tailor your workspace using the toolbar's toggle menu to show or hide specific columns (e.g., 'File', 'Score', 'Company', 'Comments') to save screen real estate.
- **In-Place Editing (Comments & Company):** Double-click directly on the 'Comments' or 'Company' cells to instantly leave a note or override the candidate's current employer. Edits are seamlessly saved to the JSON file on blur or Enter.
- **Smart Company Tracking:** The database automatically displays the candidate's most recent employer from their extracted work history. If manually overridden, your custom entry is safely isolated without destroying the original parsed experience data.
- **Smart Tooltips & Gestures:** Hover over table elements to reveal full, untruncated job titles and exact import timestamps. Double-click on core fields to instantly generate and open a live DOCX preview.
- **(NEW) Range Selection:** A toggle button next to the master checkbox activates range-select mode. Click a checkbox to set the anchor, activate the button, then click another checkbox to select (or deselect) the entire range between them.
- **(NEW) Auto-Scroll During Batch Ops:** The grid automatically scrolls to keep the currently processing row visible, so you never lose track of progress on large batches.
- **(NEW) Smart Sort During Batch Ops:** The grid temporarily sorts by name during batch operations for consistent top-to-bottom processing, then restores your original sort order (including sort direction arrow) on completion.

### AI CV Matcher (Deep Context Analyzer)

- **Deep Context Analysis:** The Matcher evaluates the candidate's *entire* work history and education structure, not just their isolated skills list, providing a highly accurate assessment of their real commercial experience.
- **Chain of Thought (CoT) Reasoning:** The AI "thinks out loud" and explains its logic as a Senior Recruiter before calculating the final score, drastically reducing hallucinations.
- **JD Scoring:** Compares your entire candidate database against a Job Description (JD) and provides a matching score from 0 to 100. Generates a detailed breakdown: Verdict, Pros (Strengths), and Missing Skills for each person.
- **JD Hash Caching:** If the job description hasn't changed, the script retrieves previous analysis results from the local cache without making redundant API calls. Automatically exports results to a CSV report.

### AI CV Modifier (Mass Resume Editor)

- **Prompt-Driven Edits:** Allows for bulk editing of resumes via prompts before exporting. A recruiter can select several candidates and type: "Keep only the 3 most recent jobs, remove hobbies, and translate to German."
- **Non-Destructive:** The AI creates new, modified versions of the DOCX files in a separate `modified/` folder without altering the original data in the database.

### (NEW) AI CV Tailor (Job-Specific Customization)

- **JD-Aware Rewriting:** Select candidates and paste a Job Description. The AI rewrites each CV to emphasize relevant experience without inventing new data.
- **Optional Anonymization:** Combine tailoring with anonymization in a single pass for client-ready blind CVs.
- **Batch Processing:** Process multiple candidates with per-row status tracking and tailoring notes.
- **Separate Output:** Tailored CVs are saved to the `tailored/` folder (or `blind/` if anonymized).

### MLOps QA Audit (Parsing Quality Analytics)

- **Batch & Selected QA:** The program takes a random sample of resumes (3, 5, 10, or all) or a specific "Selected CVs" list and runs a deep background audit comparing the source file with the parsing result.
- **(NEW) Three Comparison Modes:**
  - Input PDF/DOCX vs. extracted JSON
  - JSON vs. generated DOCX
  - Full pipeline: Input -> JSON -> DOCX
- **Batch Auto-Fix (Self-Healing):** Select multiple candidates with less-than-perfect QA scores and hit "Auto Fix". The AI automatically cross-references the source files against the error reports to repair the JSON structures in the background.
- **(NEW) Lossless Safety Gate:** Auto-fix is automatically rejected if the fixed JSON has fewer strings or characters than the original, preventing accidental data loss during repairs.
- **(NEW) Re-QA After Auto-Fix:** After each fix, a verification audit confirms the fix actually improved quality. If the new score is worse, the fix is rolled back.
- **(NEW) Configurable Auto-Fix Threshold:** Set the minimum QA score (default 90/100) that triggers automatic repair. Scores above the threshold are considered acceptable.
- **Macro-Analysis:** An AI Data Scientist aggregates all errors, identifies systemic patterns of data loss or hallucinations, and outputs a detailed Markdown report with concrete recommendations for improving the main extraction prompt.
- **(NEW) Multi-Format Reports:** QA results are saved as JSON, CSV, and Markdown simultaneously for different consumption needs.

### Resilient Architecture & Transparent Billing

- **Tri-File Decoupling:** The codebase is split into a highly scalable tri-file system (`cv_engine.py` for core neural logic, `ai_tasks.py` for heavy background processing, and `main_03_54.py` for the Flet UI). This completely isolates the UI thread from the data engine, drastically improving stability and preventing UI freezing.
- **Cross-Platform & Executable Ready:** Optimized for seamless `.app` compilation on macOS via PyInstaller. UI components dynamically adjust their heights to match native OS fonts without clipping, and template dependencies (`docxtpl`) are safely bundled for portable standalone deployment.
- **Base64 Link Armor:** External URL generation (for Google X-Ray and LinkedIn searching) utilizes Base64 encoding to prevent Markdown parser conflicts and ensure stable UI rendering.
- **Global Status Bar:** A progress bar at the bottom of the screen features smart, persistent Estimated Time of Arrival (ETA) calculation.
- **Unit Economics:** A real-time panel tracks inbound and outbound Gemini API tokens, showing cumulative usage in the status bar. Individual operation logs display token counts per API call.
- **Global Crash Reporter:** System-wide exception handling safely intercepts unhandled errors and redirects formatted tracebacks directly to the built-in "Logs" terminal for easy debugging.
- **(NEW) Gemini API Proxy Support:** For restricted corporate networks, configure a proxy URL in Settings to route all Gemini API calls through a relay server. The companion web app at `webqcv.onrender.com` serves as the default proxy endpoint.
- **(NEW) Prompt Versioning:** A built-in Prompt Editor (AI Core Logic tab) lets you modify all 9 AI prompts with full version history, save/restore/compare, and track lineage across prompt iterations.

---

## Settings ("Settings" Tab)

Here you can configure logically organized sections:

- **API Keys:** Set your Gemini API Key and GitHub PAT (Personal Access Token) for increased API limits.
- **(NEW) Gemini Proxy URL:** Route API calls through a relay server for restricted networks.
- **Workspace Path:** Define where all your source files, JSONs, templates, reports, and DOCXs are stored.
- **Import Strategy:** Select global defaults between Fast Import, Auto-QA, or Auto-QA & Auto-Fix modes. (Default: Auto-QA & Auto-Fix).
- **(NEW) Auto-Fix Threshold:** Set the minimum QA score that triggers auto-repair (default: 90/100).
- **(NEW) Generate DOCX on Import:** Toggle whether DOCX files are automatically created during import.
- **Active DOCX Template:** A dynamic dropdown that lists all Word documents currently stored in your `templates` folder. Select which template to use for generation, or click "Open Templates" to add new ones.
- **Naming Conventions:** Choose file naming templates for JSON files, exported CVs, and anonymous resumes (e.g., automatically save anonymous resumes as `CV_Alexei_L.docx`).
- **Preserve Original Job Title:** Keep the candidate's original, untruncated job title in the generated DOCX instead of the LLM-standardized version.
- **(NEW) Tab Visibility:** Toggle advanced tools on or off: X-Ray Builder, GitHub Tools, CV Matcher, Modify CV, CV Tailor, AI Core Logic.
- **Theme:** Switch between Light and Dark modes.
- **Reset Billing Counters:** Clear your tracked API usage.

---

## Web Version

The companion web application at **webqcv.onrender.com** provides:

- **Single-file CV conversion** (PDF/DOCX -> JSON + DOCX) from any browser
- **Gemini API proxy** for desktop clients on restricted networks
- Per-job processing with background queue
- Download generated DOCX and JSON results

---

## System Requirements

- **macOS:** 10.15+ (Catalina or later). If macOS blocks the app from launching, go to **System Settings -> Privacy & Security**, scroll down to the Security section, and click **"Open Anyway"**.
- **API Key:** A Google Gemini API key is required (free tier available at ai.google.dev).
- **Model:** Google Gemini 2.0 Flash (`gemini-2.0-flash`).
