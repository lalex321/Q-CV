import flet as ft
import time
import os
import sys
import json
import re
import copy
import csv
import shutil
import threading
import platform
import subprocess
import hashlib
import urllib.parse
import base64
import unicodedata
import textwrap
import random
import traceback
from google import genai
from google.genai import types as genai_types
import re
from datetime import datetime
from pathlib import Path

import cv_engine
from cv_engine import (
    APP_VERSION, MODEL_NAME, PRICE_1M_IN, PRICE_1M_OUT,
    SCRIPT_NAME, DEFAULT_WORKSPACE, DEFAULT_CONFIG,
    DEFAULT_PROMPTS, CV_JSON_SCHEMA, CURRENT_PROMPT_MASTER_VERSION,
    load_config, save_config, init_workspace_folders, open_folder,
    sanitize_json, extract_text_from_docx, process_file_gemini,
    generate_docx_from_json, MASTER_PROMPTS_FILE, ensure_master_prompts_registry,
    get_master_prompt_text, get_master_prompt_entry, save_master_prompt_version
)

from ai_tasks import *
from ai_tasks import _parse_llm_json_payload, _qa_audit_get_latest, _retry_generate


def normalize_qa_audit(qa_audit):
    """qa_audit may be dict (current) or list of dicts (history). Return a dict."""
    if isinstance(qa_audit, list):
        for x in reversed(qa_audit):
            if isinstance(x, dict):
                return x
        return {}
    if isinstance(qa_audit, dict):
        return qa_audit
    return {}


def _qa_mode_slug(mode: str) -> str:
    return {
        "input_json": "input_json",
        "json_docx": "json_docx",
        "full_pipeline": "full_pipeline",
    }.get(mode or "", "qa_report")


def _reports_dir(config: dict, workspace_folders: dict) -> Path:
    fallback = Path(workspace_folders.get("REPORTS", "")).expanduser()
    preferred = Path(config.get("workspace_path") or DEFAULT_WORKSPACE).expanduser() / "reports"
    reports = preferred if preferred.parent.exists() else fallback
    reports.mkdir(parents=True, exist_ok=True)
    return reports


def _qa_rows_to_csv_rows(rows: list, mode: str) -> tuple[list[str], list[list[str]]]:
    if mode == "full_pipeline":
        headers = ["Input File", "In→JSON", "JSON→DOCX", "Total", "Comments"]
        data = [[
            str(r.get("input_file", "")),
            "" if r.get("input_json") is None else str(r.get("input_json")),
            "" if r.get("json_docx") is None else str(r.get("json_docx")),
            "" if r.get("total_quality") is None else str(r.get("total_quality")),
            str(r.get("comments", "")),
        ] for r in rows]
    elif mode == "json_docx":
        headers = ["Input File", "DOCX Quality", "Comments"]
        data = [[
            str(r.get("input_file", "")),
            "" if r.get("quality") is None else str(r.get("quality")),
            str(r.get("comments", "")),
        ] for r in rows]
    else:
        headers = ["Input File", "Quality", "Comments"]
        data = [[
            str(r.get("input_file", "")),
            "" if r.get("quality") is None else str(r.get("quality")),
            str(r.get("comments", "")),
        ] for r in rows]
    return headers, data


def _save_qa_reports(payload: dict, config: dict, workspace_folders: dict) -> dict:
    mode = payload.get("mode", "input_json")
    rows = payload.get("rows", []) or []
    summary_md = payload.get("summary_md", "") or ""
    report_dir = _reports_dir(config, workspace_folders)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = f"{_qa_mode_slug(mode)}_{stamp}"

    # lightweight stats
    numeric_scores = []
    for r in rows:
        for key in ("quality", "input_json", "json_docx", "total_quality"):
            v = r.get(key)
            if isinstance(v, (int, float)):
                numeric_scores.append(float(v))
    avg_score = round(sum(numeric_scores) / len(numeric_scores), 2) if numeric_scores else None

    report_json = {
        "mode": mode,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "workspace_path": config.get("workspace_path") or DEFAULT_WORKSPACE,
        "row_count": len(rows),
        "average_score": avg_score,
        "summary_md": summary_md,
        "rows": rows,
    }

    json_path = report_dir / f"{stem}.json"
    csv_path = report_dir / f"{stem}.csv"
    md_path = report_dir / f"{stem}.md"

    json_path.write_text(json.dumps(report_json, ensure_ascii=False, indent=2), encoding="utf-8")

    headers, csv_rows = _qa_rows_to_csv_rows(rows, mode)
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(csv_rows)

    md_parts = [
        f"# QA Report — {mode}",
        "",
        f"Generated at: {report_json['generated_at']}",
        f"Rows: {len(rows)}",
    ]
    if avg_score is not None:
        md_parts.append(f"Average score: {avg_score}%")
    md_parts.extend(["", "## Summary", "", summary_md or "_No summary generated._", "", "## Files", ""])
    headers_md, csv_rows_md = _qa_rows_to_csv_rows(rows, mode)
    md_parts.append("| " + " | ".join(headers_md) + " |")
    md_parts.append("| " + " | ".join(["---"] * len(headers_md)) + " |")
    for row in csv_rows_md:
        safe = [str(x).replace("\n", " ").replace("|", "\\|") for x in row]
        md_parts.append("| " + " | ".join(safe) + " |")
    md_path.write_text("\n".join(md_parts), encoding="utf-8")

    return {"json": str(json_path), "csv": str(csv_path), "md": str(md_path), "dir": str(report_dir)}


# Initialize config early so main() can use it
config = load_config()
WORKSPACE_FOLDERS = init_workspace_folders(config.get('workspace_path', DEFAULT_WORKSPACE))
cv_engine.set_gemini_proxy_url(config.get("gemini_proxy_url", ""))


def _strip_hash_suffix(name: str) -> str:
    # Remove trailing _abcd_abcd patterns before extension or end
    return re.sub(r"(_[0-9a-f]{4})_\1(?=\.|$)", r"\1", name, flags=re.I)

def _cv_get_name(item: dict) -> str:
    """
    Safe display name extractor for both v1/v2 and for lightweight DB items.
    Order: item.data -> basics.name -> header first/last -> stored meta fields -> filename.
    """
    if not isinstance(item, dict):
        return "Unknown"

    # Some list items store parsed json in item["data"]
    data = item.get("data")
    if isinstance(data, dict):
        item = {**item, **data}

    # direct meta fields (if stored)
    for k in ("name", "full_name", "candidate_name", "cv_name"):
        v = item.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    basics = item.get("basics") or {}
    if isinstance(basics, dict):
        nm = (basics.get("name") or "").strip()
        if nm:
            return nm

    header = item.get("header") or {}
    if isinstance(header, dict):
        fn = (header.get("first_name") or "").strip()
        ln = (header.get("last_name") or "").strip()
        if fn or ln:
            return (fn + " " + ln).strip()

    # fallback to filename-like fields
    fnm = (
        item.get("_source_filename")
        or item.get("source_filename")
        or item.get("filename")
        or item.get("file")  # DB uses this for json filename
        or ""
    )
    if not isinstance(fnm, str):
        fnm = str(fnm)
    fnm = fnm.strip()
    fnm = _strip_hash_suffix(fnm)
    for ext in (".pdf", ".docx", ".json"):
        if fnm.lower().endswith(ext):
            fnm = fnm[:-len(ext)]
    parts = [p for p in fnm.split("_") if p and p.lower() not in ("cv","debug")]
    if len(parts) >= 2:
        return f"{parts[0]} {parts[1]}"
    return parts[0] if parts else "Unknown"


def _cv_get_role(item: dict) -> str:
    """Safe role extractor for both v1/v2 and lightweight DB items."""
    if not isinstance(item, dict):
        return "-"
    data = item.get("data")
    if isinstance(data, dict):
        item = {**item, **data}

    for k in ("role", "position", "title"):
        v = item.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    basics = item.get("basics") or {}
    if isinstance(basics, dict):
        r = (basics.get("current_title") or basics.get("title") or "").strip()
        if r:
            return r

    header = item.get("header") or {}
    if isinstance(header, dict):
        r = (header.get("position") or "").strip()
        if r:
            return r

    exp = item.get("experience") or item.get("work_experience") or []
    if isinstance(exp, list) and exp:
        first = exp[0]
        if isinstance(first, dict):
            r = (first.get("role") or first.get("position") or "").strip()
            if r:
                return r
    return "-"


def _cv_get_company(item: dict) -> str:
    """Safe company extractor for both v1/v2 and lightweight DB items."""
    if not isinstance(item, dict):
        return ""
    data = item.get("data")
    if isinstance(data, dict):
        item = {**item, **data}

    for k in ("company", "company_name"):
        v = item.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    exp = item.get("experience") or item.get("work_experience") or []
    if isinstance(exp, list) and exp:
        first = exp[0]
        if isinstance(first, dict):
            c = (first.get("company_name") or first.get("company") or "").strip()
            if c:
                return c
    return ""


def main(page: ft.Page):
    global config, WORKSPACE_FOLDERS
    
    page.title = SCRIPT_NAME
    page.theme_mode = ft.ThemeMode.LIGHT if config.get("ui_theme") == "Light" else ft.ThemeMode.DARK
    page.window_width = config.get("window_width", 1300)
    page.window_min_width = 1200
    page.window_height = config.get("window_height", 850)
    page.padding = 0

    def on_window_event(e):
        if e.data in ("resize", "resized"):
            config["window_width"] = page.window_width
            config["window_height"] = page.window_height
            save_config(config)
    page.on_window_event = on_window_event

    db_files = []
    current_filtered_items = []
    last_csv_count = 0
    current_nav_label = "CVs"
    range_select_mode = {"active": False}
    last_checked_idx = None

    task_state = {"cancel": False, "running": False}

    # ==========================================
    # GLOBAL UI COMPONENTS & CALLBACKS
    # ==========================================
    billing_status_text = ft.Text("", size=12, color="white", weight="bold")
    global_task_progress_bar = ft.ProgressBar(width=300, value=0, visible=False, color="blue", bgcolor="#5a6373", bar_height=8)
    global_task_status_text = ft.Text("", size=12, color="white", visible=False)
    logs_view = ft.ListView(expand=True, spacing=4)
    
    def cancel_current_task():
        task_state["cancel"] = True
        btn_global_stop.visible = False
        stopping_indicator.visible = True
        page.update()
        log_msg("🛑 Stop requested. Halting tasks...", "orange")

    btn_global_stop = ft.ElevatedButton(
        "Stop", icon=ft.icons.CANCEL, color="white", bgcolor="red",
        tooltip="Stop current task", on_click=lambda e: cancel_current_task(),
        visible=False, height=25, style=ft.ButtonStyle(padding=ft.padding.symmetric(horizontal=10, vertical=0))
    )
    stopping_indicator = ft.Text("⏳ Stopping...", color="#e5c07b", size=12, visible=False, weight=ft.FontWeight.BOLD)

    status_bar = ft.Container(
        bgcolor="#3F4651", height=40, padding=ft.padding.symmetric(horizontal=15),
        content=ft.Row([
            ft.Container(content=ft.Row([global_task_progress_bar, global_task_status_text]), width=400),
            ft.Container(expand=True),
            ft.Row([stopping_indicator, btn_global_stop, ft.Container(width=10), ft.Icon("account_balance_wallet", color="white", size=14), billing_status_text])
        ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN)
    )

    def update_billing_ui():
        i = config.get("total_in_tokens", 0)
        o = config.get("total_out_tokens", 0)
        c = config.get("total_spent_usd", 0.0)
        billing_status_text.value = f"API Usage: {i:,} In | {o:,} Out | Total Spent: ${c:.4f}"
        page.update()

    def update_billing(in_tok, out_tok, cost):
        config["total_in_tokens"] = config.get("total_in_tokens", 0) + in_tok
        config["total_out_tokens"] = config.get("total_out_tokens", 0) + out_tok
        config["total_spent_usd"] = config.get("total_spent_usd", 0.0) + cost
        save_config(config)
        update_billing_ui()

    def log_msg(msg, color="default"):
        cmap = {"default": "#cccccc", "black": "#cccccc", "blue": "#61afef", "green": "#98c379", "red": "#e06c75", "orange": "#e5c07b", "purple": "#c678dd"}
        logs_view.controls.append(ft.Text(f"> {msg}", color=cmap.get(color, color), size=13, font_family="monospace",selectable=True))
        if len(logs_view.controls) > 500: logs_view.controls.pop(0)
        if current_nav_label == "Logs":
            try: logs_view.scroll_to(offset=-1, duration=50)
            except Exception: pass
        page.update()

    def show_snack(message, action_name=None, action_handler=None):
        snack = ft.SnackBar(ft.Text(message, size=14), bgcolor="#3F4651")
        if action_name and action_handler:
            snack.action = action_name; snack.on_action = action_handler
        page.snack_bar = snack
        page.snack_bar.open = True
        page.update()

    def set_global_progress(val, txt, vis):
        global_task_progress_bar.value = val
        global_task_progress_bar.visible = vis
        global_task_status_text.value = txt
        global_task_status_text.visible = vis
        page.update()

    def render_table_and_update():
        render_table()
        page.update()

    def handle_api_error():
        config["api_key"] = ""
        save_config(config)
        set_api.value = ""
        task_state["cancel"] = True
        page.run_task(lambda: show_snack("API Key Error! Please enter a valid key."))
        page.run_task(lambda: require_api_key())

    # 🔗 INTERFACE FOR COMMUNICATION WITH ai_tasks.py
    cbs = {
        'log': log_msg,
        'progress': set_global_progress,
        'snack': lambda msg, a_name=None, a_path=None: show_snack(msg, action_name=a_name, action_handler=lambda _: open_folder(a_path) if a_path else None),
        'render': render_table_and_update,
        'billing': update_billing,
        'api_error': handle_api_error
    }

    # ==========================================
    # GLOBAL CRASH REPORTER
    # ==========================================
    def custom_excepthook(exc_type, exc_value, exc_tb):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_tb)
            return
        err_msg = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        try: page.run_task(lambda: log_msg(f"💥 GLOBAL CRASH:\n{err_msg}", "red"))
        except Exception: print(f"GLOBAL CRASH (UI unavailable):\n{err_msg}")

    def custom_thread_excepthook(args):
        err_msg = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
        try: page.run_task(lambda: log_msg(f"💥 THREAD CRASH ({args.thread.name}):\n{err_msg}", "red"))
        except Exception: print(f"THREAD CRASH (UI unavailable):\n{err_msg}")

    sys.excepthook = custom_excepthook
    threading.excepthook = custom_thread_excepthook
        
    def clear_logs(e):
        logs_view.controls.clear()
        page.update()

    # ==========================================
    # API KEY DIALOG
    # ==========================================
    api_dialog_input = ft.TextField(label="Gemini API Key", password=True, can_reveal_password=True, width=400)
    api_dialog_error = ft.Text(color="red", visible=False, size=12)
    api_dialog_progress = ft.ProgressRing(width=20, height=20, visible=False)

    def verify_and_save_api_key(e):
        test_key = api_dialog_input.value.strip()
        if not test_key:
            api_dialog_error.value = "Please enter an API Key."; api_dialog_error.visible = True; page.update(); return
        config["api_key"] = test_key
        save_config(config)
        set_api.value = test_key
        api_key_dialog.open = False
        show_snack("API Key saved.")
        page.update()

    btn_verify_api = ft.ElevatedButton("Save", on_click=verify_and_save_api_key, bgcolor="#2196F3", color="white")
    api_key_dialog = ft.AlertDialog(
        modal=True, title=ft.Text("Welcome! Setup Gemini API Key"),
        content=ft.Column([ft.Text("To use AI generation, please provide your API key.", size=12, color="grey"), api_dialog_input, api_dialog_error], tight=True),
        actions=[ft.TextButton("Cancel", on_click=lambda e: setattr(api_key_dialog, 'open', False) or page.update()), btn_verify_api],
        actions_alignment=ft.MainAxisAlignment.END,
    )
    page.overlay.append(api_key_dialog)

    def require_api_key():
        if not config.get("api_key"):
            api_dialog_input.value = ""; api_dialog_error.visible = False; api_key_dialog.open = True; page.update()
            return False
        return True

    if not config.get("api_key"):
        threading.Thread(target=lambda: time.sleep(0.5) or require_api_key(), daemon=True).start()

    def safe_int(val):
        try: return int(re.sub(r'[^\d]', '', str(val)))
        except (ValueError, TypeError): return 0

    def get_available_templates():
        t_dir = WORKSPACE_FOLDERS.get("TEMPLATES", "")
        if os.path.exists(t_dir):
            files = sorted([f for f in os.listdir(t_dir) if f.endswith('.docx') and not f.startswith('~')])
            if files: return files
        return ["quantori_classic.docx"]

    # ==========================================
    # NAVIGATION ROUTING
    # ==========================================
    def update_nav_rail():
        dests = [ft.NavigationRailDestination(icon="storage", label="CVs")]
        if config.get("show_xray_tab", False):
            dests.append(ft.NavigationRailDestination(icon="person_search", label="X-Ray Builder"))
        if config.get("show_github_tab", False):
            dests.append(ft.NavigationRailDestination(icon="manage_search", label="GitHub Miner"))
        if config.get("show_matcher_tab", False):
            dests.append(ft.NavigationRailDestination(icon="compare_arrows", label="CV Matcher"))
        if config.get("show_modify_tab", False):
            dests.append(ft.NavigationRailDestination(icon="edit_document", label="Modify CV"))
        if config.get("show_tailor_tab", False):
            dests.append(ft.NavigationRailDestination(icon="tune", label="CV Tailor"))
        if config.get("show_qa_tab", False):
            dests.append(ft.NavigationRailDestination(icon="psychology", label="AI Core Logic"))
        dests.extend([
            ft.NavigationRailDestination(icon="terminal", label="Logs"),
            ft.NavigationRailDestination(icon="settings", label="Settings"),
        ])
        nav_rail.destinations = dests
        try:
            curr_label = current_nav_label
            for i, d in enumerate(dests):
                if d.label == curr_label:
                    nav_rail.selected_index = i
                    break
        except Exception: nav_rail.selected_index = 0
        page.update()

    def change_view(e=None, label=None):
        nonlocal current_nav_label, last_csv_count
        if e: label = e.control.destinations[e.control.selected_index].label
        if current_nav_label == "Settings" and label != "Settings":
            apply_settings(e=None, force_save=True); show_snack("Settings auto-saved!") 
            
        current_nav_label = label
        view_database.visible = (label == "CVs")
        view_sourcing.visible = (label == "X-Ray Builder")
        view_matcher.visible = (label == "CV Matcher")
        view_modifier.visible = (label == "Modify CV")
        view_tailor.visible = (label == "CV Tailor")
        view_github_miner.visible = (label == "GitHub Miner")
        view_ai_core.visible = (label == "AI Core Logic")
        view_logs.visible = (label == "Logs")
        view_settings.visible = (label == "Settings")
        
        if label == "Settings":
            available_templates = get_available_templates()
            set_active_template.options = [ft.dropdown.Option(t) for t in available_templates]
            if set_active_template.value not in available_templates and available_templates:
                set_active_template.value = available_templates[0]
        if label == "CVs": 
            if not task_state.get("running"): load_db_data()
            else: render_table()
        if label == "Logs":
            try: logs_view.scroll_to(offset=-1, duration=50)
            except Exception: pass
        if label == "CV Matcher":
            db_count = len(db_files)
            if not matcher_results_table.visible: matcher_info.value = f"{db_count} CVs in database."
            if db_count > last_csv_count and last_csv_count > 0:
                warning_banner.content.controls[1].value = f"⚠️ {db_count - last_csv_count} new CV(s) in database! Click 'Analyze Database' to update the report."
                warning_banner.visible = True
            else: warning_banner.visible = False
        page.update()

    def get_target_filename(item, ext=".docx"):
        export_pref = config.get("export_naming_template", "Source Filename (source.docx)")
        if export_pref == "CV_FirstName_LastName.docx":
            full_name = item['data'].get('basics', {}).get('name', '')
            if full_name and full_name.lower() not in ['unknown', 'candidate', '']:
                safe_name = re.sub(r'[^\w\s-]', '', full_name).strip()
                parts = safe_name.split()
                if len(parts) >= 2: return f"CV_{parts[0]}_{parts[-1]}{ext}"
                elif len(parts) == 1: return f"CV_{parts[0]}{ext}"
        return item['file'].replace('.json', ext)

    def preview_single_cv(item):
        out = get_target_filename(item, ".docx")
        target_path = os.path.join(WORKSPACE_FOLDERS["OUTPUT"], out)
        show_snack(f"Generating live preview (DOCX)...")
        try:
            generate_docx_from_json(item['data'], target_path, config)
            log_msg(f"Live generated: {out}", "green")
        except Exception as ex:
            log_msg(f"Crash during {out}: {str(ex)}", "red"); show_snack(f"Error generating document. Check logs."); return
        try:
            if platform.system() == "Darwin":
                subprocess.run(["killall", "-9", "qlmanage"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                subprocess.Popen(["qlmanage", "-p", target_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                time.sleep(0.2)
                subprocess.run(["osascript", "-e", 'tell application "System Events" to set frontmost of process "qlmanage" to true'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            elif platform.system() == "Windows": os.startfile(target_path)
            else: subprocess.Popen(["xdg-open", target_path])
        except Exception as e: log_msg(f"Failed to open preview: {e}", "red")
        
    def preview_cv_by_filename(fname):
        for item in db_files:
            if item['file'] == fname:
                preview_single_cv(item)
                return
        show_snack("CV not found in current database.")

    def open_original_file(item):
        src_filename = item['data'].get('_source_filename')
        if not src_filename:
            base = item['file'].replace('.json', '')
            pdf_path = os.path.join(WORKSPACE_FOLDERS["SOURCE"], base + '.pdf')
            docx_path = os.path.join(WORKSPACE_FOLDERS["SOURCE"], base + '.docx')
            if os.path.exists(pdf_path): src_filename = base + '.pdf'
            elif os.path.exists(docx_path): src_filename = base + '.docx'
            else: src_filename = base + '.pdf' 
        src_path = os.path.join(WORKSPACE_FOLDERS["SOURCE"], src_filename)
        if os.path.exists(src_path):
            if platform.system() == "Windows": os.startfile(src_path)
            elif platform.system() == "Darwin": subprocess.Popen(["open", src_path])
            else: subprocess.Popen(["xdg-open", src_path])
        else: show_snack(f"Original file not found: {src_filename}")

    # ==========================================
    # BACKGROUND TASK WRAPPER (Locking UI)
    # ==========================================
    def run_in_background(target_func, *args):
        nonlocal current_sort_col, current_sort_asc
        task_state["cancel"] = False
        btn_global_stop.visible = True
        stopping_indicator.visible = False

        btn_import.disabled = True; btn_generate.disabled = True; btn_anonymize.disabled = True
        btn_batch_autofix.disabled = True; btn_delete.disabled = True; btn_analyze.disabled = True
        btn_run_modifier.disabled = True; btn_mine.disabled = True; btn_generate_xray.disabled = True
        qa_btn_run.disabled = True; search_input.disabled = True; search_clear_btn.disabled = True
        task_state["running"] = True

        saved_sort_col, saved_sort_asc = current_sort_col, current_sort_asc
        current_sort_col = 3; current_sort_asc = True
        page.update()

        def wrapper():
            nonlocal current_sort_col, current_sort_asc
            try: target_func(*args)
            finally:
                current_sort_col, current_sort_asc = saved_sort_col, saved_sort_asc
                btn_global_stop.visible = False
                task_state["running"] = False
                btn_global_stop.visible = False
                stopping_indicator.visible = False
                btn_import.disabled = False; btn_generate.disabled = False; btn_anonymize.disabled = False
                btn_batch_autofix.disabled = False; btn_delete.disabled = False; btn_analyze.disabled = False
                btn_run_modifier.disabled = False; btn_mine.disabled = False; btn_generate_xray.disabled = False
                qa_btn_run.disabled = False; search_input.disabled = False; search_clear_btn.disabled = False
                render_table()
                page.update()
        threading.Thread(target=wrapper, daemon=True).start()

    # ==========================================
    # 🟢 SINGLE CV AI CROSS-CHECK & LIVE FIX
    # ==========================================
    def act_auto_fix(item, qa_report_text, dialog_controls):
        if not require_api_key(): return

        btn_fix = dialog_controls['btn_fix']
        progress = dialog_controls['progress']
        status = dialog_controls['status']
        md_text = dialog_controls['md_text']

        btn_fix.disabled = True; progress.visible = True; status.visible = True
        status.value = "✨ Executing Auto-Fix..."
        md_text.value += "\n\n---\n### 🛠️ Auto-Fix Terminal Log\n"
        page.update()

        def _fix_task():
            try:
                md_text.value += "⏳ `[1/6]` Creating backup of current JSON...\n"; page.update()
                json_path = os.path.join(WORKSPACE_FOLDERS["JSON"], item['file'])
                shutil.copy2(json_path, json_path.replace('.json', '.bak'))

                md_text.value += "⏳ `[2/6]` Loading source document...\n"; page.update()
                src_filename = item['data'].get('_source_filename')
                src_path = os.path.join(WORKSPACE_FOLDERS["SOURCE"], src_filename)

                client = genai.Client(api_key=config.get("api_key"))
                old_data_copy = copy.deepcopy(item['data'])
                base_m = lossless_metrics(item['data'])
                qa_score_before = _qa_audit_get_latest(item['data'].get('qa_audit', {})).get('score', 0)

                is_docx = src_path.lower().endswith('.docx')
                if is_docx:
                    source_for_gemini = extract_text_from_docx(src_path)
                else:
                    _safe_name = unicodedata.normalize('NFKD', os.path.basename(src_path)).encode('ascii', 'ignore').decode('ascii') or "cv_file"
                    mime = 'application/pdf' if src_path.lower().endswith('.pdf') else 'image/jpeg'
                    with open(src_path, 'rb') as _fh:
                        sample = client.files.upload(file=_fh, config=genai_types.UploadFileConfig(mime_type=mime, display_name=_safe_name))
                    _upload_wait = 0
                    while sample.state.name == "PROCESSING":
                        time.sleep(1)
                        _upload_wait += 1
                        if _upload_wait > 300:
                            raise TimeoutError(f"File upload timed out after 5 min: {os.path.basename(src_path)}")
                        sample = client.files.get(name=sample.name)
                    source_for_gemini = sample

                fix_prompt = config.get("prompt_autofix", DEFAULT_PROMPTS["prompt_autofix"]).replace("{current_json_str}", json.dumps(item['data'], ensure_ascii=False)).replace("{qa_report_text}", qa_report_text)

                md_text.value += "🧠 `[3/6]` Sending fix instructions to Gemini...\n"; page.update()
                fix_resp = _retry_generate(client, MODEL_NAME, [fix_prompt, source_for_gemini]) if is_docx else _retry_generate(client, MODEL_NAME, [source_for_gemini, fix_prompt])

                md_text.value += "📥 `[4/6]` Response received. Validating + safe merge...\n"; page.update()
                res_text = getattr(fix_resp, 'text', '') or ''
                fixed_data = sanitize_json(_parse_llm_json_payload(res_text))

                safe_data = safe_apply_autofix(item['data'], fixed_data)
                safe_data = sanitize_json(safe_data)

                new_m = lossless_metrics(safe_data)
                min_chars = int(base_m["char_count"] * 0.985)
                min_strs = max(0, base_m["str_count"] - 1)
                if (new_m["str_count"] < min_strs) or (new_m["char_count"] < min_chars):
                    md_text.value += f"⚠️ **Lossless gate rejected fix** (strings: {base_m['str_count']} → {new_m['str_count']}, chars: {base_m['char_count']} → {new_m['char_count']}). Original kept.\n"
                    status.value = "⚠️ Fix rejected (data loss detected)"; status.color = "orange"; page.update()
                    return

                md_text.value += "🔬 `[5/6]` Re-QA: verifying fix actually improved quality...\n"; page.update()
                try:
                    clean_fixed = copy.deepcopy(safe_data)
                    for k in ['match_analysis', '_source_filename', '_source_hash', 'import_date', 'qa_audit']:
                        clean_fixed.pop(k, None)
                    prompt_reqa = config.get("prompt_qa", DEFAULT_PROMPTS["prompt_qa"]).replace("{json_str}", json.dumps(clean_fixed, ensure_ascii=False))
                    resp_reqa = _retry_generate(client, MODEL_NAME, [prompt_reqa, source_for_gemini]) if is_docx else _retry_generate(client, MODEL_NAME, [source_for_gemini, prompt_reqa])
                    m_reqa = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', resp_reqa.text, re.DOTALL)
                    if not m_reqa: m_reqa = re.search(r'(\{[\s\S]*?"score"[\s\S]*?\})', resp_reqa.text)
                    if m_reqa:
                        reqa_result = json.loads(m_reqa.group(1))
                        new_score = reqa_result.get('score', 0)
                        if new_score > qa_score_before:
                            update_qa_audit_lossless(safe_data, reqa_result)
                            md_text.value += f"✅ Score improved: {qa_score_before} → {new_score}/100\n"
                        else:
                            md_text.value += f"↩️ **Fix reverted**: re-QA score {new_score} ≤ original {qa_score_before}. Original kept.\n"
                            status.value = f"↩️ Fix reverted (score didn't improve)"; status.color = "orange"; page.update()
                            return
                    else:
                        md_text.value += "⚠️ Re-QA parse failed — applying fix anyway.\n"
                        update_qa_audit_lossless(safe_data, {"score": qa_score_before, "status": f"Auto-Fixed on {time.strftime('%Y-%m-%d')} (re-QA unavailable)"})
                except Exception as reqa_err:
                    md_text.value += f"⚠️ Re-QA error ({reqa_err}) — applying fix anyway.\n"
                    update_qa_audit_lossless(safe_data, {"score": qa_score_before, "status": f"Auto-Fixed on {time.strftime('%Y-%m-%d')} (re-QA failed)"})

                md_text.value += "💾 `[6/6]` Saving...\n"; page.update()
                with open(json_path, 'w', encoding='utf-8') as f: json.dump(safe_data, f, indent=2, ensure_ascii=False)
                item['data'] = safe_data; item['ts'] = os.path.getmtime(json_path)

                diff_msgs = []
                for k in safe_data:
                    if k in ['qa_audit', 'match_analysis', '_source_filename', '_source_hash', 'import_date']: continue
                    if json.dumps(old_data_copy.get(k), sort_keys=True) != json.dumps(safe_data.get(k), sort_keys=True):
                        diff_msgs.append(f"- **`{k.capitalize()}`** section was updated.")
                if not diff_msgs: diff_msgs.append("- Only internal formatting/structure was repaired.")

                md_text.value += "\n**What was changed:**\n" + "\n".join(diff_msgs)
                status.value = "✅ Fix complete!"; status.color = "green"; btn_fix.visible = False
                log_msg(f"✅ JSON Auto-Fixed for {item['data'].get('basics', {}).get('name', 'Candidate')}.", "green")

            except Exception as e:
                md_text.value += f"\n❌ **ERROR:** {str(e)}\n"; status.value = "❌ Fix failed"; status.color = "red"
            finally:
                progress.visible = False; render_table(); page.update()

        threading.Thread(target=_fix_task, daemon=True).start()

    def act_cross_check(item):
        if not require_api_key(): return
        src_filename = item['data'].get('_source_filename')
        if not src_filename:
            base = item['file'].replace('.json', '')
            if os.path.exists(os.path.join(WORKSPACE_FOLDERS["SOURCE"], base + '.pdf')): src_filename = base + '.pdf'
            else: src_filename = base + '.docx'
        src_path = os.path.join(WORKSPACE_FOLDERS["SOURCE"], src_filename)
        if not os.path.exists(src_path): return show_snack("Original file not found for audit.")

        qa_data = normalize_qa_audit(item['data'].get('qa_audit', {}))
        cached_score = qa_data.get('score', -1)
        has_cache = cached_score != -1

        audit_result_text = ft.Markdown("", selectable=True, extension_set="gitHubWeb", expand=True)
        audit_progress = ft.ProgressRing(visible=not has_cache)
        audit_status = ft.Text("Analyzing document against JSON...", italic=True, visible=not has_cache)
        
        def create_fix_handler(i_ref, md_ref, c_ref):
            return lambda e: act_auto_fix(i_ref, md_ref.value, c_ref)
            
        btn_auto_fix = ft.ElevatedButton("✨ Auto-Fix JSON", icon="auto_fix_high", bgcolor="#9c27b0", color="white", visible=False)
        dialog_controls = {'btn_fix': btn_auto_fix, 'progress': audit_progress, 'status': audit_status, 'md_text': audit_result_text}
        btn_auto_fix.on_click = create_fix_handler(item, audit_result_text, dialog_controls)

        cand_name = item['data'].get('basics', {}).get('name', 'Candidate')
        
        if has_cache:
            m_val = qa_data.get('missing', []); h_val = qa_data.get('hallucinations', [])
            m_str = "\n".join([f"- {x}" for x in m_val]) if isinstance(m_val, list) else str(m_val)
            h_str = "\n".join([f"- {x}" for x in h_val]) if isinstance(h_val, list) else str(h_val)
            
            md_content = f"### 📊 Cached QA Audit Result\n**Score:** {cached_score}/100\n\n"
            if m_str and m_str.lower() != "none": md_content += f"**🔴 Missing Data:**\n{m_str}\n\n"
            if h_str and h_str.lower() != "none": md_content += f"**🟡 Hallucinations:**\n{h_str}\n\n"
            if cached_score >= 98: md_content += "✅ **Perfect extraction.** No critical discrepancies found."
            audit_result_text.value = md_content
            if cached_score < 100: btn_auto_fix.visible = True

        audit_dialog = ft.AlertDialog(
            title=ft.Row([ft.Icon(ft.icons.FACT_CHECK, color="blue"), ft.Text(f"AI Audit: {cand_name}")]),
            content=ft.Container(content=ft.Column([ft.Row([audit_progress, audit_status], alignment=ft.MainAxisAlignment.CENTER), audit_result_text], tight=True, scroll="auto"), width=750, height=500),
            actions=[btn_auto_fix, ft.TextButton("Close", on_click=lambda e: setattr(audit_dialog, 'open', False) or page.update())]
        )
        page.overlay.append(audit_dialog)
        audit_dialog.open = True; page.update()

        if not has_cache:
            def _run_audit():
                try:
                    client = genai.Client(api_key=config.get("api_key"))
                    clean_data = copy.deepcopy(item['data'])
                    for k in ['match_analysis', '_source_filename', '_source_hash', 'import_date', 'qa_audit']: clean_data.pop(k, None)
                    prompt = config.get("prompt_qa", DEFAULT_PROMPTS["prompt_qa"]).replace("{json_str}", json.dumps(clean_data, ensure_ascii=False))

                    log_msg(f"   🔬 [QA Audit] Sending {src_filename} and JSON for cross-check...", "blue")
                    if src_path.lower().endswith('.docx'): resp_qa = client.models.generate_content(model=MODEL_NAME, contents=[prompt, extract_text_from_docx(src_path)])
                    else:
                        _mime = 'application/pdf' if src_path.lower().endswith('.pdf') else 'image/jpeg'
                        _safe_name = unicodedata.normalize('NFKD', os.path.basename(src_path)).encode('ascii', 'ignore').decode('ascii') or 'file'
                        with open(src_path, 'rb') as _fh:
                            sample = client.files.upload(file=_fh, config=genai_types.UploadFileConfig(mime_type=_mime, display_name=_safe_name))
                        upload_wait = 0
                        while sample.state.name == "PROCESSING":
                            time.sleep(1)
                            upload_wait += 1
                            if upload_wait > 300:
                                raise TimeoutError(f"File upload timed out: {os.path.basename(src_path)}")
                            sample = client.files.get(name=sample.name)
                        resp_qa = client.models.generate_content(model=MODEL_NAME, contents=[sample, prompt])

                    audit_result_text.value = resp_qa.text
                    match = re.search(r'```json\s*(\{.*?\})\s*```', resp_qa.text, re.DOTALL) or re.search(r'(\{[\s\S]*?"score"[\s\S]*?\})', resp_qa.text)
                    if match:
                        try:
                            qa_result = extract_first_json_object(match.group(1))
                            item['data']['qa_audit'] = qa_result
                            with open(os.path.join(WORKSPACE_FOLDERS["JSON"], item['file']), 'w', encoding='utf-8') as f: json.dump(item['data'], f, indent=2, ensure_ascii=False)
                            if qa_result.get('score', 100) < 100: btn_auto_fix.visible = True
                        except Exception as e: log_msg(f"⚠️ Failed to parse QA result: {e}", "orange")

                    audit_progress.visible = False; audit_status.visible = False; render_table(); page.update()
                except Exception as e:
                    audit_progress.visible = False; audit_status.value = f"Error: {str(e)}"; audit_status.color = "red"; page.update()

            threading.Thread(target=_run_audit, daemon=True).start()

    # ==========================================
    # TABS: INTERFACE AND LOGIC
    # ==========================================
    def format_tooltip(text): return textwrap.fill(str(text), width=80) if text else ""

    # --- TAB 1: SOURCING (X-Ray Builder) ---
    sourcing_input = ft.TextField(
        label="Describe the ideal candidate (skills, location, etc.)", multiline=True, min_lines=3, max_lines=6,
        value=config.get("last_sourcing_query", ""), hint_text="e.g., Need a Python Dev with AWS...",
        on_change=lambda e: save_config(config.update({"last_sourcing_query": e.control.value}) or config)
    )
    sourcing_results = ft.ListView(expand=True, spacing=10)
    btn_generate_xray = ft.ElevatedButton("Generate X-Ray Queries", icon="auto_awesome", bgcolor="#2196F3", color="white")

    def on_xray_card_generated(platform_name, desc, query_str):
        # BASE64 ARMOR
        google_base = base64.b64decode("aHR0cHM6Ly93d3cuZ29vZ2xlLmNvbS9zZWFyY2g/cT0=").decode('utf-8')
        google_url = f"{google_base}{urllib.parse.quote(query_str)}"
        card = ft.Card(content=ft.Container(padding=15, content=ft.Column([
            ft.Row([ft.Icon(name="search" if platform_name.lower() == "search" else "public", color="blue"), ft.Text(platform_name, weight="bold", size=16), ft.Text(f"- {desc}", color="grey", size=13)]),
            ft.TextField(value=query_str, read_only=True, multiline=True, text_size=13, border_color="#eeeeee"),
            ft.Row([
                ft.ElevatedButton("Open in Google", icon="open_in_new", url=google_url, bgcolor="#e3f2fd", color="#1565c0"),
                ft.IconButton(icon="copy", tooltip="Copy to clipboard", on_click=lambda e, val=query_str: page.set_clipboard(val) or show_snack("Copied!"))
            ])
        ])))
        sourcing_results.controls.append(card)
        page.update()

    def act_generate_xray(e):
        if not require_api_key() or len(sourcing_input.value) < 5: return show_snack("Please enter a longer description.")
        sourcing_results.controls.clear()
        run_in_background(run_xray_task, sourcing_input.value, config, cbs, on_xray_card_generated)
    btn_generate_xray.on_click = act_generate_xray

    view_sourcing = ft.Column([
        ft.Text("AI Sourcing Assistant (X-Ray Builder) (beta)", size=24, weight="bold"),
        ft.Text("Describe the ideal candidate. Gemini will create perfect Boolean Search formulas for you.", color="grey"),
        sourcing_input, ft.Row([btn_generate_xray]), ft.Divider(), sourcing_results
    ], visible=False, expand=True)

    # --- TAB 3: MODIFY CV ---
    modifier_input = ft.TextField(
        label="How should I modify the selected CVs?", multiline=True, min_lines=4, max_lines=8,
        value=config.get("last_modifier_query", ""), hint_text="e.g., Keep only 3 latest jobs. Translate to German.",
        on_change=lambda e: save_config(config.update({"last_modifier_query": e.control.value}) or config)
    )
    btn_run_modifier = ft.ElevatedButton("Modify & Generate CV", icon="auto_fix_high", bgcolor="#2196F3", color="white")
    
    def act_run_modifier(e):
        items = get_selected()
        if not items: return show_snack("Please select CVs in the 'CVs' tab first!")
        if len(modifier_input.value) < 5: return show_snack("Please enter a modification request!")
        if not require_api_key(): return
        run_in_background(run_modify_task, items, modifier_input.value, config, WORKSPACE_FOLDERS, task_state, db_files, cbs)
    btn_run_modifier.on_click = act_run_modifier

    view_modifier = ft.Column([
        ft.Text("AI CV Editor (beta)", size=24, weight="bold"), ft.Text("Select CVs, then describe how to change them. Original files will NOT be overwritten.", color="grey"),
        modifier_input, ft.Row([btn_run_modifier, ft.Container(expand=True), ft.ElevatedButton("Open Modified Folder", icon="folder_open", on_click=lambda _: open_folder(WORKSPACE_FOLDERS["MODIFIED"]), color="blue")]),
    ], visible=False, expand=True)

    # --- TAB 4: CV TAILOR ---
    tailor_jd_input = ft.TextField(
        label="Job Description", multiline=True, min_lines=6, max_lines=10,
        value=config.get("last_tailor_jd", ""),
        hint_text="Paste the full job description here...",
        on_change=lambda e: save_config(config.update({"last_tailor_jd": e.control.value}) or config)
    )
    tailor_anonymize_cb = ft.Checkbox(label="Anonymize", value=config.get("tailor_anonymize", False), on_change=lambda e: save_config(config.update({"tailor_anonymize": e.control.value}) or config))
    btn_run_tailor = ft.ElevatedButton("Tailor & Generate CV", icon="tune", bgcolor="#4CAF50", color="white")

    tailor_results_table = ft.DataTable(
        columns=[ft.DataColumn(ft.Text(n, weight="bold")) for n in ["Name", "Status", "Changes Made", "File"]],
        rows=[], visible=False, column_spacing=15, data_row_min_height=40, data_row_max_height=80, heading_row_height=40
    )
    tailor_table_container = ft.Container(
        content=ft.Column([tailor_results_table], scroll="auto"),
        expand=True, border=ft.border.all(1, "#eeeeee"), padding=5, visible=False
    )

    def on_tailor_row_update(cand_name, status, notes, filename):
        status_color = "green" if "✅" in status else "red"
        tailor_results_table.rows.append(ft.DataRow(cells=[
            ft.DataCell(ft.Text(cand_name, weight="bold", size=12, width=150)),
            ft.DataCell(ft.Text(status, color=status_color, size=12, width=80)),
            ft.DataCell(ft.Container(content=ft.Text(notes, size=12, max_lines=3, overflow=ft.TextOverflow.ELLIPSIS), width=350, tooltip=notes)),
            ft.DataCell(ft.Text(filename, size=11, color="grey", width=200)),
        ]))
        tailor_results_table.visible = True
        tailor_table_container.visible = True
        page.update()

    def act_run_tailor(e):
        items = get_selected()
        if not items: return show_snack("Please select CVs in the 'CVs' tab first!")
        if len(tailor_jd_input.value) < 10: return show_snack("Please enter a Job Description!")
        if not require_api_key(): return
        tailor_results_table.rows.clear()
        tailor_results_table.visible = False
        tailor_table_container.visible = False
        page.update()
        run_in_background(run_tailor_task, items, tailor_jd_input.value, config, WORKSPACE_FOLDERS, task_state, db_files, cbs, tailor_anonymize_cb.value, on_tailor_row_update)
    btn_run_tailor.on_click = act_run_tailor

    view_tailor = ft.Column([
        ft.Text("CV Tailor", size=24, weight="bold"),
        ft.Text("Select CVs in the 'CVs' tab, paste a Job Description, and generate tailored versions.", color="grey"),
        ft.Text("Paste JD below:", color="grey"),
        tailor_jd_input,
        ft.Row([btn_run_tailor, tailor_anonymize_cb, ft.Container(expand=True),
                ft.ElevatedButton("Open Tailored Folder", icon="folder_open", on_click=lambda _: open_folder(WORKSPACE_FOLDERS["TAILORED"]), color="blue")]),
        ft.Divider(),
        tailor_table_container,
    ], visible=False, expand=True)

    # --- TAB 5: GITHUB MINER ---
    miner_keywords = ft.TextField(label="Tech Stack (Keywords)", value=config.get("last_miner_keywords", ""), on_change=lambda e: save_config(config.update({"last_miner_keywords": e.control.value}) or config), expand=True)
    miner_location = ft.TextField(label="Location Filter (Optional)", value=config.get("last_miner_location", ""), on_change=lambda e: save_config(config.update({"last_miner_location": e.control.value}) or config), width=300)
    miner_stars = ft.TextField(label="Min Repo Stars", value=config.get("last_miner_stars", "100"), on_change=lambda e: save_config(config.update({"last_miner_stars": e.control.value}) or config), width=150)
    miner_results_view = ft.ListView(expand=True, spacing=10)
    btn_mine = ft.ElevatedButton("Start Mining", icon="travel_explore", bgcolor="#2196F3", color="white")

    def _import_single_github(login, btn_control):
        if not require_api_key(): return
        btn_control.disabled = True; btn_control.text = "Importing..."; page.update()
        try:
            token = config.get("github_token")
            user_data = gh_api_request(f"/users/{login}", token, cbs)
            repos_data = gh_api_request(f"/users/{login}/repos?sort=updated&per_page=10", token, cbs)
            if not user_data: raise Exception("Failed to fetch user data")
            
            prompt = config.get("prompt_github", DEFAULT_PROMPTS["prompt_github"]).replace("{prompt_schema_only}", CV_JSON_SCHEMA).replace("{gh_full_data}", json.dumps({"user": user_data, "recent_repos": repos_data or []}, ensure_ascii=False))
            
            resp = genai.Client(api_key=config.get("api_key")).models.generate_content(model=MODEL_NAME, contents=prompt)
            
            i_tok = getattr(resp.usage_metadata, 'prompt_token_count', 0); o_tok = getattr(resp.usage_metadata, 'candidates_token_count', 0)
            cost = (i_tok / 1_000_000 * PRICE_1M_IN) + (o_tok / 1_000_000 * PRICE_1M_OUT); update_billing(i_tok, o_tok, cost)
            
            txt = resp.text.replace('```json', '').replace('```', '').strip()
            new_data = sanitize_json(extract_first_json_object(txt))
            
            file_hash = hashlib.md5(login.encode('utf-8')).hexdigest()[:4]
            out_json = f"CV_GitHub_{login}_{file_hash}.json"
            
            domain_gh = "github"
            new_data['import_date'] = time.time(); new_data['_source_filename'] = f"{domain_gh}.com/{login}"; new_data['_source_hash'] = f"gh_{file_hash}"; new_data['_comment'] = "Source: GitHub" 
            
            with open(os.path.join(WORKSPACE_FOLDERS["JSON"], out_json), 'w', encoding='utf-8') as f: json.dump(new_data, f, indent=2, ensure_ascii=False)
            log_msg(f"   ✅ Imported {login} to CV Database ({i_tok + o_tok:,} tokens)", "green")
            btn_control.text = "Imported!"; btn_control.icon = "check"; btn_control.bgcolor = "grey"; page.update()
            load_db_data(); show_snack(f"Successfully imported @{login} to CV database!")
        except Exception as e:
            log_msg(f"Failed to import GitHub profile {login}: {e}", "red"); btn_control.text = "Error"; btn_control.disabled = False; page.update()

    def on_gh_card_generated(login, name, user_loc, company, email, bio, meta, html_url):
        btn_import_gh = ft.ElevatedButton("Analyze & Import", icon="download", color="white", bgcolor="green", height=30, on_click=lambda e: threading.Thread(target=_import_single_github, args=(login, e.control), daemon=True).start())
        # BASE64 ARMOR FOR LINKEDIN
        li_base = base64.b64decode("aHR0cHM6Ly93d3cubGlua2VkaW4uY29tL3NlYXJjaC9yZXN1bHRzL3Blb3BsZS8/a2V5d29yZHM9").decode('utf-8')
        li_url = f"{li_base}{urllib.parse.quote(name)}"
        
        card = ft.Card(content=ft.Container(padding=15, content=ft.Column([
            ft.Row([ft.Icon("person", color="blue"), ft.Text(f"{name} (@{login})", weight="bold", size=16), ft.Container(expand=True), ft.Text(f"From repo: {meta['repo']} ({meta['contributions']} commits)", size=12, color="grey")]),
            ft.Text(bio, italic=True, size=13),
            ft.Row([ft.Icon("location_on", size=14, color="grey"), ft.Text(user_loc if user_loc else "Unknown", size=13), ft.Icon("business", size=14, color="grey"), ft.Text(company, size=13), ft.Icon("email", size=14, color="grey"), ft.Text(email, size=13)]),
            ft.Row([btn_import_gh, ft.Container(width=10), ft.ElevatedButton("Open GitHub", url=html_url, height=30), ft.ElevatedButton("Search on LinkedIn", url=li_url, height=30, color="blue", bgcolor="#e3f2fd")])
        ])))
        miner_results_view.controls.append(card)
        page.update()

    def act_mine_github(e):
        if not config.get("github_token"): return show_snack("Please configure GitHub PAT in Settings first!", "Go to Settings", lambda _: change_view(label="Settings"))
        if len(miner_keywords.value) < 3: return show_snack("Please enter keywords!")
        miner_results_view.controls.clear()
        run_in_background(run_mine_github_task, miner_keywords.value, miner_location.value, miner_stars.value, config, task_state, cbs, on_gh_card_generated)
    btn_mine.on_click = act_mine_github

    view_github_miner = ft.Column([
        ft.Text("GitHub Sourcing Miner (beta)", size=24, weight="bold"), ft.Text("Find active developers directly from GitHub repositories.", color="grey"),
        ft.Row([miner_keywords, miner_location, miner_stars, btn_mine], alignment=ft.MainAxisAlignment.START), ft.Divider(), miner_results_view
    ], visible=False, expand=True)

    # --- TAB 6: LOGS ---
    logs_toolbar = ft.Row([ft.Container(expand=True), ft.ElevatedButton("Clear Logs", icon="delete_outline", on_click=clear_logs, color="grey")])
    terminal_container = ft.Container(content=logs_view, expand=True, bgcolor="#1e1e1e", border_radius=5, padding=10)
    view_logs = ft.Column([logs_toolbar, terminal_container], visible=False, expand=True)

    # --- TAB 0: CVs (CUSTOM GRID) ---
    cv_count_text = ft.Text("0 CVs", color="grey", size=13, weight="bold")
    current_sort_col = 4; current_sort_asc = False
    
    def handle_custom_sort(col_idx):
        if task_state.get("running"): return
        nonlocal current_sort_col, current_sort_asc
        if current_sort_col == col_idx: current_sort_asc = not current_sort_asc
        else: current_sort_col = col_idx; current_sort_asc = True if col_idx in [1, 2, 3, 7, 8] else False
        render_table()

    def handle_master_checkbox(e):
        if task_state.get("running"): return
        val = e.control.value
        for item in current_filtered_items: item['selected'] = val
        render_table()

    master_checkbox = ft.Checkbox(value=False, on_change=handle_master_checkbox)

    def toggle_range_select(e=None):
        range_select_mode["active"] = not range_select_mode["active"]
        if range_select_mode["active"]:
            range_select_btn.icon_color = "blue"
            range_select_btn.tooltip = "Range select ON — click last row to select range"
        else:
            range_select_btn.icon_color = ft.colors.with_opacity(0.4, ft.colors.ON_SURFACE)
            range_select_btn.tooltip = "Select range (click to activate, then click last row)"
        page.update()

    range_select_btn = ft.IconButton(
        icon=ft.icons.FORMAT_LIST_BULLETED,
        icon_size=16,
        icon_color=ft.colors.with_opacity(0.4, ft.colors.ON_SURFACE),
        tooltip="Select range (click to activate, then click last row)",
        on_click=toggle_range_select,
        width=30, height=30,
        style=ft.ButtonStyle(padding=ft.padding.all(4)),
    )

    sort_icons = { i: ft.Icon(size=14, visible=False, color="blue") for i in range(1, 9) }
    
    def get_header_cell(label, width=None, expand=None, col_idx=None, center=False):
        content = ft.Row([ft.Text(label, size=12, weight="bold")], alignment=ft.MainAxisAlignment.CENTER if center else ft.MainAxisAlignment.START)
        if col_idx: content.controls.append(sort_icons[col_idx])
        c_args = {"content": content, "padding": ft.padding.symmetric(vertical=10)}
        if width: c_args["width"] = width
        if expand: c_args["expand"] = expand
        if col_idx: c_args["on_click"] = lambda e: handle_custom_sort(col_idx); c_args["ink"] = True 
        return ft.Container(**c_args)

    header_row = ft.Container(content=ft.Row([], vertical_alignment=ft.CrossAxisAlignment.CENTER, spacing=10), bgcolor=ft.colors.with_opacity(0.05, ft.colors.ON_SURFACE), padding=ft.padding.only(left=10, right=20), border=ft.border.only(bottom=ft.border.BorderSide(1, ft.colors.with_opacity(0.1, ft.colors.ON_SURFACE))))
    cv_list_view = ft.ListView(expand=True, spacing=0)

    def load_db_data():
        if task_state.get("running"): return 
        nonlocal db_files; db_files.clear()
        json_f = WORKSPACE_FOLDERS["JSON"]
        if os.path.exists(json_f):
            for f in sorted([f for f in os.listdir(json_f) if f.endswith('.json')]):
                try:
                    p = os.path.join(json_f, f)
                    with open(p, 'r', encoding='utf-8') as jf: db_files.append({'file': f, 'data': json.load(jf), 'ts': os.path.getmtime(p), 'selected': False})
                except Exception as e: log_msg(f"⚠️ Skipped invalid JSON: {f} ({e})", "orange")
        render_table()

    def handle_checkbox_change(e, item_ref, idx):
        nonlocal last_checked_idx
        if task_state.get("running"): return
        if range_select_mode["active"] and last_checked_idx is not None and last_checked_idx != idx:
            lo, hi = sorted([last_checked_idx, idx])
            for i in range(lo, hi + 1):
                if 0 <= i < len(current_filtered_items):
                    current_filtered_items[i]['selected'] = e.control.value
            range_select_mode["active"] = False
            range_select_btn.icon_color = ft.colors.with_opacity(0.4, ft.colors.ON_SURFACE)
            range_select_btn.tooltip = "Select range (click to activate, then click last row)"
            last_checked_idx = None
            render_table()
            return
        item_ref['selected'] = e.control.value
        last_checked_idx = idx
        if current_filtered_items: master_checkbox.value = all(item.get('selected', False) for item in current_filtered_items)
        page.update()

    def render_table():
        nonlocal current_filtered_items
        search_text = search_input.value.lower().strip() if search_input.value else ""
        search_tokens = search_text.split() if search_text else []
        
        filtered = []
        for item in db_files:
            b = item['data'].get('basics', {})
            comp_val_search = str(b.get('current_company', ''))
            if not comp_val_search and item['data'].get('experience'): comp_val_search = str(item['data']['experience'][0].get('company_name', ''))
            
            full_txt = " ".join([item['file'].lower(), str(b.get('name', '')).lower(), str(b.get('current_title', '')).lower(), comp_val_search.lower(), json.dumps(item['data'].get('skills', {})).lower(), str(item['data'].get('_comment', '')).lower()])
            
            s_pref = ""
            if search_tokens:
                if all(t in full_txt for t in search_tokens): s_pref = f"🔍 Found Match\n---\n"
                else: continue
            filtered.append((item, s_pref))
                
        def get_comp_sort(x):
            c = x[0]['data'].get('basics', {}).get('current_company', '')
            if not c and x[0]['data'].get('experience'): c = x[0]['data']['experience'][0].get('company_name', '')
            return str(c).strip().lower()

        if current_sort_col is not None:
            rev = not current_sort_asc
            if current_sort_col == 1: filtered.sort(key=lambda x: str(x[0]['data'].get('basics', {}).get('name', '')).lower(), reverse=rev)
            elif current_sort_col == 2: filtered.sort(key=lambda x: str(x[0]['data'].get('basics', {}).get('current_title', '')).lower(), reverse=rev)
            elif current_sort_col == 3: filtered.sort(key=lambda x: x[0]['file'].lower(), reverse=rev)
            elif current_sort_col == 4: filtered.sort(key=lambda x: x[0]['ts'], reverse=rev)
            elif current_sort_col == 5: filtered.sort(key=lambda x: normalize_qa_audit(x[0]['data'].get('qa_audit', {})).get('score', -1), reverse=rev)
            elif current_sort_col == 6: filtered.sort(key=lambda x: str(x[0]['data'].get('_comment', '')).lower(), reverse=rev)
            elif current_sort_col == 7: filtered.sort(key=get_comp_sort, reverse=rev)
            elif current_sort_col == 8: filtered.sort(key=lambda x: safe_int(x[0]['data'].get('match_analysis', {}).get('score', -1)), reverse=rev)

        current_filtered_items = [x[0] for x in filtered]
        master_checkbox.value = all(item.get('selected', False) for item in current_filtered_items) if current_filtered_items else False
            
        for k, v in sort_icons.items():
            v.visible = (k == current_sort_col)
            v.name = ft.icons.ARROW_UPWARD if current_sort_asc else ft.icons.ARROW_DOWNWARD

        sc_c, sc_s, sc_f, sc_com = config.get("show_col_company", True), config.get("show_col_score", True), config.get("show_col_file", True), config.get("show_col_comments", True)
        
        h_cells = [ft.Container(ft.Row([master_checkbox, range_select_btn], spacing=0), width=65), get_header_cell("Name", expand=1, col_idx=1), get_header_cell("Role", expand=1, col_idx=2)]
        if sc_c: h_cells.append(get_header_cell("Company", expand=1, col_idx=7))
        if sc_s: h_cells.append(get_header_cell("Score", width=60, col_idx=8))
        if sc_f: h_cells.append(get_header_cell("File", width=200, col_idx=3))
        if sc_com: h_cells.append(get_header_cell("Comments", expand=1, col_idx=6))
        h_cells.extend([get_header_cell("Date", width=80, col_idx=4, center=True), get_header_cell("QA", width=50, col_idx=5, center=True), get_header_cell("View", width=80, center=True)])
        header_row.content.controls = h_cells

        new_rows = []
        for row_idx, (item, s_prefix) in enumerate(filtered):
            name = _cv_get_name(item)
            role = _cv_get_role(item)
            d_str = time.strftime('%d %b %y', time.localtime(item['ts']))
            
            # --- FACTORIES (SAFE CLICK HANDLERS) ---
            def create_dt_handler(item_ref): return lambda e: preview_single_cv(item_ref)
            def create_src_handler(item_ref): return lambda e: open_original_file(item_ref)
            def create_audit_handler(item_ref): return lambda e: act_cross_check(item_ref)
            def create_cb_handler(item_ref, i): return lambda e: handle_checkbox_change(e, item_ref, i)
            
            dt_h = create_dt_handler(item)
            src_h = create_src_handler(item)
            aud_h = create_audit_handler(item)
            cb = ft.Checkbox(value=item.get('selected', False), on_change=create_cb_handler(item, row_idx))
            # ----------------------------------
            
            src_fname = item['data'].get('_source_filename')
            base = item['file'].replace('.json', '')
            src_exists = os.path.exists(os.path.join(WORKSPACE_FOLDERS["SOURCE"], src_fname)) if src_fname else (os.path.exists(os.path.join(WORKSPACE_FOLDERS["SOURCE"], base + '.pdf')) or os.path.exists(os.path.join(WORKSPACE_FOLDERS["SOURCE"], base + '.docx')))

            qa_s = normalize_qa_audit(item['data'].get('qa_audit', {})).get('score', -1)
            is_dis = not src_exists
            if is_dis: aud_col, tt_qa = ft.colors.with_opacity(0.3, "purple"), s_prefix + "Cannot audit: Source file unavailable"
            else:
                aud_col = "purple" if qa_s == -1 else (ft.colors.GREEN if qa_s == 100 else (ft.colors.GREEN_700 if qa_s >= 98 else (ft.colors.ORANGE if qa_s >= 85 else ft.colors.RED)))
                m_str = str(normalize_qa_audit(item['data'].get('qa_audit', {})).get('missing', ''))
                tt_qa = s_prefix + f"Score: {qa_s}/100\nMissing: {m_str[:50]}..." if qa_s != -1 else s_prefix + "AI Cross-Check"

            src_btn = ft.IconButton(icon=ft.icons.DESCRIPTION, icon_color="grey" if src_exists else ft.colors.with_opacity(0.3, "grey"), icon_size=16, padding=0, width=30, height=30, tooltip="View Source" if src_exists else "No Source", on_click=src_h if src_exists else None, disabled=not src_exists)
            aud_btn = ft.IconButton(icon=ft.icons.FACT_CHECK, icon_color=aud_col, icon_size=16, padding=0, width=30, height=30, tooltip=tt_qa, on_click=aud_h if not is_dis else None, disabled=is_dis)
            prv_btn = ft.IconButton(icon=ft.icons.REMOVE_RED_EYE, icon_color="blue", icon_size=16, padding=0, width=30, height=30, tooltip="Preview DOCX", on_click=dt_h)

            m_score = item['data'].get('match_analysis', {}).get('score', None)
            if m_score is not None:
                n_sc = safe_int(m_score); sc_col = "green" if n_sc >= 70 else ("orange" if n_sc >= 40 else "red")
                sc_disp = ft.Text(str(m_score), weight="bold", color=sc_col, size=13, tooltip=format_tooltip(f"Verdict: {item['data'].get('match_analysis', {}).get('verdict', '')}"))
            else: sc_disp = ft.Text("-", color="grey", size=13, tooltip="Not analyzed yet")

            c_val = str(item['data'].get('basics', {}).get('current_company', '')).replace('\n', ' ').strip()
            if not c_val and item['data'].get('experience'): c_val = str(item['data']['experience'][0].get('company_name', '')).strip()
            
            c_cont = ft.Container(expand=1)
            
            def create_save_c(i_ref, cont_ref):
                def _save(e):
                    if 'basics' not in i_ref['data']: i_ref['data']['basics'] = {}
                    i_ref['data']['basics']['current_company'] = e.control.value.strip()
                    try:
                        with open(os.path.join(WORKSPACE_FOLDERS["JSON"], i_ref['file']), 'w', encoding='utf-8') as f: json.dump(i_ref['data'], f, indent=2, ensure_ascii=False)
                    except Exception as e_save: log_msg(f"Failed to save company edit: {e_save}", "red")
                    render_table()
                return _save

            def create_edit_c(i_ref, v_val, cont_ref):
                def _edit(e):
                    tf = ft.TextField(value=v_val, autofocus=True, text_size=12, content_padding=ft.padding.symmetric(horizontal=8, vertical=0), height=30, expand=True, on_blur=create_save_c(i_ref, cont_ref), on_submit=create_save_c(i_ref, cont_ref))
                    cont_ref.content = tf; cont_ref.update()
                return _edit
                
            c_cont.content = ft.GestureDetector(on_double_tap=create_edit_c(item, c_val, c_cont), content=ft.Container(content=ft.Text(c_val, size=12, max_lines=1, overflow=ft.TextOverflow.ELLIPSIS), height=30, alignment=ft.alignment.center_left, tooltip="Double-click to edit company"))

            com_val = str(item['data'].get('_comment', ''))
            com_cont = ft.Container(expand=1)
            
            def create_save_com(i_ref, cont_ref):
                def _save_cm(e):
                    i_ref['data']['_comment'] = e.control.value.strip()
                    try:
                        with open(os.path.join(WORKSPACE_FOLDERS["JSON"], i_ref['file']), 'w', encoding='utf-8') as f: json.dump(i_ref['data'], f, indent=2, ensure_ascii=False)
                    except Exception as e: log_msg(f"Failed to save comment: {e}", "red")
                    render_table()
                return _save_cm

            def create_edit_com(i_ref, v_val, cont_ref):
                def _edit_cm(e):
                    tf = ft.TextField(value=v_val, autofocus=True, text_size=12, content_padding=ft.padding.symmetric(horizontal=8, vertical=0), height=30, expand=True, on_blur=create_save_com(i_ref, cont_ref), on_submit=create_save_com(i_ref, cont_ref))
                    cont_ref.content = tf; cont_ref.update()
                return _edit_cm

            com_cont.content = ft.GestureDetector(on_double_tap=create_edit_com(item, com_val, com_cont), content=ft.Container(content=ft.Text(com_val, size=12, max_lines=1, overflow=ft.TextOverflow.ELLIPSIS), height=30, alignment=ft.alignment.center_left, tooltip="Double-click to edit comment"))
            
            # --- TOOLTIP RESTORE ---
            tt_default = s_prefix + "Double-click to preview DOCX"
            orig_role = item['data'].get('basics', {}).get('current_title_original', '')
            tt_role = s_prefix + textwrap.fill(orig_role if orig_role else role, width=50)
            tt_date = s_prefix + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(item['ts']))

            r_cells = [
                ft.Container(cb, width=40),
                ft.Container(ft.GestureDetector(on_double_tap=dt_h, content=ft.Text(name, weight="bold", size=12, max_lines=1, overflow=ft.TextOverflow.ELLIPSIS, tooltip=tt_default)), expand=1),
                ft.Container(ft.GestureDetector(on_double_tap=dt_h, content=ft.Text(role, size=12, max_lines=1, overflow=ft.TextOverflow.ELLIPSIS, tooltip=tt_role)), expand=1)
            ]
            if sc_c: r_cells.append(c_cont)
            if sc_s: r_cells.append(ft.Container(sc_disp, width=60, alignment=ft.alignment.center_left))
            if sc_f: r_cells.append(ft.Container(ft.GestureDetector(on_double_tap=dt_h, content=ft.Text(item['file'], italic=True, color="grey", size=12, max_lines=1, overflow=ft.TextOverflow.ELLIPSIS, tooltip=tt_default)), width=200))
            if sc_com: r_cells.append(com_cont)
            
            r_cells.extend([
                ft.Container(ft.GestureDetector(on_double_tap=dt_h, content=ft.Text(d_str, size=12, color="grey", tooltip=tt_date)), width=80),
                ft.Container(aud_btn, width=50),
                ft.Container(ft.Row([prv_btn, src_btn], spacing=0), width=80)
            ])
            # ---------------------------------

            row_color = ft.colors.with_opacity(0.3, ft.colors.AMBER) if item.get('_status') == 'processing' else None
            new_rows.append(ft.Container(content=ft.Row(r_cells, vertical_alignment=ft.CrossAxisAlignment.CENTER, spacing=10), bgcolor=row_color, height=35, padding=ft.padding.only(left=10, right=10), border=ft.border.only(bottom=ft.border.BorderSide(1, ft.colors.with_opacity(0.1, ft.colors.ON_SURFACE)))))
            
        cv_list_view.controls = new_rows; cv_count_text.value = f"{len(current_filtered_items)} / {len(db_files)} CVs"; page.update()
        if task_state.get("running"):
            proc_idx = next((i for i, x in enumerate(current_filtered_items) if x.get('_status') == 'processing'), None)
            if proc_idx is not None:
                try: cv_list_view.scroll_to(offset=max(0, (proc_idx - 3) * 35), duration=200)
                except Exception: pass

    def get_selected(): return [item for item in db_files if item.get('selected', False)]

    import_picker = ft.FilePicker()
    import_picker.on_result = lambda e: run_in_background(run_import_task, [f.path for f in e.files], config, WORKSPACE_FOLDERS, task_state, db_files, cbs) if require_api_key() and e.files else None
    page.overlay.append(import_picker)

    btn_style = ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8), padding=ft.padding.symmetric(horizontal=12))
    btn_import = ft.ElevatedButton("Import CVs", on_click=lambda _: import_picker.pick_files(allow_multiple=True, allowed_extensions=["pdf", "docx", "png", "jpg", "jpeg"]), bgcolor=ft.colors.BLUE_600, color=ft.colors.WHITE, style=btn_style)
    btn_generate = ft.ElevatedButton("Generate CVs", on_click=lambda _: run_in_background(run_generate_task, get_selected(), config, WORKSPACE_FOLDERS, task_state, db_files, cbs) if get_selected() else show_snack("Please select CVs!"), bgcolor=ft.colors.BLUE_600, color=ft.colors.WHITE, style=btn_style)
    btn_open_out = ft.IconButton(icon=ft.icons.FOLDER_OPEN, icon_color="blue", tooltip="Open Output Folder", on_click=lambda _: open_folder(WORKSPACE_FOLDERS["OUTPUT"]))
    btn_anonymize = ft.ElevatedButton("Anonymize", on_click=lambda _: run_in_background(run_anonymize_task, get_selected(), config, WORKSPACE_FOLDERS, task_state, db_files, cbs) if get_selected() and require_api_key() else show_snack("Please select CVs!"), bgcolor=ft.colors.BLUE_600, color=ft.colors.WHITE, style=btn_style)
    btn_open_blind = ft.IconButton(icon=ft.icons.FOLDER_OPEN, icon_color="orange", tooltip="Open Blind Folder", on_click=lambda _: open_folder(WORKSPACE_FOLDERS["BLIND"]))
    
    def act_batch_autofix(e):
        items = get_selected()
        if not items: return show_snack("Please select CVs first!")
        if not require_api_key(): return
        needs_fix = [i for i in items if normalize_qa_audit(i['data'].get('qa_audit', {})).get('score', -1) < 100]
        if not needs_fix: return show_snack("Selected CVs already have a perfect score (100).")
        run_in_background(run_batch_autofix_task, items, config, WORKSPACE_FOLDERS, task_state, db_files, cbs)
        
    btn_batch_autofix = ft.ElevatedButton("Auto Fix", icon="auto_fix_high", on_click=act_batch_autofix, bgcolor=ft.colors.PURPLE_600, color=ft.colors.WHITE, style=btn_style)
    
    def toggle_col(e, config_key): config[config_key] = not config.get(config_key, True); save_config(config); e.control.checked = config[config_key]; render_table()
    col_menu = ft.PopupMenuButton(icon=ft.icons.TUNE, tooltip="Customize Columns", items=[
        ft.PopupMenuItem(text="Show 'Company'", checked=config.get("show_col_company", True), on_click=lambda e: toggle_col(e, "show_col_company")),
        ft.PopupMenuItem(text="Show 'Score'", checked=config.get("show_col_score", True), on_click=lambda e: toggle_col(e, "show_col_score")),
        ft.PopupMenuItem(text="Show 'File'", checked=config.get("show_col_file", True), on_click=lambda e: toggle_col(e, "show_col_file")),
        ft.PopupMenuItem(text="Show 'Comments'", checked=config.get("show_col_comments", True), on_click=lambda e: toggle_col(e, "show_col_comments")),
    ])
    
    def execute_delete(e):
        items = get_selected()
        for item in items:
            try: os.remove(os.path.join(WORKSPACE_FOLDERS["JSON"], item['file'])); log_msg(f"Deleted JSON: {item['file']}", "red")
            except Exception as ex: log_msg(f"Failed to delete {item['file']}: {ex}", "red")
        show_snack(f"Successfully deleted {len(items)} file(s)."); load_db_data(); delete_dialog.open = False; page.update()
    
    delete_dialog = ft.AlertDialog(modal=True, title=ft.Text("Confirm Deletion"), content=ft.Text("Are you sure you want to delete the selected files?"), actions=[ft.TextButton("Cancel", on_click=lambda e: setattr(delete_dialog, 'open', False) or page.update()), ft.TextButton("Delete", on_click=execute_delete, style=ft.ButtonStyle(color="red"))], actions_alignment=ft.MainAxisAlignment.END)
    btn_delete = ft.ElevatedButton("Delete", on_click=lambda e: (setattr(delete_dialog.content, 'value', f"Delete {len(get_selected())} CVs?") or setattr(delete_dialog, 'open', True) or page.update()) if get_selected() else show_snack("Select CVs!"), bgcolor=ft.colors.RED_600, color=ft.colors.WHITE, style=btn_style)
    btn_refresh = ft.IconButton(icon=ft.icons.REFRESH, tooltip="Refresh Database", on_click=lambda e: load_db_data())

    search_timer = None
    def handle_search(e):
        nonlocal search_timer
        if search_timer: search_timer.cancel()
        search_timer = threading.Timer(0.2, render_table); search_timer.start()
    search_input = ft.TextField(hint_text="Search", prefix_icon=ft.icons.SEARCH, on_change=handle_search, height=35, text_size=13, content_padding=ft.padding.only(left=10, right=10, top=5, bottom=5), expand=True)
    search_clear_btn = ft.IconButton(icon=ft.icons.CLOSE, on_click=lambda e: setattr(search_input, 'value', "") or render_table(), icon_size=16)

    cv_toolbar = ft.Container(content=ft.Row([
        ft.Row([btn_import, ft.Row([btn_generate, btn_open_out], spacing=0), ft.Row([btn_anonymize, btn_open_blind], spacing=0), btn_batch_autofix], spacing=5),
        ft.Container(width=15), ft.Container(content=ft.Row([search_input, search_clear_btn, ft.Container(width=5), cv_count_text], vertical_alignment=ft.CrossAxisAlignment.CENTER, spacing=0), expand=True), ft.Container(width=15),
        ft.Row([col_menu, btn_delete, btn_refresh], spacing=5),
    ], vertical_alignment=ft.CrossAxisAlignment.CENTER), padding=ft.padding.only(bottom=10))

    view_database = ft.Column([cv_toolbar, ft.Container(content=ft.Column([header_row, cv_list_view], spacing=0), expand=True, border=ft.border.all(1, "#eeeeee"))], visible=True, expand=True)

    # --- TAB 2: CV MATCHER ---
    matcher_info = ft.Text("Ready to analyze CVs.", size=16, weight="bold")
    warning_banner = ft.Container(content=ft.Row([ft.Icon("warning_amber_rounded", color="white"), ft.Text("⚠️ New CVs in database!", color="white", weight="bold")]), bgcolor="orange", padding=10, border_radius=5, visible=False)
    jd_input = ft.TextField(label="Job Description", value=config.get("last_jd", ""), multiline=True, min_lines=4, max_lines=8, on_change=lambda e: save_config(config.update({"last_jd": e.control.value}) or config))
    matcher_results_table = ft.DataTable(columns=[ft.DataColumn(ft.Text(n, weight="bold")) for n in ["Score", "Name", "Verdict", "Pros", "Missing Skills", "View"]], rows=[], visible=False, column_spacing=15, data_row_min_height=40, data_row_max_height=55, heading_row_height=40)
    table_container = ft.Container(content=ft.Column([matcher_results_table], scroll="auto"), expand=True, border=ft.border.all(1, "#eeeeee"), padding=5, visible=False)

    def load_latest_report_to_ui():
        nonlocal last_csv_count
        reports_dir = WORKSPACE_FOLDERS["REPORTS"]
        csv_files = [os.path.join(reports_dir, f) for f in os.listdir(reports_dir) if f.endswith('.csv')]
        if not csv_files: last_csv_count = 0; return
        latest_csv = max(csv_files, key=os.path.getmtime)
        try:
            with open(latest_csv, 'r', encoding='utf-8') as f: rows = list(csv.DictReader(f))
            last_csv_count = len(rows); matcher_results_table.rows.clear()
            for p in rows:
                score_val = str(p.get('Score', '0'))
                num_score = safe_int(score_val)
                score_color = "green" if num_score >= 70 else ("orange" if num_score >= 40 else "red")
                
                # --- FACTORY FOR PREVIEW IN MATCHER ---
                def create_preview_handler(fname): return lambda e: preview_cv_by_filename(fname)
                dt_handler = create_preview_handler(p.get('Filename', ''))
                
                matcher_results_table.rows.append(ft.DataRow(cells=[
                    ft.DataCell(ft.GestureDetector(on_double_tap=dt_handler, content=ft.Container(content=ft.Text(score_val, weight="bold", color=score_color, size=13), width=50))),
                    ft.DataCell(ft.GestureDetector(on_double_tap=dt_handler, content=ft.Container(content=ft.Text(str(p.get('Name', 'Unknown')), weight="bold", size=12), width=150))),
                    ft.DataCell(ft.GestureDetector(on_double_tap=dt_handler, content=ft.Container(content=ft.Text(str(p.get('Verdict', '')), size=12, max_lines=2, overflow=ft.TextOverflow.ELLIPSIS), width=200, tooltip=format_tooltip(p.get('Verdict', ''))))),
                    ft.DataCell(ft.GestureDetector(on_double_tap=dt_handler, content=ft.Container(content=ft.Text(str(p.get('Pros', '')), size=12, max_lines=2, overflow=ft.TextOverflow.ELLIPSIS), width=250, tooltip=format_tooltip(p.get('Pros', ''))))),
                    ft.DataCell(ft.GestureDetector(on_double_tap=dt_handler, content=ft.Container(content=ft.Text(str(p.get('Missing Skills', '')), size=12, max_lines=2, overflow=ft.TextOverflow.ELLIPSIS), width=250, tooltip=format_tooltip(p.get('Missing Skills', ''))))),
                    ft.DataCell(ft.IconButton(icon=ft.icons.REMOVE_RED_EYE, icon_color="blue", tooltip="Preview CV", on_click=dt_handler))
                ]))
            matcher_results_table.visible = True; table_container.visible = True; log_msg(f"Loaded previous report: {os.path.basename(latest_csv)}", "blue")
        except Exception as e: log_msg(f"⚠️ Failed to load previous report: {e}", "orange")

    def on_matcher_complete(parsed_all, cands):
        nonlocal last_csv_count
        last_csv_count = len(parsed_all)
        matcher_results_table.rows.clear()
        for p in parsed_all:
            score_val = str(p.get('score', 0)); num_score = safe_int(score_val); score_color = "green" if num_score >= 70 else ("orange" if num_score >= 40 else "red")
            try: c_idx = int(p.get('id', -1))
            except (ValueError, TypeError): c_idx = -1
            fname_orig = cands[c_idx]['file'] if (0 <= c_idx < len(cands)) else "Unknown"

            # --- FACTORY FOR PREVIEW IN MATCHER ---
            def create_preview_handler(fname): return lambda e: preview_cv_by_filename(fname)
            dt_handler = create_preview_handler(fname_orig)

            matcher_results_table.rows.append(ft.DataRow(cells=[
                ft.DataCell(ft.GestureDetector(on_double_tap=dt_handler, content=ft.Container(content=ft.Text(score_val, weight="bold", color=score_color, size=13), width=50))),
                ft.DataCell(ft.GestureDetector(on_double_tap=dt_handler, content=ft.Container(content=ft.Text(str(p.get('name', 'Unknown')), weight="bold", size=12), width=150))),
                ft.DataCell(ft.GestureDetector(on_double_tap=dt_handler, content=ft.Container(content=ft.Text(str(p.get('verdict', '')), size=12, max_lines=2, overflow=ft.TextOverflow.ELLIPSIS), width=200, tooltip=format_tooltip(p.get('verdict', ''))))),
                ft.DataCell(ft.GestureDetector(on_double_tap=dt_handler, content=ft.Container(content=ft.Text(str(p.get('pros', '')), size=12, max_lines=2, overflow=ft.TextOverflow.ELLIPSIS), width=250, tooltip=format_tooltip(p.get('pros', ''))))),
                ft.DataCell(ft.GestureDetector(on_double_tap=dt_handler, content=ft.Container(content=ft.Text(str(p.get('missing_skills', '')), size=12, max_lines=2, overflow=ft.TextOverflow.ELLIPSIS), width=250, tooltip=format_tooltip(p.get('missing_skills', ''))))),
                ft.DataCell(ft.IconButton(icon=ft.icons.REMOVE_RED_EYE, icon_color="blue", tooltip="Preview CV", on_click=dt_handler))
            ]))
        matcher_results_table.visible = True; table_container.visible = True; page.update()

    def run_matcher_action(e):
        if not db_files: return show_snack("Database is empty! Import CVs first.")
        if len(jd_input.value) < 10: return show_snack("Please enter Job Description!")
        if not require_api_key(): return
        table_container.visible = False; matcher_results_table.visible = False; warning_banner.visible = False
        run_in_background(run_matcher_task, copy.deepcopy(db_files), jd_input.value, config, WORKSPACE_FOLDERS, task_state, cbs, on_matcher_complete)

    btn_analyze = ft.ElevatedButton("Analyze Database", icon="rocket_launch", on_click=run_matcher_action)
    view_matcher = ft.Column([ft.Row([matcher_info, ft.Container(expand=True)]), warning_banner, ft.Text("Paste JD below:", color="grey"), jd_input, ft.Row([btn_analyze, ft.Container(expand=True), ft.ElevatedButton("Open Reports Folder", icon="folder_open", on_click=lambda _: open_folder(WORKSPACE_FOLDERS["REPORTS"]), color="blue")]), ft.Divider(), table_container], visible=False, expand=True)

    # --- TAB 4: MLOPS & AI CORE LOGIC (UNIFIED) ---
    qa_compare_mode = ft.RadioGroup(
        value=config.get("qa_compare_mode", "input_json"),
        content=ft.Column([
            ft.Radio(value="input_json", label="Compare input PDF/DOCX & JSON"),
            ft.Radio(value="json_docx", label="Compare JSON & output DOCX"),
            ft.Radio(value="full_pipeline", label="Compare input PDF/DOCX & JSON & output DOCX"),
        ], spacing=2),
        on_change=lambda e: save_config(config.update({"qa_compare_mode": e.control.value}) or config)
    )
    qa_sample_size = ft.Dropdown(label="Sample Size", options=[ft.dropdown.Option("All available"), ft.dropdown.Option("Selected CVs")], value=config.get("last_qa_sample_size", "All available"), width=200, on_change=lambda e: save_config(config.update({"last_qa_sample_size": e.control.value}) or config))
    qa_btn_copy = ft.IconButton(icon=ft.icons.COPY, tooltip="Copy summary to Clipboard", on_click=lambda e: page.set_clipboard(qa_result_md.value) or show_snack("Copied!"), disabled=True)
    qa_result_md = ft.Markdown("", selectable=True, extension_set="gitHubWeb", visible=False)

    qa_results_table = ft.DataTable(
        columns=[],
        rows=[],
        column_spacing=24,
        heading_row_height=44,
        data_row_min_height=44,
        data_row_max_height=56,
        expand=True,
    )
    qa_results_title = ft.Text("QA Results", weight="bold", visible=False)
    qa_table_box = ft.Container(
        visible=False,
        border=ft.border.all(1, "#eeeeee"),
        border_radius=4,
        padding=6,
        content=ft.Column([qa_results_table], scroll="auto", height=300),
    )
    qa_report_title = ft.Text("Macro QA Report", weight="bold", visible=False)
    qa_report_box = ft.Container(
        visible=False,
        border=ft.border.all(1, "#eeeeee"),
        border_radius=4,
        padding=10,
        content=qa_result_md,
    )
    qa_results_container = ft.Container(
        visible=False,
        expand=True,
        padding=10,
        border=ft.border.all(1, "#eeeeee"),
        border_radius=5,
        content=ft.Column([qa_results_title, qa_table_box, qa_report_title, qa_report_box], scroll="auto", expand=True, spacing=8)
    )

    def _pct(v):
        return "N/A" if v is None else f"{v}%"

    _QA_W_FILE = 250
    _QA_W_SCORE = 105
    _QA_W_TOTAL = 90
    _QA_W_COMMENTS = 700

    def _qa_header(text, width=None, center=False):
        content = ft.Text(text, weight="bold", text_align=ft.TextAlign.CENTER if center else ft.TextAlign.LEFT)
        if width is None:
            return content
        return ft.Container(content=content, width=width, alignment=ft.alignment.center if center else ft.alignment.center_left)

    def _qa_text_cell(text, width=None, center=False, bold=False, tooltip=None):
        content = ft.Text(
            str(text),
            size=12,
            weight="bold" if bold else None,
            text_align=ft.TextAlign.CENTER if center else ft.TextAlign.LEFT,
            max_lines=2 if not center else 1,
            overflow=ft.TextOverflow.ELLIPSIS,
            tooltip=tooltip,
        )
        if width is None:
            return content
        return ft.Container(content=content, width=width, alignment=ft.alignment.center if center else ft.alignment.center_left)

    def _set_qa_table_columns(mode):
        if mode == "full_pipeline":
            qa_results_table.columns = [
                ft.DataColumn(_qa_header("Input File", _QA_W_FILE)),
                ft.DataColumn(_qa_header("In→JSON", _QA_W_SCORE, center=True), numeric=True),
                ft.DataColumn(_qa_header("JSON→DOCX", _QA_W_SCORE, center=True), numeric=True),
                ft.DataColumn(_qa_header("Total", _QA_W_TOTAL, center=True), numeric=True),
                ft.DataColumn(_qa_header("Comments", _QA_W_COMMENTS)),
            ]
        else:
            label = "DOCX Quality" if mode == "json_docx" else "Quality"
            qa_results_table.columns = [
                ft.DataColumn(_qa_header("Input File", _QA_W_FILE)),
                ft.DataColumn(_qa_header(label, _QA_W_SCORE + 10, center=True), numeric=True),
                ft.DataColumn(_qa_header("Comments", _QA_W_COMMENTS)),
            ]

    def _render_qa_rows(rows, mode):
        _set_qa_table_columns(mode)
        qa_results_table.rows.clear()
        for row in rows or []:
            comments = str(row.get("comments", ""))
            if mode == "full_pipeline":
                qa_results_table.rows.append(ft.DataRow(cells=[
                    ft.DataCell(_qa_text_cell(str(row.get("input_file", "")), _QA_W_FILE)),
                    ft.DataCell(_qa_text_cell(_pct(row.get("input_json")), _QA_W_SCORE, center=True, bold=True)),
                    ft.DataCell(_qa_text_cell(_pct(row.get("json_docx")), _QA_W_SCORE, center=True, bold=True)),
                    ft.DataCell(_qa_text_cell(_pct(row.get("total_quality")), _QA_W_TOTAL, center=True, bold=True)),
                    ft.DataCell(_qa_text_cell(comments, _QA_W_COMMENTS, tooltip=comments)),
                ]))
            else:
                qa_results_table.rows.append(ft.DataRow(cells=[
                    ft.DataCell(_qa_text_cell(str(row.get("input_file", "")), _QA_W_FILE)),
                    ft.DataCell(_qa_text_cell(_pct(row.get("quality")), _QA_W_SCORE + 10, center=True, bold=True)),
                    ft.DataCell(_qa_text_cell(comments, _QA_W_COMMENTS, tooltip=comments)),
                ]))

    _set_qa_table_columns(qa_compare_mode.value)

    qa_rows_state = []

    def on_qa_row(row):
        nonlocal qa_rows_state
        if not isinstance(row, dict):
            return
        display = dict(row)
        if qa_compare_mode.value == "full_pipeline":
            # Normalize incoming row keys from ai_tasks
            if display.get("input_json") is None and display.get("quality") is not None:
                display["input_json"] = display.get("quality")
            if display.get("total_quality") is None and display.get("quality") is not None:
                display["total_quality"] = display.get("quality")
        key = str(display.get("input_file", ""))
        # Drop placeholder row after first real row
        if key and key != "(batch starting)":
            qa_rows_state = [r for r in qa_rows_state if str(r.get("input_file", "")) != "(batch starting)"]
        updated = False
        for i, existing in enumerate(qa_rows_state):
            if str(existing.get("input_file", "")) == key and key:
                qa_rows_state[i] = display
                updated = True
                break
        if not updated:
            qa_rows_state.append(display)
        qa_results_title.value = f"QA Results ({len(qa_rows_state)} rows)"
        qa_results_title.visible = True
        qa_table_box.visible = True
        qa_results_container.visible = True
        _render_qa_rows(qa_rows_state, qa_compare_mode.value)
        page.update()

    def on_qa_complete(payload):
        nonlocal qa_rows_state
        mode = payload.get("mode", qa_compare_mode.value) if isinstance(payload, dict) else qa_compare_mode.value
        md_text = payload.get("summary_md", "") if isinstance(payload, dict) else str(payload)
        rows = payload.get("rows", []) if isinstance(payload, dict) else []
        if rows:
            qa_rows_state.clear()
            qa_rows_state.extend(rows)
        rows = qa_rows_state
        saved_paths = {}
        if isinstance(payload, dict):
            try:
                saved_paths = _save_qa_reports(payload, config, WORKSPACE_FOLDERS)
            except Exception as ex:
                log_msg(f"Failed to save QA reports: {ex}", "red")
        qa_result_md.value = md_text
        qa_result_md.visible = bool(md_text)
        qa_results_title.visible = bool(rows)
        qa_table_box.visible = bool(rows)
        qa_report_title.visible = bool(md_text)
        qa_report_box.visible = bool(md_text)
        qa_results_container.visible = bool(rows) or bool(md_text)
        _render_qa_rows(rows, mode)
        qa_btn_copy.disabled = not bool(md_text)
        if md_text and "Error" not in md_text and mode == "input_json":
            btn_auto_tune.disabled = False
        if saved_paths:
            show_snack(f"QA reports saved to {saved_paths.get('dir')}")
            log_msg(f"QA reports saved: {saved_paths.get('json')} | {saved_paths.get('csv')} | {saved_paths.get('md')}", "green")
        page.update()

    def act_run_batch_qa(e):
        if not require_api_key(): return
        valid_cands = []
        for item in db_files:
            if qa_sample_size.value == "Selected CVs" and not item.get('selected', False): continue
            src_fname = item['data'].get('_source_filename'); base = item['file'].replace('.json', '')
            p = os.path.join(WORKSPACE_FOLDERS["SOURCE"], src_fname) if src_fname else (os.path.join(WORKSPACE_FOLDERS["SOURCE"], base + '.pdf') if os.path.exists(os.path.join(WORKSPACE_FOLDERS["SOURCE"], base + '.pdf')) else os.path.join(WORKSPACE_FOLDERS["SOURCE"], base + '.docx'))
            if os.path.exists(p): valid_cands.append({'item': item, 'src_path': p})
        if not valid_cands: return show_snack("No valid CVs found for the audit!")
        sc = len(valid_cands) if qa_sample_size.value in ["All available", "Selected CVs"] else int(qa_sample_size.value)
        if sc > len(valid_cands): sc = len(valid_cands)
        nonlocal qa_rows_state
        qa_rows_state = [{"input_file": "(batch starting)", "input_json": None, "json_docx": None, "total_quality": None, "comments": "Waiting for first QA callback..."}]
        qa_btn_copy.disabled = True
        qa_result_md.value = "*Aggregating logs and generating MLOps analysis...*"
        qa_result_md.visible = True
        qa_results_container.visible = True
        qa_results_title.value = "QA Results (starting...)"
        qa_results_title.visible = True
        _set_qa_table_columns(qa_compare_mode.value)
        _render_qa_rows(qa_rows_state, qa_compare_mode.value)
        btn_auto_tune.disabled = True
        page.update()
        run_in_background(run_batch_qa_task, random.sample(valid_cands, sc), sc, config, WORKSPACE_FOLDERS, task_state, cbs, on_qa_complete)

    qa_btn_run = ft.ElevatedButton("Run Batch QA Audit", icon="bug_report", bgcolor="#2196F3", color="white", on_click=act_run_batch_qa)
    btn_auto_tune = ft.ElevatedButton("Auto-Tune Master Prompt", icon="auto_awesome", disabled=True)

    qa_controls = ft.Column([
        ft.Text("Run batch micro-audits to find systemic data loss. The Wizard will tune your PROMPT_MASTER.", color="grey"),
        qa_compare_mode,
        ft.Row([qa_sample_size, qa_btn_run, qa_btn_copy, btn_auto_tune], wrap=True, vertical_alignment=ft.CrossAxisAlignment.CENTER),
        ft.Divider(),
        qa_results_container,
    ], expand=True, spacing=10)

    
    cbs['qa_row'] = on_qa_row
    cbs['qa_progress'] = on_qa_progress if 'on_qa_progress' in locals() else (lambda *a, **k: None)

    prompt_text_fields = {}
    prompt_lab_content = ft.Column(scroll="auto", expand=True, spacing=15)
    master_prompt_registry = ensure_master_prompts_registry()

    def _sorted_master_prompt_entries():
        reg = ensure_master_prompts_registry()
        entries = [e for e in reg.get("prompts", []) if isinstance(e, dict)]
        return sorted(entries, key=lambda e: int(e.get("version", 0)))

    def _master_prompt_options():
        opts = []
        for entry in _sorted_master_prompt_entries():
            ver = int(entry.get("version", 0))
            title = entry.get("title") or f"Master Prompt v{ver}"
            status = entry.get("status") or "saved"
            opts.append(ft.dropdown.Option(str(ver), f"v{ver} — {title} [{status}]"))
        return opts

    def _master_prompt_meta(version_value):
        try:
            version_value = int(version_value)
        except Exception:
            version_value = config.get("active_prompt_version", CURRENT_PROMPT_MASTER_VERSION)
        entry = get_master_prompt_entry(version_value, registry=ensure_master_prompts_registry()) or {}
        title = entry.get("title") or f"Master Prompt v{version_value}"
        status = entry.get("status") or "saved"
        based_on = entry.get("based_on")
        notes = (entry.get("notes") or "").strip()
        note_tail = f" — {notes}" if notes else ""
        base_tail = f", based on v{based_on}" if based_on else ""
        return f"Version v{version_value}: {title} [{status}{base_tail}]{note_tail}"

    p_defs = [
        ("prompt_master_inst", "⚙️ Master Parser"),
        ("prompt_qa", "🔬 QA Audit"),
        ("prompt_autofix", "✨ Auto-Fix"),
        ("prompt_matcher", "⚖️ CV Matcher"),
        ("prompt_modifier", "✍️ CV Modifier"),
        ("prompt_tailor", "🎯 CV Tailor"),
        ("prompt_github", "🐙 GitHub Miner"),
        ("prompt_xray", "🔎 X-Ray Builder"),
        ("prompt_anonymize", "🕵️ Anonymizer"),
    ]

    for p_key, p_label in p_defs:
        tf = ft.TextField(
            value=config.get(p_key, DEFAULT_PROMPTS.get(p_key, "")),
            multiline=True,
            min_lines=3,
            max_lines=15,
            text_size=12,
            text_style=ft.TextStyle(font_family="monospace"),
        )
        prompt_text_fields[p_key] = tf

        master_version_dd = None
        master_meta_txt = None

        if p_key == "prompt_master_inst":
            master_version_dd = ft.Dropdown(
                label="Saved Master Prompt Version",
                options=_master_prompt_options(),
                value=str(config.get("active_prompt_version", CURRENT_PROMPT_MASTER_VERSION)),
                width=360,
                text_size=13,
                height=50,
            )
            master_meta_txt = ft.Text(
                _master_prompt_meta(master_version_dd.value),
                size=11,
                color="grey",
                selectable=True,
            )

            def _refresh_master_prompt_controls(version_dd, meta_txt, selected_value=None):
                reg = ensure_master_prompts_registry()
                if version_dd is None or meta_txt is None:
                    return
                selected_value = str(selected_value or config.get("active_prompt_version", CURRENT_PROMPT_MASTER_VERSION))
                version_dd.options = _master_prompt_options()
                versions = {opt.key for opt in version_dd.options}
                version_dd.value = selected_value if selected_value in versions else str(config.get("active_prompt_version", CURRENT_PROMPT_MASTER_VERSION))
                meta_txt.value = _master_prompt_meta(version_dd.value)

        def create_save_handler(k, t, version_dd=None, meta_txt=None):
            def _save(e):
                config[k] = t.value
                if k == "prompt_master_inst":
                    based_on = config.get("active_prompt_version") or config.get("prompt_master_version") or CURRENT_PROMPT_MASTER_VERSION
                    new_ver, _registry = save_master_prompt_version(
                        t.value,
                        title="Prompt Editor save",
                        notes="Saved from Prompt Editor.",
                        based_on=based_on,
                        make_active=True,
                        status="experimental",
                    )
                    config["prompt_master_user_edited"] = True
                    config["active_prompt_version"] = new_ver
                    config["prompt_master_version"] = new_ver
                    config["_prompt_master_upgrade_warning"] = False
                    if version_dd and meta_txt:
                        _refresh_master_prompt_controls(version_dd, meta_txt, str(new_ver))
                        version_dd.update()
                        meta_txt.update()
                    save_config(config)
                    show_snack(f"Saved Master Prompt as version v{new_ver}")
                    return
                save_config(config)
                show_snack(f"Saved: {k}")
            return _save

        def create_reset_handler(k, t, version_dd=None, meta_txt=None):
            def _reset(e):
                if k == "prompt_master_inst":
                    default_ver = CURRENT_PROMPT_MASTER_VERSION
                    default_txt = get_master_prompt_text(default_ver) or DEFAULT_PROMPTS.get(k, "")
                    t.value = default_txt
                    config[k] = default_txt
                    config["prompt_master_user_edited"] = False
                    config["active_prompt_version"] = default_ver
                    config["prompt_master_version"] = default_ver
                    config["_prompt_master_upgrade_warning"] = False
                    save_config(config)
                    if version_dd and meta_txt:
                        _refresh_master_prompt_controls(version_dd, meta_txt, str(default_ver))
                        version_dd.update()
                        meta_txt.update()
                    t.update()
                    show_snack(f"Reset Master Prompt to default v{default_ver}")
                    return
                t.value = DEFAULT_PROMPTS.get(k, "")
                config[k] = DEFAULT_PROMPTS.get(k, "")
                save_config(config)
                t.update()
                show_snack(f"Reset: {k}")
            return _reset

        btn_save_prompt = ft.ElevatedButton(
            "Save as New Version" if p_key == "prompt_master_inst" else "Save",
            icon="save",
            on_click=create_save_handler(p_key, tf, master_version_dd, master_meta_txt),
            bgcolor="green",
            color="white"
        )
        btn_reset_prompt = ft.ElevatedButton(
            "Reset to Default", icon="restore", on_click=create_reset_handler(p_key, tf, master_version_dd, master_meta_txt), color="red"
        )

        extra_controls = []
        if p_key == "prompt_master_inst":
            def create_master_load_handler(text_field, version_dd, meta_txt):
                def _load_selected_version(e):
                    selected = (version_dd.value if version_dd else None) or str(config.get("active_prompt_version", CURRENT_PROMPT_MASTER_VERSION))
                    selected_txt = get_master_prompt_text(selected)
                    if not selected_txt:
                        show_snack(f"Master Prompt v{selected} has no stored text")
                        return
                    text_field.value = selected_txt
                    meta_txt.value = _master_prompt_meta(selected)
                    text_field.update()
                    meta_txt.update()
                    show_snack(f"Loaded Master Prompt v{selected} into editor")
                return _load_selected_version

            def create_master_activate_handler(text_field, version_dd, meta_txt):
                def _activate_selected_version(e):
                    selected = (version_dd.value if version_dd else None) or str(config.get("active_prompt_version", CURRENT_PROMPT_MASTER_VERSION))
                    selected_txt = get_master_prompt_text(selected)
                    if not selected_txt:
                        show_snack(f"Master Prompt v{selected} has no stored text")
                        return
                    text_field.value = selected_txt
                    config["prompt_master_inst"] = selected_txt
                    config["prompt_master_user_edited"] = False
                    config["active_prompt_version"] = int(selected)
                    config["prompt_master_version"] = int(selected)
                    config["_prompt_master_upgrade_warning"] = False
                    save_config(config)
                    _refresh_master_prompt_controls(version_dd, meta_txt, selected)
                    text_field.update()
                    if version_dd:
                        version_dd.update()
                    if meta_txt:
                        meta_txt.update()
                    show_snack(f"Activated Master Prompt v{selected}")
                return _activate_selected_version

            extra_controls = [
                ft.Text(f"Registry file: {MASTER_PROMPTS_FILE}", size=11, color="grey", selectable=True),
                ft.Row([
                    master_version_dd,
                    ft.ElevatedButton("Load Version", icon="download", on_click=create_master_load_handler(tf, master_version_dd, master_meta_txt)),
                    ft.ElevatedButton("Activate Version", icon="published_with_changes", on_click=create_master_activate_handler(tf, master_version_dd, master_meta_txt)),
                ], wrap=True, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                master_meta_txt,
            ]

        prompt_lab_content.controls.append(
            ft.Card(
                content=ft.Container(
                    padding=15,
                    content=ft.Column([
                        ft.Text(p_label, weight="bold", size=16),
                        *extra_controls,
                        tf,
                        ft.Row([btn_save_prompt, btn_reset_prompt]),
                    ])
                )
            )
        )

        prompt_editor_view = ft.Column([
            ft.Text("⚠️ Modify prompts carefully. Do NOT change {variables}.", color="orange"),
            ft.Divider(),
            prompt_lab_content,
        ], expand=True, spacing=10)
    ai_core_tabs = ft.Tabs(
        selected_index=0,
        animation_duration=150,
        tabs=[
            ft.Tab(text="QA & Auto-Tuning", content=ft.Container(content=qa_controls, padding=ft.padding.only(top=10))),
            ft.Tab(text="Prompt Editor", content=ft.Container(content=prompt_editor_view, padding=ft.padding.only(top=10))),
        ],
        expand=True,
    )

    view_ai_core = ft.Column([
        ft.Text("AI Core Logic (beta)", size=24, weight="bold"),
        ai_core_tabs,
    ], visible=False, expand=True)

    # AUTO-TUNER WIZARD LOGIC
    # --- SETTINGS TAB ---
    def apply_settings(e=None, force_save=False):
        global config, WORKSPACE_FOLDERS
        for k, ui_c in zip([
            "api_key", "github_token", "gemini_proxy_url", "workspace_path", "import_mode", "generate_docx_on_import",
            "anon_cut_name", "anon_remove_creds", "anon_mask_companies", "keep_initial_current_title",
            "show_xray_tab", "show_github_tab", "show_matcher_tab", "show_modify_tab", "show_tailor_tab", "show_qa_tab",
            "active_template", "json_naming_template", "export_naming_template", "naming_template",
            "ui_theme", "qa_compare_mode", "autofix_threshold"
        ], [
            set_api, set_github_token, set_proxy_url, set_workspace, set_import_mode, set_generate_docx,
            set_anon_name, set_anon_creds, set_anon_comps, set_keep_initial_title,
            set_show_xray, set_show_github, set_show_matcher, set_show_modify, set_show_tailor, set_show_qa,
            set_active_template, set_json_naming, set_export_naming, set_naming,
            set_theme, qa_compare_mode, set_autofix_threshold
        ]):
            config[k] = ui_c.value
        save_config(config)
        cv_engine.set_gemini_proxy_url(config.get("gemini_proxy_url", ""))
        WORKSPACE_FOLDERS = init_workspace_folders(config["workspace_path"]); update_nav_rail()
        if e or force_save: page.theme_mode = ft.ThemeMode.LIGHT if config["ui_theme"] == "Light" else ft.ThemeMode.DARK; page.update()

    workspace_picker = ft.FilePicker()
    workspace_picker.on_result = lambda e: (setattr(set_workspace, 'value', e.path) or apply_settings()) if e.path else None
    page.overlay.append(workspace_picker)

    set_api = ft.TextField(label="Gemini API Key", value=config.get("api_key", ""), password=True, can_reveal_password=True, text_size=13, on_blur=lambda e: apply_settings())
    set_github_token = ft.TextField(label="GitHub PAT (For API Limits)", value=config.get("github_token", ""), password=True, can_reveal_password=True, text_size=13, on_blur=lambda e: apply_settings())
    set_proxy_url = ft.TextField(label="Gemini Proxy URL (optional, for restricted networks)", value=config.get("gemini_proxy_url", ""), hint_text="https://webqcv.onrender.com", text_size=13, on_blur=lambda e: apply_settings())
    set_workspace = ft.TextField(label="Workspace Path", value=config.get("workspace_path", DEFAULT_WORKSPACE), text_size=13, expand=True, on_blur=lambda e: apply_settings())
    btn_browse = ft.ElevatedButton("Browse...", icon="folder", on_click=lambda _: workspace_picker.get_directory_path())

    set_import_mode = ft.RadioGroup(content=ft.Column([ft.Radio(value="none", label="Fast Import (Skip QA)"), ft.Radio(value="qa", label="Auto-QA (Audit Only)"), ft.Radio(value="fix", label="Auto-QA & Auto-Fix")], spacing=5), value=config.get("import_mode", "fix"))
    set_autofix_threshold = ft.TextField(label="Auto-Fix Threshold", value=str(config.get("autofix_threshold", 90)), width=130, text_size=13, suffix_text="/100", keyboard_type=ft.KeyboardType.NUMBER)
    set_generate_docx = ft.Checkbox(label="Generate DOCX", value=config.get("generate_docx_on_import", False))
    set_keep_initial_title = ft.Switch(label="Preserve original Job Title in DOCX", value=config.get("keep_initial_current_title", False))
    set_anon_name = ft.Switch(label="Cut Last Name", value=config.get("anon_cut_name", True))
    set_anon_creds = ft.Switch(label="Remove Credentials (Emails, Phones)", value=config.get("anon_remove_creds", True))
    set_anon_comps = ft.Switch(label="Mask Company Names (AI)", value=config.get("anon_mask_companies", True))
    
    set_show_xray = ft.Switch(label="Show X-Ray Builder", value=config.get("show_xray_tab", False), on_change=lambda e: apply_settings(e=e))
    set_show_github = ft.Switch(label="Show GitHub Tools", value=config.get("show_github_tab", False), on_change=lambda e: apply_settings(e=e))
    set_show_matcher = ft.Switch(label="Show CV Matcher", value=config.get("show_matcher_tab", False), on_change=lambda e: apply_settings(e=e))
    set_show_modify = ft.Switch(label="Show Modify CV", value=config.get("show_modify_tab", False), on_change=lambda e: apply_settings(e=e))
    set_show_tailor = ft.Switch(label="Show CV Tailor", value=config.get("show_tailor_tab", False), on_change=lambda e: apply_settings(e=e))
    set_show_qa = ft.Switch(label="Show AI Core Logic Tab", value=config.get("show_qa_tab", False), on_change=lambda e: apply_settings(e=e))
    set_active_template = ft.Dropdown(label="Active DOCX Template", options=[ft.dropdown.Option(t) for t in get_available_templates()], value=config.get("active_template", "quantori_classic.docx"), text_size=13, height=50, on_change=lambda e: apply_settings(e=e))
    set_json_naming = ft.Dropdown(label="JSON Files Naming", options=[ft.dropdown.Option("Source Filename (source.json)"), ft.dropdown.Option("CV_FirstName_LastName.json")], value=config.get("json_naming_template", "CV_FirstName_LastName.json"), text_size=13, height=50)
    set_export_naming = ft.Dropdown(label="Exported CV Naming", options=[ft.dropdown.Option("Source Filename (source.docx)"), ft.dropdown.Option("CV_FirstName_LastName.docx")], value=config.get("export_naming_template", "CV_FirstName_LastName.docx"), text_size=13, height=50)
    set_naming = ft.Dropdown(label="Anonymous CV Naming", options=[ft.dropdown.Option("CV FirstName FirstLetter (CV_Alexei_L.docx)"), ft.dropdown.Option("SourceName + _a (source_a.docx)")], value=config.get("naming_template", "CV FirstName FirstLetter (CV_Alexei_L.docx)"), text_size=13, height=50)
    set_theme = ft.Dropdown(label="App UI Theme", options=[ft.dropdown.Option("Light"), ft.dropdown.Option("Dark")], value=config.get("ui_theme", "Light"), text_size=13, height=50, on_change=lambda e: apply_settings(e=e))
    
    def rst_bil(e): config["total_in_tokens"] = 0; config["total_out_tokens"] = 0; config["total_spent_usd"] = 0.0; save_config(config); update_billing_ui(); show_snack("Billing reset.")
    btn_reset_billing = ft.ElevatedButton("Reset Billing Counters", icon="refresh", color="red", on_click=rst_bil)

    version_text = ft.Text(f"App Version: {APP_VERSION}", color="grey", size=12)

    col_left = ft.Column([
        ft.Text("API Keys & Core Settings", weight="bold", color="#2196F3"),
        set_api,
        set_github_token,
        set_proxy_url,
        ft.Row([set_workspace, btn_browse]),
        ft.Container(height=10),
        
        ft.Text("Import & Processing", weight="bold", color="#2196F3"),
        ft.Row([set_import_mode, ft.Container(width=20), ft.Column([ft.Container(content=set_generate_docx, padding=ft.padding.only(top=6)), set_autofix_threshold], spacing=10)], alignment=ft.MainAxisAlignment.START, vertical_alignment=ft.CrossAxisAlignment.START),
        ft.Container(height=10),
        
        ft.Text("Naming Conventions", weight="bold", color="#2196F3"), 
        set_json_naming, 
        set_export_naming, 
        set_naming,
    ], expand=True, spacing=10)

    col_right = ft.Column([
        ft.Text("Anonymization Rules", weight="bold", color="#2196F3"),
        set_anon_name, 
        set_anon_creds, 
        set_anon_comps,
        ft.Container(height=10),
        
        ft.Text("DOCX Templates & Formatting", weight="bold", color="#2196F3"), 
        set_active_template,
        ft.Row([set_keep_initial_title, ft.ElevatedButton("Open Templates", icon="folder_open", on_click=lambda _: open_folder(WORKSPACE_FOLDERS["TEMPLATES"]))]), 
        ft.Container(height=10), 
        
        ft.Text("Advanced Tools", weight="bold", color="#2196F3"),
        ft.Column([
            set_show_xray,
            set_show_github,
            set_show_matcher,
            set_show_modify,
            set_show_tailor,
            set_show_qa,
        ], spacing=2),
        ft.Container(height=10),
        ft.Text("App Preferences", weight="bold", color="#2196F3"),
        ft.Row([set_theme, btn_reset_billing]),
    ], expand=True, spacing=10)

    view_settings = ft.Column([
        ft.Row([ft.Text("Application Settings", size=24, weight="bold"), ft.Container(expand=True), version_text]), 
        ft.Divider(), 
        ft.Row([
            ft.Container(content=col_left, expand=True, padding=ft.padding.only(right=20)),
            ft.VerticalDivider(width=1, color="#eeeeee"),
            ft.Container(content=col_right, expand=True, padding=ft.padding.only(left=10))
        ], expand=True, vertical_alignment=ft.CrossAxisAlignment.START)
    ], visible=False, expand=True)

    # --- INITIALIZATION ---
    nav_rail = ft.NavigationRail(
        selected_index=0, min_width=100, label_type="all",
        destinations=[], on_change=change_view
    )

    page.overlay.append(delete_dialog)
    
    main_ui = ft.Container(
        content=ft.Row([
            nav_rail, 
            ft.VerticalDivider(width=1), 
            ft.Container(content=ft.Stack([view_database, view_sourcing, view_matcher, view_modifier, view_tailor, view_github_miner, view_ai_core, view_logs, view_settings]), expand=True, padding=10)
        ], expand=True),
        expand=True,
        padding=10
    )


    prompt_upgrade_dialog = None

    def show_prompt_upgrade_warning_once():
        nonlocal prompt_upgrade_dialog
        if not config.get("_prompt_master_upgrade_warning"):
            return

        def _use_new_default(e):
            config["prompt_master_inst"] = DEFAULT_PROMPTS["prompt_master_inst"]
            config["prompt_master_user_edited"] = False
            config["prompt_master_version"] = CURRENT_PROMPT_MASTER_VERSION
            config["_prompt_master_upgrade_warning"] = False
            save_config(config)
            if "prompt_master_inst" in prompt_text_fields:
                prompt_text_fields["prompt_master_inst"].value = config["prompt_master_inst"]
            prompt_upgrade_dialog.open = False
            page.update()
            show_snack("Master Prompt updated to new default.")

        def _keep_custom(e):
            config["prompt_master_user_edited"] = True
            config["prompt_master_version"] = CURRENT_PROMPT_MASTER_VERSION
            config["_prompt_master_upgrade_warning"] = False
            save_config(config)
            prompt_upgrade_dialog.open = False
            page.update()
            show_snack("Keeping your custom Master Prompt.")

        prompt_upgrade_dialog = ft.AlertDialog(
            modal=True,
            title=ft.Text("Master Prompt update available"),
            content=ft.Text(
                "This workspace contains an older custom Master Prompt. "
                "You can keep your custom version or replace it with the new default."
            ),
            actions=[
                ft.TextButton("Keep Custom", on_click=_keep_custom),
                ft.ElevatedButton("Use New Default", on_click=_use_new_default),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        page.dialog = prompt_upgrade_dialog
        prompt_upgrade_dialog.open = True
        page.update()

    page.add(
        ft.Column([
            main_ui,
            status_bar
        ], expand=True, spacing=0)
    )

    log_msg(f"Workspace loaded: {config.get('workspace_path')}", "blue")
    update_nav_rail()
    update_billing_ui()
    load_db_data() 
    load_latest_report_to_ui()
    change_view(label="CVs")
    show_prompt_upgrade_warning_once()


def extract_first_json_object(text: str):
    """Extract the first valid JSON object/array from a string.
    Accepts plain JSON, fenced ```json blocks, or JSON embedded in other text.
    Raises ValueError if nothing valid is found.
    """
    if text is None:
        raise ValueError("No text to parse")
    s = str(text).strip()

    # Strip common markdown fences
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s*```\s*$", "", s)

    # Fast path
    try:
        return json.loads(s)
    except Exception:
        pass

    # Try fenced JSON anywhere in the text
    m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", s, flags=re.IGNORECASE)
    if m:
        candidate = m.group(1).strip()
        try:
            return json.loads(candidate)
        except Exception:
            s = candidate

    # Scan for first JSON object/array
    for start_m in re.finditer(r"[\[{]", s):
        start = start_m.start()
        opener = s[start]
        closer = "}" if opener == "{" else "]"
        for end in range(len(s), start + 1, -1):
            if s[end - 1] != closer:
                continue
            chunk = s[start:end]
            try:
                return json.loads(chunk)
            except Exception:
                continue

    raise ValueError("No valid JSON found in model output")

if __name__ == "__main__":
    ft.app(target=main)
