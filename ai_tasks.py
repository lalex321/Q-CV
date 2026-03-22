import time
import os
import json
import re
import copy
import csv
import shutil
import hashlib
import urllib.parse
import urllib.request
import urllib.error
import base64
from google import genai
from google.genai import types as genai_types


def _retry_generate(client, model_name, contents):
    max_retries = 3
    delay = 5
    for attempt in range(max_retries):
        try:
            return client.models.generate_content(model=model_name, contents=contents)
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "Resource exhausted" in err_str or "Quota" in err_str:
                if attempt < max_retries - 1:
                    print(f"⚠️ API 429 Limit hit. Sleeping for {delay} seconds... (Attempt {attempt+1}/{max_retries})")
                    time.sleep(delay)
                else:
                    raise e
            else:
                raise e

from cv_engine import (
    MODEL_NAME, PRICE_1M_IN, PRICE_1M_OUT, DEFAULT_PROMPTS, CV_JSON_SCHEMA,
    process_file_gemini, extract_text_from_docx, sanitize_json,
    generate_docx_from_json, smart_anonymize_data
)

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def calc_eta(start_time, current_idx, total_items):
    if current_idx > 0:
        elapsed = time.time() - start_time
        avg_time = elapsed / current_idx
        rem_files = total_items - current_idx
        eta_secs = int(avg_time * rem_files)
        return f" | ETA: {eta_secs // 60}m {eta_secs % 60}s" if eta_secs > 60 else f" | ETA: {eta_secs}s"
    return " | ETA: Calculating..."

def calc_eta_excluding_skips(start_time, processed_count, total_items, skipped_fast=0):
    """ETA based only on actually processed (slow) files, excluding fast-skipped duplicates."""
    if processed_count > 0:
        elapsed = time.time() - start_time
        avg_time = elapsed / processed_count
        rem_files = total_items - skipped_fast - processed_count
        if rem_files < 0:
            rem_files = 0
        eta_secs = int(avg_time * rem_files)
        return f" | ETA: {eta_secs // 60}m {eta_secs % 60}s" if eta_secs > 60 else f" | ETA: {eta_secs}s"
    return " | ETA: Calculating..."


def get_target_filename(item, config, ext=".docx"):
    export_pref = config.get("export_naming_template", "Source Filename (source.docx)")
    if export_pref == "CV_FirstName_LastName.docx":
        full_name = item['data'].get('basics', {}).get('name', '')
        if full_name and full_name.lower() not in ['unknown', 'candidate', '']:
            safe_name = re.sub(r'[^\w\s-]', '', full_name).strip()
            parts = safe_name.split()
            if len(parts) >= 2: return f"CV_{parts[0]}_{parts[-1]}{ext}"
            elif len(parts) == 1: return f"CV_{parts[0]}{ext}"
    return item['file'].replace('.json', ext)

def check_api_error(err_str, cbs):
    if "API_KEY_INVALID" in err_str or "400" in err_str or "403" in err_str:
        cbs['api_error']()
        return True
    return False




def _reports_dir(folders):
    path = None
    if isinstance(folders, dict):
        path = folders.get("REPORTS")
    if not path:
        path = os.path.expanduser("~/Documents/Quantori_CV_Workplace/reports")
    os.makedirs(path, exist_ok=True)
    return path


def _parse_llm_json_payload(res_text: str):
    txt = res_text or ""
    match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', txt, re.DOTALL)
    if match:
        txt = match.group(1)
    else:
        start = txt.find('{')
        if start >= 0:
            txt = txt[start:]
    parsed, _ = json.JSONDecoder().raw_decode(txt)
    return parsed


def _save_bad_llm_payload(folders, prefix, cand_name, raw_text, meta=None):
    reports = _reports_dir(folders)
    safe = re.sub(r'[^\w\-]+', '_', str(cand_name or 'candidate')).strip('_') or 'candidate'
    ts = time.strftime('%Y%m%d_%H%M%S')
    base = f"{prefix}_{safe}_{ts}"
    txt_path = os.path.join(reports, base + '.txt')
    meta_path = os.path.join(reports, base + '.json')
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write(raw_text or '')
    with open(meta_path, 'w', encoding='utf-8') as f:
        json.dump(meta or {}, f, indent=2, ensure_ascii=False)
    return txt_path


def _qa_audit_get_latest(qa_audit):
    """qa_audit may be dict or list-of-dicts (history). Return dict."""
    if isinstance(qa_audit, list):
        for x in reversed(qa_audit):
            if isinstance(x, dict):
                return x
        return {}
    if isinstance(qa_audit, dict):
        return qa_audit
    return {}

def _merge_list_lossless(old_list, new_list):
    """Merge lists without losing old items. Keeps order: new items first, then old missing."""
    if not isinstance(old_list, list):
        old_list = []
    if not isinstance(new_list, list):
        new_list = []
    out = []
    seen = set()
    for x in new_list + old_list:
        key = json.dumps(x, ensure_ascii=False, sort_keys=True) if isinstance(x, (dict, list)) else str(x)
        if key in seen:
            continue
        seen.add(key)
        out.append(x)
    return out


def update_qa_audit_lossless(data: dict, qa_result: dict):
    """Update data['qa_audit'] without losing previous audits."""
    if not isinstance(data, dict):
        return
    prev = data.get("qa_audit")
    prev_list = []
    if isinstance(prev, list):
        prev_list = [x for x in prev if isinstance(x, dict)]
    elif isinstance(prev, dict) and prev:
        prev_list = [prev]

    qa = qa_result if isinstance(qa_result, dict) else {}
    qa.setdefault("history", [])
    if isinstance(qa["history"], list):
        qa["history"] = _merge_list_lossless(prev_list, qa["history"])
    else:
        qa["history"] = prev_list
    data["qa_audit"] = qa

def safe_apply_autofix(base_data, fixed_data):
    """Apply auto-fix results WITHOUT losing any original information.

    Strategy:
    - Start from base_data (lossless source of truth).
    - Replace only a small allowlist of sections with fixed_data's versions.
    - For list-like fields, merge losslessly (union) to avoid deletions.
    - Preserve raw / source metadata / anything unknown.
    """
    if not isinstance(base_data, dict):
        base_data = {}
    if not isinstance(fixed_data, dict):
        fixed_data = {}

    out = copy.deepcopy(base_data)

    # Sections that auto-fix is allowed to improve (others stay untouched)
    allow_keys_replace = [
        "summary",
        "skills",
        "technical_skills",
        "experience",
        "work_experience",
        "education",
        "certifications",
        "languages",
        "other_sections",
        "extras",
        "other",
    ]

    for k in allow_keys_replace:
        if k not in fixed_data:
            continue
        v_new = fixed_data.get(k)
        v_old = out.get(k)

        # Merge lists losslessly (do not allow deletions)
        if isinstance(v_old, list) or isinstance(v_new, list):
            out[k] = _merge_list_lossless(v_old, v_new)
        # Merge dicts shallowly (keep old keys if new misses them)
        elif isinstance(v_old, dict) and isinstance(v_new, dict):
            merged = copy.deepcopy(v_old)
            merged.update(v_new)
            out[k] = merged
        else:
            out[k] = v_new

    # Always preserve raw/source/meta if present in base
    for k in ["raw", "_source_filename", "_source_hash", "import_date", "_comment"]:
        if k in base_data:
            out[k] = base_data[k]

    # qa_audit: keep history (lossless)
    prev_a = base_data.get("qa_audit")
    prev_latest = _qa_audit_get_latest(prev_a)
    new_a = _qa_audit_get_latest(fixed_data.get("qa_audit"))

    history = []
    if isinstance(prev_a, list):
        history.extend([x for x in prev_a if isinstance(x, dict)])
    elif isinstance(prev_a, dict) and prev_a:
        history.append(prev_a)

    if prev_latest and (not new_a or new_a != prev_latest):
        # ensure previous latest is in history (already likely)
        pass

    # Build new audit dict, keep history inside
    audit = new_a if new_a else {}
    if not isinstance(audit, dict):
        audit = {}

    audit.setdefault("history", [])
    if isinstance(audit["history"], list):
        # merge history losslessly
        audit["history"] = _merge_list_lossless(history, audit["history"])
    else:
        audit["history"] = history

    out["qa_audit"] = audit

    return out

def lossless_metrics(obj):
    """Compute a simple 'information mass' metric for lossless gating.
    Counts non-empty strings and total characters across the structure.
    Ignores known volatile keys (qa_audit, UI flags, etc.).
    """
    IGNORE_KEYS = {
        "qa_audit", "match_analysis", "_status", "selected", "ts",
        "import_date", "_comment",
    }
    str_count = 0
    char_count = 0

    def walk(x, key=None):
        nonlocal str_count, char_count
        if isinstance(x, str):
            s = x.strip()
            if s:
                str_count += 1
                char_count += len(s)
            return
        if isinstance(x, list):
            for it in x:
                walk(it)
            return
        if isinstance(x, dict):
            for k, v in x.items():
                if k in IGNORE_KEYS:
                    continue
                walk(v, k)
            return

    walk(obj)
    return {"str_count": str_count, "char_count": char_count}

# ==========================================
# 1. GENERATE CVs TASK
# ==========================================
def run_generate_task(items, config, folders, task_state, db_files, cbs):
    try:
        total_items = len(items)
        cbs['progress'](0, "Calculating ETA...", True)
        cbs['log'](f"Generating {total_items} DOCX...", "blue")
        
        start_time = time.time()
        for idx, item in enumerate(items):
            if task_state.get("cancel"):
                cbs['log']("⏹️ Generation aborted by user.", "orange")
                break
            
            item['_status'] = 'processing'
            cbs['render']()
            
            out = get_target_filename(item, config, ".docx")
            current_idx = idx + 1
            eta_str = calc_eta(start_time, idx, total_items)
            
            cbs['progress'](idx / total_items, f"Generating {current_idx}/{total_items}: {out}{eta_str}", True)
                
            try:
                t_path = os.path.join(folders["OUTPUT"], out)
                saved_path = generate_docx_from_json(item['data'], t_path, config)
                if saved_path: cbs['log'](f"   Done: {os.path.basename(saved_path)}", "green")
            except Exception as ex: 
                cbs['log'](f"   ❌ Crash during {out}: {str(ex)}", "red")
            
            item['_status'] = None
            item['selected'] = False
            cbs['render']()

        for item in db_files: item['selected'] = False
        cbs['snack']("Generation complete!", "Open DOCXs", folders["OUTPUT"])
    finally:
        cbs['progress'](0, "", False)
        cbs['render']()

# ==========================================
# 2. ANONYMIZE CVs TASK
# ==========================================
def run_anonymize_task(items, config, folders, task_state, db_files, cbs):
    try:
        total_items = len(items)
        cbs['progress'](0, "Calculating ETA...", True)
        cbs['log'](f"Anonymizing {total_items} CVs into DOCX...", "orange")
        
        start_time = time.time()
        blind_pref = config.get("naming_template", "CV FirstName FirstLetter (CV_Alexei_L.docx)")
        api_key = config.get("api_key", "")
        
        for idx, item in enumerate(items):
            if task_state.get("cancel"):
                cbs['log']("⏹️ Anonymization aborted by user.", "orange")
                break
            
            item['_status'] = 'processing'
            cbs['render']()
            
            current_idx = idx + 1
            cand_name = str(item['data']['basics'].get('name', 'Unknown')).replace('\n', ' ').strip()
            eta_str = calc_eta(start_time, idx, total_items)
            
            cbs['progress'](idx / total_items, f"Anonymizing {current_idx}/{total_items}: {cand_name}{eta_str}", True)
            
            try:
                blind_data, in_tok, out_tok, cost = smart_anonymize_data(item['data'], api_key, config)
                if cost > 0: cbs['billing'](in_tok, out_tok, cost)
                    
                file_hash = hashlib.md5(item['file'].encode('utf-8')).hexdigest()[:4]
                if blind_pref == "CV FirstName FirstLetter (CV_Alexei_L.docx)":
                    short_name = blind_data.get('basics', {}).get('name', 'Candidate')
                    safe_name = re.sub(r'[^\w\s]', '', short_name).strip().replace(' ', '_')
                    out_filename = f"CV_{safe_name}_{file_hash}.docx"
                else:
                    base_n = os.path.splitext(item['file'])[0]
                    out_filename = f"{base_n}_a_{file_hash}.docx"

                t_path = os.path.join(folders["BLIND"], out_filename)
                saved_path = generate_docx_from_json(blind_data, t_path, config)
                
                cost_str = f" (Cost: ${cost:.4f})" if cost > 0 else " (Free)"
                if saved_path: cbs['log'](f"   Saved: {os.path.basename(saved_path)}{cost_str}", "green")
            except Exception as ex: 
                err = str(ex)
                cbs['log'](f"   ❌ Crash during anonymization: {err}", "red")
                if check_api_error(err, cbs): break

            item['_status'] = None
            item['selected'] = False
            cbs['render']()

        for item in db_files: item['selected'] = False
        cbs['snack']("Anonymization complete!", "Open Blind", folders["BLIND"])
    finally:
        cbs['progress'](0, "", False)
        cbs['render']()

# ==========================================
# 3. BATCH AUTO-FIX TASK
# ==========================================
def run_batch_autofix_task(items, config, folders, task_state, db_files, cbs):
    try:
        total_items = len(items)
        cbs['progress'](0, "Calculating ETA...", True)
        cbs['log'](f"Starting Batch Auto-Fix for {total_items} CVs...", "purple")
        
        start_time = time.time()
        client = genai.Client(api_key=config.get("api_key", ""))
        
        for idx, item in enumerate(items):
            if task_state.get("cancel"):
                cbs['log']("⏹️ Batch Auto-Fix aborted by user.", "orange")
                break
                
            cand_name = str(item['data'].get('basics', {}).get('name', 'Unknown')).strip()
            current_idx = idx + 1
            eta_str = calc_eta(start_time, idx, total_items)
            
            cbs['progress'](idx / total_items, f"Preparing {current_idx}/{total_items}: {cand_name}{eta_str}", True)
            
            qa_data = _qa_audit_get_latest(item['data'].get('qa_audit', {}))
            score = _qa_audit_get_latest(qa_data).get('score', -1)
            
            if score == 100:
                cbs['log'](f"   ⏩ Skipped {cand_name}: Perfect score (100).", "orange")
                item['selected'] = False
                continue
            
            item['_status'] = 'processing'
            cbs['render']()
                
            try:
                src_filename = item['data'].get('_source_filename')
                if not src_filename:
                    base = item['file'].replace('.json', '')
                    if os.path.exists(os.path.join(folders["SOURCE"], base + '.pdf')): src_filename = base + '.pdf'
                    else: src_filename = base + '.docx'
                
                src_path = os.path.join(folders["SOURCE"], src_filename)
                if not os.path.exists(src_path):
                    cbs['log'](f"   ❌ Source file not found for {cand_name}", "red")
                    item['_status'] = None
                    item['selected'] = False
                    cbs['render']()
                    continue

                source_for_gemini = None
                is_docx = src_path.lower().endswith('.docx')
                
                if is_docx:
                    source_for_gemini = extract_text_from_docx(src_path)
                else:
                    mime = 'application/pdf' if src_path.lower().endswith('.pdf') else 'image/jpeg'
                    import unicodedata as _ud
                    _safe_name = _ud.normalize('NFKD', os.path.basename(src_path)).encode('ascii', 'ignore').decode('ascii') or "cv_file"
                    sample = client.files.upload(file=src_path, config=genai_types.UploadFileConfig(mime_type=mime, display_name=_safe_name))
                    while sample.state.name == "PROCESSING":
                        if task_state.get("cancel"): break
                        time.sleep(1)
                        sample = client.files.get(name=sample.name)
                    if task_state.get("cancel"): break
                    source_for_gemini = sample

                if score == -1:
                    cbs['progress'](idx / total_items, f"Quick Audit {current_idx}/{total_items}: {cand_name}{eta_str}", True)
                    cbs['log'](f"   🔬 No audit found for {cand_name}. Running quick QA...", "blue")
                    
                    clean_data = copy.deepcopy(item['data'])
                    for k in ['match_analysis', '_source_filename', '_source_hash', 'import_date', 'qa_audit', '_status', 'selected']:
                        clean_data.pop(k, None)
                        
                    prompt = config.get("prompt_qa", DEFAULT_PROMPTS["prompt_qa"]).replace("{json_str}", json.dumps(clean_data, ensure_ascii=False))

                    resp_qa = _retry_generate(client, MODEL_NAME, [prompt, source_for_gemini]) if is_docx else _retry_generate(client, MODEL_NAME, [source_for_gemini, prompt])

                    i_tok = getattr(resp_qa.usage_metadata, 'prompt_token_count', 0)
                    o_tok = getattr(resp_qa.usage_metadata, 'candidates_token_count', 0)
                    cost = (i_tok / 1_000_000 * PRICE_1M_IN) + (o_tok / 1_000_000 * PRICE_1M_OUT)
                    cbs['billing'](i_tok, o_tok, cost)

                    qa_text = resp_qa.text
                    match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', qa_text, re.DOTALL)
                    if not match: match = re.search(r'(\{[\s\S]*?"score"[\s\S]*?\})', qa_text)
                    
                    if match:
                        qa_result = json.loads(match.group(1))
                        score = qa_result.get('score', 100)
                        update_qa_audit_lossless(item['data'], qa_result)
                        qa_data = qa_result
                        with open(os.path.join(folders["JSON"], item['file']), 'w', encoding='utf-8') as f:
                            json.dump(item['data'], f, indent=2, ensure_ascii=False)
                        
                    if score == 100:
                        cbs['log'](f"   ✅ Quick Audit done. Perfect score (100). No fix needed.", "green")
                        item['_status'] = None
                        item['selected'] = False
                        cbs['render']()
                        continue
                    else:
                        cbs['log'](f"   ⚠️ Quick Audit done. Score: {score}/100. Proceeding to fix...", "orange")

                cbs['progress'](idx / total_items, f"Applying Fix {current_idx}/{total_items}: {cand_name}{eta_str}", True)

                json_path = os.path.join(folders["JSON"], item['file'])
                shutil.copy2(json_path, json_path.replace('.json', '.bak'))

                fix_prompt = config.get("prompt_autofix", DEFAULT_PROMPTS["prompt_autofix"]).replace("{current_json_str}", json.dumps(item['data'], ensure_ascii=False)).replace("{qa_report_text}", json.dumps(qa_data, ensure_ascii=False))
                
                cbs['log'](f"   🚀 Fixing {cand_name}...", "blue")
                
                fix_resp = _retry_generate(client, MODEL_NAME, [fix_prompt, source_for_gemini]) if is_docx else _retry_generate(client, MODEL_NAME, [source_for_gemini, fix_prompt])
                
                i_tok = getattr(fix_resp.usage_metadata, 'prompt_token_count', 0)
                o_tok = getattr(fix_resp.usage_metadata, 'candidates_token_count', 0)
                cost = (i_tok / 1_000_000 * PRICE_1M_IN) + (o_tok / 1_000_000 * PRICE_1M_OUT)
                cbs['billing'](i_tok, o_tok, cost)
                
                res_text = getattr(fix_resp, 'text', '') or ''
                try:
                    fixed_data = _parse_llm_json_payload(res_text)
                except Exception as parse_err:
                    bad_path = _save_bad_llm_payload(folders, 'autofix_invalid_json', cand_name, res_text, {
                        'candidate': cand_name, 'reason': 'invalid_json', 'stage': 'batch_autofix', 'error': str(parse_err)
                    })
                    cbs['log'](f"   ❌ Fix failed for {cand_name}: invalid JSON from model. Saved raw response to {bad_path}", 'red')
                    item['_status'] = None
                    item['selected'] = False
                    cbs['render']()
                    continue
                fixed_data = sanitize_json(fixed_data)

                # Mark fix result, but DO NOT drop any original information.
                base_m = lossless_metrics(item['data'])

                fixed_data['qa_audit'] = {
                    "score": 100,
                    "missing": [],
                    "hallucinations": [],
                    "status": f"Batch Auto-Fixed on {time.strftime('%Y-%m-%d')}"
                }

                safe_data = safe_apply_autofix(item['data'], fixed_data)
                safe_data = sanitize_json(safe_data)

                # Lossless safety gate: never allow auto-fix to reduce overall information mass
                new_m = lossless_metrics(safe_data)
                # allow tiny reductions due to whitespace/control-char cleanup
                min_chars = int(base_m["char_count"] * 0.985)
                min_strs = max(0, base_m["str_count"] - 1)
                if (new_m["str_count"] < min_strs) or (new_m["char_count"] < min_chars):
                    bad_path = _save_bad_llm_payload(folders, 'autofix_lossless_gate', cand_name, res_text, {
                        'candidate': cand_name, 'reason': 'lossless_gate', 'stage': 'batch_autofix', 'before': base_m, 'after': new_m
                    })
                    cbs['log'](f"   ⚠️ Auto-Fix rejected for {cand_name}: lossless gate (before strs/chars={base_m['str_count']}/{base_m['char_count']}, after={new_m['str_count']}/{new_m['char_count']}). Saved details to {bad_path}", "red")
                    # record rejection in qa_audit without changing data
                    rej = {
                        "score": score if isinstance(score, int) else -1,
                        "status": "Auto-fix rejected by lossless gate",
                        "lossless_gate": {"before": base_m, "after": new_m},
                    }
                    update_qa_audit_lossless(item['data'], rej)
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(item['data'], f, indent=2, ensure_ascii=False)
                    item['_status'] = None
                    item['selected'] = False
                    cbs['render']()
                    continue


                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(safe_data, f, indent=2, ensure_ascii=False)

                item['data'] = safe_data
                item['ts'] = os.path.getmtime(json_path) 
                cbs['log'](f"   ✅ Auto-Fixed: {cand_name} (Cost: ${cost:.4f})", "green")
                
            except Exception as e:
                bad_path = _save_bad_llm_payload(folders, 'autofix_exception', cand_name, locals().get('res_text', ''), {
                    'candidate': cand_name, 'reason': 'exception', 'stage': 'batch_autofix', 'error': str(e)
                })
                cbs['log'](f"   ❌ Fix failed for {cand_name}: {str(e)} | Saved details to {bad_path}", "red")
                if check_api_error(str(e), cbs): break
            
            item['_status'] = None
            item['selected'] = False
            cbs['render']()
            
        for item in db_files: item['selected'] = False
        cbs['snack']("Batch Auto-Fix completed!")

    finally:
        cbs['progress'](0, "", False)
        cbs['render']()

# ==========================================
# 4. IMPORT TASK
# ==========================================
def run_import_task(files_paths, config, folders, task_state, db_files, cbs):
    try:
        total_items = len(files_paths)
        cbs['progress'](0, "Calculating ETA...", True)
        cbs['log'](f"Selected {total_items} files for import.", "blue")
        
        api_key = config.get("api_key", "")
        json_pref = config.get("json_naming_template", "CV_FirstName_LastName.json")
        import_mode = config.get("import_mode", "fix")
        
        existing_hashes = {item['data'].get('_source_hash') for item in db_files if item['data'].get('_source_hash')}
        start_time = time.time()
        
        skipped_fast = 0  # duplicate-content skips (fast)
        processed_slow = 0  # files that actually went through model (slow)
        for idx, path in enumerate(files_paths):
            if task_state.get("cancel"):
                cbs['log']("⏹️ Import queue aborted by user.", "orange")
                break
            
            filename = os.path.basename(path)
            current_idx = idx + 1
            eta_str = calc_eta_excluding_skips(start_time, processed_slow, total_items, skipped_fast)
            
            cbs['progress'](idx / total_items, f"Importing {current_idx}/{total_items}: {filename}{eta_str}", True)

            try:
                with open(path, 'rb') as afile: file_hash = hashlib.md5(afile.read()).hexdigest()
            except Exception as e:
                cbs['log'](f"   ❌ Could not read file {filename}: {e}", "red")
                continue
            
            if file_hash in existing_hashes:
                cbs['log'](f"   ⏩ Skipped (Duplicate content): {filename}", "orange")
                skipped_fast += 1
                continue
            
            base_name, ext = os.path.splitext(filename)
            cbs['log'](f"   Processing: {filename}...", "default")
            
            try:
                custom_inst = config.get("prompt_master_inst", DEFAULT_PROMPTS["prompt_master_inst"])
                result = process_file_gemini(path, api_key, custom_inst, task_state)
                processed_slow += 1
                
                if result and result[0]:
                    data, in_tok, out_tok, base_cost = result
                    total_cost = base_cost
                    cbs['billing'](in_tok, out_tok, base_cost)

                    # --- 🟢 AUTO-DETECTOR LINKEDIN ---
                    links_dump = json.dumps(data.get('basics', {}).get('links', [])).lower()
                    if "linkedin.com" in links_dump or "linkedin" in filename.lower() or filename.lower().startswith("profile"):
                        data['_comment'] = "Source: LinkedIn"
                    # ---------------------------------
                    
                    new_name = str(data.get('basics', {}).get('name', '')).strip().lower()
                    new_role = str(data.get('basics', {}).get('current_title', '')).strip().lower()
                    
                    is_semantic_duplicate = False
                    if new_name and new_role:
                        for item in db_files:
                            db_name = str(item['data'].get('basics', {}).get('name', '')).strip().lower()
                            db_role = str(item['data'].get('basics', {}).get('current_title', '')).strip().lower()
                            if new_name == db_name and new_role == db_role:
                                is_semantic_duplicate = True; break
                    
                    if is_semantic_duplicate:
                        cbs['log'](f"   ⏩ Skipped (Semantic duplicate: {new_name}): {filename}", "orange")
                        continue
                    
                    if json_pref == "CV_FirstName_LastName.json":
                        full_name = data.get('basics', {}).get('name', '')
                        if full_name and full_name.lower() not in ['unknown', 'candidate', '']:
                            safe_name = re.sub(r'[^\w\s-]', '', full_name).strip()
                            parts = safe_name.split()
                            if len(parts) >= 2: out_json = f"CV_{parts[0]}_{parts[-1]}_{file_hash[:4]}.json"
                            elif len(parts) == 1: out_json = f"CV_{parts[0]}_{file_hash[:4]}.json"
                            else: out_json = f"{base_name}_{file_hash[:4]}.json"
                        else: out_json = f"{base_name}_{file_hash[:4]}.json"
                    else: out_json = f"{base_name}_{file_hash[:4]}.json"

                    new_source_filename = out_json.replace('.json', ext)

                    dest_path = os.path.join(folders["SOURCE"], new_source_filename)
                    try: shutil.copy2(path, dest_path)
                    except: pass

                    data['import_date'] = time.time()
                    data['_source_filename'] = new_source_filename
                    data['_source_hash'] = file_hash
                    target_json_path = os.path.join(folders["JSON"], out_json)

                    if import_mode in ["qa", "fix"]:
                        cbs['log'](f"   🔬 [Auto-QA] Analyzing extraction...", "blue")
                        try:
                            clean_data = copy.deepcopy(data)
                            for k in ['match_analysis', '_source_filename', '_source_hash', 'import_date', 'qa_audit']: clean_data.pop(k, None)
                            
                            prompt_audit = config.get("prompt_qa", DEFAULT_PROMPTS["prompt_qa"]).replace("{json_str}", json.dumps(clean_data, ensure_ascii=False))
                            client = genai.Client(api_key=api_key)

                            if path.lower().endswith('.docx'):
                                text_doc = extract_text_from_docx(path)
                                resp_qa = _retry_generate(client, MODEL_NAME, [prompt_audit, text_doc])
                            else:
                                mime = 'application/pdf' if path.lower().endswith('.pdf') else 'image/jpeg'
                                import unicodedata as _ud
                                _safe_name = _ud.normalize('NFKD', os.path.basename(path)).encode('ascii', 'ignore').decode('ascii') or "cv_file"
                                sample = client.files.upload(file=path, config=genai_types.UploadFileConfig(mime_type=mime, display_name=_safe_name))
                                while sample.state.name == "PROCESSING":
                                    if task_state.get("cancel"): break
                                    time.sleep(1)
                                    sample = client.files.get(name=sample.name)
                                if task_state.get("cancel"): break
                                resp_qa = _retry_generate(client, MODEL_NAME, [sample, prompt_audit])

                            i_tok = getattr(resp_qa.usage_metadata, 'prompt_token_count', 0)
                            o_tok = getattr(resp_qa.usage_metadata, 'candidates_token_count', 0)
                            qa_cost = (i_tok / 1_000_000 * PRICE_1M_IN) + (o_tok / 1_000_000 * PRICE_1M_OUT)
                            total_cost += qa_cost
                            cbs['billing'](i_tok, o_tok, qa_cost)

                            qa_text = resp_qa.text
                            match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', qa_text, re.DOTALL)
                            if not match: match = re.search(r'(\{[\s\S]*?"score"[\s\S]*?\})', qa_text)
                            
                            if match:
                                qa_result = json.loads(match.group(1))
                                score = qa_result.get('score', 100)
                                update_qa_audit_lossless(data, qa_result)
                                
                                fix_threshold = int(config.get("autofix_threshold", 90))
                                if score < fix_threshold and import_mode == "fix":
                                    cbs['log'](f"   ⚠️ [Auto-QA] Score {score}/100 (threshold {fix_threshold}). Running Auto-Fix...", "orange")
                                    prompt_fix = config.get("prompt_autofix", DEFAULT_PROMPTS["prompt_autofix"]).replace("{current_json_str}", json.dumps(data, ensure_ascii=False)).replace("{qa_report_text}", qa_text)

                                    fix_resp = _retry_generate(client, MODEL_NAME, [prompt_fix, text_doc]) if path.lower().endswith('.docx') else _retry_generate(client, MODEL_NAME, [sample, prompt_fix])

                                    i_tok = getattr(fix_resp.usage_metadata, 'prompt_token_count', 0)
                                    o_tok = getattr(fix_resp.usage_metadata, 'candidates_token_count', 0)
                                    fix_cost = (i_tok / 1_000_000 * PRICE_1M_IN) + (o_tok / 1_000_000 * PRICE_1M_OUT)
                                    total_cost += fix_cost
                                    cbs['billing'](i_tok, o_tok, fix_cost)
                                    
                                    res_text = getattr(fix_resp, 'text', '') or ''
                                    try:
                                        fixed_data = _parse_llm_json_payload(res_text)
                                    except Exception as parse_err:
                                        bad_path = _save_bad_llm_payload(folders, 'autofix_import_invalid_json', filename, res_text, {
                                            'candidate': filename, 'reason': 'invalid_json', 'stage': 'import_autofix', 'error': str(parse_err)
                                        })
                                        cbs['log'](f"   ❌ Auto-Fix failed during import: invalid JSON from model. Saved raw response to {bad_path}", 'red')
                                        raise
                                    fixed_data = sanitize_json(fixed_data)

                                    # Re-QA the fixed data to verify it actually improved
                                    try:
                                        cbs['log'](f"   🔬 [Re-QA] Verifying fix quality...", "blue")
                                        clean_fixed = copy.deepcopy(fixed_data)
                                        for k in ['match_analysis', '_source_filename', '_source_hash', 'import_date', 'qa_audit']:
                                            clean_fixed.pop(k, None)
                                        prompt_reqa = config.get("prompt_qa", DEFAULT_PROMPTS["prompt_qa"]).replace("{json_str}", json.dumps(clean_fixed, ensure_ascii=False))
                                        resp_reqa = _retry_generate(client, MODEL_NAME, [prompt_reqa, text_doc]) if path.lower().endswith('.docx') else _retry_generate(client, MODEL_NAME, [sample, prompt_reqa])
                                        i_tok = getattr(resp_reqa.usage_metadata, 'prompt_token_count', 0)
                                        o_tok = getattr(resp_reqa.usage_metadata, 'candidates_token_count', 0)
                                        total_cost += (i_tok / 1_000_000 * PRICE_1M_IN) + (o_tok / 1_000_000 * PRICE_1M_OUT)
                                        cbs['billing'](i_tok, o_tok, (i_tok / 1_000_000 * PRICE_1M_IN) + (o_tok / 1_000_000 * PRICE_1M_OUT))
                                        reqa_text = resp_reqa.text
                                        m_reqa = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', reqa_text, re.DOTALL)
                                        if not m_reqa: m_reqa = re.search(r'(\{[\s\S]*?"score"[\s\S]*?\})', reqa_text)
                                        if m_reqa:
                                            reqa_result = json.loads(m_reqa.group(1))
                                            new_score = reqa_result.get('score', 0)
                                            if new_score > score:
                                                fixed_data['qa_audit'] = reqa_result
                                                fixed_data['_source_filename'] = new_source_filename
                                                fixed_data['_source_hash'] = file_hash
                                                fixed_data['import_date'] = data['import_date']
                                                with open(target_json_path.replace('.json', '.bak'), 'w', encoding='utf-8') as f:
                                                    json.dump(data, f, indent=2, ensure_ascii=False)
                                                data = fixed_data
                                                cbs['log'](f"   ✨ Auto-Fix applied: {score} → {new_score}/100", "green")
                                            else:
                                                cbs['log'](f"   ↩️ Auto-Fix reverted: fix score {new_score} ≤ original {score}. Keeping original.", "orange")
                                        else:
                                            raise ValueError("Re-QA response parse failed")
                                    except Exception as reqa_err:
                                        # Re-QA failed — apply fix anyway (conservative fallback)
                                        fixed_data['qa_audit'] = {"score": score, "missing": [], "hallucinations": [], "status": f"Auto-Fixed on import ({time.strftime('%Y-%m-%d')}), re-QA unavailable"}
                                        fixed_data['_source_filename'] = new_source_filename
                                        fixed_data['_source_hash'] = file_hash
                                        fixed_data['import_date'] = data['import_date']
                                        with open(target_json_path.replace('.json', '.bak'), 'w', encoding='utf-8') as f:
                                            json.dump(data, f, indent=2, ensure_ascii=False)
                                        data = fixed_data
                                        cbs['log'](f"   ✨ Auto-Fix applied (re-QA unavailable: {reqa_err}).", "green")
                                elif score < fix_threshold and import_mode == "qa":
                                    cbs['log'](f"   ⚠️ [Auto-QA] Score {score}/100 (below threshold {fix_threshold}). Auto-fix is disabled in settings.", "orange")
                                elif score < 100:
                                    cbs['log'](f"   ✅ [Auto-QA] Score {score}/100 (above threshold {fix_threshold}). No fix needed.", "green")
                                else:
                                    cbs['log'](f"   ✅ [Auto-QA] Perfect extraction (100/100).", "green")
                        except Exception as e:
                            cbs['log'](f"   ❌ Auto-QA/Fix failed during import: {e}", "red")

                    with open(target_json_path, 'w', encoding='utf-8') as jf:
                        json.dump(data, jf, indent=2, ensure_ascii=False)

                    generated_docx = None
                    if config.get('generate_docx_on_import', False):
                        try:
                            out_docx = get_target_filename({'file': out_json, 'data': data}, config, '.docx')
                            target_docx_path = os.path.join(folders['OUTPUT'], out_docx)
                            generate_docx_from_json(data, target_docx_path, config)
                            generated_docx = out_docx
                            cbs['log'](f"   📄 Generated DOCX: {out_docx}", 'blue')
                        except Exception as docx_ex:
                            cbs['log'](f"   ⚠️ DOCX generation failed: {docx_ex}", 'orange')

                    existing_hashes.add(file_hash)
                    db_files.append({'file': out_json, 'data': data, 'ts': os.path.getmtime(target_json_path), 'selected': False})

                    if generated_docx:
                        cbs['log'](f"   ✅ Saved: {out_json} + {generated_docx} (Total Cost: ${total_cost:.4f})", "green")
                    else:
                        cbs['log'](f"   ✅ Saved: {out_json} (Total Cost: ${total_cost:.4f})", "green")
                    cbs['render']()
                    
                else: 
                    cbs['log'](f"   ❌ Failed to process: {filename}", "red")
            
            except Exception as ex: 
                err = str(ex)
                try:
                    if locals().get('res_text'):
                        bad_path = _save_bad_llm_payload(folders, 'import_exception', filename, locals().get('res_text', ''), {'candidate': filename, 'reason': 'exception', 'stage': 'import', 'error': err})
                        cbs['log'](f"   Error: {err} | Saved details to {bad_path}", "red")
                    else:
                        cbs['log'](f"   Error: {err}", "red")
                except Exception:
                    cbs['log'](f"   Error: {err}", "red")
                if check_api_error(err, cbs): break

        cbs['log']("Import process finished.", "blue")
        cbs['snack']("Import finished.", "Open JSONs", folders["JSON"])
    finally:
        cbs['progress'](0, "", False)
        cbs['render']()

# ==========================================
# 5. MATCHER TASK
# ==========================================
def run_matcher_task(cands, jd_val, config, folders, task_state, cbs, on_complete_cb):
    try:
        cbs['progress'](0, "Calculating ETA...", True)
        cbs['log'](f"Running Matcher on {len(cands)} CVs...", "blue")
        
        parsed_all = []
        client = genai.Client(api_key=config.get("api_key", ""))
        current_jd_hash = hashlib.md5(jd_val.strip().lower().encode('utf-8')).hexdigest()
        
        start_time = time.time()
        api_total_time = 0.0
        analyzed_count = 0
        session_cost = 0.0

        for i, cand in enumerate(cands):
            if task_state.get("cancel"):
                cbs['log']("⏹️ AI Analysis aborted by user. Saving partial results...", "orange")
                break

            cand_name = str(cand['data']['basics'].get('name', 'Unknown')).replace('\n', ' ').strip()
            
            if analyzed_count > 0:
                avg_api_time = api_total_time / analyzed_count
                rem_files = len(cands) - i
                eta_secs = int(avg_api_time * rem_files)
                eta_str = f" | ETA: ~{eta_secs // 60}m {eta_secs % 60}s" if eta_secs > 60 else f" | ETA: ~{eta_secs}s"
            else:
                eta_str = " | ETA: Calculating..."
                
            cbs['progress'](i / len(cands), f"Analyzing {i+1}/{len(cands)}: {cand_name}{eta_str}", True)
            
            cached_analysis = cand['data'].get('match_analysis', {})
            if cached_analysis.get('jd_hash') == current_jd_hash:
                cbs['log'](f"   ⏩ Cached: {cand_name} (Free)", "orange")
                cached_item = copy.deepcopy(cached_analysis)
                cached_item['id'] = i
                cached_item['name'] = cand_name
                parsed_all.append(cached_item)
                continue
            
            api_start_time = time.time()
            cand_data = {
                "id": i, "name": cand_name, "title": cand['data']['basics'].get('current_title', ''), 
                "skills": cand['data'].get('skills', {}), "summary": cand['data'].get('summary', {}),
                "experience": cand['data'].get('experience', []), "education": cand['data'].get('education', [])
            }
            
            prompt = config.get("prompt_matcher", DEFAULT_PROMPTS["prompt_matcher"]).replace("{jd_val}", jd_val[:3000]).replace("{cand_data}", json.dumps(cand_data, ensure_ascii=False)).replace("{i}", str(i))
            
            try:
                resp = _retry_generate(client, MODEL_NAME, prompt)
                in_tok = getattr(resp.usage_metadata, 'prompt_token_count', 0)
                out_tok = getattr(resp.usage_metadata, 'candidates_token_count', 0)
                cost = (in_tok / 1_000_000 * PRICE_1M_IN) + (out_tok / 1_000_000 * PRICE_1M_OUT)
                session_cost += cost
                cbs['billing'](in_tok, out_tok, cost)
                
                cbs['log'](f"   ✅ Analyzed: {cand_name} (Cost: ${cost:.4f})", "default")
                
                txt = resp.text.strip().replace('```json', '').replace('```', '').strip()
                parsed_item = json.loads(txt)
                if isinstance(parsed_item, list) and len(parsed_item) > 0: parsed_item = parsed_item[0]
                
                if isinstance(parsed_item, dict):
                    parsed_item['jd_hash'] = current_jd_hash
                    cand['data']['match_analysis'] = parsed_item
                    parsed_all.append(parsed_item)
                    
                    try:
                        with open(os.path.join(folders["JSON"], cand['file']), 'w', encoding='utf-8') as jf:
                            json.dump(cand['data'], jf, indent=2, ensure_ascii=False)
                    except Exception as e:
                        cbs['log'](f"Could not save cache to {cand['file']}: {e}", "orange")

            except Exception as ex: 
                err = str(ex)
                cbs['log'](f"Error analyzing {cand_name}: {err}", "red")
                if check_api_error(err, cbs): break
                    
            api_time = time.time() - api_start_time
            api_total_time += api_time
            analyzed_count += 1

        try:
            if not parsed_all: return

            clean_parsed_all = []
            for p in parsed_all:
                if not isinstance(p, dict): continue
                try: p['score'] = int(re.sub(r'[^\d]', '', str(p.get('score', 0))))
                except: p['score'] = 0
                clean_parsed_all.append(p)
                
            parsed_all = sorted(clean_parsed_all, key=lambda x: x['score'], reverse=True)
            
            fname = f"Ranking_Report_{time.strftime('%Y%m%d_%H%M')}.csv"
            fpath = os.path.join(folders["REPORTS"], fname)
            
            with open(fpath, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['Score', 'Name', 'Verdict', 'Pros', 'Missing Skills', 'Filename']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for p in parsed_all:
                    try: c_idx = int(p.get('id', -1))
                    except: c_idx = -1
                    fname_orig = cands[c_idx]['file'] if (0 <= c_idx < len(cands)) else "Unknown"
                    writer.writerow({'Score': p.get('score'), 'Name': p.get('name'), 'Verdict': p.get('verdict'), 'Pros': p.get('pros'), 'Missing Skills': p.get('missing_skills'), 'Filename': fname_orig})
            
            cbs['log'](f"✅ Saved: {fname} (Analysis Cost: ${session_cost:.4f})", "green")
            cbs['snack']("AI Analysis complete!", "Open Reports", folders["REPORTS"])
            
            # Pass results to UI for table rendering
            on_complete_cb(parsed_all, cands)
            
        except Exception as ex: 
            cbs['log'](f"Error rendering results: {ex}", "red")
            cbs['snack']("Error displaying AI Analysis. Check logs.")
    finally:
        cbs['progress'](0, "", False)
        cbs['render']()

# ==========================================
# 6. BATCH QA AUDIT
# ==========================================

def run_batch_qa_task(valid_candidates, sample_count, config, folders, task_state, cbs, on_complete_cb):
    try:
        cbs['progress'](0, "Calculating ETA...", True)
        client = genai.Client(api_key=config.get("api_key", ""))

        aggregated_reports = []
        rows = []
        qa_cb = cbs.get('qa_row')
        start_time = time.time()
        mode = config.get("qa_compare_mode", "input_json")

        def emit_row(row):
            if not qa_cb:
                return
            try:
                qa_cb(row)
            except Exception as ex:
                try:
                    cbs['log'](f"QA row callback failed: {ex}", "orange")
                except Exception:
                    pass

        def summarize_comments(qa_result: dict) -> str:
            issues = []
            missing = qa_result.get('missing_data') or []
            hallucinations = qa_result.get('hallucinations') or []
            if isinstance(missing, list) and missing:
                issues.append("Missing: " + ", ".join(str(x) for x in missing[:2]))
            if isinstance(hallucinations, list) and hallucinations:
                issues.append("Hallucinations: " + ", ".join(str(x) for x in hallucinations[:2]))
            if not issues:
                overall = qa_result.get('overall_assessment') or qa_result.get('comments') or ""
                return str(overall)[:160]
            return " | ".join(issues)[:220]

        for i, cand in enumerate(valid_candidates):
            if task_state.get("cancel"):
                cbs['log']("⏹️ Batch QA aborted by user.", "orange")
                break

            cand_name = cand['item']['data'].get('basics', {}).get('name', f'Candidate {i+1}')
            src_path = cand['src_path']
            eta_str = calc_eta(start_time, i, sample_count)
            cbs['progress'](i / sample_count, f"Micro-Audit {i+1}/{sample_count}: {cand_name}{eta_str}", True)

            display_file = os.path.basename(src_path)
            running_row = {
                "input_file": display_file,
                "quality": None,
                "input_json": None,
                "json_docx": None,
                "total_quality": None,
                "comments": "Running...",
            }
            emit_row(running_row)

            clean_data = copy.deepcopy(cand['item']['data'])
            for k in ['match_analysis', '_source_filename', '_source_hash', 'import_date', 'qa_audit']:
                clean_data.pop(k, None)

            score_ij = None
            score_jd = None
            comments = "Completed"

            # --- Stage 1: Input → JSON ---
            if mode in ["input_json", "full_pipeline"]:
                try:
                    prompt = config.get("prompt_qa", DEFAULT_PROMPTS["prompt_qa"]).replace("{json_str}", json.dumps(clean_data, ensure_ascii=False))
                    if src_path.lower().endswith('.docx'):
                        resp_qa = _retry_generate(client, MODEL_NAME, [prompt, extract_text_from_docx(src_path)])
                    else:
                        mime = 'application/pdf' if src_path.lower().endswith('.pdf') else 'image/jpeg'
                        import unicodedata as _ud
                        _safe_name = _ud.normalize('NFKD', os.path.basename(src_path)).encode('ascii', 'ignore').decode('ascii') or "cv_file"
                        sample = client.files.upload(file=src_path, config=genai_types.UploadFileConfig(mime_type=mime, display_name=_safe_name))
                        while sample.state.name == "PROCESSING":
                            if task_state.get("cancel"):
                                break
                            time.sleep(1)
                            sample = client.files.get(name=sample.name)
                        if task_state.get("cancel"):
                            return
                        resp_qa = _retry_generate(client, MODEL_NAME, [sample, prompt])

                    i_tok = getattr(resp_qa.usage_metadata, 'prompt_token_count', 0)
                    o_tok = getattr(resp_qa.usage_metadata, 'candidates_token_count', 0)
                    cbs['billing'](i_tok, o_tok, (i_tok / 1_000_000 * PRICE_1M_IN) + (o_tok / 1_000_000 * PRICE_1M_OUT))

                    res_text = resp_qa.text
                    aggregated_reports.append(f"--- Report for {cand_name} ---\n{res_text}\n")

                    qa_result = None
                    match = re.search(r'```json\s*(\{.*?\})\s*```', res_text, re.DOTALL)
                    if not match:
                        match = re.search(r'(\{[\s\S]*?"score"[\s\S]*?\})', res_text)
                    if match:
                        try:
                            qa_result = json.loads(match.group(1))
                            update_qa_audit_lossless(cand['item']['data'], qa_result)
                            with open(os.path.join(folders["JSON"], cand['item']['file']), 'w', encoding='utf-8') as f:
                                json.dump(cand['item']['data'], f, indent=2, ensure_ascii=False)
                        except Exception:
                            cbs['log'](f"Failed to parse JSON score for {cand_name}", "orange")

                    if isinstance(qa_result, dict):
                        score_ij = qa_result.get('score')
                        comments = summarize_comments(qa_result)

                except Exception as ex:
                    cbs['log'](f"   ❌ Stage 1 (In→JSON) failed for {cand_name}: {ex}", "red")
                    comments = f"Stage 1 error: {ex}"

            # --- Stage 2: JSON → DOCX ---
            if mode in ["json_docx", "full_pipeline"]:
                try:
                    docx_name = get_target_filename(cand['item'], config, ".docx")
                    docx_path = os.path.join(folders["OUTPUT"], docx_name)
                    if os.path.exists(docx_path):
                        docx_text = extract_text_from_docx(docx_path)
                        prompt_jd = config.get("prompt_qa_docx", DEFAULT_PROMPTS["prompt_qa_docx"]).replace("{json_str}", json.dumps(clean_data, ensure_ascii=False))
                        resp_jd = _retry_generate(client, MODEL_NAME, [prompt_jd, docx_text])

                        i_tok = getattr(resp_jd.usage_metadata, 'prompt_token_count', 0)
                        o_tok = getattr(resp_jd.usage_metadata, 'candidates_token_count', 0)
                        cbs['billing'](i_tok, o_tok, (i_tok / 1_000_000 * PRICE_1M_IN) + (o_tok / 1_000_000 * PRICE_1M_OUT))

                        res_text_jd = resp_jd.text
                        aggregated_reports.append(f"--- DOCX Report for {cand_name} ---\n{res_text_jd}\n")

                        jd_result = None
                        match_jd = re.search(r'```json\s*(\{.*?\})\s*```', res_text_jd, re.DOTALL)
                        if not match_jd:
                            match_jd = re.search(r'(\{[\s\S]*?"score"[\s\S]*?\})', res_text_jd)
                        if match_jd:
                            try:
                                jd_result = json.loads(match_jd.group(1))
                            except Exception:
                                cbs['log'](f"Failed to parse DOCX QA score for {cand_name}", "orange")

                        if isinstance(jd_result, dict):
                            score_jd = jd_result.get('score')
                            jd_comments = summarize_comments(jd_result)
                            if mode == "json_docx":
                                comments = jd_comments
                            elif jd_comments and jd_comments != "Completed":
                                comments = (comments.rstrip(" |") + " | " + jd_comments).strip(" |")
                    else:
                        cbs['log'](f"   ⚠️ DOCX not found for {cand_name}: {docx_name}", "orange")
                        if mode == "json_docx":
                            comments = "DOCX not found — generate DOCX first"

                except Exception as ex:
                    cbs['log'](f"   ❌ Stage 2 (JSON→DOCX) failed for {cand_name}: {ex}", "red")
                    if mode == "json_docx":
                        comments = f"Stage 2 error: {ex}"

            # Build final row
            scores_available = [s for s in [score_ij, score_jd] if s is not None]
            total_quality = int(round(sum(scores_available) / len(scores_available))) if scores_available else None
            if mode == "input_json":
                quality = score_ij
            elif mode == "json_docx":
                quality = score_jd
            else:
                quality = total_quality

            final_row = {
                "input_file": display_file,
                "quality": quality,
                "input_json": score_ij if mode == "full_pipeline" else None,
                "json_docx": score_jd if mode in ["json_docx", "full_pipeline"] else None,
                "total_quality": total_quality if mode == "full_pipeline" else None,
                "comments": comments,
            }
            rows.append(final_row)
            emit_row(final_row)

            cbs['log'](f"   🔬 Audited: {cand_name}", "blue")
            cbs['render']()

        if task_state.get("cancel") or not aggregated_reports:
            return

        cbs['progress'](0.95, "Analyzing micro-reports for systemic patterns...", True)

        macro_prompt = f"""You are a Lead Data Scientist & AI Prompt Engineer.
        We are optimizing our CV extraction engine. Here are micro-audit reports for {len(aggregated_reports)} candidates:
        {''.join(aggregated_reports)}

        YOUR TASK: Analyze patterns. Do NOT output individual reports. Provide Markdown summary:
        1. 🔴 **Systemic Data Losses**
        2. 🟡 **Frequent Hallucinations**
        3. 💡 **Prompt Engineering Recommendations**
        """
        try:
            final_resp = _retry_generate(client, MODEL_NAME, macro_prompt)
            i_tok = getattr(final_resp.usage_metadata, 'prompt_token_count', 0)
            o_tok = getattr(final_resp.usage_metadata, 'candidates_token_count', 0)
            cbs['billing'](i_tok, o_tok, (i_tok / 1_000_000 * PRICE_1M_IN) + (o_tok / 1_000_000 * PRICE_1M_OUT))

            report_fname = f"QA_Audit_Report_{time.strftime('%Y%m%d_%H%M')}.md"
            with open(os.path.join(folders["REPORTS"], report_fname), 'w', encoding='utf-8') as f:
                f.write(final_resp.text)

            cbs['log'](f"✅ Batch QA Analytics Complete! Saved to {report_fname}", "green")
            cbs['snack']("Batch QA Analysis Complete!", "Open Reports", folders["REPORTS"])
            on_complete_cb({"mode": mode, "summary_md": final_resp.text, "rows": rows})
        except Exception as ex:
            cbs['log'](f"Error during Macro-Analysis: {ex}", "red")
            on_complete_cb({"mode": mode, "summary_md": f"**Error generating macro-analysis:** {ex}", "rows": rows})

    finally:
        cbs['progress'](0, "", False)
        cbs['render']()

# ==========================================
# 7. MODIFY CV TASK
# ==========================================
def run_modify_task(items, user_req, config, folders, task_state, db_files, cbs):
    try:
        total_items = len(items)
        cbs['progress'](0, "Calculating ETA...", True)
        cbs['log'](f"Modifying {total_items} CVs...", "blue")

        client = genai.Client(api_key=config.get("api_key", ""))
        start_time = time.time()
        session_cost = 0.0

        for i, item in enumerate(items):
            if task_state.get("cancel"):
                cbs['log']("⏹️ AI Modification aborted by user.", "orange")
                break
                
            item['_status'] = 'processing'
            cbs['render']()

            current_idx = i + 1
            cand_name = str(item['data']['basics'].get('name', 'Unknown')).replace('\n', ' ').strip()
            eta_str = calc_eta(start_time, i, total_items)
            
            cbs['progress'](i / total_items, f"Modifying {current_idx}/{total_items}: {cand_name}{eta_str}", True)
            
            input_json_str = json.dumps(item['data'], ensure_ascii=False)
            prompt = config.get("prompt_modifier", DEFAULT_PROMPTS["prompt_modifier"]).replace("{user_req}", user_req).replace("{input_json_str}", input_json_str)
            
            try:
                resp = _retry_generate(client, MODEL_NAME, prompt)
                i_tok = getattr(resp.usage_metadata, 'prompt_token_count', 0)
                o_tok = getattr(resp.usage_metadata, 'candidates_token_count', 0)
                cost = (i_tok / 1_000_000 * PRICE_1M_IN) + (o_tok / 1_000_000 * PRICE_1M_OUT)
                session_cost += cost
                cbs['billing'](i_tok, o_tok, cost)
                
                txt = resp.text.strip().replace('```json', '').replace('```', '').strip()
                mod_data = sanitize_json(json.loads(txt)) 
                
                out_orig = get_target_filename(item, config, ".docx")
                base_name, ext = os.path.splitext(out_orig)
                out_mod_filename = f"{base_name}_modified{ext}"
                target_path = os.path.join(folders["MODIFIED"], out_mod_filename)
                
                generate_docx_from_json(mod_data, target_path, config)
                cbs['log'](f"   ✅ Modified & Saved: {out_mod_filename} (Cost: ${cost:.4f})", "green")
                
            except Exception as ex: 
                err = str(ex)
                cbs['log'](f"Error modifying {cand_name}: {err}", "red")
                if check_api_error(err, cbs): break
            
            item['_status'] = None
            item['selected'] = False
            cbs['render']()
        
        for item in db_files: item['selected'] = False
        if session_cost > 0:
            cbs['log'](f"Modification Session Complete (Cost: ${session_cost:.4f})", "blue")
            cbs['snack']("CV Modification complete!", "Open Folder", folders["MODIFIED"])
    finally:
        cbs['progress'](0, "", False)
        cbs['render']()

# ==========================================
# 8. GITHUB MINER TASK
# ==========================================
def gh_api_request(endpoint, token, cbs):
    try:
        # MARKDOWN GUARD (break the string)
        base_gh_url = "https" + "://" + "api.github.com"
        req = urllib.request.Request(f"{base_gh_url}{endpoint}")
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "application/vnd.github.v3+json")
        
        proxy_handler = urllib.request.ProxyHandler({})
        opener = urllib.request.build_opener(proxy_handler)
        
        with opener.open(req) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        cbs['log'](f"GitHub API Error: {e.code} on {endpoint}", "red")
        return None
    except Exception as e:
        cbs['log'](f"Network Error: {e}", "red")
        return None

def run_mine_github_task(keywords, location, min_stars, config, task_state, cbs, on_card_generated):
    try:
        cbs['progress'](0, "Searching top repositories...", True)
        token = config.get("github_token")
        cbs['log'](f"Mining GitHub for '{keywords}'...", "purple")
        
        query = urllib.parse.quote(keywords)
        stars_query = f"stars:>={min_stars}"
        
        repos_data = gh_api_request(f"/search/repositories?q={query}+{stars_query}&sort=stars&order=desc&per_page=10", token, cbs)
        
        if not repos_data or 'items' not in repos_data or len(repos_data['items']) == 0:
            cbs['log']("No repositories found matching criteria.", "orange")
            return
            
        repos = repos_data['items']
        cbs['log'](f"Found {len(repos)} repositories. Extracting contributors...", "blue")
        
        unique_users = {}
        for i, repo in enumerate(repos):
            if task_state.get("cancel"): break
            repo_name = repo['full_name']
            cbs['progress']((i / len(repos)) * 0.5, f"Scanning repo {i+1}/{len(repos)}: {repo_name}", True)
            
            contribs = gh_api_request(f"/repos/{repo_name}/contributors?per_page=5", token, cbs)
            if not contribs: continue
            
            for c in contribs:
                login = c.get('login')
                if login and login not in unique_users and '[bot]' not in login:
                    unique_users[login] = {'repo': repo_name, 'contributions': c.get('contributions')}
                    
        users_list = list(unique_users.items())
        total_users = len(users_list)
        cbs['log'](f"Found {total_users} unique contributors. Filtering profiles...", "blue")
        
        found_count = 0
        
        for j, (login, meta) in enumerate(users_list):
            if task_state.get("cancel"): break
            cbs['progress'](0.5 + ((j / total_users) * 0.5), f"Analyzing user {j+1}/{total_users}: {login}", True)
            
            user_profile = gh_api_request(f"/users/{login}", token, cbs)
            if not user_profile: continue
            
            user_loc = user_profile.get('location') or ""
            if location and location.lower() not in user_loc.lower(): continue
            
            found_count += 1
            
            name = user_profile.get('name') or login
            company = user_profile.get('company') or "Independent"
            email = user_profile.get('email') or "Hidden"
            bio = user_profile.get('bio') or "No bio provided."
            html_url = user_profile.get('html_url')
            
            # Pass data to UI for card rendering
            on_card_generated(login, name, user_loc, company, email, bio, meta, html_url)
            
        cbs['log'](f"✅ Mining complete! Found {found_count} matching candidates.", "green")
        cbs['snack'](f"Found {found_count} matching developers!")
        
    finally:
        cbs['progress'](0, "", False)

# ==========================================
# 9. X-RAY TASK
# ==========================================
def run_xray_task(user_input, config, cbs, on_card_generated):
    try:
        client = genai.Client(api_key=config.get("api_key", ""))
        prompt = config.get("prompt_xray", DEFAULT_PROMPTS["prompt_xray"]).replace("{user_input}", user_input[:1000])

        resp = _retry_generate(client, MODEL_NAME, prompt)
        
        i_tok = getattr(resp.usage_metadata, 'prompt_token_count', 0)
        o_tok = getattr(resp.usage_metadata, 'candidates_token_count', 0)
        cost = (i_tok / 1_000_000 * PRICE_1M_IN) + (o_tok / 1_000_000 * PRICE_1M_OUT)
        cbs['billing'](i_tok, o_tok, cost)
        
        txt = resp.text.replace('```json', '').replace('```', '').strip()
        queries = json.loads(txt)
        
        for q in queries:
            platform_name = q.get('platform', 'Search')
            desc = q.get('description', '')
            query_str = q.get('query', '')
            # Pass to UI for card rendering
            on_card_generated(platform_name, desc, query_str)
        
        cbs['log'](f"Generated {len(queries)} X-Ray queries (Cost: ${cost:.4f})", "blue")
    except Exception as ex:
        cbs['log'](f"Error generating queries: {ex}", "red")
        cbs['snack']("Error generating queries. Check logs.")