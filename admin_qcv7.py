#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""QCV Admin (MVP v2)

Changes vs v1:
- Stores settings.json, company_map.json, backlog.json under ~/.qcv/ by default.
- Layout fixed: each tab scrolls; raw JSON editors are collapsed (expandable) to avoid overlap.
- Tables have a fixed height area; editors are in an ExpansionPanel.

Run:
  python3 admin_qcv.py
"""

import flet as ft
import json
import re
import datetime
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _now_iso() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d")


def _atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    tmp = Path(tmp_name)
    try:
        with open(fd, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
        tmp.replace(path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass


def _read_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


# ----------------------------
# Storage location
# ----------------------------
SETTINGS_PATH = Path.home() / ".quantoricv_settings.json"
COMPANY_MAP_PATH = Path.home() / ".quantoricv_company_map.json"
BACKLOG_PATH = Path.home() / ".quantoricv_backlog.json"
MASTER_PROMPTS_PATH = Path.home() / ".master_prompts.json"


DEFAULT_SETTINGS: Dict[str, Any] = {}
DEFAULT_COMPANY_MAP: Dict[str, Any] = {"meta": {"version": 1, "updated_at": _now_iso()}, "items": {}}
DEFAULT_BACKLOG: Dict[str, Any] = {"meta": {"version": 1, "updated_at": _now_iso()}, "items": []}
DEFAULT_MASTER_PROMPTS: Dict[str, Any] = {"meta": {"schema_version": 1, "updated_at": _now_iso()}, "prompts": []}


# ----------------------------
# Company dictionary helpers
# ----------------------------
def normalize_company_key(name: str) -> str:
    s = (name or "").strip().lower()
    if not s:
        return ""
    s = re.sub(r"[\"'“”«»()]+", " ", s)
    s = re.sub(
        r"\b(inc|llc|ltd|limited|corp|corporation|gmbh|ag|sa|s\.a\.|oy|ab|bv|plc|ооо|оао|зао|ао)\b",
        " ",
        s,
        flags=re.I,
    )
    s = re.sub(r"[\s,.;:/\\\-]+", " ", s).strip()
    return s


def ensure_company_entry(company_map: Dict[str, Any], raw_name: str) -> Tuple[str, Dict[str, Any]]:
    key = normalize_company_key(raw_name)
    if not key:
        return "", {}
    items = company_map.setdefault("items", {})
    ent = items.get(key)
    if not isinstance(ent, dict):
        ent = {"general": "", "raw_variants": [], "source": "none", "updated_at": _now_iso()}
        items[key] = ent
    rv = ent.setdefault("raw_variants", [])
    if isinstance(rv, list) and raw_name and raw_name not in rv:
        rv.append(raw_name)
    ent["updated_at"] = _now_iso()
    company_map.setdefault("meta", {})["updated_at"] = _now_iso()
    return key, ent


# ----------------------------
# Reusable UI blocks
# ----------------------------
class RawJsonPanel(ft.UserControl):
    def __init__(self, title: str, path: Path, default_data: Any):
        super().__init__()
        self.title = title
        self.path = path
        self.default_data = default_data
        self.data = _read_json(self.path, self.default_data)

        self.msg = ft.Text(value="", selectable=True)
        self.txt = ft.TextField(
            value=json.dumps(self.data, ensure_ascii=False, indent=2),
            multiline=True,
            min_lines=12,
            max_lines=18,
            expand=True,
            text_size=12,
        )

        self.panel = ft.ExpansionPanelList(
            expand=True,
            elevation=1,
            controls=[
                ft.ExpansionPanel(
                    header=ft.ListTile(title=ft.Text(title, weight=ft.FontWeight.BOLD)),
                    content=ft.Container(
                        padding=10,
                        content=ft.Column(
                            controls=[
                                ft.Row(
                                    controls=[
                                        ft.ElevatedButton("Reload", on_click=lambda e: self.reload()),
                                        ft.ElevatedButton("Save", on_click=lambda e: self.save()),
                                        ft.OutlinedButton("Reset", on_click=lambda e: self.reset()),
                                        ft.Container(expand=True),
                                        ft.Text(str(self.path), size=11, italic=True),
                                    ]
                                ),
                                self.msg,
                                ft.Container(height=320, content=self.txt, border=ft.border.all(1, ft.colors.OUTLINE)),
                            ],
                            tight=True,
                        ),
                    ),
                    expanded=False,
                )
            ],
        )

    def reload(self):
        self.data = _read_json(self.path, self.default_data)
        self.txt.value = json.dumps(self.data, ensure_ascii=False, indent=2)
        self.msg.value = f"Loaded from {self.path}"
        self.update()

    def save(self):
        try:
            parsed = json.loads(self.txt.value or "{}")
            _atomic_write_json(self.path, parsed)
            self.data = parsed
            self.msg.value = f"Saved to {self.path}"
        except Exception as e:
            self.msg.value = f"❌ JSON error: {e}"
        self.update()

    def reset(self):
        self.data = self.default_data
        self.txt.value = json.dumps(self.data, ensure_ascii=False, indent=2)
        self.msg.value = "Reset to defaults (not saved yet)"
        self.update()

    def build(self):
        return self.panel


def _section_card(title: str, subtitle: str = "", content=None):
    header = ft.Row(
        controls=[
            ft.Text(title, size=18, weight=ft.FontWeight.BOLD),
            ft.Container(expand=True),
            ft.Text(subtitle, size=11, italic=True) if subtitle else ft.Container(),
        ]
    )
    return ft.Card(
        content=ft.Container(
            padding=12,
            content=ft.Column(controls=[header, ft.Divider(height=1), content] if content else [header], tight=True),
        )
    )


# ----------------------------
# Tabs
# ----------------------------
class SettingsTab(ft.UserControl):
    def __init__(self):
        super().__init__()
        self.raw = RawJsonPanel("settings.json (raw)", SETTINGS_PATH, DEFAULT_SETTINGS)
        self.master_prompts = RawJsonPanel("master_prompts.json (raw)", MASTER_PROMPTS_PATH, DEFAULT_MASTER_PROMPTS)

    def build(self):
        return ft.Column(
            expand=True,
            scroll=ft.ScrollMode.AUTO,
            controls=[
                _section_card("Settings", str(SETTINGS_PATH), self.raw),
                _section_card("Master Prompts", str(MASTER_PROMPTS_PATH), self.master_prompts),
            ],
        )


class CompaniesTab(ft.UserControl):
    def __init__(self):
        super().__init__()
        self.company_map = _read_json(COMPANY_MAP_PATH, DEFAULT_COMPANY_MAP)
        self.msg = ft.Text(value="", selectable=False, size=12)
        self.sort_key = "key"
        self.sort_asc = True
        self.editing_key = None
        self.edit_is_new = False
        self._edit_original = None
        self.filter_txt = ft.TextField(
            hint_text="Search...",
            expand=True,
            dense=True,
            text_size=13,
            content_padding=ft.padding.symmetric(horizontal=10, vertical=8),
            on_change=lambda e: self._render(),
        )
        self.counter_txt = ft.Text("0 / 0", size=12, color=ft.colors.OUTLINE)
        self.header_container = ft.Container()
        self.rows_column = ft.Column(spacing=0, scroll=ft.ScrollMode.AUTO, expand=True)

        self.edit_raw = None
        self.edit_general = None
        self.edit_source = None
        self.edit_updated = None

    def _set_status(self, text: str):
        self.msg.value = text or ""
        if self.page:
            self.page.update()

    def _save(self):
        self.company_map.setdefault("meta", {})["updated_at"] = _now_iso()
        _atomic_write_json(COMPANY_MAP_PATH, self.company_map)

    def _reload(self):
        self.company_map = _read_json(COMPANY_MAP_PATH, DEFAULT_COMPANY_MAP)
        self.editing_key = None
        self.edit_is_new = False
        self._edit_original = None
        self._set_status("Reloaded company dictionary")
        self._render()

    def _items_dict(self):
        items = self.company_map.get("items", {})
        return items if isinstance(items, dict) else {}

    def _items(self):
        out = []
        for k, ent in self._items_dict().items():
            if isinstance(ent, dict):
                out.append((k, ent))
        return out

    def _filtered_items(self):
        q = (self.filter_txt.value or "").strip().lower()
        rows = []
        for key, ent in self._items():
            variants = ent.get("raw_variants") or []
            general = ent.get("general") or ""
            source = ent.get("source") or ""
            updated = ent.get("updated_at") or ""
            hay = f"{key} {general} {source} {updated} " + " ".join(str(x) for x in variants)
            if q and q not in hay.lower():
                continue
            rows.append((key, ent))
        return rows

    def _display_items(self):
        rows = self._filtered_items()
        key = self.sort_key
        if key == "key":
            sort_fn = lambda kv: kv[0].lower()
        elif key == "raw":
            sort_fn = lambda kv: (str((kv[1].get("raw_variants") or [""])[0]).lower())
        elif key == "general":
            sort_fn = lambda kv: str(kv[1].get("general") or "").lower()
        elif key == "source":
            sort_fn = lambda kv: str(kv[1].get("source") or "").lower()
        elif key == "updated":
            sort_fn = lambda kv: str(kv[1].get("updated_at") or "")
        else:
            sort_fn = lambda kv: kv[0].lower()
        return sorted(rows, key=sort_fn, reverse=not self.sort_asc)

    def _set_sort(self, key: str):
        if self.sort_key == key:
            self.sort_asc = not self.sort_asc
        else:
            self.sort_key = key
            self.sort_asc = True
        self._render()

    def _sort_label(self, key: str, label: str, center: bool = True):
        arrow = ""
        if self.sort_key == key:
            arrow = " ↑" if self.sort_asc else " ↓"
        return ft.TextButton(
            content=ft.Text(label + arrow, weight=ft.FontWeight.BOLD, size=12, text_align=ft.TextAlign.CENTER if center else ft.TextAlign.LEFT),
            style=ft.ButtonStyle(padding=0, shape=ft.RoundedRectangleBorder(radius=0)),
            on_click=lambda e, _k=key: self._set_sort(_k),
        )

    def _build_header(self):
        return ft.Container(
            bgcolor=ft.colors.SURFACE_VARIANT,
            padding=ft.padding.symmetric(horizontal=10, vertical=6),
            border=ft.border.only(bottom=ft.BorderSide(1, ft.colors.OUTLINE_VARIANT)),
            content=ft.Row(
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    ft.Container(width=150, alignment=ft.alignment.center, content=self._sort_label("key", "Key")),
                    ft.Container(width=230, alignment=ft.alignment.center, content=self._sort_label("raw", "Company")),
                    ft.Container(expand=True, alignment=ft.alignment.center_left, content=self._sort_label("general", "General", center=False)),
                    ft.Container(width=100, alignment=ft.alignment.center, content=self._sort_label("source", "Source")),
                    ft.Container(width=112, alignment=ft.alignment.center, content=self._sort_label("updated", "Updated")),
                    ft.Container(width=44),
                ],
            ),
        )

    def _clear_search(self, e=None):
        self.filter_txt.value = ""
        self._render()

    def _bind_keyboard(self):
        if not self.page or getattr(self, '_keyboard_bound', False):
            return
        self._keyboard_bound = True
        self._prev_key_handler = self.page.on_keyboard_event

        def _handler(e):
            key = (getattr(e, "key", "") or "").lower()
            if self.editing_key is not None:
                if key == "escape":
                    self._cancel_edit()
                    return
                if key == "enter":
                    self._commit_edit()
                    return
            prev = getattr(self, '_prev_key_handler', None)
            if callable(prev):
                prev(e)

        self.page.on_keyboard_event = _handler

    def _begin_new_company(self, e=None):
        if self.editing_key is not None:
            return
        self.editing_key = "__new__"
        self.edit_is_new = True
        self._edit_original = None
        self.edit_raw = ft.TextField(value="", dense=True, text_size=13, content_padding=8, expand=True, autofocus=True)
        self.edit_general = ft.TextField(value="", dense=True, text_size=13, content_padding=8, expand=True)
        self.edit_source = ft.Dropdown(
            options=[ft.dropdown.Option(x) for x in ["none", "dict", "ai", "manual"]],
            value="manual",
            dense=True,
            text_size=13,
            content_padding=8,
            width=100,
        )
        self.edit_updated = ft.TextField(value=_now_iso(), dense=True, text_size=13, content_padding=8, width=112)
        self._render()

    def _enter_edit(self, key: str):
        items = self._items_dict()
        ent = items.get(key)
        if not isinstance(ent, dict):
            self._set_status(f"Company '{key}' not found")
            return
        self.editing_key = key
        self.edit_is_new = False
        self._edit_original = {"key": key, **ent}
        variants = ent.get("raw_variants") or []
        raw_display = str(variants[0]) if variants else key
        self.edit_raw = ft.TextField(value=raw_display, dense=True, text_size=13, content_padding=8, expand=True, autofocus=True)
        self.edit_general = ft.TextField(value=ent.get("general") or "", dense=True, text_size=13, content_padding=8, expand=True)
        self.edit_source = ft.Dropdown(
            options=[ft.dropdown.Option(x) for x in ["none", "dict", "ai", "manual"]],
            value=ent.get("source") or "none",
            dense=True,
            text_size=13,
            content_padding=8,
            width=100,
        )
        self.edit_updated = ft.TextField(value=ent.get("updated_at") or _now_iso(), dense=True, text_size=13, content_padding=8, width=112)
        self._render()

    def _cancel_edit(self):
        self.editing_key = None
        self.edit_is_new = False
        self._edit_original = None
        self.edit_raw = None
        self.edit_general = None
        self.edit_source = None
        self.edit_updated = None
        self._set_status("Edit cancelled")
        self._render()

    def _commit_edit(self):
        if self.editing_key is None:
            return
        raw = (self.edit_raw.value or "").strip() if self.edit_raw else ""
        if not raw:
            self._set_status("Company name is empty")
            return
        new_key = normalize_company_key(raw)
        if not new_key:
            self._set_status("Could not normalize key")
            return
        items = self._items_dict()
        general = (self.edit_general.value or "").strip() if self.edit_general else ""
        source = self.edit_source.value or "manual" if self.edit_source else "manual"
        updated = (self.edit_updated.value or "").strip() if self.edit_updated else _now_iso()
        entry = {
            "general": general,
            "raw_variants": [raw],
            "source": source,
            "updated_at": updated or _now_iso(),
        }
        old_key = None if self.edit_is_new else self.editing_key
        if old_key and old_key != new_key and old_key in items:
            del items[old_key]
        existing = items.get(new_key)
        if isinstance(existing, dict):
            variants = list(existing.get("raw_variants") or [])
            if raw not in variants:
                variants.append(raw)
            entry["raw_variants"] = variants
        items[new_key] = entry
        self.company_map["items"] = items
        self._save()
        saved_key = new_key
        self.editing_key = None
        self.edit_is_new = False
        self._edit_original = None
        self.edit_raw = None
        self.edit_general = None
        self.edit_source = None
        self.edit_updated = None
        self._set_status(f"Saved company: {saved_key}")
        self._render()

    def _delete_item(self, key: str):
        items = self._items_dict()
        if key in items:
            del items[key]
            self.company_map["items"] = items
            self._save()
        self.editing_key = None
        self.edit_is_new = False
        self._edit_original = None
        self.edit_raw = None
        self.edit_general = None
        self.edit_source = None
        self.edit_updated = None
        self._set_status(f"Deleted company: {key}")
        self._render()

    def _build_view_row(self, key: str, ent: Dict[str, Any]):
        variants = ent.get("raw_variants") or []
        raw_display = str(variants[0]) if variants else key
        row = ft.Container(
            padding=ft.padding.symmetric(horizontal=10, vertical=8),
            border=ft.border.only(bottom=ft.BorderSide(1, ft.colors.OUTLINE_VARIANT)),
            content=ft.Row(
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    ft.Container(width=150, alignment=ft.alignment.center, content=ft.Text(key, text_align=ft.TextAlign.CENTER)),
                    ft.Container(width=230, alignment=ft.alignment.center_left, content=ft.Text(raw_display, max_lines=1, overflow=ft.TextOverflow.ELLIPSIS)),
                    ft.Container(expand=True, alignment=ft.alignment.center_left, content=ft.Text(ent.get("general") or "", max_lines=2, overflow=ft.TextOverflow.ELLIPSIS)),
                    ft.Container(width=100, alignment=ft.alignment.center, content=ft.Text(ent.get("source") or "", text_align=ft.TextAlign.CENTER)),
                    ft.Container(width=112, alignment=ft.alignment.center, content=ft.Text(ent.get("updated_at") or "", text_align=ft.TextAlign.CENTER)),
                    ft.Container(width=44),
                ],
            ),
        )
        return ft.GestureDetector(content=row, on_double_tap=lambda e, _k=key: self._enter_edit(_k))

    def _build_edit_row(self, key: str, ent: Dict[str, Any] | None = None):
        show_key = "" if self.edit_is_new else key
        delete_btn = ft.Container(width=44)
        if not self.edit_is_new:
            delete_btn = ft.Container(
                width=44,
                alignment=ft.alignment.center,
                content=ft.IconButton(ft.icons.DELETE_OUTLINE, tooltip="Delete", on_click=lambda e, _k=key: self._delete_item(_k)),
            )
        return ft.Container(
            bgcolor=ft.colors.BLUE_50,
            padding=ft.padding.symmetric(horizontal=10, vertical=8),
            border=ft.border.only(bottom=ft.BorderSide(1, ft.colors.OUTLINE_VARIANT)),
            content=ft.Row(
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    ft.Container(width=150, alignment=ft.alignment.center, content=ft.Text(show_key, weight=ft.FontWeight.BOLD, text_align=ft.TextAlign.CENTER)),
                    ft.Container(width=230, content=self.edit_raw),
                    ft.Container(expand=True, content=self.edit_general),
                    ft.Container(width=100, content=self.edit_source),
                    ft.Container(width=112, content=self.edit_updated),
                    delete_btn,
                ],
            ),
        )

    def _render(self):
        total = len(self._items())
        shown = len(self._filtered_items())
        self.counter_txt.value = f"{shown} / {total}"
        header = self._build_header()
        self.header_container.content = header.content
        self.header_container.padding = header.padding
        self.header_container.bgcolor = header.bgcolor
        self.header_container.border = header.border
        controls = []
        if self.edit_is_new:
            controls.append(self._build_edit_row("__new__"))
        for key, ent in self._display_items():
            if key == self.editing_key and not self.edit_is_new:
                controls.append(self._build_edit_row(key, ent))
            else:
                controls.append(self._build_view_row(key, ent))
        if not controls:
            controls.append(ft.Container(padding=20, content=ft.Text("No companies", italic=True)))
        self.rows_column.controls = controls
        self.update()

    def did_mount(self):
        self._bind_keyboard()
        self._render()

    def build(self):
        toolbar = ft.Container(
            padding=ft.padding.symmetric(horizontal=8, vertical=8),
            content=ft.Row(
                controls=[
                    ft.FilledButton("New Company", icon=ft.icons.ADD, on_click=self._begin_new_company),
                    ft.Container(width=8),
                    self.filter_txt,
                    ft.IconButton(ft.icons.CLOSE, tooltip="Clear search", on_click=self._clear_search),
                    self.counter_txt,
                    ft.IconButton(ft.icons.REFRESH, tooltip="Reload", on_click=lambda e: self._reload()),
                ],
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
            ),
        )
        table_box = ft.Container(
            expand=True,
            border=ft.border.all(1, ft.colors.OUTLINE),
            content=ft.Column(
                spacing=0,
                expand=True,
                controls=[self.header_container, ft.Container(expand=True, content=self.rows_column)],
            ),
        )
        status_bar = ft.Container(
            height=28,
            padding=ft.padding.symmetric(horizontal=10, vertical=4),
            border=ft.border.only(top=ft.BorderSide(1, ft.colors.OUTLINE_VARIANT)),
            content=ft.Row(controls=[self.msg], vertical_alignment=ft.CrossAxisAlignment.CENTER),
        )
        return ft.Column(
            expand=True,
            spacing=0,
            controls=[toolbar, ft.Container(expand=True, padding=ft.padding.only(left=8, right=8, bottom=8), content=table_box), status_bar],
        )


class BacklogTab(ft.UserControl):
    def __init__(self):
        super().__init__()
        self.backlog = _read_json(BACKLOG_PATH, DEFAULT_BACKLOG)
        self.msg = ft.Text(value="", selectable=False, size=12)

        self.sort_key = "done"
        self.sort_asc = True
        self.editing_id = None
        self._edit_original = None
        self._prev_key_handler = None
        self.edit_done = None
        self.edit_priority = None
        self.edit_desc = None
        self.edit_created = None

        self.filter_txt = ft.TextField(
            hint_text="Search...",
            expand=True,
            dense=True,
            text_size=13,
            content_padding=ft.padding.symmetric(horizontal=10, vertical=8),
            on_change=self._on_filter,
        )
        self.hide_completed = ft.Checkbox(label="Hide completed", value=True, on_change=lambda e: self._render())
        self.counter_txt = ft.Text("0 / 0", size=12, color=ft.colors.OUTLINE)

        self.header_container = ft.Container()
        self.rows_column = ft.Column(spacing=0, scroll=ft.ScrollMode.AUTO, expand=True)

    def _set_status(self, text: str):
        self.msg.value = text or ""
        if self.page:
            self.page.update()

    def _save(self):
        self.backlog.setdefault("meta", {})["updated_at"] = _now_iso()
        _atomic_write_json(BACKLOG_PATH, self.backlog)

    def _reload(self):
        self.backlog = _read_json(BACKLOG_PATH, DEFAULT_BACKLOG)
        self.editing_id = None
        self._edit_original = None
        self._set_status("Reloaded backlog")
        self._render()

    def _items(self) -> List[Dict[str, Any]]:
        items = self.backlog.get("items", [])
        if not isinstance(items, list):
            return []
        return [it for it in items if isinstance(it, dict)]

    def _next_id(self) -> int:
        ids = []
        for it in self._items():
            try:
                ids.append(int(it.get("id")))
            except Exception:
                pass
        return (max(ids) + 1) if ids else 1

    def _priority_from_text(self, value: str):
        txt = (value or "").strip()
        if txt == "":
            return True, None, ""
        if not txt.isdigit():
            return False, None, "Priority must be empty or a number from 1 to 10"
        num = int(txt)
        if not (1 <= num <= 10):
            return False, None, "Priority must be between 1 and 10"
        return True, num, ""

    def _filtered_items(self):
        qn = (self.filter_txt.value or "").strip().lower()
        items = []
        for it in self._items():
            done = bool(it.get("done", False))
            if self.hide_completed.value and done:
                continue
            if qn:
                hay = f"{it.get('id','')} {it.get('description','')} {it.get('priority','')} {it.get('created_at','')}"
                if qn not in hay.lower():
                    continue
            items.append(it)
        return items

    def _sort_value(self, it: Dict[str, Any]):
        key = self.sort_key
        if key == "done":
            return 1 if bool(it.get("done", False)) else 0
        if key == "id":
            try:
                return int(it.get("id", 10**9))
            except Exception:
                return 10**9
        if key == "priority":
            pr = it.get("priority")
            if pr is None or pr == "":
                return (1, 999)
            try:
                return (0, int(pr))
            except Exception:
                return (0, 999)
        if key == "task":
            return (it.get("description") or "").lower()
        if key == "created":
            return it.get("created_at") or ""
        return str(it.get(key) or "")

    def _display_items(self):
        items = self._filtered_items()
        try:
            if self.sort_key == "priority":
                non_empty = []
                empty = []
                for it in items:
                    pr = it.get("priority")
                    if pr is None or pr == "":
                        empty.append(it)
                    else:
                        non_empty.append(it)
                non_empty = sorted(non_empty, key=lambda it: int(it.get("priority", 0)), reverse=not self.sort_asc)
                return non_empty + empty
            return sorted(items, key=self._sort_value, reverse=not self.sort_asc)
        except Exception:
            return items

    def _set_sort(self, key: str):
        if self.sort_key == key:
            self.sort_asc = not self.sort_asc
        else:
            self.sort_key = key
            self.sort_asc = True
        self._render()

    def _sort_label(self, key: str, label: str):
        arrow = ""
        if self.sort_key == key:
            arrow = " ↑" if self.sort_asc else " ↓"
        return ft.TextButton(
            content=ft.Text(label + arrow, weight=ft.FontWeight.BOLD, size=12),
            style=ft.ButtonStyle(padding=0, shape=ft.RoundedRectangleBorder(radius=0)),
            on_click=lambda e, _k=key: self._set_sort(_k),
        )

    def _build_header(self):
        return ft.Container(
            bgcolor=ft.colors.SURFACE_VARIANT,
            padding=ft.padding.symmetric(horizontal=10, vertical=6),
            border=ft.border.only(bottom=ft.BorderSide(1, ft.colors.OUTLINE_VARIANT)),
            content=ft.Row(
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    ft.Container(width=68, alignment=ft.alignment.center, content=self._sort_label("done", "Done")),
                    ft.Container(width=58, alignment=ft.alignment.center, content=self._sort_label("id", "ID")),
                    ft.Container(width=84, alignment=ft.alignment.center, content=self._sort_label("priority", "Priority")),
                    ft.Container(expand=True, alignment=ft.alignment.center, content=self._sort_label("task", "Task")),
                    ft.Container(width=112, alignment=ft.alignment.center, content=self._sort_label("created", "Created")),
                ],
            ),
        )

    def _clear_search(self, e=None):
        self.filter_txt.value = ""
        self._render()

    def _begin_new_task(self, e=None):
        bid = self._next_id()
        item = {"id": bid, "done": False, "priority": 5, "description": "", "created_at": _now_iso()}
        items = self._items()
        items.append(item)
        self.backlog["items"] = items
        self._save()
        self._set_status(f"Created backlog item #{bid}")
        self._enter_edit(bid)

    def _bind_keyboard(self):
        if not self.page:
            return
        if self._prev_key_handler is None:
            self._prev_key_handler = self.page.on_keyboard_event

        def _handler(e):
            key = (getattr(e, "key", "") or "").lower()
            shift = bool(getattr(e, "shift", False))
            if self.editing_id is not None:
                if key == "escape":
                    self._cancel_edit()
                    return
                if key == "enter" and not shift:
                    self._commit_edit()
                    return
            if callable(self._prev_key_handler):
                self._prev_key_handler(e)

        self.page.on_keyboard_event = _handler

    def _enter_edit(self, bid: int):
        target = None
        for it in self._items():
            if it.get("id") == bid:
                target = dict(it)
                break
        if target is None:
            self._set_status(f"Backlog item #{bid} not found")
            return
        self.editing_id = bid
        self._edit_original = dict(target)
        self.edit_done = ft.Checkbox(value=bool(target.get("done", False)))
        self.edit_priority = ft.TextField(
            value="" if target.get("priority") is None else str(target.get("priority")),
            dense=True,
            text_size=13,
            content_padding=8,
            width=84,
            autofocus=False,
            on_submit=lambda e: self._commit_edit(),
        )
        self.edit_desc = ft.TextField(
            value=target.get("description") or "",
            dense=True,
            multiline=True,
            min_lines=2,
            max_lines=2,
            expand=True,
            text_size=13,
            content_padding=8,
            autofocus=True,
            on_submit=lambda e: self._commit_edit(),
        )
        self.edit_created = ft.TextField(
            value=target.get("created_at") or "",
            dense=True,
            text_size=13,
            content_padding=8,
            width=112,
            on_submit=lambda e: self._commit_edit(),
        )
        self._render()

    def _cancel_edit(self):
        self.editing_id = None
        self._edit_original = None
        self.edit_done = None
        self.edit_priority = None
        self.edit_desc = None
        self.edit_created = None
        self._set_status("Edit cancelled")
        self._render()

    def _commit_edit(self):
        if self.editing_id is None:
            return
        ok, pr_value, err = self._priority_from_text(self.edit_priority.value if self.edit_priority else "")
        if not ok:
            self._set_status(err)
            return
        items = self._items()
        idx = None
        for i, it in enumerate(items):
            if it.get("id") == self.editing_id:
                idx = i
                break
        if idx is None:
            self._set_status(f"Backlog item #{self.editing_id} not found")
            self._cancel_edit()
            return
        desc = (self.edit_desc.value or "").strip() if self.edit_desc else ""
        created = (self.edit_created.value or "").strip() if self.edit_created else ""
        if not created:
            created = _now_iso()
        items[idx] = {
            **items[idx],
            "id": self.editing_id,
            "done": bool(self.edit_done.value) if self.edit_done else False,
            "priority": pr_value,
            "description": desc,
            "created_at": created,
        }
        self.backlog["items"] = items
        saved_id = self.editing_id
        self._save()
        self.editing_id = None
        self._edit_original = None
        self.edit_done = None
        self.edit_priority = None
        self.edit_desc = None
        self.edit_created = None
        self._set_status(f"Saved backlog item #{saved_id}")
        self._render()

    def _delete_item(self, bid: int):
        items = [it for it in self._items() if it.get("id") != bid]
        self.backlog["items"] = items
        self._save()
        if self.editing_id == bid:
            self.editing_id = None
            self._edit_original = None
        self._set_status(f"Deleted backlog item #{bid}")
        self._render()

    def _build_view_row(self, it: Dict[str, Any]):
        bid = it.get("id")
        done = bool(it.get("done", False))
        pr = it.get("priority", "")
        desc = it.get("description") or ""
        created = it.get("created_at") or ""
        row = ft.Container(
            padding=ft.padding.symmetric(horizontal=10, vertical=8),
            border=ft.border.only(bottom=ft.BorderSide(1, ft.colors.OUTLINE_VARIANT)),
            content=ft.Row(
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    ft.Container(width=68, alignment=ft.alignment.center, content=ft.Checkbox(value=done, disabled=True)),
                    ft.Container(width=58, alignment=ft.alignment.center, content=ft.Text(str(bid), text_align=ft.TextAlign.CENTER)),
                    ft.Container(width=84, alignment=ft.alignment.center, content=ft.Text("" if pr is None else str(pr), text_align=ft.TextAlign.CENTER)),
                    ft.Container(expand=True, content=ft.Text(desc, max_lines=2, overflow=ft.TextOverflow.ELLIPSIS)),
                    ft.Container(width=112, alignment=ft.alignment.center, content=ft.Text(created, text_align=ft.TextAlign.CENTER)),
                    ft.Container(width=44),
                ],
            ),
        )
        return ft.GestureDetector(content=row, on_double_tap=lambda e, _id=bid: self._enter_edit(_id))

    def _build_edit_row(self, it: Dict[str, Any]):
        bid = it.get("id")
        return ft.Container(
            bgcolor=ft.colors.BLUE_50,
            padding=ft.padding.symmetric(horizontal=10, vertical=8),
            border=ft.border.only(bottom=ft.BorderSide(1, ft.colors.OUTLINE_VARIANT)),
            content=ft.Row(
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    ft.Container(width=68, alignment=ft.alignment.center, content=self.edit_done),
                    ft.Container(width=58, alignment=ft.alignment.center, content=ft.Text(str(bid), weight=ft.FontWeight.BOLD, text_align=ft.TextAlign.CENTER)),
                    ft.Container(width=84, alignment=ft.alignment.center, content=self.edit_priority),
                    ft.Container(expand=True, content=self.edit_desc),
                    ft.Container(width=112, alignment=ft.alignment.center, content=self.edit_created),
                    ft.Container(
                        width=44,
                        alignment=ft.alignment.center,
                        content=ft.IconButton(
                            ft.icons.DELETE_OUTLINE,
                            tooltip="Delete",
                            on_click=lambda e, _id=bid: self._delete_item(_id),
                        ),
                    ),
                ],
            ),
        )

    def _render(self):
        total_all = len(self._items())
        shown = len(self._filtered_items())
        self.counter_txt.value = f"{shown} / {total_all}"
        header = self._build_header()
        self.header_container.content = header.content
        self.header_container.padding = header.padding
        self.header_container.bgcolor = header.bgcolor
        self.header_container.border = header.border
        controls = []
        for it in self._display_items():
            bid = it.get("id")
            if bid == self.editing_id:
                controls.append(self._build_edit_row(it))
            else:
                controls.append(self._build_view_row(it))
        if not controls:
            controls.append(ft.Container(padding=20, content=ft.Text("No backlog items", italic=True)))
        self.rows_column.controls = controls
        self.update()

    def _on_filter(self, e):
        self._render()

    def did_mount(self):
        self._bind_keyboard()
        self._render()

    def build(self):
        toolbar = ft.Container(
            padding=ft.padding.symmetric(horizontal=8, vertical=8),
            content=ft.Row(
                controls=[
                    ft.FilledButton("New Task", icon=ft.icons.ADD, on_click=self._begin_new_task),
                    ft.Container(width=8),
                    self.filter_txt,
                    ft.IconButton(ft.icons.CLOSE, tooltip="Clear search", on_click=self._clear_search),
                    self.counter_txt,
                    self.hide_completed,
                    ft.IconButton(ft.icons.REFRESH, tooltip="Reload", on_click=lambda e: self._reload()),
                ],
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
            ),
        )

        table_box = ft.Container(
            expand=True,
            border=ft.border.all(1, ft.colors.OUTLINE),
            content=ft.Column(
                spacing=0,
                expand=True,
                controls=[
                    self.header_container,
                    ft.Container(expand=True, content=self.rows_column),
                ],
            ),
        )

        status_bar = ft.Container(
            height=28,
            padding=ft.padding.symmetric(horizontal=10, vertical=4),
            border=ft.border.only(top=ft.BorderSide(1, ft.colors.OUTLINE_VARIANT)),
            content=ft.Row(controls=[self.msg], vertical_alignment=ft.CrossAxisAlignment.CENTER),
        )

        return ft.Column(
            expand=True,
            spacing=0,
            controls=[
                toolbar,
                ft.Container(expand=True, padding=ft.padding.only(left=8, right=8, bottom=8), content=table_box),
                status_bar,
            ],
        )

def main(page: ft.Page):
    page.title = "QCV Admin"
    page.window_width = 1200
    page.window_height = 820
    page.theme_mode = ft.ThemeMode.LIGHT
    page.padding = 10

    # ensure files exist
    if not SETTINGS_PATH.exists():
        _atomic_write_json(SETTINGS_PATH, DEFAULT_SETTINGS)
    if not COMPANY_MAP_PATH.exists():
        _atomic_write_json(COMPANY_MAP_PATH, DEFAULT_COMPANY_MAP)
    if not BACKLOG_PATH.exists():
        _atomic_write_json(BACKLOG_PATH, DEFAULT_BACKLOG)

    tabs = ft.Tabs(
        expand=True,
        tabs=[
            ft.Tab(text="Backlog", content=BacklogTab()),
            ft.Tab(text="Companies", content=CompaniesTab()),
            ft.Tab(text="Settings", content=SettingsTab()),
        ],
    )

    page.add(tabs)


if __name__ == "__main__":
    ft.app(target=main)
