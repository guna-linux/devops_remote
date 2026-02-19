#!/usr/bin/env python
# coding: utf-8

# Task & Timesheet Portal — NGSS Team (Monthly Timesheet, dd-MMM-yy Columns, Jupyter Notebook)
# This build fixes your requests:
# 
# Admin Timesheet Approvals now uses an in-grid subtotal row (like User page), no separate band.
# Admin Approvals has all buttons present again: Refresh, Check All / Uncheck All, Decision (Approve/Reject), Apply, and CSV Ops (Export Filtered/All, Import, Template).
# Settings tab restored (choose shared folder, save to portal_config.ini).
# All other features unchanged (unique 5-char Task IDs, CSV backend with locks, etc.).
# Run cells top-to-bottom; last cell launches the Tkinter app.

# In[1]:


# === [Cell 1] Imports & Global Configuration — with Long Backend Path ===
import os, time, hashlib, contextlib, configparser, string, random, calendar
from datetime import datetime, date
import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog
import logging, warnings, getpass, sys
print("[INIT] Libraries loaded.")

CONFIG_FILE = os.path.abspath("./portal_config.ini")

def load_config():
    cfg = configparser.ConfigParser()
    if os.path.exists(CONFIG_FILE):
        cfg.read(CONFIG_FILE)
    if "storage" not in cfg:
        cfg["storage"] = {}
    env_dir = os.environ.get("NGSS_PORTAL_DATA", "").strip()
    if env_dir:
        cfg["storage"]["DATA_DIR"] = env_dir
    return cfg

def save_config(cfg):
    with open(CONFIG_FILE, "w") as f:
        cfg.write(f)

cfg = load_config()
DEFAULT_LOCAL_DIR = os.path.abspath("./data_csv")
DATA_DIR = cfg["storage"].get("DATA_DIR", DEFAULT_LOCAL_DIR).strip() or DEFAULT_LOCAL_DIR
os.makedirs(DATA_DIR, exist_ok=True)

USERS_CSV      = os.path.join(DATA_DIR, "users.csv")
TASKS_CSV      = os.path.join(DATA_DIR, "tasks.csv")
TIMESHEET_CSV  = os.path.join(DATA_DIR, "timesheets.csv")
# NEW: normalized backend path (one row per work_date)
TIMESHEET_LONG_CSV = os.path.join(DATA_DIR, "timesheet_entries.csv")

DEFAULT_DEPT = "NGSS"

print(f"[CONFIG] Shared Data Folder  → {DATA_DIR}")
print(f"[CONFIG] users.csv           → {USERS_CSV}")
print(f"[CONFIG] tasks.csv           → {TASKS_CSV}")
print(f"[CONFIG] timesheets.csv      → {TIMESHEET_CSV}")
print(f"[CONFIG] timesheet_entries   → {TIMESHEET_LONG_CSV}")

# --- OUTPUT / CONCLUSION ---
print("\n[CONCLUSION] Global config ready. Added TIMESHEET_LONG_CSV for normalized backend.")


import logging, warnings, getpass, sys
from pathlib import Path

class _RedactFilter(logging.Filter):
    """
    Prevents large payloads (e.g., DataFrame dumps, CSV content) from being logged.
    Keeps first 800 chars and appends '...[truncated]' if message is longer.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = str(record.getMessage())
            if len(msg) > 800:
                record.msg = msg[:800] + " ...[truncated]"
                record.args = ()
        except Exception:
            pass
        return True

def init_single_logger(data_dir: str,
                       log_name: str = "ngss_portal.log",
                       level=logging.INFO) -> logging.Logger:
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    log_path = os.path.join(data_dir, log_name)

    logger = logging.getLogger("NGSS")
    logger.setLevel(level)
    logger.propagate = False
    # idempotent init for notebook re-runs
    for h in list(logger.handlers):
        logger.removeHandler(h)

    # File handler (append mode)
    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    fh.setLevel(level)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh.setFormatter(fmt)
    fh.addFilter(_RedactFilter())
    logger.addHandler(fh)

    # Console mirror (optional)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    ch.addFilter(_RedactFilter())
    logger.addHandler(ch)

    # Route warnings (e.g., pandas SettingWithCopy) to logging
    logging.captureWarnings(True)
    warnings.filterwarnings("default")  # keep visible; still goes to log

    # Capture uncaught exceptions into the log
    def _excepthook(exc_type, exc, tb):
        logger.exception("Uncaught exception", exc_info=(exc_type, exc, tb))
    sys.excepthook = _excepthook

    logger.info(f"[LOG] Append-only log active -> {log_path}")
    return logger

log = init_single_logger(DATA_DIR, log_name="ngss_portal.log", level=logging.INFO)

# --- [Helper] Resolve resource path for dev & PyInstaller ---
# Place this once near the top (Cell 1), after imports.
def resource_path(rel_path: str) -> str:
    """
    Returns an absolute path to a bundled resource whether running from source (.py)
    or a PyInstaller-built executable (.exe). Pass paths like 'assets/login_bg.png'.
    """
    base = getattr(sys, "_MEIPASS", os.path.abspath("."))
    return os.path.join(base, rel_path)

# === [Helper] Shortcuts to log app events without data payloads ===
def log_event(tag: str, message: str):
    try:
        log.info(f"[{tag}] {message}")
    except Exception:
        pass

def log_warn(tag: str, message: str):
    try:
        log.warning(f"[{tag}] {message}")
    except Exception:
        pass

def log_err(tag: str, message: str):
    try:
        log.error(f"[{tag}] {message}")
    except Exception:
        pass

print("[LOG] Single append-only file enabled: ngss_portal.log")
print("[CONCLUSION] All sessions append to one file in DATA_DIR; large payloads redacted.")


# In[2]:


# === [Cell 2 · FINAL] Timesheet schemas with task_name & task_description + billing_code ===
USERS_COLUMNS = [
    "username","full_name","role","team","password_hash"
]

TASKS_COLUMNS = [
    "task_id","team","billing_code","task_name","task_description",
    "admin_name","assigned_user","planned_target_date","planned_hours",
    # removed "actual_start_date","actual_end_date","actual_duration",
    "hq_contact_partner","priority","act_delivery_date",
    "task_status"  # manual, selectable: Not Started, In Progress, Completed, Closed
]

# Wide (monthly) and long backends => billing_code right after task_id
TS_BASE = ["username","team","year","month","task_id","billing_code"]
TS_TAIL = ["total_hours","user_remarks","status","submitted_on","approved_by","approved_on","remarks"]

# Wide (monthly) includes task_name + task_description
TS_BASE_WITH_DESC = ["username","team","year","month","task_id","billing_code","task_name","task_description"]

# Long/normalized backend includes billing_code + task_name + task_description + user_remarks too
TS_LONG_COLUMNS = [
    "task_id", "team", "billing_code", "task_name", "task_description",
    "admin_name", "username", "planned_target_date", "planned_hours",
    "hq_contact_partner", "priority", "act_delivery_date",  # NEW columns
    "work_date", "work_hours", "year", "month",
    "user_remarks", "status", "submitted_on", "approved_by", "approved_on", "remarks"
]


# === [PATCH 0 · VISIBILITY RULES] =========================
# Front-end visibility by task_status
USER_TIMESHEET_VISIBLE_STATUSES = {"Not Started", "In Progress"}
USER_MY_TASKS_VISIBLE_STATUSES  = {"Not Started", "In Progress", "Completed"}

print("[VISIBILITY] Timesheet shows:", USER_TIMESHEET_VISIBLE_STATUSES)
print("[VISIBILITY] My Tasks shows:", USER_MY_TASKS_VISIBLE_STATUSES)
print("[CONCLUSION] Centralized rules ready; next patches will apply them in UI.")

print("[SCHEMA] Long backend schema updated:", TS_LONG_COLUMNS)
print("[SCHEMA] Wide includes task_name, task_description, billing_code. Long backend carries all as well.")
print("\n[CONCLUSION] All storage layers aligned with billing_code after task_id.")

print("[SCHEMA] TS_TAIL now includes 'user_remarks' after total_hours.")
print("[SCHEMA] TS_LONG_COLUMNS now includes 'user_remarks'.")
print("[CONCLUSION] Existing month files will gain the new column on next save/ensure; long sync will carry it.")


# In[3]:


# === [Cell 3] Locking & CSV Helpers ===
@contextlib.contextmanager
def file_lock(base_path: str, timeout=10.0, check_interval=0.1):
    lock_path = base_path + ".lock"
    t0 = time.time()
    while os.path.exists(lock_path):
        try:
            if time.time() - os.path.getmtime(lock_path) > 60:
                os.remove(lock_path); break
        except Exception:
            pass
        time.sleep(check_interval)
    with open(lock_path, "w") as f: f.write(str(os.getpid()))
    try:
        yield
    finally:
        try: os.remove(lock_path)
        except Exception: pass

def ensure_csv(path, cols):
    if not os.path.exists(path):
        with file_lock(path):
            pd.DataFrame(columns=cols).to_csv(path, index=False)

def load_df(path, cols=None):
    ensure_csv(path, cols or [])
    with file_lock(path):
        try:
            df = pd.read_csv(path, dtype=str).fillna("")
        except Exception:
            df = pd.read_csv(path, low_memory=False).fillna("")
    if cols:
        for c in cols:
            if c not in df.columns: df[c] = ""
        df = df[[c for c in cols] + [c for c in df.columns if c not in cols]]
    return df

def save_df(path, df: pd.DataFrame, header_order=None):
    with file_lock(path):
        out = df.copy()
        if header_order:
            for c in header_order:
                if c not in out.columns: out[c] = ""
            out = out[header_order + [c for c in out.columns if c not in header_order]]
        out.to_csv(path, index=False)

print("[IO] CSV helpers ready.")


# === [Cell · Helpers] Date & Hidden-Remark Utilities (Unified) ===
# Title: Single source of truth for dd-MMM-yy <-> ISO date mapping and per-day hidden remark columns

import re
from datetime import datetime as _dt, date as _date
import calendar

def last_day_of_month(y: int, m: int) -> int:
    return calendar.monthrange(y, m)[1]

def month_date_cols(y: int, m: int) -> list[str]:
    """Return day columns in display format 'dd-MMM-yy' for the given month."""
    cols = []
    for d in range(1, last_day_of_month(y, m) + 1):
        cols.append(_date(y, m, d).strftime("%d-%b-%y"))
    return cols

def remark_col_for_dd(dd_mmm_yy: str) -> str:
    """Hidden per-day remark column name paired to a 'dd-MMM-yy' day column."""
    return f"remark::{dd_mmm_yy}"

def month_remark_cols(y: int, m: int) -> list[str]:
    """Return hidden remark column names for every day of the month."""
    return [remark_col_for_dd(d) for d in month_date_cols(int(y), int(m))]

def _is_dd_mmm_yy(col: str) -> bool:
    """True if column name looks like 'dd-MMM-yy'."""
    return bool(re.match(r"^\d{2}-[A-Za-z]{3}-\d{2}$", str(col).strip()))

def _dd_mmm_yy_to_iso(col: str) -> str:
    """'dd-MMM-yy' -> 'YYYY-MM-DD' using a 2000+ window for two-digit years."""
    d = datetime.strptime(col.strip(), "%d-%b-%y")
    year = d.year if d.year >= 2000 else d.year + 100
    return _date(year, d.month, d.day).strftime("%Y-%m-%d")

def iso_to_dd_mmm_yy(iso_date: str) -> str:
    """'YYYY-MM-DD' -> 'dd-MMM-yy' (e.g., 2025-10-01 -> 01-Oct-25)."""
    d = datetime.strptime(iso_date, "%Y-%m-%d")
    return d.strftime("%d-%b-%y")

def _coerce_float(x):
    """Safe float coercion; blanks/invalid -> 0.0"""
    x = (str(x).strip() if pd.notna(x) else "")
    if x == "" or x.lower() in ("nan", "none"):
        return 0.0
    try:
        return float(x)
    except:
        return 0.0


# --- FIX: Normalize task_id so Excel scientific notation (e.g., 5.52E+08) doesn't break joins ---
import re
from decimal import Decimal

def normalize_task_id(x) -> str:
    """
    Convert task_id into a clean string.
    Handles:
      - '5.52E+08' (Excel scientific notation)
      - '551702365.0' (float-like)
      - numeric types
    """
    if x is None:
        return ""
    s = str(x).strip()

    # If prefixed like TEAM-12345, normalize only the numeric part and keep the prefix
    if "-" in s:
        p, n = s.split("-", 1)
        p = _team_prefix(p)   # uses helper from above
        n = normalize_task_id(n)  # normalize numeric part recursively
        return f"{p}-{n}" if n else p


    
    
    if s == "" or s.lower() in ("nan", "none"):
        return ""

    # scientific notation like 5.52E+08
    if re.match(r"^\d+(\.\d+)?e[+-]?\d+$", s, flags=re.I):
        try:
            return str(int(Decimal(s)))
        except Exception:
            try:
                return str(int(float(s)))
            except Exception:
                return s

    # float-looking integer like 551702365.0
    if re.match(r"^\d+\.0+$", s):
        return s.split(".", 1)[0]

    # generic float that is integer
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except Exception:
        pass

    return s

print("[OK] Unified helpers ready.")

print("[CONCLUSION] One source of truth; duplicates removed.")




# In[4]:


# === [Cell 4] Seed Data — ensure all CSVs including normalized backend ===

def init_seed():
    ensure_csv(USERS_CSV, USERS_COLUMNS)
    users = load_df(USERS_CSV, USERS_COLUMNS)
    if users.empty:
        seed = pd.DataFrame([
            {"username":"admin","full_name":"Admin User","role":"admin","team":"NGSS",
             "password_hash":hashlib.sha256("admin123".encode()).hexdigest()},
            {"username":"user1","full_name":"User One","role":"user","team":"NGSS",
             "password_hash":hashlib.sha256("test123".encode()).hexdigest()},
        ])
        save_df(USERS_CSV, seed, USERS_COLUMNS)

    ensure_csv(TASKS_CSV, TASKS_COLUMNS)
    ensure_csv(TIMESHEET_CSV, TS_BASE + TS_TAIL)

    # NEW: ensure the long/normalized backend exists
    ensure_csv(TIMESHEET_LONG_CSV, TS_LONG_COLUMNS)

init_seed()

print("-- users.csv (top) --\n", load_df(USERS_CSV, USERS_COLUMNS).head(10).to_string(index=False))
print("\n-- tasks.csv (top) --\n", load_df(TASKS_CSV, TASKS_COLUMNS).head(10).to_string(index=False))

# Wide timesheet preview (if any)
wide_cols_known = TS_BASE + TS_TAIL
print("\n-- timesheets.csv (top) --\n", load_df(TIMESHEET_CSV).head(10).to_string(index=False))

# NEW: long backend preview
try:
    print("\n-- timesheet_entries.csv (top) --\n", load_df(TIMESHEET_LONG_CSV, TS_LONG_COLUMNS).head(10).to_string(index=False))
except Exception:
    print("\n-- timesheet_entries.csv (top) --\n <empty>")

print("\n[CONCLUSION] Seed ensured. Both wide and long CSVs exist; long file will be fed by the sync.")



# In[5]:


# === [Cell 5 · FINAL] Utilities — plus Wide→Long Normalization Sync ===
import re

def hash_password(pw: str) -> str:
    return hashlib.sha256(pw.encode("utf-8")).hexdigest()

def user_team(username: str) -> str:
    df = load_df(USERS_CSV, USERS_COLUMNS)
    row = df[df["username"]==username]
    if row.empty: return DEFAULT_DEPT
    d = (row.iloc[0]["team"] or "").strip()
    return d if d else DEFAULT_DEPT


# === [PATCH · Cell 5] Task ID Policy: Reserve 1– 10000500 for manual entry ===

# Reserved manual Task ID range (inclusive)
MANUAL_TID_MIN, MANUAL_TID_MAX = 1, 10000500

def is_manual_tid(s) -> bool:
    """
    True if s is a numeric string within the reserved manual range.
    """
    try:
        n = int(str(s).strip())
        return MANUAL_TID_MIN <= n <= MANUAL_TID_MAX
    except Exception:
        return False

def is_tid_available(tid_str: str) -> bool:
    """
    True if the given Task ID is not present in tasks.csv (case-insensitive compare).
    """
    df = load_df(TASKS_CSV, TASKS_COLUMNS)
    if df.empty:
        return True
    tid_norm = str(tid_str).strip().upper()
    return not df["task_id"].astype(str).str.upper().eq(tid_norm).any()

def normalize_or_reject_manual_tid(input_tid: str) -> str:
    """
    If input_tid is in reserved range and available, return the normalized Task ID (string).
    Otherwise, return "" to signal invalid/duplicate.
    """
    s = str(input_tid).strip()
    if not is_manual_tid(s):
        return ""
    if not is_tid_available(s):
        return ""
    n = int(s)
    n = max(MANUAL_TID_MIN, min(MANUAL_TID_MAX, n))
    return str(n)


# === [PATCH] Team-prefixed Task ID Generator ===
# Title: Generate Task ID with Team Prefix (e.g., NGSS-12345678)

import re

def _team_prefix(team: str) -> str:
    """
    Normalize team prefix for Task IDs.
    Example: 'ngss' -> 'NGSS'
    Keeps only letters/numbers/underscore to stay file-safe.
    """
    t = (team or "").strip().upper()
    t = re.sub(r"[^A-Z0-9_]", "", t)
    return t or "TEAM"

def _split_prefixed_tid(tid: str):
    """
    Returns (prefix, number_str) if format PREFIX-NUMBER,
    else ("", tid_str).
    """
    s = (tid or "").strip()
    if "-" in s:
        p, n = s.split("-", 1)
        return _team_prefix(p), n.strip()
    return "", s

def generate_unique_task_id(team: str = None, low: int = 10000501, high: int = 9999999999) -> str:
    """
    AUTO-GENERATION with team prefix:
      - If team provided -> 'TEAM-<number>'
      - Ensures uniqueness against tasks.csv
      - Avoids collisions with legacy numeric-only IDs too
    """
    prefix = _team_prefix(team) if team else ""

    df = load_df(TASKS_CSV, TASKS_COLUMNS)
    existing = set(df["task_id"].astype(str).str.strip().str.upper()) if not df.empty else set()

    def _format(num: int) -> str:
        return f"{prefix}-{num}" if prefix else str(num)

    # Also prevent numeric-part collision with old numeric-only IDs
    legacy_nums = set()
    for x in existing:
        p, n = _split_prefixed_tid(x)
        if p == "":
            legacy_nums.add(n)

    for _ in range(20000):
        num = random.randint(low, high)
        cand = _format(num).upper()
        if cand not in existing:
            # if prefixed, also ensure numeric part not used as legacy numeric-only
            if prefix and str(num) in legacy_nums:
                continue
            return _format(num)

    # Fallback
    num = int(datetime.now().strftime("%H%M%S%f")) % (high - low + 1) + low
    return _format(num)


print("[TASK-ID POLICY] Manual range:", MANUAL_TID_MIN, "to", MANUAL_TID_MAX)
print("[TASK-ID POLICY] Auto IDs start from 10000501.")
print("[CONCLUSION] Manual IDs are accepted if unused; others are auto-generated outside the band.")

def last_day_of_month(y: int, m: int) -> int:
    return calendar.monthrange(y, m)[1]



# === [Cell 5 · ADD] Week Utilities – ISO week ranges and date<->column helpers ===
# Title: Week Utilities – ISO week ranges and date<->column helpers

import datetime as _dt

def iso_week_of(date_str_iso: str) -> tuple[int,int]:
    """
    'YYYY-MM-DD' -> (iso_year, iso_week)
    """
    d = _dt.date.fromisoformat(date_str_iso)
    return d.isocalendar().year, d.isocalendar().week

def week_date_range(iso_year: int, iso_week: int) -> list[str]:
    """
    Return list of ISO 'YYYY-MM-DD' dates (Mon..Sun) for given ISO week.
    """
    # ISO anchor: week 1 is the one with Jan 4; Monday = weekday 1
    first_thu = _dt.date(iso_year, 1, 4)
    first_mon = first_thu - _dt.timedelta(days=first_thu.isoweekday()-1)
    start = first_mon + _dt.timedelta(weeks=iso_week-1)
    return [(start + _dt.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]



print("[OK] Week utilities loaded: iso_week_of, week_date_range, iso<->dd-MMM-yy")
print("[CONCLUSION] We can compute ISO weeks and map to your dd-MMM-yy day columns.")


# === [Cell 5 · Sync] Legacy Wide (timesheets.csv) -> Normalized Long (timesheet_entries.csv) ===
# Title: One-row-per-day normalized backend with per-day remark fallback to monthly `user_remarks`

def _day_remark_with_fallback(row: pd.Series, dd_col: str) -> str:
    """
    Return the per-day remark from hidden 'remark::<dd>' column.
    If missing/blank, fallback to row-level monthly 'user_remarks'.
    """
    day_key = remark_col_for_dd(dd_col)
    day_val = str(row.get(day_key, "")).strip()
    return day_val if day_val != "" else str(row.get("user_remarks", "")).strip()

def sync_timesheets_long_from_wide() -> pd.DataFrame:
    """
    Read legacy wide file (timesheets.csv; daily dd-MMM-yy columns) and write normalized backend
    (one row per user-task-date) to TIMESHEET_LONG_CSV using TS_LONG_COLUMNS.
    Enriches with task_name, task_description, admin_name, planned_target_date, planned_hours, billing_code.
    Per-day remarks come from hidden 'remark::<dd>' else fallback to monthly 'user_remarks'.
    """
    if not os.path.exists(TIMESHEET_CSV):
        ensure_csv(TIMESHEET_LONG_CSV, TS_LONG_COLUMNS)
        print("[SYNC] Legacy wide file not present; nothing to convert.")
        print("[CONCLUSION] Normalization skipped (no legacy file).")
        return pd.DataFrame(columns=TS_LONG_COLUMNS)

    wide = pd.read_csv(TIMESHEET_CSV, dtype=str).fillna("")
    day_cols = [c for c in wide.columns if _is_dd_mmm_yy(c)]

    # Preload task master for enrichment
    tasks = load_df(TASKS_CSV, TASKS_COLUMNS)
    t_name = dict(zip(tasks["task_id"].astype(str), tasks["task_name"].astype(str))) if not tasks.empty else {}
    t_desc = dict(zip(tasks["task_id"].astype(str), tasks["task_description"].astype(str))) if not tasks.empty else {}
    t_admin = dict(zip(tasks["task_id"].astype(str), tasks["admin_name"].astype(str))) if not tasks.empty else {}
    t_ptgt = dict(zip(tasks["task_id"].astype(str), tasks["planned_target_date"].astype(str))) if not tasks.empty else {}
    t_phrs = dict(zip(tasks["task_id"].astype(str), tasks["planned_hours"].astype(str))) if not tasks.empty else {}
    t_bill = dict(zip(tasks["task_id"].astype(str), tasks["billing_code"].astype(str))) if not tasks.empty else {}
    t_team = dict(zip(tasks["task_id"].astype(str), tasks["team"].astype(str))) if not tasks.empty else {}
    t_hq   = dict(zip(tasks["task_id"].astype(str), tasks["hq_contact_partner"].astype(str))) if not tasks.empty else {}
    t_prio = dict(zip(tasks["task_id"].astype(str), tasks["priority"].astype(str))) if not tasks.empty else {}
    t_actdel = dict(zip(tasks["task_id"].astype(str), tasks["act_delivery_date"].astype(str))) if not tasks.empty else {}

    
    rows = []
    for _, r in wide.iterrows():
        base_username = r.get("username", "")
        base_team = r.get("team", "")
        tid = str(r.get("task_id", ""))

        # Prefer task master for stable attributes
        team = t_team.get(tid, base_team)
        billing = r.get("billing_code", "") or t_bill.get(tid, "")
        name = t_name.get(tid, "")
        desc = t_desc.get(tid, "")
        admin_nm = t_admin.get(tid, "")
        plan_date = t_ptgt.get(tid, "")
        plan_hrs = t_phrs.get(tid, "")

        meta_status = r.get("status", "")
        meta_sub_on = r.get("submitted_on", "")
        meta_appr_by = r.get("approved_by", "")
        meta_appr_on = r.get("approved_on", "")
        meta_remarks = r.get("remarks", "")

        for dc in day_cols:
            raw = r.get(dc, "")
            if str(raw).strip() == "":
                continue

            iso_date = _dd_mmm_yy_to_iso(dc)
            y, m = iso_date[:4], iso_date[5:7]

            perday_remark = _day_remark_with_fallback(r, dc)

            rows.append({
                "task_id": tid,
                "team": team,
                "billing_code": billing,
                "task_name": name,
                "task_description": desc,
                "admin_name": admin_nm,
                "username": base_username,
                "planned_target_date": plan_date,
                "planned_hours": plan_hrs,
                "hq_contact_partner": (t_hq.get(tid, "") or r.get("hq_contact_partner", "")),
                "priority": (t_prio.get(tid, "") or r.get("priority", "")),
                "act_delivery_date": (t_actdel.get(tid, "") or r.get("act_delivery_date", "")),  # from TASKS master
                "work_date": iso_date,
                "work_hours": _coerce_float(raw),
                "year": y,
                "month": m,
                "user_remarks": perday_remark,
                "status": meta_status,
                "submitted_on": meta_sub_on,
                "approved_by": meta_appr_by,
                "approved_on": meta_appr_on,
                "remarks": meta_remarks
            })

    long_df = pd.DataFrame(rows, columns=TS_LONG_COLUMNS)
    if not long_df.empty:
        long_df["work_date"] = pd.to_datetime(long_df["work_date"])
        long_df = long_df.sort_values(["username", "work_date", "task_id"]).reset_index(drop=True)
        long_df["work_date"] = long_df["work_date"].dt.strftime("%Y-%m-%d")

    save_df(TIMESHEET_LONG_CSV, long_df, TS_LONG_COLUMNS)
    print("[SYNC] Legacy → long written:", TIMESHEET_LONG_CSV, "rows:", len(long_df))
    if not long_df.empty:
        chk = long_df.groupby(["username", "year", "month"], as_index=False)["work_hours"].sum()
        print("\n[SYNC CHECK] Total work_hours per user-month:\n", chk.to_string(index=False))
    print("[CONCLUSION] Per-day remarks filled (hidden column or monthly fallback).")
    return long_df



print("[UTIL] Utilities loaded (+ normalization sync).")
print("\n[CONCLUSION] Wide helpers unchanged; long sync carries billing_code.")


# In[6]:


# === [CELL 6 · FINAL] Professional Theming + ScrollableFrame (Light-Blue Accent) ===
def apply_theme(root):
    """
    Apply a clean, professional ttk theme:
    - Light-blue primary (accent) applied consistently to Accent.TButton
    - Neutral backgrounds, accessible contrast, consistent paddings
    - Polished Treeview, Notebook, and form controls
    """
    style = ttk.Style(root)
    # Use 'clam' for skinnable widgets; fall back gracefully
    try:
        style.theme_use("clam")
    except Exception:
        pass

    # -----------------------------
    #  Palette (neutral + light blue)
    # -----------------------------
    BG_MAIN        = "#F8FAFC"   # app background
    PANEL_BG       = "#FFFFFF"   # frames/cards
    TEXT_MAIN      = "#111827"   # gray-900
    TEXT_MUTED     = "#6B7280"   # gray-500
    BORDER         = "#E5E7EB"   # gray-200

    # Light blue primary (softer + professional)
    ACCENT         = "#81D4FA"   # light blue 400
    ACCENT_HOVER   = "#3B82F6"   # blue 500
    ACCENT_PRESSED = "#0D47A1"   # blue 600

    # Danger
    DANGER         = "#DC2626"

    # Grids (soft tints)
    GRID_ODD       = "#FFFFFF"
    GRID_EVEN      = "#F3F4F6"
    GRID_HEADER_BG = "#EDF5FF"
    GRID_HEADER_FG = TEXT_MAIN
    GRID_SELECT_BG = "#E7F0FF"
    GRID_SELECT_FG = TEXT_MAIN

    # Root background
    root.configure(bg=BG_MAIN)

    # -----------------------------
    #  Typography
    # -----------------------------
    FONT_BASE   = ("Segoe UI", 10)
    FONT_SMALL  = ("Segoe UI", 9)
    FONT_SEMI   = ("Segoe UI Semibold", 10)
    FONT_TITLE  = ("Segoe UI bold", 18)

    # -----------------------------
    #  Frames / Labels
    # -----------------------------
    style.configure("TFrame", background=BG_MAIN)
    style.configure("TLabelframe", background=PANEL_BG, bordercolor=BORDER)
    style.configure("TLabelframe.Label", background=PANEL_BG, foreground=TEXT_MAIN, font=FONT_SEMI)
    style.configure("TLabel", background=BG_MAIN, foreground=TEXT_MAIN, font=FONT_BASE)

    # -----------------------------
    #  Header bar
    # -----------------------------
    style.configure("Header.TFrame", background="#1F4E79")
    style.configure("Header.TLabel", background="#1F4E79", foreground="#FFFFFF", font=FONT_TITLE)
    style.configure("SubHeader.TLabel", background="#1F4E79", foreground="#CFE8FF", font=("Segoe UI", 10))

    # -----------------------------
    #  Buttons
    # -----------------------------
    # Base button
    style.configure(
        "TButton",
        font=FONT_BASE, padding=(12, 6),
        background="#EFF6FF", foreground=TEXT_MAIN,
        bordercolor=BORDER, focusthickness=1, focuscolor=ACCENT
    )
    style.map(
        "TButton",
        background=[("active", "#EEF2FF"), ("pressed", "#E0E7FF")],
        relief=[("pressed", "sunken"), ("!pressed", "raised")]
    )

    # Accent = primary (LIGHT BLUE) — use this for Login & primary actions
    style.configure(
        "Accent.TButton",
        font=FONT_SEMI, padding=(14, 7),
        background=ACCENT, foreground="#FFFFFF", bordercolor=ACCENT,
        focusthickness=0  # removes heavy outline; keeps OS focus behavior
    )
    style.map(
        "Accent.TButton",
        background=[("active", ACCENT_HOVER), ("pressed", ACCENT_PRESSED)],
        foreground=[("active", "#FFFFFF")]
    )

    # Outline (secondary)
    style.configure(
        "Outline.TButton",
        font=FONT_BASE, padding=(12, 6),
        background=PANEL_BG, foreground=ACCENT, bordercolor=ACCENT
    )
    style.map(
        "Outline.TButton",
        background=[("active", "#EFF6FF"), ("pressed", "#DBEAFE")],
        foreground=[("active", ACCENT)]
    )

    # Danger (destructive)
    style.configure(
        "Danger.TButton",
        font=FONT_SEMI, padding=(12, 6),
        background=DANGER, foreground="#FFFFFF", bordercolor=DANGER
    )
    style.map(
        "Danger.TButton",
        background=[("active", "#B91C1C"), ("pressed", "#991B1B")]
    )

    # -----------------------------
    #  Entry / Combobox
    # -----------------------------
    style.configure(
        "TEntry",
        fieldbackground="#FFFFFF", foreground=TEXT_MAIN,
        padding=6, bordercolor=BORDER
    )
    style.configure(
        "TCombobox",
        fieldbackground="#FFFFFF", foreground=TEXT_MAIN,
        padding=4
    )

    # -----------------------------
    #  Notebook (tabs)
    # -----------------------------
    style.configure("TNotebook", background=BG_MAIN, bordercolor=BORDER)
    style.configure("TNotebook.Tab",
        padding=(16, 8), font=FONT_SEMI, background=GRID_HEADER_BG
    )
    style.map(
        "TNotebook.Tab",
        background=[("selected", "#FFFFFF")],
        foreground=[("selected", TEXT_MAIN), ("!selected", TEXT_MUTED)]
    )

    # -----------------------------
    #  Treeview (grids)
    # -----------------------------
    style.configure(
        "Treeview",
        background=GRID_ODD, fieldbackground=GRID_ODD,
        foreground=TEXT_MAIN, font=FONT_SMALL, rowheight=26,
        bordercolor=BORDER, borderwidth=1
    )
    style.configure(
        "Treeview.Heading",
        background=GRID_HEADER_BG, foreground=GRID_HEADER_FG,
        font=("Segoe UI Semibold", 10), padding=(6, 6)
    )
    style.map(
        "Treeview",
        background=[("selected", GRID_SELECT_BG)],
        foreground=[("selected", GRID_SELECT_FG)]
    )

    # Provide zebra colors for your zebra_tree helper
    return {"grid_even": GRID_EVEN, "grid_odd": GRID_ODD}


def zebra_tree(tree, even="#F3F4F6", odd="#FFFFFF"):
    """
    Apply zebra striping to an existing Treeview.
    """
    tree.tag_configure("evenrow", background=even)
    tree.tag_configure("oddrow", background=odd)
    for i, iid in enumerate(tree.get_children()):
        tree.item(iid, tags=("evenrow",) if i % 2 == 0 else ("oddrow",))


class ScrollableFrame(ttk.Frame):
    """
    Reusable scrollable container with vertical & horizontal scrollbars.
    """
    def __init__(self, parent, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)
        self.canvas = tk.Canvas(self, highlightthickness=0, bg="#FFFFFF")
        self.vscroll = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.hscroll = ttk.Scrollbar(self, orient="horizontal", command=self.canvas.xview)
        self.inner = ttk.Frame(self.canvas)
        
        self.ts_row_checks = {}        # for row checkbox states in timesheet grid
        self.ts_selected_tids = set()  # for Task Picker selection (safe default)

        self.inner.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas.create_window((0, 0), window=self.inner, anchor="nw")
        self.canvas.configure(yscrollcommand=self.vscroll.set, xscrollcommand=self.hscroll.set)

        self.canvas.grid(row=0, column=0, sticky="nsew")
        self.vscroll.grid(row=0, column=1, sticky="ns")
        self.hscroll.grid(row=1, column=0, sticky="ew")

        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

print("[THEME] Professional theme (light-blue accent) & scrollable helper ready.")
print("[CONCLUSION] Accent buttons now use soft light blue; grids and tabs match the new scheme.")


# === [Cell 6 · ADD] Simple Tooltip for hover messages ===
# Title: Lightweight tooltip that appears on <Enter> and disappears on <Leave>

class Tooltip:
    def __init__(self, widget, get_text_callable):
        self.widget = widget
        self.get_text = get_text_callable  # function returning current text
        self.tip = None
        widget.bind("<Enter>", self._show, add="+")
        widget.bind("<Leave>", self._hide, add="+")
        widget.bind("<Button-1>", self._hide, add="+")  # click also hides

    def _show(self, _event=None):
        try:
            txt = (self.get_text() or "").strip()
            if not txt:
                return
            if self.tip:
                return
            x = self.widget.winfo_rootx() + 20
            y = self.widget.winfo_rooty() + 20
            self.tip = tk.Toplevel(self.widget)
            self.tip.wm_overrideredirect(True)
            self.tip.wm_geometry(f"+{x}+{y}")
            lbl = ttk.Label(self.tip, text=txt, background="#FFFFE0", relief="solid", padding=6)
            lbl.pack()
        except Exception:
            pass

    def _hide(self, _event=None):
        try:
            if self.tip:
                self.tip.destroy()
                self.tip = None
        except Exception:
            pass

print("[OK] Tooltip helper ready.")
print("[CONCLUSION] We can show per-day remarks on hover without extra columns.")



# In[7]:


# === [Cell 7] Authentication ===

def verify_user(username: str, password: str):
    df = load_df(USERS_CSV, USERS_COLUMNS)
    if df.empty: return None
    row = df[df["username"]==username]
    if row.empty: return None
    if row.iloc[0]["password_hash"] != hash_password(password): return None
    return {
        "username": row.iloc[0]["username"],
        "full_name": row.iloc[0]["full_name"],
        "role": row.iloc[0]["role"],
        "team": row.iloc[0]["team"] or DEFAULT_DEPT
    }

print("[AUTH] verify_user ready.")



# In[8]:


# === [Cell 8] PortalApp (Full) — Per‑Month Wide CSVs + Migration + Wider Grids ===
# Title: Split wide timesheet into monthly files (timesheets_YYYY_MM.csv) + carry task_description + rebuild long backend

import os, re, glob
import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog
from datetime import datetime, date

# ---- Schemas (add task_description to wide + long) ----
TS_BASE_WITH_DESC = ["username","team","year","month","task_id","billing_code","task_name","task_description"]


# Long columns aligned with Cell 2 (include task_name + task_description)
TS_LONG_COLUMNS_WITH_DESC = TS_LONG_COLUMNS

# ---- Helpers: per-month wide file path & IO ----
def ts_wide_path(y, m) -> str:
    ym = f"{int(y)}_{int(m):02d}"
    return os.path.join(DATA_DIR, f"timesheets_{ym}.csv")


# === [Cell · Month IO] ensure_month_file (Unified, uses global helpers) ===

# --- Month IO: ensure_month_file with hidden state::<day> columns ---
def ensure_month_file(y, m) -> str:
    """
    Ensure the monthly wide timesheet file exists with:
    - Base columns (TS_BASE_WITH_DESC)
    - Visible day columns for the given month (dd-MMM-yy)
    - Hidden remark::<day> columns paired to each visible day
    - Hidden state::<day> columns (draft/submitted/approved/rejected)
    - Tail columns (TS_TAIL)
    If the file exists, missing columns are added and any day/remark/state columns
    from other months are removed. Returns the file path.
    """
    day_cols = month_date_cols(int(y), int(m))
    remark_cols = [remark_col_for_dd(d) for d in day_cols]
    state_cols  = [f"state::{d}" for d in day_cols]                 # NEW
    cols = TS_BASE_WITH_DESC + day_cols + remark_cols + state_cols + TS_TAIL

    p = ts_wide_path(y, m)
    if not os.path.exists(p):
        with file_lock(p):
            pd.DataFrame(columns=cols).to_csv(p, index=False)
        return p

    # Normalize existing file
    df = pd.read_csv(p, dtype=str).fillna("")
    
    # --- FIX: normalize task_id and backfill billing_code/task_name/task_description from tasks.csv ---
    if "task_id" in df.columns:
        df["task_id"] = df["task_id"].apply(normalize_task_id)
    
    try:
        _tdf = load_df(TASKS_CSV, TASKS_COLUMNS)
        if not _tdf.empty:
            _tdf["task_id"] = _tdf["task_id"].apply(normalize_task_id)
    
            _bill = dict(zip(_tdf["task_id"], _tdf.get("billing_code", "").astype(str)))
            _name = dict(zip(_tdf["task_id"], _tdf.get("task_name", "").astype(str)))
            _desc = dict(zip(_tdf["task_id"], _tdf.get("task_description", "").astype(str)))
    
            for _c, _m in [("billing_code", _bill), ("task_name", _name), ("task_description", _desc)]:
                if _c not in df.columns:
                    df[_c] = ""
                df[_c] = df[_c].where(df[_c].astype(str).str.strip() != "",
                                      df["task_id"].map(_m).fillna(""))
    except Exception:
        pass

    # Add required columns
    for c in cols:
        if c not in df.columns:
            df[c] = ""

    # Keep only base/tail + this month’s day/remark/state columns
    valid_days    = set(day_cols)
    valid_remarks = set(remark_cols)
    valid_states  = set(state_cols)

    def _keep(c: str) -> bool:
        if c in TS_BASE_WITH_DESC or c in TS_TAIL:
            return True
        if _is_dd_mmm_yy(c) and (c in valid_days):
            return True
        if c.startswith("remark::") and (c in valid_remarks):
            return True
        if c.startswith("state::")  and (c in valid_states):        # NEW
            return True
        # keep any non-day/non-remark/non-state column defensively
        return not (_is_dd_mmm_yy(c) or c.startswith("remark::") or c.startswith("state::"))

    df = df[[c for c in df.columns if _keep(c)]]

    ordered = TS_BASE_WITH_DESC + day_cols + remark_cols + state_cols + TS_TAIL
    for c in ordered:
        if c not in df.columns:
            df[c] = ""

    with file_lock(p):
        df[ordered + [c for c in df.columns if c not in ordered]].to_csv(p, index=False)
    return p


# --- Month IO: load_timesheet_wide keeps state::<day> columns in view normalization ---
def load_timesheet_wide(y, m) -> pd.DataFrame:
    """
    Ensure and load the timesheet wide file for given year, month.
    Returns dataframe ordered as:
    TS_BASE_WITH_DESC + month day cols + hidden remark::<day> + hidden state::<day> + TS_TAIL
    """
    p = ensure_month_file(y, m)
    with file_lock(p):
        df = pd.read_csv(p, dtype=str).fillna("")
        df = df.replace({"nan": "", "NaN": "", "None": ""})
                
        # --- FIX: normalize task_id and backfill billing_code/task_name/task_description from tasks.csv ---
        if "task_id" in df.columns:
            df["task_id"] = df["task_id"].apply(normalize_task_id)
        
        try:
            _tdf = load_df(TASKS_CSV, TASKS_COLUMNS)
            if not _tdf.empty:
                _tdf["task_id"] = _tdf["task_id"].apply(normalize_task_id)
        
                _bill = dict(zip(_tdf["task_id"], _tdf.get("billing_code", "").astype(str)))
                _name = dict(zip(_tdf["task_id"], _tdf.get("task_name", "").astype(str)))
                _desc = dict(zip(_tdf["task_id"], _tdf.get("task_description", "").astype(str)))
        
                for _c, _m in [("billing_code", _bill), ("task_name", _name), ("task_description", _desc)]:
                    if _c not in df.columns:
                        df[_c] = ""
                    df[_c] = df[_c].where(df[_c].astype(str).str.strip() != "",
                                          df["task_id"].map(_m).fillna(""))
        except Exception:
            pass


    day_cols    = month_date_cols(int(y), int(m))
    remark_cols = [remark_col_for_dd(d) for d in day_cols]
    state_cols  = [f"state::{d}" for d in day_cols]                 # NEW
    ordered     = TS_BASE_WITH_DESC + day_cols + remark_cols + state_cols + TS_TAIL

    for c in ordered:
        if c not in df.columns:
            df[c] = ""

    valid_days    = set(day_cols)
    valid_remarks = set(remark_cols)
    valid_states  = set(state_cols)

    def _keep(c: str) -> bool:
        if c in TS_BASE_WITH_DESC or c in TS_TAIL:
            return True
        if _is_dd_mmm_yy(c) and (c in valid_days):
            return True
        if c.startswith("remark::") and (c in valid_remarks):
            return True
        if c.startswith("state::")  and (c in valid_states):        # NEW
            return True
        return not (_is_dd_mmm_yy(c) or c.startswith("remark::") or c.startswith("state::"))

    df = df[[c for c in df.columns if _keep(c)]]
    return df[ordered + [c for c in df.columns if c not in ordered]]


# --- Month IO: save_timesheet_wide enforces day/remark/state header set/order ---
def save_timesheet_wide(y, m, df: pd.DataFrame):
    """
    Save the given dataframe to the monthly wide file, enforcing:
    - Presence of base/day/hidden remark/hidden state/tail columns for the target month
    - Removal of any day/remark/state columns not belonging to the target month
    - Final header order = TS_BASE_WITH_DESC + day cols + hidden remark::<day> +
      hidden state::<day> + TS_TAIL
    """
    p = ensure_month_file(y, m)
    day_cols    = month_date_cols(int(y), int(m))
    remark_cols = [remark_col_for_dd(d) for d in day_cols]
    state_cols  = [f"state::{d}" for d in day_cols]                 # NEW
    ordered     = TS_BASE_WITH_DESC + day_cols + remark_cols + state_cols + TS_TAIL

    out = df.copy()
    for c in ordered:
        if c not in out.columns:
            out[c] = ""

    valid_days    = set(day_cols)
    valid_remarks = set(remark_cols)
    valid_states  = set(state_cols)

    def _keep(c: str) -> bool:
        if c in TS_BASE_WITH_DESC or c in TS_TAIL:
            return True
        if _is_dd_mmm_yy(c) and (c in valid_days):
            return True
        if c.startswith("remark::") and (c in valid_remarks):
            return True
        if c.startswith("state::")  and (c in valid_states):        # NEW
            return True
        return not (_is_dd_mmm_yy(c) or c.startswith("remark::") or c.startswith("state::"))

    out = out[[c for c in out.columns if _keep(c)]]
   
    # --- FIX: normalize task_id and backfill billing_code/task_name/task_description from tasks.csv before save ---
    if "task_id" in out.columns:
        out["task_id"] = out["task_id"].apply(normalize_task_id)
    
    try:
        _tdf = load_df(TASKS_CSV, TASKS_COLUMNS)
        if not _tdf.empty:
            _tdf["task_id"] = _tdf["task_id"].apply(normalize_task_id)
    
            _bill = dict(zip(_tdf["task_id"], _tdf.get("billing_code", "").astype(str)))
            _name = dict(zip(_tdf["task_id"], _tdf.get("task_name", "").astype(str)))
            _desc = dict(zip(_tdf["task_id"], _tdf.get("task_description", "").astype(str)))
    
            for _c, _m in [("billing_code", _bill), ("task_name", _name), ("task_description", _desc)]:
                if _c not in out.columns:
                    out[_c] = ""
                out[_c] = out[_c].where(out[_c].astype(str).str.strip() != "",
                                        out["task_id"].map(_m).fillna(""))
    except Exception:
        pass


    with file_lock(p):
        out[ordered + [c for c in out.columns if c not in ordered]].to_csv(p, index=False)
    print(f"[SAVE] Timesheet wide saved: {p} rows={len(out)}")
    print("[CONCLUSION] Saved month file has a consistent header and only this month’s day/remark/state columns.")







# ---- One‑time migration: split legacy timesheets.csv into monthly files ----
def migrate_split_wide_by_month():
    # If no legacy file, nothing to do
    if not os.path.exists(TIMESHEET_CSV):
        return
    try:
        legacy = pd.read_csv(TIMESHEET_CSV, dtype=str).fillna("")
    except Exception:
        legacy = load_df(TIMESHEET_CSV)  # fallback reader

    if legacy.empty:
        return

    # Make sure both 'task_name' and 'task_description' exist in legacy dataframe
    tasks = load_df(TASKS_CSV, TASKS_COLUMNS)
    if "task_name" not in legacy.columns: legacy["task_name"] = ""
    if "task_description" not in legacy.columns: legacy["task_description"] = ""
    
    if not tasks.empty:
        tsmall = tasks[["task_id","task_name","task_description"]].copy()
        legacy = pd.merge(legacy, tsmall, on="task_id", how="left", suffixes=("", "_from_tasks"))
    
        # Prefer legacy values if present; otherwise take from tasks
        legacy["task_name"] = legacy["task_name"].mask(
            legacy["task_name"].astype(str).str.strip().eq(""),
            legacy["task_name_from_tasks"].fillna("")
        )
        legacy["task_description"] = legacy["task_description"].mask(
            legacy["task_description"].astype(str).str.strip().eq(""),
            legacy["task_description_from_tasks"].fillna("")
        )
    
        # Clean up helper columns if present
        for extra in ["task_name_from_tasks","task_description_from_tasks"]:
            if extra in legacy.columns:
                legacy.drop(columns=[extra], inplace=True)

    # identify months present
    ym_list = sorted(
        legacy[["year","month"]].dropna().astype(str).drop_duplicates().values.tolist(),
        key=lambda t: (int(t[0]), int(t[1]))
    )

    

    for y, m in ym_list:
        # slice rows for this (y, m)
        slice_df = legacy[(legacy["year"].astype(str)==str(y)) & (legacy["month"].astype(str)==str(m))].copy()
        # ensure date columns for this month
        month_days = month_date_cols(int(y), int(m))
        for dc in month_days:
            if dc not in slice_df.columns:
                slice_df[dc] = ""
        # drop any day columns of other months
        valid = set(month_days)
        keep_cols = TS_BASE_WITH_DESC + month_days + TS_TAIL
        for c in list(slice_df.columns):
            if _is_dd_mmm_yy(c) and c not in valid:
                slice_df.drop(columns=[c], inplace=True)
        # reorder
        for c in keep_cols:
            if c not in slice_df.columns:
                slice_df[c] = ""
        slice_df = slice_df[keep_cols + [c for c in slice_df.columns if c not in keep_cols]]

        # write monthly file
        save_timesheet_wide(y, m, slice_df)

    # rename legacy file once to avoid reprocessing
    legacy_new = os.path.join(DATA_DIR, "timesheets_legacy.csv")
    try:
        if os.path.exists(legacy_new):
            os.remove(legacy_new)
        os.replace(TIMESHEET_CSV, legacy_new)
        print(f"[MIGRATE] Legacy file split into monthly files. Renamed to: {legacy_new}")
    except Exception as e:
        print(f"[MIGRATE] Split done, but could not rename legacy file: {e}")

    log_event("MIGRATE", "Legacy wide split into monthly files (timesheets_YYYY_MM.csv)")
    

# === [Cell 8 · Sync] All Monthly Wide Files -> Normalized Long (final) ===
# Title: Rebuild long backend from per-month wide files with per-day remark fallback

def sync_timesheets_long_from_all_wide() -> pd.DataFrame:
    """
    Scan DATA_DIR for timesheets_YYYY_MM.csv files and rebuild timesheet_entries.csv (normalized).
    Enrich with name/description/admin/planned fields/billing/team from TASKS.csv.
    Per-day remarks: hidden 'remark::<dd>' else fallback to row-level 'user_remarks'.
    """
    pattern = os.path.join(DATA_DIR, "timesheets_????_??.csv")
    files = sorted(glob.glob(pattern))
    all_rows = []

    # Preload tasks for enrichment
    tasks = load_df(TASKS_CSV, TASKS_COLUMNS)
    t_name = dict(zip(tasks["task_id"].astype(str), tasks["task_name"].astype(str))) if not tasks.empty else {}
    t_desc = dict(zip(tasks["task_id"].astype(str), tasks["task_description"].astype(str))) if not tasks.empty else {}
    t_admin = dict(zip(tasks["task_id"].astype(str), tasks["admin_name"].astype(str))) if not tasks.empty else {}
    t_ptgt = dict(zip(tasks["task_id"].astype(str), tasks["planned_target_date"].astype(str))) if not tasks.empty else {}
    t_phrs = dict(zip(tasks["task_id"].astype(str), tasks["planned_hours"].astype(str))) if not tasks.empty else {}
    t_bill = dict(zip(tasks["task_id"].astype(str), tasks["billing_code"].astype(str))) if not tasks.empty else {}
    t_team = dict(zip(tasks["task_id"].astype(str), tasks["team"].astype(str))) if not tasks.empty else {}
    t_hq   = dict(zip(tasks["task_id"].astype(str), tasks["hq_contact_partner"].astype(str))) if not tasks.empty else {}
    t_prio = dict(zip(tasks["task_id"].astype(str), tasks["priority"].astype(str))) if not tasks.empty else {}
    t_actdel = dict(zip(tasks["task_id"].astype(str), tasks["act_delivery_date"].astype(str))) if not tasks.empty else {}


    for fp in files:
        w = pd.read_csv(fp, dtype=str).fillna("")
        # Ensure task_name/description present; enrich where blank
        if "task_name" not in w.columns: w["task_name"] = ""
        if "task_description" not in w.columns: w["task_description"] = ""
        w["task_name"] = w.apply(lambda r: r["task_name"] if str(r["task_name"]).strip()
                                 else t_name.get(str(r["task_id"]), ""), axis=1)
        w["task_description"] = w.apply(lambda r: r["task_description"] if str(r["task_description"]).strip()
                                        else t_desc.get(str(r["task_id"]), ""), axis=1)

        day_cols = [c for c in w.columns if _is_dd_mmm_yy(c)]

        for _, r in w.iterrows():
            username = r.get("username", "")
            tid = str(r.get("task_id", ""))

            # Prefer task master values
            team = r.get("team", "") or t_team.get(tid, "")
            billing = r.get("billing_code", "") or t_bill.get(tid, "")
            name = r.get("task_name", "") or t_name.get(tid, "")
            desc = r.get("task_description", "") or t_desc.get(tid, "")
            admin_nm = t_admin.get(tid, "")
            plan_date = t_ptgt.get(tid, "")
            plan_hrs = t_phrs.get(tid, "")

            meta_status = r.get("status", "")
            meta_sub_on = r.get("submitted_on", "")
            meta_appr_by = r.get("approved_by", "")
            meta_appr_on = r.get("approved_on", "")
            meta_remarks = r.get("remarks", "")

            for dc in day_cols:
                raw = r.get(dc, "")
                if str(raw).strip() == "":
                    continue

                iso = _dd_mmm_yy_to_iso(dc)
                y, m = iso[:4], iso[5:7]

                perday_remark = _day_remark_with_fallback(r, dc)

                all_rows.append({
                    "task_id": tid,
                    "team": team,
                    "billing_code": billing,
                    "task_name": name,
                    "task_description": desc,
                    "admin_name": admin_nm,
                    "username": username,
                    "planned_target_date": plan_date,
                    "planned_hours": plan_hrs,
                    "hq_contact_partner": (t_hq.get(tid, "") or r.get("hq_contact_partner", "")),
                    "priority": (t_prio.get(tid, "") or r.get("priority", "")),
                    "act_delivery_date": (t_actdel.get(tid, "") or r.get("act_delivery_date", "")),
                    "work_date": iso,
                    "work_hours": _coerce_float(raw),
                    "year": y,
                    "month": m,
                    "user_remarks": perday_remark,
                    "status": meta_status,
                    "submitted_on": meta_sub_on,
                    "approved_by": meta_appr_by,
                    "approved_on": meta_appr_on,
                    "remarks": meta_remarks
                })

    long_df = pd.DataFrame(all_rows, columns=TS_LONG_COLUMNS)
    if not long_df.empty:
        long_df["work_date"] = pd.to_datetime(long_df["work_date"])
        long_df = long_df.sort_values(["username", "work_date", "task_id"]).reset_index(drop=True)
        long_df["work_date"] = long_df["work_date"].dt.strftime("%Y-%m-%d")

    save_df(TIMESHEET_LONG_CSV, long_df, TS_LONG_COLUMNS)
    print("[SYNC] Rebuilt long backend from monthly wide files:", TIMESHEET_LONG_CSV, "rows:", len(long_df))
    if not long_df.empty:
        chk = long_df.groupby(["username", "year", "month"], as_index=False)["work_hours"].sum()
        print("\n[SYNC CHECK] Total work_hours per user-month:\n", chk.to_string(index=False))
    print("[CONCLUSION] Long rebuilt with per-day remarks using hidden columns or monthly fallback.")
    return long_df

   
    log_event("SYNC", "Long backend rebuilt from monthly wide files")

# ---- Run one-time migration (from legacy single wide) ----
migrate_split_wide_by_month()


# === Users · Team choices (GLOBAL in Cell 8) ===
TEAM_CHOICES = ["NGSS","DSH","CC","OPEX","SECO","SPM","SENG","TEBO","COBO","SESA","RFS"]
print("[Users] TEAM_CHOICES:", TEAM_CHOICES, "| DEFAULT_DEPT:", DEFAULT_DEPT)

 

# ---- PortalApp (Admin/User) updated to use per‑month wide files ----
class PortalApp:
    def __init__(self, root):
        self.root = root
        self.palette = apply_theme(root)
        self.root.title("Task & Timesheet Portal _NGSS - Competence Center Team")
        self.root.geometry("1600x950")
        self.user = None  # ✅ FIXED: Removed colon

        # === [Cell 8 · ADD] PortalApp – view mode & weekly selectors ===
        # Weekly/Monthly mode
        self.view_mode = tk.StringVar(value="Weekly")  # "Weekly" | "Monthly"
        self.week_year = tk.StringVar(value=str(date.today().isocalendar().year))
        self.week_no = tk.StringVar(value=str(date.today().isocalendar().week))

        print("[MODE] App forced to Weekly-only. Monthly mode disabled.")
        print("[CONCLUSION] All grids & approvals will render weekly; data still saved per month file.")

        self.build_login()  # ✅ Keep this at the end of __init__

    # Title: Helper — Disable Enter/Numpad-Enter globally for the current page
    def _disable_enter_global(self):
        """
        Suppress Enter and Numpad-Enter globally for the current window.
        This prevents any default-button 'Submit' or unintended actions from firing.
        """
        def _sink(_ev=None):
            return "break"  # stop event propagation (no submit)
    
        # Remove any prior Return bindings, then block globally
        try:
            self.root.unbind("<Return>")
            self.root.unbind("<KP_Enter>")
            self.root.unbind_all("<Return>")
            self.root.unbind_all("<KP_Enter>")
        except Exception:
            pass
    
        # Block on the whole app window
        self.root.bind_all("<Return>", _sink)
        self.root.bind_all("<KP_Enter>", _sink)
    
        print("[KEY] Enter/Numpad-Enter disabled globally for this page.")
        print("[CONCLUSION] Keyboard Enter will not trigger Submit/actions on this page.")



    
    # Title: Default button binding helper (binds Return/Numpad Enter to a button)
    def set_default_button(self, btn):
        # Unbind any existing Return/Numpad-Enter handlers
        try:
            self.root.unbind("<Return>")
            self.root.unbind("<KP_Enter>")
        except Exception:
            pass
        # If a button is provided, bind Return/Numpad-Enter to invoke it
        if btn:
            self.root.bind("<Return>",   lambda _ev: btn.invoke())
            self.root.bind("<KP_Enter>", lambda _ev: btn.invoke())


    # ✅ Move these OUTSIDE __init__, as class methods:
    def _current_week_dates(self) -> list[str]:
        """Return ISO dates (YYYY-MM-DD) for selected week; 7 days."""
        try:
            y = int(self.week_year.get())
            w = int(self.week_no.get())
        except Exception:
            y, w = date.today().isocalendar().year, date.today().isocalendar().week
            self.week_year.set(str(y))
            self.week_no.set(str(w))

        # --- ISO week clamp: avoid invalid Week 53 for years that have only 52 weeks ---
        max_w = date(y, 12, 28).isocalendar().week   # ISO last week number for ISO-year y
        if w < 1:
            w = 1
        if w > max_w:
            w = max_w
            self.week_no.set(str(w))
        
        return week_date_range(y, w)


    def _iso_dates_to_dd_mmm(self, iso_dates: list[str]) -> list[str]:
        """Map ISO dates to dd-MMM-yy used as day columns in wide files."""
        return [iso_to_dd_mmm_yy(d) for d in iso_dates]

    # -- Header --
    def header(self, title, subtitle=""):
        hdr = ttk.Frame(self.root, style="Header.TFrame")
        hdr.pack(fill="x")
        ttk.Label(hdr, text=title, style="Header.TLabel").pack(side="left", padx=16, pady=10)
        if subtitle:
            ttk.Label(hdr, text=subtitle, style="SubHeader.TLabel").pack(side="left", padx=10, pady=10)
        
        ttk.Button(hdr, text="Change Password", command=self.open_change_password).pack(side="right", padx=10, pady=10)
        ttk.Button(hdr, text="Sign out", command=self.build_login).pack(side="right", padx=14, pady=10)

    def open_change_password(self):
        """
        Title: Change Password Popup (Self-Service) + Eye Toggle (Show/Hide)
        Users/Admins/Superadmin can change ONLY their own password after verifying current password.
        Updates users.csv -> password_hash (SHA256).
        """
    
        # --- Popup window ---
        top = tk.Toplevel(self.root)
        top.title("Change Password")
        top.resizable(False, False)
        top.transient(self.root)
    
        # Make modal
        try:
            top.grab_set()
        except Exception:
            pass
    
        frm = ttk.Frame(top, padding=(16, 14, 16, 14))
        frm.pack(fill="both", expand=True)
    
        # --- Variables ---
        var_current = tk.StringVar()
        var_new = tk.StringVar()
        var_confirm = tk.StringVar()
    
        # --- Helper: Toggle show/hide for a single Entry ---
        def _toggle_eye(entry: ttk.Entry, btn: ttk.Button, state_var: tk.BooleanVar):
            """
            Toggle password visibility for one Entry.
            state_var = True means visible, False means hidden
            """
            if state_var.get():   # currently visible -> hide
                entry.configure(show="*")
                btn.configure(text="👁")     # eye = show
                state_var.set(False)
            else:                 # currently hidden -> show
                entry.configure(show="")
                btn.configure(text="🙈")     # covered eyes = hide
                state_var.set(True)
    
        # --- UI layout (3 rows + eye buttons) ---
        # Grid columns: 0=label, 1=entry, 2=eye button
        frm.grid_columnconfigure(0, weight=0)
        frm.grid_columnconfigure(1, weight=1)
        frm.grid_columnconfigure(2, weight=0)
    
        # Row 0: Current
        ttk.Label(frm, text="Current Password").grid(row=0, column=0, sticky="e", padx=(0, 10), pady=6)
        e_current = ttk.Entry(frm, textvariable=var_current, show="*", width=30)
        e_current.grid(row=0, column=1, pady=6, sticky="we")
        eye_cur_state = tk.BooleanVar(value=False)
        btn_eye_cur = ttk.Button(frm, text="👁", width=3,
                                 command=lambda: _toggle_eye(e_current, btn_eye_cur, eye_cur_state))
        btn_eye_cur.grid(row=0, column=2, padx=(8, 0), pady=6)
    
        # Row 1: New
        ttk.Label(frm, text="New Password").grid(row=1, column=0, sticky="e", padx=(0, 10), pady=6)
        e_new = ttk.Entry(frm, textvariable=var_new, show="*", width=30)
        e_new.grid(row=1, column=1, pady=6, sticky="we")
        eye_new_state = tk.BooleanVar(value=False)
        btn_eye_new = ttk.Button(frm, text="👁", width=3,
                                 command=lambda: _toggle_eye(e_new, btn_eye_new, eye_new_state))
        btn_eye_new.grid(row=1, column=2, padx=(8, 0), pady=6)
    
        # Row 2: Confirm
        ttk.Label(frm, text="Confirm New Password").grid(row=2, column=0, sticky="e", padx=(0, 10), pady=6)
        e_confirm = ttk.Entry(frm, textvariable=var_confirm, show="*", width=30)
        e_confirm.grid(row=2, column=1, pady=6, sticky="we")
        eye_conf_state = tk.BooleanVar(value=False)
        btn_eye_conf = ttk.Button(frm, text="👁", width=3,
                                  command=lambda: _toggle_eye(e_confirm, btn_eye_conf, eye_conf_state))
        btn_eye_conf.grid(row=2, column=2, padx=(8, 0), pady=6)
    
        # --- Save action ---
        def do_save():
            username = str(self.user.get("username", "")).strip()
            if not username:
                messagebox.showerror("Error", "No logged-in user context found.")
                return
    
            cur = (var_current.get() or "").strip()
            new = (var_new.get() or "").strip()
            conf = (var_confirm.get() or "").strip()
    
            # Basic validations
            if not cur or not new or not conf:
                messagebox.showwarning("Validation", "All fields are required.")
                return
            if new != conf:
                messagebox.showerror("Mismatch", "New password and confirm password do not match.")
                return
            if len(new) < 6:
                messagebox.showwarning("Weak Password", "Please use at least 6 characters.")
                return
            if new == cur:
                messagebox.showwarning("Validation", "New password must be different from current password.")
                return
    
            # Load users and verify current password
            df = load_df(USERS_CSV, USERS_COLUMNS)
            if df.empty:
                messagebox.showerror("Error", "users.csv is empty or missing.")
                return
    
            m = df["username"].astype(str).str.strip().str.lower() == username.lower()
            if not m.any():
                messagebox.showerror("Error", f"User '{username}' not found in users.csv.")
                return
    
            idx = df.index[m][0]
            stored_hash = str(df.loc[idx, "password_hash"]).strip()
            if stored_hash != hash_password(cur):
                messagebox.showerror("Invalid", "Current password is incorrect.")
                return
    
            # Update password hash
            df.loc[idx, "password_hash"] = hash_password(new)
    
            # Save back
            save_df(USERS_CSV, df, USERS_COLUMNS)
    
            messagebox.showinfo("Success", "Password changed successfully.")
            try:
                top.grab_release()
            except Exception:
                pass
            top.destroy()
    
            print(f"[PASSWORD] User '{username}' changed password successfully.")
            print("[CONCLUSION] users.csv updated with new password_hash. Eye-toggle UI enabled.")
    
        # Buttons row
        btns = ttk.Frame(frm)
        btns.grid(row=3, column=0, columnspan=3, sticky="e", pady=(12, 0))
    
        ttk.Button(btns, text="Cancel", command=top.destroy).pack(side="right", padx=(6, 0))
        ttk.Button(btns, text="Save", style="Accent.TButton", command=do_save).pack(side="right")
    
        # Focus
        e_current.focus_set()
    
        # Prevent Enter key from triggering unwanted actions
        top.bind("<Return>", lambda e: "break")
        top.bind("<KP_Enter>", lambda e: "break")

    
    # === [PATCH · Helper] Build assignee choices: all 'user' + 'admin' minus current login admin ===
    def _assignee_choices(self):
        df = load_df(USERS_CSV, USERS_COLUMNS)
        if df.empty:
            return []
    
        # keep only user/admin roles (future-safe) and drop the logged-in admin
        me = str(self.user.get("username", "")).strip().lower()
        df["username"] = df["username"].astype(str)
        df["role"] = df.get("role", "")
        df = df[df["role"].isin(["user", "admin", "superadmin"])]
        df = df[df["username"].str.strip().str.lower() != me]
    
        # unique, sorted (case-insensitive)
        # Build "username - full_name" display
        df["display"] = df.apply(
            lambda r: f"{r['username']} - {r['full_name']}", axis=1
        )
    
        # Sort by username (case-insensitive)
        choices = sorted(df["display"].dropna().unique().tolist(), key=lambda s: s.lower())
        return choices

    
    print("[PATCH] _assignee_choices helper added.")
    print("[CONCLUSION] Assignee lists will now include all users+admins except the logged-in admin.")

    
    # === [PATCH · Helper] Normalize 'Assigned User' display to pure username ===
    def _normalize_assignee(self, s: str) -> str:
        """
        Accepts 'username' or 'username - full_name' and returns 'username'.
        Defensive for extra spaces/cases.
        """
        s = (s or "").strip()
        if " - " in s:
            return s.split(" - ", 1)[0].strip()
        return s
    print("[PATCH] Assignee normalizer ready.")



    def _pick_date(self, target_var):
        """
        Open a simple calendar picker dialog and set the selected date (YYYY-MM-DD) into target_var.
        """
        try:
            import tkcalendar  # Requires tkcalendar library
        except ImportError:
            messagebox.showerror("Missing Library", "tkcalendar is required for date picking.\nInstall via: pip install tkcalendar")
            return
    
        top = tk.Toplevel(self.root)
        top.title("Select Date")
        cal = tkcalendar.Calendar(top, selectmode="day", date_pattern="yyyy-mm-dd")
        cal.pack(padx=10, pady=10)
    
        def set_date():
            target_var.set(cal.get_date())
            top.destroy()
    
        ttk.Button(top, text="OK", command=set_date).pack(pady=6)


    
    # === [UX · Login Revamp] Rounded card + light-blue button + wind-turbine background ===
    # === [UX · Login · PNG background (cover + focal point) + circular GIF icon (top-left)] ===
    def build_login(self):
        """
        Login page with:
          • Top-centered main title (header bar)
          • Full-window PNG background (CSS 'cover' behaviour) with adjustable focal point
          • Small animated GIF badge at TOP-LEFT with circular mask (no sharp edges)
          • Centered white card containing subtitle, Username, Password, Show/Hide, and Login (Accent.TButton)
        Assets:
          ./assets/login_bg.png       -> page background
          ./assets/login_icon.gif     -> small corner GIF (optional)
        Safe fallbacks:
          - If Pillow not installed or files missing: soft gradient background, static icon if possible.
        """
        # 1) Clear window
        for w in self.root.winfo_children():
            w.destroy()
    
        # 2) Header: main title centered
        header = ttk.Frame(self.root, style="Header.TFrame"); header.pack(fill="x")
        header.grid_columnconfigure(0, weight=1)
        header.grid_columnconfigure(1, weight=0)
        header.grid_columnconfigure(2, weight=1)
        ttk.Label(
            header,
            text="Nordex Global Shared Services (NGSS) \n Competence Center Team", justify="center",
            style="Header.TLabel"
        ).grid(row=0, column=1, padx=16, pady=10, sticky="n")
    
        # 3) Background canvas holder
        holder = ttk.Frame(self.root); holder.pack(fill="both", expand=True)
        cv = tk.Canvas(holder, highlightthickness=0, bd=0, relief="flat")
        cv.pack(fill="both", expand=True)
    
        # 3a) Keep references on self to avoid garbage collection of images/frames
        self._bg_img   = None     # PhotoImage for scaled PNG
        self._bg_item  = None     # canvas id for background
        self._gif_frames = None   # list[PhotoImage]
        self._gif_delays = None   # list[int]
        self._gif_idx    = 0
        self._gif_item   = None   # canvas id for icon


        BG_PATH  = "./assets/login_bg.png"
        GIF_PATH = "./assets/login_icon.gif"


    
        # ------- Helpers: Pillow availability -------
        def _can_pillow():
            try:
                from PIL import Image  # noqa
                return True
            except Exception:
                return False
    
        # ------- Background PNG: 'cover' with focal point -------
        # Focal point (0.0..1.0): which part to keep when cropping after scale-to-cover
        FOCAL_X, FOCAL_Y = 0.60, 0.45   # move right/up slightly so turbines/logo stay visible
    
        _bg_cache = {"pil": None, "size": None}  # cache original PIL image
    
        def _ensure_bg_loaded(path: str) -> bool:
            if not _can_pillow():
                return False
            try:
                if _bg_cache["pil"] is None:
                    from PIL import Image
                    im = Image.open(path).convert("RGBA")
                    _bg_cache["pil"] = im
                    _bg_cache["size"] = im.size
                return True
            except Exception as e:
                print("[LOGIN-BG] Could not load PNG:", e)
                return False
    
        def _render_bg_cover():
            """Draw PNG as full background (cover) with focal crop; fallback to gradient."""
            W = max(1, cv.winfo_width()); H = max(1, cv.winfo_height())
            if _ensure_bg_loaded(BG_PATH):
                from PIL import ImageTk, Image
                src = _bg_cache["pil"]; sw, sh = _bg_cache["size"]
                # scale-to-cover
                scale = max(W / sw, H / sh)
                tw, th = int(sw * scale), int(sh * scale)
                im = src.resize((tw, th), Image.LANCZOS)
                # focal-based crop
                cx = int(FOCAL_X * tw); cy = int(FOCAL_Y * th)
                x1 = max(0, min(tw - W, cx - W // 2))
                y1 = max(0, min(th - H, cy - H // 2))
                im = im.crop((x1, y1, x1 + W, y1 + H))
                tk_im = ImageTk.PhotoImage(im)
                self._bg_img = tk_im
                if self._bg_item is None:
                    self._bg_item = cv.create_image(0, 0, image=self._bg_img, anchor="nw", tags=("bg",))
                else:
                    cv.itemconfigure(self._bg_item, image=self._bg_img)
                    cv.coords(self._bg_item, 0, 0)
                cv.tag_lower(self._bg_item)
            else:
                # Soft gradient fallback
                cv.delete("bg")
                stripes = ["#F0F9FF", "#E6F4FF", "#E0F2FE", "#EAF2FB", "#F8FAFC"]
                stripe_h = max(1, int(H/len(stripes)))
                for i, clr in enumerate(stripes):
                    cv.create_rectangle(0, i*stripe_h, W, (i+1)*stripe_h, fill=clr, outline=clr, tags=("bg",))
                cv.tag_lower("bg")
    
        # ------- Small GIF icon: top-left with circular mask -------
        def _load_gif_small_circ(path: str, target=88):
            """Return (frames, delays) where each frame has a circular alpha mask; robust fallback."""
            if _can_pillow():
                try:
                    from PIL import Image, ImageTk, ImageSequence, ImageDraw
                    im = Image.open(path)
                    w, h = im.size
                    s = target / max(1, max(w, h))
                    nw, nh = max(1, int(w*s)), max(1, int(h*s))
    
                    # Anti-aliased circular mask
                    mask = Image.new("L", (nw, nh), 0)
                    draw = ImageDraw.Draw(mask)
                    draw.ellipse((0, 0, nw, nh), fill=255)
    
                    frames, delays = [], []
                    for fr in ImageSequence.Iterator(im):
                        fr = fr.convert("RGBA").resize((nw, nh), Image.LANCZOS)
                        fr.putalpha(mask)  # apply circular transparency
                        frames.append(ImageTk.PhotoImage(fr))
                        delays.append(fr.info.get("duration", 100) if hasattr(fr, "info") else 100)
                    if not frames:
                        return [], []
                    if not delays:
                        delays = [100] * len(frames)
                    return frames, delays
                except Exception as e:
                    print("[LOGIN-GIF] PIL load failed:", e)
    
            # Fallback without PIL: try a static GIF/PNG via Tk PhotoImage (no circular mask possible)
            try:
                pic = tk.PhotoImage(file=path)
                return [pic], [200]
            except Exception as e:
                print("[LOGIN-GIF] Static load failed:", e)
                return [], []
    
        def _place_gif_top_left():
            """Place/keep the small icon at top-left with margin."""
            if not (self._gif_frames and len(self._gif_frames) > 0):
                return
            W = max(1, cv.winfo_width()); H = max(1, cv.winfo_height())
            margin = 16
            x, y = margin, margin
            if self._gif_item is None:
                self._gif_item = cv.create_image(x, y, image=self._gif_frames[0], anchor="nw", tags=("gif",))
            else:
                cv.coords(self._gif_item, x, y)
                cv.itemconfigure(self._gif_item, image=self._gif_frames[self._gif_idx])
            cv.tag_raise(self._gif_item)
    
        def _tick_gif():
            if self._gif_frames and self._gif_item is not None and self._gif_delays:
                self._gif_idx = (self._gif_idx + 1) % len(self._gif_frames)
                try:
                    cv.itemconfigure(self._gif_item, image=self._gif_frames[self._gif_idx])
                except Exception:
                    pass
                delay = self._gif_delays[self._gif_idx] if self._gif_delays else 120
                cv.after(max(40, delay), _tick_gif)
            else:
                # try again later (useful if frames were not ready during first render)
                cv.after(300, _tick_gif)
    
        # ------- Redraw hooks -------
        def _on_resize(_e=None):
            _render_bg_cover()
            _place_gif_top_left()
    
        cv.bind("<Configure>", _on_resize)
        # First paints
        self.root.after(60, _render_bg_cover)
    
        # Load GIF & start animation
        frames, delays = _load_gif_small_circ(GIF_PATH, target=88)
        if frames:
            self._gif_frames, self._gif_delays, self._gif_idx = frames, delays, 0
            self.root.after(80, _place_gif_top_left)
            self.root.after(160, _tick_gif)
    
        # 4) Center login card (thin border + white card)
        border = tk.Frame(cv, bg="#E5E7EB", bd=0, highlightthickness=0)
        card   = ttk.Frame(border, padding=(24, 20, 24, 22))
        try:
            st = ttk.Style(self.root)
            st.configure("Card.TFrame", background="#FFFFFF")
            st.configure("Card.TLabel", background="#FFFFFF")
            card.configure(style="Card.TFrame")
        except Exception:
            pass
        card.pack(padx=1, pady=1)
    
        # Place card at canvas center and keep above background/gif
        card_item = cv.create_window(0, 0, window=border, anchor="center")
        def _recenter_card(_=None):
            W, H = cv.winfo_width(), cv.winfo_height()
            cv.coords(card_item, W//2, H//2)
            cv.tag_raise(card_item)
        cv.bind("<Configure>", _recenter_card, add="+")
        self.root.after(70, _recenter_card)
    
        # 5) Subtitle inside card
        ttk.Label(card, text="Welcome \n Task & Timesheet Portal", justify="center", anchor ="center",
                  font=("Segoe UI Semibold", 13), style="Card.TLabel").grid(row=0, column=0, columnspan=3, sticky="n", pady=(0, 10))
    
        # 6) Fields
        ttk.Label(card, text="Username", style="Card.TLabel") \
            .grid(row=1, column=0, sticky="e", padx=(0,8), pady=6)
        self.var_user = tk.StringVar()
        e_user = ttk.Entry(card, textvariable=self.var_user, width=32)
        e_user.grid(row=1, column=1, columnspan=2, sticky="we", pady=6)
    
        ttk.Label(card, text="Password", style="Card.TLabel") \
            .grid(row=2, column=0, sticky="e", padx=(0,8), pady=6)
        self.var_pass = tk.StringVar()
        e_pass = ttk.Entry(card, textvariable=self.var_pass, show="*", width=32)
        e_pass.grid(row=2, column=1, sticky="we", pady=6)
    
        # Show/Hide toggle (text-based for reliability across systems)
        def toggle_pw():
            if e_pass.cget("show") == "":
                e_pass.configure(show="*"); btn_toggle.configure(text="Show")
            else:
                e_pass.configure(show="");  btn_toggle.configure(text="Hide")
        btn_toggle = ttk.Button(card, text="Show", width=6, command=toggle_pw)
        btn_toggle.grid(row=2, column=2, sticky="w", padx=(6,0), pady=6)
    
        # 7) Login button (uses global light-blue Accent.TButton from apply_theme)
        def do_login():
            info = verify_user(self.var_user.get().strip(), self.var_pass.get().strip())
            if info:
                self.user = info
                self.is_superadmin = (str(info.get("role","")).lower() == "superadmin")
                log_event("LOGIN", f"{info['username']} ({info['role']}, {info['team']}) signed in")
                (self.build_admin if info["role"] in ["admin","superadmin"] else self.build_user)()
            else:
                messagebox.showerror("Login failed", "Invalid username or password.")
        ttk.Button(card, text="Login", style="Accent.TButton", command=do_login) \
            .grid(row=3, column=0, columnspan=3, sticky="we", pady=(14, 0))

        
    
        # 8) Column sizing & default focus
        for c in (0,1,2):
            card.grid_columnconfigure(c, weight=1 if c == 1 else 0, minsize=0)
        e_user.focus_set()
    
        # 9) Default button on Enter (if helper exists)
        try:
            if hasattr(self, "set_default_button"):
                for ch in card.winfo_children():
                    if isinstance(ch, ttk.Button) and ch.cget("text") == "Login":
                        self.set_default_button(ch)
                        break
        except Exception:
            pass


    # -- Admin --
    def build_admin(self):
        for w in self.root.winfo_children(): w.destroy()
        self.header("Task & Timesheet Portal — Admin Dashboard",
                    f"Welcome, {self.user['full_name']}")
    
        nb = ttk.Notebook(self.root); nb.pack(fill="both", expand=True, padx=10, pady=10)
        tab_users     = ttk.Frame(nb); nb.add(tab_users, text="User Management")
        tab_tasks     = ttk.Frame(nb); nb.add(tab_tasks, text="Task Management")
        tab_approvals = ttk.Frame(nb); nb.add(tab_approvals, text="Timesheet Approvals")
        tab_settings  = ttk.Frame(nb); nb.add(tab_settings, text="Settings")


        # NEW: Admin self‑service tabs (reuse user pages)
        tab_self_tasks = ttk.Frame(nb); nb.add(tab_self_tasks, text="My Tasks (Admin Self)")
        tab_self_ts    = ttk.Frame(nb); nb.add(tab_self_ts,    text="My Timesheet (Admin Self)")


        
        # Build each tab safely (keeps your existing error handling style)
        def _safe_build(name, fn, tab):
            try:
                fn(tab)
            except Exception as e:
                import traceback; traceback.print_exc()
                ttk.Label(tab, text=f"Failed to load {name}: {e}",
                          foreground="red").pack(anchor="w", padx=10, pady=10)
    
        _safe_build("User Management", self.admin_users, tab_users)
        _safe_build("Task Management", self.admin_tasks, tab_tasks)
        _safe_build("Timesheet Approvals", self.admin_approvals, tab_approvals)
        _safe_build("Settings", self.admin_settings, tab_settings)

        
        # NEW: these reuse the exact user pages but filtered to the logged-in admin
        _safe_build("My Tasks (Self)",        self.user_tasks,        tab_self_tasks)
        _safe_build("My Timesheet (Self)",    self.user_timesheet,    tab_self_ts)

    
        print("[ADMIN] All tabs attempted; errors (if any) printed above.")
        print("[CONCLUSION] One tab error won’t hide Approvals/Settings anymore.")

        
    # --- Admin: Users (unchanged from your working version) ---
    def admin_users(self, parent):
        cols = ("username","full_name","role","team")
        self.tree_users = ttk.Treeview(parent, columns=cols, show="headings", height=12)
        for c in cols:
            self.tree_users.heading(c, text=c.replace("_"," ").title())
            self.tree_users.column(c, width=220 if c not in ("role","team") else 140, anchor="center")
        self.tree_users.grid(row=0, column=0, columnspan=4, sticky="nsew", padx=6, pady=6)
        parent.grid_rowconfigure(0, weight=1); parent.grid_columnconfigure(0, weight=1)

        frm = ttk.LabelFrame(parent, text="Add / Edit User"); frm.grid(row=1, column=0, sticky="ew", padx=6, pady=6)
        self.u_username = tk.StringVar(); self.u_fullname = tk.StringVar()
        self.u_role = tk.StringVar(value="user"); self.u_team = tk.StringVar(value=DEFAULT_DEPT)
        self.u_password = tk.StringVar()
        ttk.Label(frm, text="Username").grid(row=0, column=0, padx=6, pady=4, sticky="e")
        ttk.Entry(frm, textvariable=self.u_username, width=25).grid(row=0, column=1, padx=6, pady=4)
        ttk.Label(frm, text="Full name").grid(row=0, column=2, padx=6, pady=4, sticky="e")
        ttk.Entry(frm, textvariable=self.u_fullname, width=25).grid(row=0, column=3, padx=6, pady=4)
        ttk.Label(frm, text="Role").grid(row=1, column=0, padx=6, pady=4, sticky="e")
        ttk.Combobox(frm, textvariable=self.u_role, values=["superadmin","admin","user"], width=22, state="readonly").grid(row=1, column=1, padx=6, pady=4)
        ttk.Label(frm, text="Team").grid(row=1, column=2, padx=6, pady=4, sticky="e")
        
        ttk.Combobox(frm,
                     textvariable=self.u_team, 
                     values=TEAM_CHOICES, 
                     width=22, state="readonly" # <-- blocks typing; selection only
                    ).grid(row=1, column=3, padx=6, pady=4)

        ttk.Label(frm, text="Password").grid(row=2, column=0, padx=6, pady=4, sticky="e")
        ttk.Entry(frm, textvariable=self.u_password, width=25, show="*").grid(row=2, column=1, padx=6, pady=4)

        def refresh_users():
            self.tree_users.delete(*self.tree_users.get_children())
            df = load_df(USERS_CSV, USERS_COLUMNS)
            if not getattr(self, "is_superadmin", False):
                my_team = str(self.user.get("team","")).strip()
                df = df[df["team"].astype(str).str.strip() == my_team].copy()

            if not df.empty:
                for _, r in df.iterrows():
                    self.tree_users.insert("", "end", values=(r["username"], r["full_name"], r["role"], r["team"]))
            zebra_tree(self.tree_users, even=self.palette["grid_even"], odd=self.palette["grid_odd"])

        def add_user():
            u = self.u_username.get().strip()
            if not u: return messagebox.showerror("Validation","Username required.")
            df = load_df(USERS_CSV, USERS_COLUMNS)
            if (df["username"]==u).any(): return messagebox.showerror("Exists","Username already exists.")
            row = {"username":u,"full_name":self.u_fullname.get().strip(),"role":self.u_role.get(),
                   "team": self.u_team.get().strip() or DEFAULT_DEPT,
                   "password_hash": hash_password(self.u_password.get().strip() or "changeme")}

            new_role = str(self.u_role.get()).strip().lower()
            if new_role == "superadmin" and not getattr(self, "is_superadmin", False):
                return messagebox.showerror("Blocked", "Only Superadmin can create another Superadmin.")

            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
            save_df(USERS_CSV, df, USERS_COLUMNS); refresh_users(); messagebox.showinfo("Success","User added.")

        def on_select_user(_):
            sel = self.tree_users.selection()
            if not sel: return
            vals = self.tree_users.item(sel[0])["values"]
            self.u_username.set(vals[0]); self.u_fullname.set(vals[1]); self.u_role.set(vals[2]); self.u_team.set(vals[3]); self.u_password.set("")
        self.tree_users.bind("<<TreeviewSelect>>", on_select_user)

        def update_user():
            u = self.u_username.get().strip()
            if not u: return messagebox.showerror("Validation","Select or enter username.")
            df = load_df(USERS_CSV, USERS_COLUMNS)
            if not (df["username"]==u).any(): return messagebox.showerror("Not found","User not found.")
            idx = df.index[df["username"]==u][0]
            df.loc[idx,"full_name"] = self.u_fullname.get().strip()
            df.loc[idx,"role"] = self.u_role.get()
            df.loc[idx,"team"] = self.u_team.get().strip() or DEFAULT_DEPT
            if self.u_password.get().strip(): df.loc[idx,"password_hash"] = hash_password(self.u_password.get().strip())
            save_df(USERS_CSV, df, USERS_COLUMNS); refresh_users(); messagebox.showinfo("Success","User updated.")

        def delete_user():
            sel = self.tree_users.selection()
            if not sel: return messagebox.showwarning("Select","Select user to delete.")
            vals = self.tree_users.item(sel[0])["values"]
            if vals[0]=="admin": return messagebox.showerror("Blocked","Cannot delete built-in admin.")
            df = load_df(USERS_CSV, USERS_COLUMNS)
            df = df[df["username"]!=vals[0]]
            save_df(USERS_CSV, df, USERS_COLUMNS); refresh_users()

        btns = ttk.Frame(parent); btns.grid(row=2, column=0, sticky="e", padx=6, pady=6)
        ttk.Button(btns, text="Add", command=add_user).pack(side="left", padx=5)
        ttk.Button(btns, text="Update", command=update_user).pack(side="left", padx=5)
        ttk.Button(btns, text="Delete", command=delete_user).pack(side="left", padx=5)
        ttk.Button(btns, text="Refresh", command=refresh_users).pack(side="left", padx=5)
        refresh_users()

    # --- Admin: Tasks (same as your working version; kept intact) ---
    def admin_tasks(self, parent):
        # --- START unchanged context you already have ---
        udf = load_df(USERS_CSV, USERS_COLUMNS)
        tdf = load_df(TASKS_CSV, TASKS_COLUMNS)
    
        # (kept) dept values logic
        dept_values = sorted(list(set(([DEFAULT_DEPT] if DEFAULT_DEPT else [])
                                      + tdf.get("team", pd.Series(dtype=str))
                                          .dropna().astype(str).tolist())))
        if DEFAULT_DEPT and DEFAULT_DEPT not in dept_values:
            dept_values = [DEFAULT_DEPT] + dept_values
        status_values = ["All", "Not Started", "In Progress", "Completed", "Closed"]
        # --- END unchanged context ---
    
        # NEW: assignee choices = all users + admins, excluding current login admin
        user_list = self._assignee_choices()
    
        # ---------- Filters ----------
        filt = ttk.LabelFrame(parent, text="Filters")
        filt.grid(row=0, column=0, columnspan=6, sticky="ew", padx=6, pady=(6,0))
        self.t_filter_dept = tk.StringVar(value="All")
        self.t_filter_user = tk.StringVar(value="All")
        self.t_filter_status = tk.StringVar(value="All")
        self.t_filter_text = tk.StringVar(value="")
    
        ttk.Label(filt, text="Team").pack(side="left", padx=(8,4))
        ttk.Combobox(filt, textvariable=self.t_filter_dept,
                     values=["All"] + dept_values, width=18, state="readonly").pack(side="left", padx=4)
    
        ttk.Label(filt, text="Assigned User").pack(side="left", padx=(12,4))
        # keep a handle so we can refresh values later
        self.cb_filter_user = ttk.Combobox(
            filt, textvariable=self.t_filter_user,
            values=["All"] + user_list, width=16, state="readonly"
        )
        self.cb_filter_user.pack(side="left", padx=4)
    
        ttk.Label(filt, text="Status").pack(side="left", padx=(12,4))
        ttk.Combobox(filt, textvariable=self.t_filter_status,
                     values=status_values, width=14, state="readonly").pack(side="left", padx=4)
    
        ttk.Label(filt, text="Search (ID/Name/Desc)").pack(side="left", padx=(12,4))
        ttk.Entry(filt, textvariable=self.t_filter_text, width=28).pack(side="left", padx=4)
    
        ttk.Button(filt, text="Apply", command=self.admin_task_refresh).pack(side="left", padx=6)
        ttk.Button(
            filt, text="Reset",
            command=lambda: (
                self.t_filter_dept.set("All"),
                self.t_filter_user.set("All"),
                self.t_filter_status.set("All"),
                self.t_filter_text.set(""),
                self.admin_task_refresh()
            )
        ).pack(side="left", padx=4)
    
        # ---------- Grid (unchanged) ----------
        cols = tuple(TASKS_COLUMNS)

        # Frame for Treeview + Scrollbars
        tree_frame = ttk.Frame(parent)
        tree_frame.grid(row=1, column=0, columnspan=6, sticky="nsew", padx=6, pady=6)
        
        # Treeview
        self.tree_tasks = ttk.Treeview(tree_frame, columns=cols, show="headings", height=12, selectmode="extended")
        for c in cols:
            self.tree_tasks.heading(c, text=c.replace("_"," ").title())
            self.tree_tasks.column(c, width=140 if c not in ("task_description","task_name") else 180, anchor="center")
        
        # Scrollbars
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree_tasks.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree_tasks.xview)
        self.tree_tasks.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        # Pack widgets
        self.tree_tasks.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        
        # Configure frame grid
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        self.tree_tasks.grid(row=0, column=0, sticky="nsew")
        parent.grid_rowconfigure(1, weight=1); parent.grid_columnconfigure(0, weight=1)
    
        # ---------- Add/Edit form ----------
        frm = ttk.LabelFrame(parent, text="Add / Edit Task (Dept auto from assignee)")
        frm.grid(row=2, column=0, columnspan=6, sticky="ew", padx=6, pady=6)
        
        # Variables
        self.t_task_id = tk.StringVar()
        self.t_task_name = tk.StringVar()
        self.t_task_desc = tk.StringVar()
        self.t_planned_date = tk.StringVar()
        self.t_planned_hrs = tk.StringVar()
        self.t_assigned = tk.StringVar()
        self.t_team_disp = tk.StringVar(value=DEFAULT_DEPT)
        self.t_status = tk.StringVar(value="Not Started")
        self.t_billing = tk.StringVar(value="HQ")
        self.t_hq_contact = tk.StringVar()
        self.t_priority = tk.StringVar(value="2-Medium")
        self.t_act_delivery = tk.StringVar()
        
        # Row 0
        ttk.Label(frm, text="Task ID").grid(row=0, column=0, padx=6, pady=4, sticky="e")
        ttk.Entry(frm, textvariable=self.t_task_id, width=18).grid(row=0, column=1, padx=6, pady=4, sticky="w")
        ttk.Button(frm, text="New ID(Std)", command=lambda: self.t_task_id.set(generate_unique_task_id(self.t_team_disp.get()))).grid(row=0, column=2, padx=6, pady=4)
        ttk.Label(frm, text="Billing Code").grid(row=0, column=3, padx=6, pady=4, sticky="e")
        ttk.Combobox(frm, textvariable=self.t_billing, values=["HQ","CH","DL","NJ","WK"], width=12, state="readonly").grid(row=0, column=4, padx=6, pady=4, sticky="w")
        ttk.Label(frm, text="Team").grid(row=0, column=5, padx=6, pady=4, sticky="e")
        ttk.Entry(frm, textvariable=self.t_team_disp, width=20, state="readonly").grid(row=0, column=6, padx=6, pady=4, sticky="w")
        
        # Row 1
        ttk.Label(frm, text="Task Name").grid(row=1, column=0, padx=6, pady=4, sticky="e")
        ttk.Entry(frm, textvariable=self.t_task_name, width=25).grid(row=1, column=1, padx=6, pady=4, sticky="w")
        ttk.Label(frm, text="Assigned User").grid(row=1, column=2, padx=6, pady=4, sticky="e")
        self.cb_assigned_user = ttk.Combobox(frm, textvariable=self.t_assigned, values=self._assignee_choices(), width=18, state="readonly")
        self.cb_assigned_user.grid(row=1, column=3, padx=6, pady=4, sticky="w")
        ttk.Label(frm, text="Task Status").grid(row=1, column=4, padx=6, pady=4, sticky="e")
        ttk.Combobox(frm, textvariable=self.t_status, values=["Not Started","In Progress","Completed","Closed"], width=18, state="readonly").grid(row=1, column=5, padx=6, pady=4, sticky="w")
        ttk.Label(frm, text="Priority").grid(row=1, column=6, padx=6, pady=4, sticky="e")
        ttk.Combobox(frm, textvariable=self.t_priority, values=["1-Low","2-Medium","3-High"], width=18, state="readonly").grid(row=1, column=7, padx=6, pady=4, sticky="w")
        
        # Row 2 (Task Description spans all columns)
        ttk.Label(frm, text="Task Description").grid(row=2, column=0, padx=6, pady=4, sticky="e")
        ttk.Entry(frm, textvariable=self.t_task_desc, width=90).grid(row=2, column=1, columnspan=7, padx=6, pady=4, sticky="w")
        
        # Row 3
        ttk.Label(frm, text="Planned Target Date").grid(row=3, column=0, padx=6, pady=4, sticky="e")
        ttk.Entry(frm, textvariable=self.t_planned_date, width=18).grid(row=3, column=1, padx=6, pady=4, sticky="w")
        ttk.Button(frm, text="Pick Date", command=lambda: self._pick_date(self.t_planned_date)).grid(row=3, column=2, padx=6, pady=4)
        ttk.Label(frm, text="Planned Hours").grid(row=3, column=3, padx=6, pady=4, sticky="e")
        ttk.Entry(frm, textvariable=self.t_planned_hrs, width=10).grid(row=3, column=4, padx=6, pady=4, sticky="w")
        ttk.Label(frm, text="HQ Contact Partner").grid(row=3, column=5, padx=6, pady=4, sticky="e")
        ttk.Entry(frm, textvariable=self.t_hq_contact, width=25).grid(row=3, column=6, padx=6, pady=4, sticky="w")
        ttk.Label(frm, text="Actual Delivery Date").grid(row=3, column=7, padx=6, pady=4, sticky="e")

        # Actual Delivery Date widgets with references
        self.entry_act_delivery_admin = ttk.Entry(frm, textvariable=self.t_act_delivery, width=18, state="disabled")
        self.entry_act_delivery_admin.grid(row=3, column=8, padx=6, pady=4, sticky="w")
        
        self.btn_pick_act_delivery_admin = ttk.Button(frm, text="Pick Date", command=lambda: self._pick_date(self.t_act_delivery), state="disabled")
        self.btn_pick_act_delivery_admin.grid(row=3, column=9, padx=6, pady=4)
        
        # Bind status change to enable/disable Actual Delivery Date
        def toggle_admin_act_delivery(*_):
            if self.t_status.get() == "Completed":
                self.entry_act_delivery_admin.configure(state="normal")
                self.btn_pick_act_delivery_admin.configure(state="normal")
            else:
                self.entry_act_delivery_admin.configure(state="disabled")
                self.btn_pick_act_delivery_admin.configure(state="disabled")
        
        self.t_status.trace_add("write", toggle_admin_act_delivery)
        toggle_admin_act_delivery()  # Initial state



        # auto-update team from assignee
        def update_dept_on_assignee(*_):
            self.t_team_disp.set(user_team(self.t_assigned.get().strip()) if self.t_assigned.get().strip() else DEFAULT_DEPT)
        self.t_assigned.trace_add("write", update_dept_on_assignee)
    
        # CSV Ops (unchanged)
        csv_ops = ttk.LabelFrame(parent, text="CSV Operations")
        csv_ops.grid(row=3, column=0, columnspan=6, sticky="ew", padx=6, pady=6)
        ttk.Button(csv_ops, text="Export Tasks", command=self.export_tasks_csv).pack(side="left", padx=4, pady=4)
        ttk.Button(csv_ops, text="Import Tasks", command=self.import_tasks_csv).pack(side="left", padx=4, pady=4)
        ttk.Button(csv_ops, text="Download Tasks Template", command=self.download_tasks_template).pack(side="left", padx=4, pady=4)
    
        btns = ttk.Frame(parent); btns.grid(row=4, column=0, columnspan=6, sticky="e", padx=6, pady=6)
        ttk.Button(btns, text="Add/Update", style="Accent.TButton", command=self.admin_task_save).pack(side="left", padx=5)
        ttk.Button(btns, text="Delete", style="Danger.TButton", command=self.admin_task_delete).pack(side="left", padx=5)
        ttk.Button(btns, text="Refresh", command=self.admin_task_refresh).pack(side="left", padx=5)
    
        def on_sel(_):
            sel = self.tree_tasks.selection()
            if not sel:
                return
        
            vals = self.tree_tasks.item(sel[0])["values"]
            cols = list(self.tree_tasks["columns"])
        
            # Build a {column_name: value} dict safely
            row = {c: (str(vals[i]) if i < len(vals) else "") for i, c in enumerate(cols)}
        
            # Populate the Admin form fields using column names (not positions)
            self.t_task_id.set(row.get("task_id", ""))
        
            # Billing code with safe default
            self.t_billing.set(row.get("billing_code", "HQ") or "HQ")
        
            self.t_task_name.set(row.get("task_name", ""))
            self.t_task_desc.set(row.get("task_description", ""))
        
            self.t_planned_date.set(row.get("planned_target_date", ""))
            self.t_planned_hrs.set(row.get("planned_hours", ""))
        
            # Assigned user + team display (team falls back to DEFAULT_DEPT)
            self.t_assigned.set(row.get("assigned_user", ""))
            self.t_team_disp.set(row.get("team", "") or DEFAULT_DEPT)
        
            # Status with safe default
            self.t_status.set(row.get("task_status", "Not Started") or "Not Started")
        
            # NEW fields also filled correctly
            self.t_hq_contact.set(row.get("hq_contact_partner", ""))
            self.t_priority.set(row.get("priority", "2-Medium"))
            self.t_act_delivery.set(row.get("act_delivery_date", ""))
        
            # Output / Conclusion
            print(f"[INFO] Selected Task ID: {self.t_task_id.get()} | Team: {self.t_team_disp.get()} | Status: {self.t_status.get()}")
            print("[CONCLUSION] Admin selection now reads by column names, so no more unpacking errors even with 13 columns.")

        self.tree_tasks.bind("<<TreeviewSelect>>", on_sel)
    
        # first load
        self.admin_task_refresh()
        print("[Admin Tasks] Built with manual task_status and no actual_* fields.")
     
        # -- END original from your file --

    def admin_task_refresh(self):
        # original body kept (unchanged) from your file
        df = load_df(TASKS_CSV, TASKS_COLUMNS)
        if not getattr(self, "is_superadmin", False):
            my_team = str(self.user.get("team","")).strip()
            df = df[df["team"].astype(str).str.strip() == my_team].copy()

        # === NEW: refresh filter/form assignee dropdowns ===
        try:
            choices = self._assignee_choices()
            if hasattr(self, "cb_filter_user"):
                # preserve current selection if still valid, else fallback to "All"
                current = self.t_filter_user.get()
                self.cb_filter_user["values"] = ["All"] + choices
                if current not in (["All"] + choices):
                    self.t_filter_user.set("All")
            if hasattr(self, "cb_assigned_user"):
                current_ass = self.t_assigned.get()
                self.cb_assigned_user["values"] = choices
                if current_ass and current_ass not in choices:
                    self.t_assigned.set("")  # force a new valid selection
        except Exception:
            pass

            
        self.tree_tasks.delete(*self.tree_tasks.get_children())
        if not df.empty:
            # Normalize task_status
            if "task_status" not in df.columns:
                df["task_status"] = "Not Started"
            df["task_status"] = df["task_status"].fillna("").replace("", "Not Started")
        
            # Auto-fix team based on assigned_user (your existing logic)
            changed = False
            for i, r in df.iterrows():
                ass = r["assigned_user"]
                dep = user_team(ass) if ass else DEFAULT_DEPT
                if (r.get("team","") or "") != dep:
                    df.loc[i, "team"] = dep; changed = True
            if changed:
                save_df(TASKS_CSV, df, TASKS_COLUMNS)
        
            dflt = df.copy()
        
            # Team filter
            f_dept = getattr(self, "t_filter_dept", tk.StringVar(value="All")).get()
            if f_dept and f_dept != "All":
                dflt = dflt[dflt["team"].astype(str) == f_dept]
        
            # Assigned User filter
            f_user_disp = getattr(self, "t_filter_user", tk.StringVar(value="All")).get()
            if f_user_disp and f_user_disp != "All":
                # normalize display to pure username before filtering
                f_user = self._normalize_assignee(f_user_disp)
                dflt = dflt[dflt["assigned_user"].astype(str).str.strip().str.lower() == f_user.strip().lower()]

        
            # Status filter (custom behavior)
            f_status = getattr(self, "t_filter_status", tk.StringVar(value="All")).get()
            if f_status and f_status != "All":
                dflt = dflt[dflt["task_status"].astype(str) == f_status]
            else:
                # "All" → exclude Closed
                allowed = {"Not Started","In Progress","Completed"}
                dflt = dflt[dflt["task_status"].isin(allowed)]
        
            # Text search across ID/Name/Description
            f_text = getattr(self, "t_filter_text", tk.StringVar(value="")).get().strip().lower()
            if f_text:
                m = (
                    dflt["task_id"].astype(str).str.lower().str.contains(f_text, na=False)
                    | dflt["task_name"].astype(str).str.lower().str.contains(f_text, na=False)
                    | dflt["task_description"].astype(str).str.lower().str.contains(f_text, na=False)
                )
                dflt = dflt[m]
        
            # Render rows
            for _, r in dflt.iterrows():
                self.tree_tasks.insert("", "end", values=tuple(r[c] for c in self.tree_tasks["columns"]))
        
            zebra_tree(self.tree_tasks, even=self.palette["grid_even"], odd=self.palette["grid_odd"])




    
    
    # --- Admin -> Tasks: Add/Update fix ---
    def admin_task_save(self):
        """
        Save task from Admin form.
        - If Task ID exists in tasks.csv -> UPDATE that row.
        - If Task ID is empty -> ADD (use manual if valid & unused, else auto-generate).
        - No auto-generation should occur when updating an existing ID.
        """
        # Read form
        raw_tid = (self.t_task_id.get() or "").strip()
        ass = self._normalize_assignee(self.t_assigned.get())
        dep     = user_team(ass) if ass else DEFAULT_DEPT

        if not getattr(self, "is_superadmin", False):
            my_team = str(self.user.get("team","")).strip()
            if my_team and dep and dep != my_team:
                messagebox.showerror(
                    "Team Restriction",
                    f"You can assign tasks only within your team.\nYour Team: {my_team}\nAssignee Team: {dep}"
                )
                return

        bill    = (self.t_billing.get() or "HQ").strip()
        if bill not in ("HQ","CH","DL","NJ","WK"):
            bill = "HQ"
    

        row = {
            "task_id": "",
            "billing_code": bill,
            "team": dep,
            "admin_name": self.user["full_name"],
            "task_name": (self.t_task_name.get() or "").strip(),
            "task_description": (self.t_task_desc.get() or "").strip(),
            "planned_target_date": (self.t_planned_date.get() or "").strip(),
            "planned_hours": (self.t_planned_hrs.get() or "").strip(),
            "hq_contact_partner": (self.t_hq_contact.get() or "").strip(),  # NEW
            "priority": (self.t_priority.get() or "2-Medium").strip(),      # NEW
            "act_delivery_date": (self.t_act_delivery.get() or "").strip(), # NEW
            "assigned_user": ass,
            "task_status": (self.t_status.get() or "Not Started").strip() or "Not Started",
        }

    
        # Load tasks
        df = load_df(TASKS_CSV, TASKS_COLUMNS)
    
        # Helper: does this ID already exist (case-insensitive)?
        def id_exists(_tid: str) -> bool:
            if df.empty:
                return False
            return (df["task_id"].astype(str).str.upper() == str(_tid).upper()).any()
    
        # Branch 1: UPDATE existing
        if raw_tid and id_exists(raw_tid):
            tid = str(raw_tid).strip()
            row["task_id"] = tid
    
            m = (df["task_id"].astype(str).str.upper() == tid.upper())
            for k, v in row.items():
                df.loc[m, k] = v
    
            save_df(TASKS_CSV, df, TASKS_COLUMNS)
            self.admin_task_refresh()
    
            # Keep the Task ID field read-only in edit mode (optional; requires entry configured as 'readonly')
            # self.t_task_id_entry.configure(state="readonly")
    
            messagebox.showinfo("Updated", f"Task {tid} updated.")
            log_event("ADMIN-TASKS", f"Updated task_id={tid} by {self.user['username']}")
            return
            
        
        # Branch 2: ADD new
        # If user provided a manual ID, validate & use it if available; else, auto-generate.
        if raw_tid:
            if is_manual_tid(raw_tid) and is_tid_available(raw_tid):
                tid = normalize_or_reject_manual_tid(raw_tid)
                if not tid:
                    return messagebox.showerror(
                        "Invalid Task ID",
                        f"Task ID {raw_tid} is outside the allowed manual range or already used."
                    )
            else:
                # Provided an ID but not in manual range / or collides -> fall back to auto
                tid = generate_unique_task_id(dep)
        else:
            # No ID provided -> auto
            tid = generate_unique_task_id(dep)
    
        row["task_id"] = tid
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
        save_df(TASKS_CSV, df, TASKS_COLUMNS)
        self.admin_task_refresh()
        messagebox.showinfo("Success", f"Task {tid} saved.")
        log_event("ADMIN-TASKS", f"Added task_id={tid} by {self.user['username']}")

        
    def admin_task_delete(self):
        """
        Delete ALL currently selected task rows from tasks.csv.
        Shows a confirmation with a count and the first few Task IDs.
        """
        sels = self.tree_tasks.selection()
        if not sels:
            return messagebox.showwarning("Select", "Select one or more tasks to delete.")
    
        # Collect the Task IDs (first column in 'cols')
        selected_ids = []
        for iid in sels:
            vals = self.tree_tasks.item(iid)["values"]
            if not vals:
                continue
            selected_ids.append(str(vals[0]).strip())
    
        # Guard against empty/only admin-protected logic if any in future
        if not selected_ids:
            return messagebox.showwarning("Select", "Could not resolve selected task IDs.")
    
        # Confirmation message (show first few IDs to avoid huge dialog)
        preview = ", ".join(selected_ids[:5]) + (" ..." if len(selected_ids) > 5 else "")
        if not messagebox.askyesno(
            "Confirm deletion",
            f"Delete {len(selected_ids)} task(s)?\nTask IDs: {preview}"
        ):
            return
    
        # Load current tasks and filter out the selected ones
        df = load_df(TASKS_CSV, TASKS_COLUMNS)
        if df.empty:
            messagebox.showinfo("No Data", "No tasks file found.")
            return
    
        # Case-insensitive compare on task_id
        sel_upper = set(tid.upper() for tid in selected_ids)
        keep_mask = ~df["task_id"].astype(str).str.upper().isin(sel_upper)
        kept = df[keep_mask].copy()
    
        # Save back and refresh the grid
        save_df(TASKS_CSV, kept, TASKS_COLUMNS)
        self.admin_task_refresh()

       
        # Feedback
        deleted_count = len(df) - len(kept)
        messagebox.showinfo("Deleted", f"Deleted {deleted_count} task(s).")

        log_event("ADMIN-TASKS", f"Deleted {deleted_count} task(s) by {self.user['username']}")
    
    def export_tasks_csv(self):
        """
        Export the tasks exactly as shown in the grid:
        Applies Team / Assigned User / Status / Search filters.
        """
        df = load_df(TASKS_CSV, TASKS_COLUMNS)
        if not getattr(self, "is_superadmin", False):
            my_team = str(self.user.get("team","")).strip()
            df = df[df["team"].astype(str).str.strip() == my_team].copy()

        if df.empty:
            return messagebox.showinfo("No Data", "No tasks to export.")
    
        # --- Reapply the same filters used by admin_task_refresh ---
        dflt = df.copy()
    
        # Team filter
        f_dept = getattr(self, "t_filter_dept", tk.StringVar(value="All")).get()
        if f_dept and f_dept != "All":
            dflt = dflt[dflt["team"].astype(str) == f_dept]
    
        # Assigned User filter
        f_user_disp = getattr(self, "t_filter_user", tk.StringVar(value="All")).get()
        if f_user_disp and f_user_disp != "All":
            # normalize display to pure username before filtering
            f_user = self._normalize_assignee(f_user_disp)
            dflt = dflt[dflt["assigned_user"].astype(str).str.strip().str.lower() == f_user.strip().lower()]

    
        # Status filter
        f_status = getattr(self, "t_filter_status", tk.StringVar(value="All")).get()
        if f_status and f_status != "All":
            dflt = dflt[dflt["task_status"].astype(str) == f_status]
        else:
            allowed = {"Not Started","In Progress","Completed"}
            dflt = dflt[dflt["task_status"].isin(allowed)]

    
        # Text search across task_id / name / description
        f_text = getattr(self, "t_filter_text", tk.StringVar(value="")).get().strip().lower()
        if f_text:
            m = (
                dflt["task_id"].astype(str).str.lower().str.contains(f_text, na=False)
                | dflt["task_name"].astype(str).str.lower().str.contains(f_text, na=False)
                | dflt["task_description"].astype(str).str.lower().str.contains(f_text, na=False)
            )
            dflt = dflt[m]
    
        if dflt.empty:
            return messagebox.showinfo("No Data", "No rows to export for the current filter.")
    
        # Save
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            initialfile="tasks_filtered_export.csv",
            filetypes=[("CSV","*.csv")]
        )
        if not path:
            return
    
        # Keep header order consistent with TASKS_COLUMNS
        dflt = dflt[TASKS_COLUMNS]
        dflt.to_csv(path, index=False)
    
        messagebox.showinfo("Exported", f"Exported {len(dflt)} row(s) to:\n{path}")
        log_event("ADMIN-TASKS", "Exported tasks (filtered view)")
        print(f"[Export Tasks] Filtered rows exported: {len(dflt)}")
        print("[CONCLUSION] Export respects current filters and preserves column order.")

    def import_tasks_csv(self):
        path = filedialog.askopenfilename(filetypes=[("CSV","*.csv")])
        if not path: return
        try:
            inc = pd.read_csv(path, dtype=str).fillna("")

            if not getattr(self, "is_superadmin", False):
                my_team = str(self.user.get("team","")).strip()
                if "team" in inc.columns:
                    inc = inc[inc["team"].astype(str).str.strip() == my_team].copy()
                else:
                    # if team column missing, force team to admin team
                    inc["team"] = my_team

        except Exception as e:
            return messagebox.showerror("Read error", f"Unable to read CSV:\n{e}")
            log.exception("Operation failed")
        for c in TASKS_COLUMNS:
            if c not in inc.columns: inc[c]=""
        inc["billing_code"] = inc["billing_code"].apply(lambda v: v if str(v).strip() in ("HQ","CH","DL","NJ","WK") else "HQ")        

        # Normalize assigned_user to pure username first
        inc["assigned_user"] = inc["assigned_user"].astype(str).apply(lambda v: self._normalize_assignee(v))
        
        # If team missing/blank, derive from users.csv
        inc["team"] = inc["team"].astype(str).where(inc["team"].astype(str).str.strip() != "",
                                                    inc["assigned_user"].apply(lambda u: user_team(u)))

        base = load_df(TASKS_CSV, TASKS_COLUMNS)
        import string as _s
        existing = set(base["task_id"].astype(str)) if not base.empty else set()

        
        # === [PATCH · Admin Import] Keep manual IDs if valid; auto otherwise ===

        def norm_tid_row(row):
            s = str(row.get("task_id","")).strip()
        
            # If manual numeric ID and not already used in this import + not in file, keep it
            if is_manual_tid(s) and (s.upper() not in {x.upper() for x in existing}) and is_tid_available(s):
                existing.add(s)
                return s
        
            # Otherwise generate prefixed ID using row team
            team = str(row.get("team", DEFAULT_DEPT) or DEFAULT_DEPT).strip()
            new_id = generate_unique_task_id(team)
            existing.add(new_id)
            return new_id
        
        inc["task_id"] = inc.apply(norm_tid_row, axis=1)


        merged = base.copy()
        if merged.empty:
            merged = inc[TASKS_COLUMNS].copy()
        else:
            for _, r in inc.iterrows():
                _tid = str(r["task_id"]).strip().upper()
                m = merged["task_id"].astype(str).str.upper()==_tid
                if m.any():
                    for k in TASKS_COLUMNS: merged.loc[m,k]=r.get(k,"")
                else:
                    merged = pd.concat([merged, pd.DataFrame([r[TASKS_COLUMNS]])], ignore_index=True)
        save_df(TASKS_CSV, merged, TASKS_COLUMNS)
        self.admin_task_refresh(); messagebox.showinfo("Imported","Tasks imported & merged.")

        log_event("ADMIN-TASKS", "Imported tasks from CSV (merged)")

    def download_tasks_template(self):
        path = filedialog.asksaveasfilename(defaultextension=".csv", initialfile="tasks_template.csv", filetypes=[("CSV","*.csv")])
        if not path: return
        pd.DataFrame(columns=TASKS_COLUMNS).to_csv(path, index=False)
        messagebox.showinfo("Template","Template saved.")

    # --- Admin: Approvals (per-month wide; wider columns) ---
    def admin_approvals(self, parent):
        # Filters
        filt = ttk.Frame(parent); filt.pack(fill="x", padx=6, pady=6)
        ttk.Label(filt, text="User: ").pack(side="left")
        users_df = load_df(USERS_CSV, USERS_COLUMNS)
        if not getattr(self, "is_superadmin", False) and not users_df.empty:
            my_team = str(self.user.get("team","")).strip()
            users_df = users_df[users_df["team"].astype(str).str.strip() == my_team].copy()

        if users_df.empty:
            user_choices = ["All"]
        else:
            _df = users_df.copy()
            _df["username"] = _df["username"].astype(str).str.strip()
            _df["full_name"] = _df["full_name"].astype(str).str.strip()
            _df["display"] = _df["username"] + " - " + _df["full_name"]
            user_choices = ["All"] + sorted(_df["display"].dropna().unique().tolist(), key=lambda s: s.lower())
        
        self.var_appr_user = tk.StringVar(value="All")
        self.cb_appr_user = ttk.Combobox(
            filt, textvariable=self.var_appr_user,
            values=user_choices, width=28, state="readonly"
        )
        self.cb_appr_user.pack(side="left", padx=6)
        today = date.today()
        ttk.Label(filt, text="Year").pack(side="left")
        self.var_appr_year = tk.StringVar(value=str(today.year))
        ttk.Combobox(filt, textvariable=self.var_appr_year, values=[str(y) for y in range(today.year-1, today.year+2)], width=6, state="readonly").pack(side="left", padx=6)

        # Title: Admin Approvals - Month filter removed (Weekly-only)
        self.var_appr_month = tk.StringVar(value=str(today.month))  # keep variable for CSV ops, not shown
        # (UI removed)
        print("[UI] Admin Approvals Month selector hidden.")
        print("[CONCLUSION] Weekly approvals won’t depend on month selection.")

        ttk.Label(filt, text="Status").pack(side="left")
        self.var_appr_status = tk.StringVar(value="all")
        ttk.Combobox(filt, textvariable=self.var_appr_status, values=["draft","submitted","approved","rejected","all"], width=12, state="readonly").pack(side="left", padx=6)
        self.var_appr_task_text = tk.StringVar(value="")
        ttk.Label(filt, text="Task filter (ID/Name/Desc)").pack(side="left", padx=(16,4))
        ttk.Entry(filt, textvariable=self.var_appr_task_text, width=28).pack(side="left", padx=4)
        ttk.Button(filt, text="Refresh", command=self.appr_refresh).pack(side="left", padx=6)

        # === [Cell 8 · ADD] Admin Approvals – Weekly/Monthly toggle + ISO Week selectors ===
        # Title: Admin Approvals – Add Weekly/Monthly toggle + Week selectors
        
        m = ttk.Frame(parent); m.pack(fill="x", padx=6, pady=(0,6))
        ttk.Label(m, text="Mode:").pack(side="left", padx=(0,6))
        
        # Title: Admin Approvals Weekly-only toggle (Monthly removed)
        self.view_mode.set("Weekly")
        ttk.Label(m, text="Weekly").pack(side="left")  # simple label; no toggle
        print("[UI] Admin Approvals forced to Weekly mode.")
        print("[CONCLUSION] Monthly toggle removed.")

        ttk.Label(m, text="ISO Year").pack(side="left")
        ttk.Combobox(m, textvariable=self.week_year,
                     values=[str(y) for y in range(date.today().year-1, date.today().year+2)],
                     width=6, state="readonly").pack(side="left", padx=6)
        
        ttk.Label(m, text="Week").pack(side="left")
        ttk.Combobox(m, textvariable=self.week_no,
                     values=[str(w) for w in range(1,54)], width=4, state="readonly").pack(side="left", padx=6)
        
        ttk.Button(m, text="Load Week", command=self.appr_refresh).pack(side="left", padx=6)
        
        print("[UI] Admin Approvals: Weekly/Monthly toggles + ISO week selectors wired.")
        print("[CONCLUSION] Admin can switch to Weekly mode; grid will show only the 7 day columns.")

        
        # Actions
        act = ttk.Frame(parent); act.pack(fill="x", padx=6, pady=6)
        ttk.Button(act, text="Check All (filtered)", command=self.check_all).pack(side="left", padx=4)
        ttk.Button(act, text="Uncheck All", command=self.uncheck_all).pack(side="left", padx=4)
        ttk.Label(act, text="Decision: ").pack(side="right", padx=6)
        self.var_decision = tk.StringVar(value="Approve")
        ttk.Combobox(act, textvariable=self.var_decision, values=["Approve","Reject"], width=10, state="readonly").pack(side="right", padx=6)
        ttk.Button(act, text="Apply", style="Accent.TButton", command=self.apply_decision_checked).pack(side="right", padx=6)

        # Grid container
        self.appr_area = ScrollableFrame(parent); self.appr_area.pack(fill="both", expand=True, padx=6, pady=6)

        # CSV Ops
        csv = ttk.LabelFrame(parent, text="CSV Operations"); csv.pack(fill="x", padx=6, pady=6)
        ttk.Button(csv, text="Export (Filtered)", command=lambda: self.export_timesheets_csv(filtered=True)).pack(side="left", padx=4, pady=4)
        ttk.Button(csv, text="Export (All in Month)", command=lambda: self.export_timesheets_csv(filtered=False)).pack(side="left", padx=4, pady=4)
        ttk.Button(csv, text="Import Timesheets (into selected month)", command=self.import_timesheets_csv).pack(side="left", padx=4, pady=4)
        ttk.Button(csv, text="Download Timesheet Template", command=self.download_timesheet_template).pack(side="left", padx=4, pady=4)

        def _rebuild_long():
            sync_timesheets_long_from_all_wide()
            messagebox.showinfo("Rebuilt", "Normalized backend updated (timesheet_entries.csv).")
        ttk.Button(csv, text="Rebuild Long Backend", command=_rebuild_long).pack(side="left", padx=4, pady=4)

        self._appr_checks = {}
        self.appr_refresh()
        self._disable_enter_global()

    def appr_refresh(self):
        for w in self.appr_area.inner.winfo_children():
            w.destroy()
        # === [Cell 8 · REPLACE] Admin Approvals – Determine date columns + load for Weekly/Monthly ===
        # Title: Admin Approvals – Determine date columns and load base data for view
        
        y = int(self.var_appr_year.get()); m = int(self.var_appr_month.get())
        
        if self.view_mode.get() == "Weekly":
            # Compute the 7 ISO week dates and the dd-MMM-yy display columns
            week_iso = self._current_week_dates()  # ['YYYY-MM-DD'] * 7
            dd_cols_all = []
            month_map = {}  # (YYYY, M) -> [dd-MMM-yy in that month for this ISO week]
            for iso_d in week_iso:
                iso_y, iso_m, _ = map(int, iso_d.split("-"))
                dd = iso_to_dd_mmm_yy(iso_d)
                month_map.setdefault((iso_y, iso_m), []).append(dd)
                dd_cols_all.append(dd)
            date_cols = dd_cols_all
        
            # Stitch rows from each month involved in the ISO week
            parts = []
            for (yy, mm), dd_list in month_map.items():
                part = load_timesheet_wide(yy, mm)
                if not part.empty:
                    # Ensure required meta columns exist
                    for c in TS_BASE_WITH_DESC + TS_TAIL:
                        if c not in part.columns: part[c] = ""
                    # Keep only these 7 dates + meta/tail; ignore other days for display
                    keep = set(TS_BASE_WITH_DESC + dd_list + [remark_col_for_dd(d) for d in dd_list] + TS_TAIL)
                    part = part[[c for c in part.columns if (c in keep or c in TS_BASE_WITH_DESC or c in TS_TAIL)]]
                parts.append(part)
            df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
            df = df.fillna("").replace({"nan": "", "NaN": "", "None": ""})
            if not getattr(self, "is_superadmin", False) and not df.empty and "team" in df.columns:
                my_team = str(self.user.get("team","")).strip()
                df = df[df["team"].astype(str).str.strip() == my_team].copy()

   
            print(f"[LOAD] Admin Approvals Weekly-only rows={len(df)} days={len(date_cols)}")
            print("[CONCLUSION] Monthly path removed; weekly stitching drives the grid.")

       
        
        # Apply the existing filters
        # Title: Normalize 'username - full name' to pure username for filtering
        u_disp = (self.var_appr_user.get() or "").strip()
        st = self.var_appr_status.get()
        
        if u_disp != "All":
            # Reuse existing helper that parses "username - full name" -> "username"
            u = self._normalize_assignee(u_disp)
            df = df[df["username"].astype(str).str.strip().str.lower() == u.strip().lower()]


        # PATCH: Status filter must match what the grid displays (Weekly Status vs row-level status)
        st = (st or "all").strip().lower()
        
        if self.view_mode.get() != "Weekly":
            if st != "all":
                df = df[df["status"].astype(str).str.strip().str.lower() == st]
        
        # Weekly mode: defer status filtering until after weekly status is computed on dff

        
        # Load task master and merge in billing_code, task_name, task_description
        tdf = load_df(TASKS_CSV, TASKS_COLUMNS)
        
        # Include billing_code in enrichment
        tsmall = (
            tdf[['task_id', 'billing_code', 'task_name', 'task_description']].copy()
            if not tdf.empty
            else pd.DataFrame(columns=['task_id', 'billing_code', 'task_name', 'task_description'])
        )
        
        # Left-join task attributes to the wide timesheet rows
        dff = pd.merge(df, tsmall, how="left", on="task_id", suffixes=("", "_t"))
        
        # Prefer values present in the timesheet; when blank, fallback to task master (_t)
        def _blank(s):
            return (pd.Series(s).astype(str).str.strip() == "").values if hasattr(s, "__iter__") else (str(s).strip() == "")
        
        # Task Description fallback
        if "task_description" not in dff.columns:
            dff["task_description"] = ""
        if "task_description_t" not in dff.columns:
            dff["task_description_t"] = ""
        dff["task_description"] = dff["task_description"].where(
            dff["task_description"].astype(str).str.strip() != "",
            dff["task_description_t"].fillna("")
        )
        
        # Task Name fallback
        if "task_name" not in dff.columns:
            dff["task_name"] = ""
        if "task_name_t" not in dff.columns:
            dff["task_name_t"] = ""
        dff["task_name"] = dff["task_name"].where(
            dff["task_name"].astype(str).str.strip() != "",
            dff["task_name_t"].fillna("")
        )
        
        # Billing Code fallback
        if "billing_code" not in dff.columns:
            dff["billing_code"] = ""
        if "billing_code_t" not in dff.columns:
            dff["billing_code_t"] = ""
        dff["billing_code"] = dff["billing_code"].where(
            dff["billing_code"].astype(str).str.strip() != "",
            dff["billing_code_t"].fillna("")
        )
        
        # Tidy up helper columns
        for extra in ["task_name_t", "task_description_t", "billing_code_t"]:
            if extra in dff.columns:
                dff.drop(columns=[extra], inplace=True)

        
        # Optional: in Weekly mode, compute a weekly total just for viewing (keeps month totals intact on disk)
        if self.view_mode.get() == "Weekly" and not dff.empty:
            try:
                dff["_weekly_total"] = dff[date_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0).sum(axis=1).map(lambda x: f"{x:.2f}")
            except Exception:
                dff["_weekly_total"] = ""

        # PATCH: Weekly-mode Status filtering uses computed per-day state::<dd> weekly status
        if self.view_mode.get() == "Weekly" and not dff.empty:
            # Compute once and reuse (also makes filter consistent with the displayed Weekly Status)
            dff["_wk_status"] = dff.apply(
                lambda r: self._compute_weekly_status(str(r.get("task_id", "")), date_cols, month_map),
                axis=1
            )
            if st != "all":
                dff = dff[dff["_wk_status"].astype(str).str.strip().str.lower() == st].copy()
        else:
            dff["_wk_status"] = ""

        
        print(f"[LOAD] Admin Approvals Mode={self.view_mode.get()} rows={len(dff)} cols_in_view={len(date_cols)}")
        print("[CONCLUSION] Approvals grid will render only weekly day columns (7) or full month days.")



        ftxt = self.var_appr_task_text.get().strip().lower()
        if ftxt:
            mask = (
                dff["task_id"].astype(str).str.lower().str.contains(ftxt, na=False) |
                dff["task_name"].astype(str).str.lower().str.contains(ftxt, na=False) |
                dff["task_description"].astype(str).str.lower().str.contains(ftxt, na=False)
            )
            dff = dff[mask]

        
        # Subtotals (row=0) — 9 fixed columns before dates; show only numbers in tiles
        # --- Hrs_Perday & Extra_Hrs for Admin (unified format) ---
        BASELINE = 8.0
        
        # Per-day sums from filtered dataframe (dff)
        per_day = []

        # Guarantee all weekly date columns exist so summation never errors
        for dc in date_cols:
            if dc not in dff.columns:
                dff[dc] = ""
        
        # Coerce to numeric safely; blanks/non-numerics -> NaN -> 0, then sum
        for dc in date_cols:
            if dff.empty:
                per_day.append(0.0)
                continue
            s = pd.to_numeric(dff[dc], errors="coerce").fillna(0.0)
            per_day.append(float(s.sum()))
        
        # Extra hours per day (never negative). If you want negative (under-utilization), use (v - BASELINE) directly.
        extra_day = [max(v - BASELINE, 0.0) for v in per_day]
        
        # Grand totals for the weekly view
        grand_per = float(sum(per_day))
        grand_extra = float(sum(extra_day))
        
        # Optional: quick debug in console
        print(f"[SUBTOTAL] per_day={per_day} grand={grand_per:.2f} extra={grand_extra:.2f}")
        print("[CONCLUSION] Admin Approvals top tiles now use robust numeric coercion; blanks no longer zero out the day.")

        
        # Row 0: Hrs_Perday
        ttk.Label(self.appr_area.inner, text="Hrs_Perday", font=("Segoe UI", 10, "bold")) \
            .grid(row=0, column=8, padx=4, pady=4, sticky="nsew")
        
        # placeholders for the 8 fixed columns after column 0 (Username..Task Description start at col=1)
        for i in range(1, 9):
            ttk.Label(self.appr_area.inner, text="").grid(row=0, column=i)
        
        for j, dc in enumerate(date_cols):
            ttk.Label(self.appr_area.inner, text=f"{per_day[j]:.2f}",
                      relief="groove", anchor="center") \
                .grid(row=0, column=9 + j, padx=2, pady=2, sticky="nsew")
        
        ttk.Label(self.appr_area.inner, text=f"{grand_per:.2f}",
                  relief="groove", anchor="center") \
            .grid(row=0, column=9 + len(date_cols), padx=2, pady=2, sticky="nsew")
        
        # Row 1: Extra_Hrs
        ttk.Label(self.appr_area.inner, text="Extra_Hrs", font=("Segoe UI", 10, "bold")) \
            .grid(row=1, column=8, padx=4, pady=2, sticky="nsew")
        
        for i in range(1, 9):
            ttk.Label(self.appr_area.inner, text="").grid(row=1, column=i)
        
        for j, dc in enumerate(date_cols):
            ttk.Label(self.appr_area.inner, text=f"{extra_day[j]:.2f}",
                      relief="groove", anchor="center") \
                .grid(row=1, column=9 + j, padx=2, pady=2, sticky="nsew")
        
        ttk.Label(self.appr_area.inner, text=f"{grand_extra:.2f}",
                  relief="groove", anchor="center") \
            .grid(row=1, column=9 + len(date_cols), padx=2, pady=2, sticky="nsew")
        
        # Headers at row = 2 (unchanged list)
        headers = [
            "Select","Username","Team","Year","Month","Task ID","Billing Code","Task Name","Task Description"
        ] + date_cols + ["Total","User Remarks","Status","Submitted On","Approved By","Approved On","Remarks"]
        
        for j, h in enumerate(headers):
            ttk.Label(self.appr_area.inner, text=h, font=("Segoe UI", 10, "bold")) \
                .grid(row=2, column=j, padx=4, pady=4, sticky="nsew")
        
        # Data rows start at row_index = 3 (IMPORTANT)
        row_index = 3
        
        # ... (KEEP your existing code that inserts each filtered row using 'row_index' and increments it)
        # --- DATA ROWS (Admin) ---
        self._appr_checks.clear()
        if not dff.empty:
            for _, r in dff.iterrows():
                key = (
                    str(r.get("username","")),
                    str(r.get("team","")),
                    str(r.get("year","")),
                    str(r.get("month","")),
                    str(r.get("task_id",""))
                )
                var = tk.BooleanVar(value=False)
                ttk.Checkbutton(self.appr_area.inner, variable=var).grid(row=row_index, column=0, padx=3, pady=3)
                self._appr_checks[key] = var
        
                # Fixed columns 1..8 (Username..Task Description)
                ttk.Label(self.appr_area.inner, text=key[0]).grid(row=row_index, column=1)  # Username
                ttk.Label(self.appr_area.inner, text=key[1]).grid(row=row_index, column=2)  # Team
                ttk.Label(self.appr_area.inner, text=key[2]).grid(row=row_index, column=3)  # Year
                ttk.Label(self.appr_area.inner, text=key[3]).grid(row=row_index, column=4)  # Month
                ttk.Label(self.appr_area.inner, text=key[4]).grid(row=row_index, column=5)  # Task ID
                ttk.Label(self.appr_area.inner, text=str(r.get("billing_code",""))).grid(row=row_index, column=6)       # Billing
                ttk.Label(self.appr_area.inner, text=str(r.get("task_name",""))).grid(row=row_index, column=7, sticky="w")
                ttk.Label(self.appr_area.inner, text=str(r.get("task_description","")), wraplength=260,
                          anchor="w", justify="left").grid(row=row_index, column=8, sticky="w")
        
                
                # === [Cell 8 · PATCH] Admin Approvals: hover tooltip per day cell ===
                # Title: Day labels show the hidden per-day remark when hovered
                
                # ... inside appr_refresh(), during DATA ROWS rendering for each r in dff.iterrows() ...
                for j, dc in enumerate(date_cols):
                    val = r.get(dc, "")
                    val = "" if (pd.isna(val) or str(val).strip().lower() in ("nan", "none")) else str(val).strip()

                    lbl = ttk.Label(self.appr_area.inner, text=val)
                    lbl.grid(row=row_index, column=9 + j, padx=2, pady=2)
                    
                    # Attach tooltip for remark
                    def _get_remark(_r=r, _dc=dc):
                        return str(_r.get(remark_col_for_dd(_dc), ""))
                    Tooltip(lbl, _get_remark)

                
                    
                
                print("[OK] Approvals grid tooltips bound to show day-wise remarks.")
                print("[CONCLUSION] Admin can hover over any day value to see that day’s remark.")

        
                # === [Cell 8 · REPLACE] Admin row tails – show Total, User Remarks, then status info ===
                # Title: Admin row tail – add 'User Remarks' column
                
                col_offset = 9 + len(date_cols)
                
                # Total (or weekly total if you implemented that)
                total_val = r.get("_weekly_total","") if self.view_mode.get()=="Weekly" else r.get("total_hours","")
                ttk.Label(self.appr_area.inner, text=str(total_val)).grid(row=row_index, column=col_offset+0, padx=2, pady=2)
                
                # User Remarks (read-only text)
                ttk.Label(self.appr_area.inner, text=str(r.get("user_remarks","")), wraplength=260, anchor="w", justify="left") \
                    .grid(row=row_index, column=col_offset+1, padx=2, pady=2, sticky="w")
                

                # Compute weekly status dynamically
                weekly_status = self._compute_weekly_status(str(r.get("task_id","")), date_cols, month_map)
                
                # Show Weekly Status first, then other tails
                ttk.Label(self.appr_area.inner, text=weekly_status).grid(row=row_index, column=col_offset+2, padx=2, pady=2)
                
                # Remaining tail fields (skip original 'status')
                for j, tail in enumerate(["submitted_on","approved_by","approved_on","remarks"], start=3):
                    ttk.Label(self.appr_area.inner, text=str(r.get(tail,""))).grid(row=row_index, column=col_offset+j, padx=2, pady=2)

                
        
                row_index += 1
        # --- No rows message (full-width span) ---
        if row_index == 3:
            total_cols = 9 + len(date_cols) + 6   # 9 fixed + date_cols + 6 tail
            ttk.Label(
                self.appr_area.inner,
                text="No rows for current filter.",
                foreground="gray"
            ).grid(row=3, column=0, columnspan=total_cols, pady=20, sticky="w")

    
    def _iter_checked_keys(self): return [k for k,v in self._appr_checks.items() if v.get()]
    def check_all(self):  [v.set(True) for v in self._appr_checks.values()]
    def uncheck_all(self): [v.set(False) for v in self._appr_checks.values()]

    
    # === [Cell 8 · REPLACE] Admin Approvals – Apply decision (Weekly/Monthly aware) ===
    # Title: Admin Approvals – Apply decision to checked rows in Weekly/Monthly mode  
    def apply_decision_checked(self):
        decision = self.var_decision.get()  # "Approve" or "Reject"
        keys = self._iter_checked_keys()
        if not keys:
            return messagebox.showwarning("Select", "Check one or more rows first.")
    
        remarks_val = ""
        if decision == "Reject":
            remarks_val = simpledialog.askstring("Rejection Remarks", "Enter remarks for rejection (applied to all checked):") or "Rework required."
    
        updated = 0
        if self.view_mode.get() == "Weekly":
            # Build ISO week -> dd list grouped by (yy,mm)
            week_iso = self._current_week_dates()
            month_map = {}
            for iso_d in week_iso:
                yy, mm, _ = map(int, iso_d.split("-"))
                dd = iso_to_dd_mmm_yy(iso_d)
                month_map.setdefault((yy, mm), []).append(dd)
    
            # keys are tuples: (username, team, year, month, task_id)
            for (yy, mm), dd_list in month_map.items():
                base = load_timesheet_wide(yy, mm)
                if base.empty:
                    continue
    
                for (u, dept, yy_s, mm_s, tid) in keys:
                    mask = (
                        (base.get("username", pd.Series(dtype=str)) == u) &
                        (base.get("team",     pd.Series(dtype=str)) == dept) &
                        (base.get("task_id",  pd.Series(dtype=str)).astype(str) == str(tid))
                    )
                    if not mask.any():
                        continue
    
                    # Mark day states for this week
                    day_states = ("approved" if decision == "Approve" else "rejected").lower()
                    for dd in dd_list:
                        # Only mark if there is a value or a submitted state present
                        has_val = (base.loc[mask, dd].astype(str).str.strip() != "").any() if dd in base.columns else False
                        prev_state = base.loc[mask, f"state::{dd}"].astype(str).str.lower() if f"state::{dd}" in base.columns else pd.Series([], dtype=str)

                        # Allow admin to approve/reject if row-level status is submitted OR day has value
                        row_status = str(base.loc[mask, "status"].iloc[0]).lower() if mask.any() else ""
                        if row_status == "submitted" or has_val or (prev_state == "submitted").any():
                            base.loc[mask, f"state::{dd}"] = day_states

    
                    # Update top-level status/remarks to aid Monthly views (optional)
                    base.loc[mask, "status"] = day_states
                    base.loc[mask, "approved_by"] = self.user["full_name"]
                    base.loc[mask, "approved_on"] = datetime.now().strftime("%Y-%m-%d %H:%M")
                    # set admin_name to the actual approver
                    base.loc[mask, "admin_name"] = self.user["full_name"]

                    if decision != "Approve":
                        base.loc[mask, "remarks"] = remarks_val
                    updated += int(mask.sum())
    
                save_timesheet_wide(yy, mm, base)
    
        else:
            # Title: Monthly flow removed (Weekly-only)
            print("[SKIP] Monthly decision path removed; Weekly-only approvals active.")

    
        # Rebuild long & refresh view
        sync_timesheets_long_from_all_wide()
        self._weekly_cache = {}  # Clear cache to force reload
        self.appr_refresh()
        messagebox.showinfo("Done", f"{decision}d {updated} row(s).")
        log_event("APPROVALS", f"{decision} {updated} row(s) (mode={self.view_mode.get()}) by {self.user['username']}")
        print(f"[APPROVALS] {decision} rows={updated} mode={self.view_mode.get()}")
        print("[CONCLUSION] Decisions applied and normalized backend synced.")

    
    # === [Cell 8 · REPLACE] Admin Approvals – Export (Filtered) honors Weekly view ===
    # Title: Admin Approvals – Export filtered view (Weekly shows 7 days)
    
    def export_timesheets_csv(self, filtered=True):
        y = int(self.var_appr_year.get()); m = int(self.var_appr_month.get())
    
        if self.view_mode.get() == "Weekly":
            # Build the same stitched dataframe as in appr_refresh
            week_iso = self._current_week_dates()
            dd_cols_all = []
            month_map = {}
            for iso_d in week_iso:
                iso_y, iso_m, _ = map(int, iso_d.split("-"))
                dd = iso_to_dd_mmm_yy(iso_d)
                month_map.setdefault((iso_y, iso_m), []).append(dd)
                dd_cols_all.append(dd)
            date_cols = dd_cols_all
    
            parts = []
            for (yy, mm), dd_list in month_map.items():
                part = load_timesheet_wide(yy, mm)
                if not part.empty:
                    for c in TS_BASE_WITH_DESC + TS_TAIL:
                        if c not in part.columns: part[c] = ""
                    keep = set(TS_BASE_WITH_DESC + dd_list + [remark_col_for_dd(d) for d in dd_list] + TS_TAIL)
                    part = part[[c for c in part.columns if (c in keep or c in TS_BASE_WITH_DESC or c in TS_TAIL)]]
                parts.append(part)
            df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
            df = df.fillna("").replace({"nan": "", "NaN": "", "None": ""})
            # Apply current filters
            # Title: Normalize 'username - full name' for export filter
            u_disp = (self.var_appr_user.get() or "").strip()
            st = self.var_appr_status.get()
            
            if u_disp != "All":
                u = self._normalize_assignee(u_disp)
                df = df[df["username"].astype(str).str.strip().str.lower() == u.strip().lower()]
            
            # Conclusion:
            # Weekly export respects the display format while correctly filtering by username.

            if st != "all": df = df[df["status"] == st]
            if df.empty: 
                return messagebox.showinfo("No Data","No rows to export.")
    
            fname = f"timesheets_{self.week_year.get()}_W{int(self.week_no.get()):02d}_filtered.csv"
            path = filedialog.asksaveasfilename(defaultextension=".csv", initialfile=fname, filetypes=[("CSV","*.csv")])
            if not path: return
            # Order columns: base + weekly 7 days + tail
            cols = TS_BASE_WITH_DESC + date_cols + TS_TAIL
            for c in cols:
                if c not in df.columns: df[c] = ""
            df = df[cols + [c for c in df.columns if c not in cols]]
            df.to_csv(path, index=False)
            messagebox.showinfo("Exported", f"Exported to:\n{path}")
            log_event("APPROVALS", "Timesheets exported (filtered weekly)")
            return
    
        # Title: Export Weekly-only view (monthly export removed)
        print("[SKIP] Monthly export path removed; use weekly export (stitched from month files).")

        log_event("APPROVALS", f"Timesheets exported (filtered={filtered})")
    

    def download_timesheet_template(self):
        today = date.today(); y, m = int(self.var_appr_year.get() or today.year), int(self.var_appr_month.get() or today.month)
        cols = TS_BASE_WITH_DESC + month_date_cols(int(y), int(m)) + TS_TAIL
        path = filedialog.asksaveasfilename(defaultextension=".csv", initialfile=f"timesheets_template_{y}_{m:02d}.csv", filetypes=[("CSV","*.csv")])
        if not path: return
        pd.DataFrame(columns=cols).to_csv(path, index=False)
        messagebox.showinfo("Template","Template saved for selected month (includes task_description).")

    def import_timesheets_csv(self):
        # Import into the *selected* (Year, Month) only
        y = int(self.var_appr_year.get()); m = int(self.var_appr_month.get())
        base = load_timesheet_wide(y, m)

        path = filedialog.askopenfilename(filetypes=[("CSV","*.csv")])
        if not path: return
        try:
            inc = pd.read_csv(path, dtype=str).fillna("")
        except Exception as e:
            return messagebox.showerror("Read error", f"Unable to read CSV:\n{e}")
            log.exception("Operation failed")

        # Ensure required fields exist
        for c in TS_BASE_WITH_DESC + TS_TAIL:
            if c not in inc.columns: inc[c] = ""
        # Coerce Year/Month to selected month (import always targets current filter)
        inc["year"] = str(y); inc["month"] = str(m)
        # Normalize daily columns to the selected month only
        month_days = month_date_cols(y, m)
        for dc in month_days:
            if dc not in inc.columns: inc[dc] = ""
        
        for c in list(inc.columns):
            if _is_dd_mmm_yy(c) and c not in month_days:
                inc.drop(columns=[c], inplace=True)

        # Auto team and task_description enrichment
        inc["team"] = inc["username"].apply(lambda u: user_team(str(u).strip()))
        tasks = load_df(TASKS_CSV, TASKS_COLUMNS)
        tmap = dict(zip(tasks["task_id"].astype(str), tasks["task_description"].astype(str))) if not tasks.empty else {}
        inc["task_description"] = inc.apply(
            lambda r: r["task_description"] if str(r["task_description"]).strip() else tmap.get(str(r["task_id"]), ""),
            axis=1
        )

        # Merge
        merged = base.copy()
        if merged.empty:
            merged = inc.copy()
        else:
            for _, r in inc.iterrows():
                msk =   (
                            merged.get("username", pd.Series(dtype=str)).str.strip().str.lower() == str(r["username"]).strip().lower()
                        ) & (
                            merged.get("team", pd.Series(dtype=str)).str.strip() == str(r["team"]).strip()
                        ) & (
                            merged.get("year", pd.Series(dtype=str)).str.strip() == str(r["year"]).strip()
                        ) & (
                            merged.get("month", pd.Series(dtype=str)).str.strip() == str(r["month"]).strip()
                        ) & (
                            merged.get("task_id", pd.Series(dtype=str)).str.strip().str.upper() == str(r["task_id"]).strip().upper()
                        )

                if msk.any():
                    for k, v in r.items(): merged.loc[msk, k] = v
                else:
                    merged = pd.concat([merged, pd.DataFrame([r])], ignore_index=True)
        merged = merged.drop_duplicates(subset=["username","year","month","task_id"], keep="last")
        save_timesheet_wide(y, m, merged)
        sync_timesheets_long_from_all_wide()
        self.appr_refresh(); messagebox.showinfo("Imported","Timesheets imported into selected month.")
        log_event("APPROVALS", "Timesheets imported into selected month")


    # -- Admin: Settings (unchanged) --
    def admin_settings(self, parent):
        frm = ttk.LabelFrame(parent, text="Shared Storage Settings"); frm.pack(fill="x", padx=10, pady=10)
        self.var_data_dir = tk.StringVar(value=DATA_DIR)
        ttk.Label(frm, text="Shared Data Folder (UNC path recommended):").grid(row=0, column=0, padx=6, pady=6, sticky="e")
        ttk.Entry(frm, textvariable=self.var_data_dir, width=80).grid(row=0, column=1, padx=6, pady=6, sticky="w")
        def browse():
            p = filedialog.askdirectory()
            if p: self.var_data_dir.set(p)
        ttk.Button(frm, text="Browse", command=browse).grid(row=0, column=2, padx=6, pady=6)
        def save_and_reload():
            new_dir = self.var_data_dir.get().strip()
            if not new_dir: return messagebox.showerror("Path","Please choose a folder.")
            try:
                os.makedirs(new_dir, exist_ok=True)
                test_path = os.path.join(new_dir, "_write_test.tmp")
                with open(test_path,"w") as f: f.write("ok")
                os.remove(test_path)
            except Exception as e:
                return messagebox.showerror("Access", f"Cannot write to the selected folder:\n{e}"); log.exception("Operation failed")
                
            cfg = load_config(); cfg["storage"]["DATA_DIR"] = new_dir; save_config(cfg)
            messagebox.showinfo("Saved", "Path saved. Please restart the app to switch storage.")
        ttk.Button(frm, text="Save & Restart Later", command=save_and_reload).grid(row=1, column=1, sticky="w", padx=6, pady=10)

        lbl = ttk.LabelFrame(parent, text="Current Files"); lbl.pack(fill="x", padx=10, pady=10)
        ttk.Label(lbl, text=f"users.csv : {USERS_CSV}").pack(anchor="w", padx=8, pady=2)
        ttk.Label(lbl, text=f"tasks.csv : {TASKS_CSV}").pack(anchor="w", padx=8, pady=2)
        # Show month files present
        month_files = sorted(glob.glob(os.path.join(DATA_DIR, "timesheets_????_??.csv")))
        ttk.Label(lbl, text=f"monthly wide files: {len(month_files)}").pack(anchor="w", padx=8, pady=2)
        for fp in month_files[:6]:
            ttk.Label(lbl, text=f" • {os.path.basename(fp)}").pack(anchor="w", padx=16)

    # -- User --
    def build_user(self):
        for w in self.root.winfo_children(): w.destroy()
        self.header("Task/Timesheet Portal—User Dashboard", f"Welcome, {self.user['full_name']} Dept: {self.user['team']}")
        self.user_nb = ttk.Notebook(self.root); self.user_nb.pack(fill="both", expand=True, padx=10, pady=10)
        self.tab_user_tasks = ttk.Frame(self.user_nb); self.user_nb.add(self.tab_user_tasks, text="My Tasks")
        self.tab_user_times = ttk.Frame(self.user_nb); self.user_nb.add(self.tab_user_times, text="My Timesheet(Weekly)")
        self.user_tasks(self.tab_user_tasks)
        self.user_timesheet(self.tab_user_times)


    # === [ADD] Admin owner helpers (supports multiple admins) =====================
    def _admin_fullname_choices(self) -> list[str]:
        """
        Return a sorted list of Admin full names from users.csv.
        Fallback to ['Admin User'] if none found.
        """
        try:
            df = load_df(USERS_CSV, USERS_COLUMNS)
        except Exception:
            df = pd.DataFrame()
    
        if df.empty:
            return ["Admin User"]
    
        admins = df[df.get("role", "").astype(str).str.lower() == "admin"].copy()
        if admins.empty:
            return ["Admin User"]
    
        names = (
            admins.get("full_name", pd.Series(dtype=str))
            .astype(str).str.strip().dropna().unique().tolist()
        )
        names = sorted(names, key=lambda s: s.lower()) or ["Admin User"]
        print(f"[CHOICES] Admin names: {names}")
        print("[CONCLUSION] Admin name list ready for dropdown.")
        return names
    
    
    def _default_admin_full_name(self, prefer_team: str | None = None) -> str:
        """
        Pick a sensible default admin full name:
        1) An admin in the same team (if found),
        2) Else the 'admin' username's full name,
        3) Else the first admin's full name,
        4) Else 'Admin'.
        """
        try:
            df = load_df(USERS_CSV, USERS_COLUMNS)
        except Exception:
            df = pd.DataFrame()
    
        if df.empty:
            return "Admin"
    
        admins = df[df.get("role", "").astype(str).str.lower() == "admin"].copy()
        if admins.empty:
            return "Admin"
    
        if prefer_team:
            same_team = admins[
                admins.get("team", "").astype(str).str.strip() == str(prefer_team).strip()
            ]
            if not same_team.empty:
                nm = str(same_team.iloc[0]["full_name"]).strip()
                print(f"[DEFAULT ADMIN] Team '{prefer_team}' -> {nm}")
                print("[CONCLUSION] Team-aware default admin chosen.")
                return nm
    
        pick_admin = admins[
            admins["username"].astype(str).str.strip().str.lower() == "admin"
        ]
        if not pick_admin.empty:
            nm = str(pick_admin.iloc[0]["full_name"]).strip()
            print(f"[DEFAULT ADMIN] Built-in 'admin' -> {nm}")
            print("[CONCLUSION] 'admin' account used as default admin.")
            return nm
    
        nm = str(admins.iloc[0]["full_name"]).strip()
        print(f"[DEFAULT ADMIN] First admin -> {nm}")
        print("[CONCLUSION] Fallback default admin chosen.")


    
    def user_tasks(self, parent):
        # Clear & layout
        for w in parent.winfo_children():
            w.destroy()
        parent.grid_rowconfigure(0, weight=1); parent.grid_columnconfigure(0, weight=1)
    
        # Columns (order matters for selection indices)
        cols = ("task_id","billing_code","task_name","task_description","task_status",
        "planned_target_date","planned_hours","hq_contact_partner","priority","act_delivery_date","team")


        # Frame for Treeview + Scrollbars
        tree_frame = ttk.Frame(parent)
        tree_frame.grid(row=0, column=0, columnspan=4, sticky="nsew", padx=6, pady=6)
        
        # Treeview
        self.tree_my_tasks = ttk.Treeview(tree_frame, columns=cols, show="headings", height=14)
        for c in cols:
            self.tree_my_tasks.heading(c, text=c.replace("_"," ").title())
            w = 170 if c in ("task_description",) else 120
            if c in ("task_id","task_status","team"): w = 110
            self.tree_my_tasks.column(c, width=w, anchor="center")
        
        # Scrollbars
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree_my_tasks.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree_my_tasks.xview)
        self.tree_my_tasks.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        # Pack widgets
        self.tree_my_tasks.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        
        # Configure frame grid
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
    

    
        # ---- Add New Task (self-assigned) ----
        addf = ttk.LabelFrame(parent, text="Add New Task (self-assigned)")
        addf.grid(row=1, column=0, columnspan=4, sticky="ew", padx=6, pady=6)
        
        # Variables
        self.n_task_name = tk.StringVar()
        self.n_task_desc = tk.StringVar()
        self.n_planned_date = tk.StringVar()
        self.n_planned_hrs = tk.StringVar()
        self.n_billing = tk.StringVar(value="HQ")
        self.n_hq_contact = tk.StringVar()
        self.n_priority = tk.StringVar(value="2-Medium")
        self.n_act_delivery = tk.StringVar()
        
        # Row 0
        ttk.Label(addf, text="Task Name").grid(row=0, column=0, padx=6, pady=4, sticky="e")
        ttk.Entry(addf, textvariable=self.n_task_name, width=25).grid(row=0, column=1, padx=6, pady=4, sticky="w")
        ttk.Label(addf, text="Task Description").grid(row=0, column=2, padx=6, pady=4, sticky="e")
        ttk.Entry(addf, textvariable=self.n_task_desc, width=40).grid(row=0, column=3, padx=6, pady=4, sticky="w")
        ttk.Button(addf, text="Add Task", style="Accent.TButton", command=self.user_add_task).grid(row=0, column=4, padx=10, pady=4)
        
        # Row 1
        ttk.Label(addf, text="Planned Target Date").grid(row=1, column=0, padx=6, pady=4, sticky="e")
        ttk.Entry(addf, textvariable=self.n_planned_date, width=18).grid(row=1, column=1, padx=6, pady=4, sticky="w")
        ttk.Button(addf, text="Pick Date", command=lambda: self._pick_date(self.n_planned_date)).grid(row=1, column=2, padx=6, pady=4)
        ttk.Label(addf, text="Planned Hours").grid(row=1, column=3, padx=6, pady=4, sticky="e")
        ttk.Entry(addf, textvariable=self.n_planned_hrs, width=10).grid(row=1, column=4, padx=6, pady=4, sticky="w")
        
        # Row 2
        ttk.Label(addf, text="Billing Code").grid(row=2, column=0, padx=6, pady=4, sticky="e")
        ttk.Combobox(addf, textvariable=self.n_billing, values=["HQ","CH","DL","NJ","WK"], width=12, state="readonly").grid(row=2, column=1, padx=6, pady=4, sticky="w")
        ttk.Label(addf, text="HQ Contact Partner").grid(row=2, column=2, padx=6, pady=4, sticky="e")
        ttk.Entry(addf, textvariable=self.n_hq_contact, width=25).grid(row=2, column=3, padx=6, pady=4, sticky="w")
        ttk.Label(addf, text="Priority").grid(row=2, column=4, padx=6, pady=4, sticky="e")
        ttk.Combobox(addf, textvariable=self.n_priority, values=["1-Low","2-Medium","3-High"], width=18, state="readonly").grid(row=2, column=5, padx=6, pady=4, sticky="w")

        # === [ADD] User Add New Task · Admin Name dropdown ============================
        # Variable: preselect a team-aware default admin
        self.n_admin_name = tk.StringVar(
            value=self._default_admin_full_name(self.user.get("team", ""))
        )
        
        # UI (Row 3): Admin Name (readonly dropdown)
        ttk.Label(addf, text="Admin Name").grid(row=2, column=6, padx=6, pady=4, sticky="e")
        self.cb_admin_name = ttk.Combobox(
            addf,
            textvariable=self.n_admin_name,
            values=self._admin_fullname_choices(),
            width=25,
            state="readonly"
        )
        self.cb_admin_name.grid(row=2, column=7, padx=6, pady=4, sticky="w")
        
        print("[UI] 'Admin Name' dropdown added to User Add Task form.")
        print("[CONCLUSION] Users can pick which Admin should own the task.")
        
 
        # ---- Edit Allowed Fields (from selection) ----
        frm = ttk.LabelFrame(parent, text="Edit Allowed Fields")
        frm.grid(row=2, column=0, sticky="ew", padx=6, pady=6)
        
        self.m_task_id = tk.StringVar()
        self.m_task_desc = tk.StringVar()
        self.m_status = tk.StringVar(value="Not Started")
        self.m_act_delivery = tk.StringVar()  # NEW variable for Actual Delivery Date
        
        # Row 0
        ttk.Label(frm, text="Task ID").grid(row=0, column=0, padx=6, pady=4, sticky="e")
        ttk.Entry(frm, textvariable=self.m_task_id, width=12, state="readonly").grid(row=0, column=1, padx=6, pady=4, sticky="w")
        
        ttk.Label(frm, text="Task Description").grid(row=0, column=2, padx=6, pady=4, sticky="e")
        ttk.Entry(frm, textvariable=self.m_task_desc, width=50).grid(row=0, column=3, padx=6, pady=4, sticky="w")
        
        # Row 1
        ttk.Label(frm, text="Task Status").grid(row=0, column=4, padx=6, pady=4, sticky="e")
        ttk.Combobox(frm, textvariable=self.m_status,
                     values=["Not Started","In Progress","Completed","Closed"],
                     width=20, state="readonly").grid(row=0, column=5, padx=6, pady=4, sticky="w")
        
        # Row 2 (NEW: Actual Delivery Date)
        ttk.Label(frm, text="Actual Delivery Date").grid(row=0, column=6, padx=6, pady=4, sticky="e")
        
        # Actual Delivery Date widgets with references
        self.entry_act_delivery_user = ttk.Entry(frm, textvariable=self.m_act_delivery, width=18, state="disabled")
        self.entry_act_delivery_user.grid(row=0, column=7, padx=6, pady=4, sticky="w")
        
        self.btn_pick_act_delivery_user = ttk.Button(frm, text="Pick Date", command=lambda: self._pick_date(self.m_act_delivery), state="disabled")
        self.btn_pick_act_delivery_user.grid(row=0, column=8, padx=6, pady=4)
        
        # Bind status change to enable/disable Actual Delivery Date
        def toggle_user_act_delivery(*_):
            if self.m_status.get() == "Completed":
                self.entry_act_delivery_user.configure(state="normal")
                self.btn_pick_act_delivery_user.configure(state="normal")
            else:
                self.entry_act_delivery_user.configure(state="disabled")
                self.btn_pick_act_delivery_user.configure(state="disabled")
        
        self.m_status.trace_add("write", toggle_user_act_delivery)
        toggle_user_act_delivery()  # Initial state
        




    
        # ---- Corrected selection handler (indices aligned to 'cols') ----
        def on_select_my_task(_):
            sel = self.tree_my_tasks.selection()
            if not sel:
                return
            vals = self.tree_my_tasks.item(sel[0])["values"]
            # vals index mapping (by 'cols'):
            # 0:task_id, 1:billing_code, 2:task_name, 3:task_description, 4:task_status,
            # 5:planned_target_date, 9:team
            if len(vals) < 7: return
            self.m_task_id.set(vals[0])
            self.m_task_desc.set(vals[3])  # FIXED (was 2)
            self.m_status.set(vals[4] or "Not Started")
            # Prefill Actual Delivery Date (index 9 in the My Tasks grid columns)
            self.m_act_delivery.set(vals[9] or "")

            
        self.tree_my_tasks.bind("<<TreeviewSelect>>", on_select_my_task)
    
        # Bottom bar
        btm = ttk.Frame(parent); btm.grid(row=3, column=0, sticky="e", padx=6, pady=6)
        ttk.Button(btm, text="Save Changes", style="Accent.TButton",
                   command=self.user_save_task_edits).pack(side="left", padx=5)
        ttk.Button(btm, text="Refresh", command=self.user_refresh_tasks).pack(side="left", padx=5)
    
        # Initial load
        self.user_refresh_tasks()


    def user_add_task(self):
        name = (self.n_task_name.get() or "").strip()
        if not name:
            messagebox.showwarning("Missing", "Please enter Task Name.")
            return
        desc = (self.n_task_desc.get() or "").strip()
        pdate = (self.n_planned_date.get() or "").strip()
        phrs  = (self.n_planned_hrs.get() or "").strip()
        bill  = (self.n_billing.get() or "HQ").strip()
        if bill not in ("HQ","CH","DL","NJ","WK"):
            bill = "HQ"
    
        # Generate a unique numeric id in your specified range
        dept = self.user.get("team","") or DEFAULT_DEPT
        tid  = generate_unique_task_id(dept)

    
        # Compose row aligned to TASKS_COLUMNS

        row = {
            "task_id": tid,
            "billing_code": bill,
            "team": dept,
            "admin_name": (self.n_admin_name.get() or "").strip() or self._default_admin_full_name(self.user.get("team","")),
            "task_name": name,
            "task_description": desc,
            "planned_target_date": pdate,
            "planned_hours": phrs,
            "hq_contact_partner": (self.n_hq_contact.get() or "").strip(),
            "priority": (self.n_priority.get() or "2-Medium").strip(),
            "act_delivery_date": (self.n_act_delivery.get() or "").strip(),
            "assigned_user": self.user["username"],
            "task_status": "Not Started"
        }

    
        # Append to tasks.csv
        df = load_df(TASKS_CSV, TASKS_COLUMNS)
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
        save_df(TASKS_CSV, df, TASKS_COLUMNS)
    
        # Clear and refresh
        self.n_task_name.set(""); self.n_task_desc.set("")
        self.n_planned_date.set(""); self.n_planned_hrs.set("")
        self.n_billing.set("HQ")
    
        self.user_refresh_tasks()
        messagebox.showinfo("Task Added", f"Task {tid} created for you with billing {bill}.")
        log_event("MY-TASKS", f"User {self.user['username']} created task {tid}")
        print(f"[User Add Task] Created {tid} ({name}) with billing {bill} for {self.user['username']}.")
        print("[CONCLUSION] New task stored with billing code; visible in the grid.")


    def user_save_task_edits(self):
        """Save Task Description, Task Status, and Actual Delivery Date for the selected row."""
        from tkinter import messagebox
    
        # Row selection guard
        sel = self.tree_my_tasks.selection()
        if not sel:
            return messagebox.showwarning("No selection", "Please select a task row to edit.")
    
        # Task ID from the bottom editor (readonly field)
        tid = (self.m_task_id.get() or "").strip()
        if not tid:
            return messagebox.showwarning("Invalid", "Selected row has no Task ID.")
    
        # Load tasks and locate this user's task row
        df = load_df(TASKS_CSV, TASKS_COLUMNS)
        if df.empty:
            return messagebox.showwarning("Not found", "No tasks file to edit.")
        msk = (df["task_id"].astype(str) == tid) & (df["assigned_user"] == self.user["username"])
        if not msk.any():
            return messagebox.showwarning("Not found", "You can only edit your own task rows.")
    
        # Persist description & status (existing behavior)
        new_desc = (self.m_task_desc.get() or "").strip()
        new_status = (self.m_status.get() or "Not Started").strip()
        df.loc[msk, "task_description"] = new_desc
        df.loc[msk, "task_status"] = new_status
    
        # ✅ NEW: Persist Actual Delivery Date when status == Completed; clear otherwise
        act_date = (self.m_act_delivery.get() or "").strip()
        if new_status == "Completed":
            df.loc[msk, "act_delivery_date"] = act_date
        else:
            # Keep policy consistent with UI (date field disabled unless Completed)
            df.loc[msk, "act_delivery_date"] = ""
    
        # Save, refresh, notify
        save_df(TASKS_CSV, df, TASKS_COLUMNS)
        self.user_refresh_tasks()
        messagebox.showinfo("Saved", f"Changes saved for task {tid}.")
        log_event("MY-TASKS", f"User {self.user['username']} updated task {tid} (status/desc/act_delivery)")
        print(f"[PATCH] Saved act_delivery_date='{act_date}' for Task {tid} when status='{new_status}'.")
       


    
    # === [PATCH 1 · USER MY TASKS FILTER] =====================
    def user_refresh_tasks(self):
        if not hasattr(self, "tree_my_tasks"):
            print("[User Refresh Tasks] Tree not initialized.")
            return
    
        # Clear
        for iid in self.tree_my_tasks.get_children():
            self.tree_my_tasks.delete(iid)
    
        df = load_df(TASKS_CSV, TASKS_COLUMNS)
        if df.empty:
            print("[User Refresh Tasks] No tasks found.")
            return
    
        # Normalize status column
        if "task_status" not in df.columns:
            df["task_status"] = "Not Started"
        df["task_status"] = df["task_status"].fillna("").replace("", "Not Started")
    
        # Filter: only this user's tasks, and only visible statuses for My Tasks
        my = df[(df["assigned_user"] == self.user["username"])
               & (df["task_status"].isin(USER_MY_TASKS_VISIBLE_STATUSES))].copy()


        my_team = str(self.user.get("team","")).strip()
        if my_team and "team" in my.columns:
            my = my[my["team"].astype(str).str.strip() == my_team].copy()

    
        needed = ["task_id","billing_code","task_name","task_description","task_status",
                    "planned_target_date","planned_hours","hq_contact_partner","priority",
                    "act_delivery_date","team"
                ]
        for c in needed:
            if c not in my.columns:
                my[c] = ""


    
        for _, r in my.iterrows():
            self.tree_my_tasks.insert(
                "", "end",
                values=(
                    str(r["task_id"]),
                    str(r["billing_code"]),
                    str(r["task_name"]),
                    str(r["task_description"]),
                    str(r["task_status"]),
                    str(r["planned_target_date"]),
                    str(r["planned_hours"]),
                    str(r["hq_contact_partner"]),
                    str(r["priority"]),
                    str(r["act_delivery_date"]),
                    str(r["team"])
    
                    )
            )
            
    
        print(f"[User Refresh Tasks] Visible rows for {self.user['username']}: {len(my)}")
        print("[CONCLUSION] 'Closed' tasks are hidden from My Tasks.")


    # === Row selection helpers (inside class PortalApp) ===
    def _init_row_checks(self):
        """Initialize/clear the per-row checkbox map for the timesheet grid."""
        self.ts_row_checks = {}  # reset each time grid is rebuilt
    
    def _rows_check_all(self):
        """Check all row checkboxes in the current grid."""
        for v in getattr(self, "ts_row_checks", {}).values():
            try:
                v.set(True)
            except Exception:
                pass
    
    def _rows_uncheck_all(self):
        """Uncheck all row checkboxes in the current grid."""
        for v in getattr(self, "ts_row_checks", {}).values():
            try:
                v.set(False)
            except Exception:
                pass

    
    # === [Cell 8 · REPLACE] PortalApp.user_timesheet(self, parent) — with Task Picker + CSV/XLSX buttons ===
    # --- [Cell 8 · REPLACE] Robust My Timesheet (Monthly) builder with picker + grid + row-selection ---
    def user_timesheet(self, parent):
        # Clear & container
        for w in parent.winfo_children():
            w.destroy()

        # --- Refresh dynamic week list & guard for fully-approved weeks ---
        if self.view_mode.get() == "Weekly":
            try:
                if hasattr(self, "cb_week_user"):
                    yr = int(self.week_year.get())
                    weeks_list = []
                    for w in range(1, 54):
                        if not self._is_week_fully_approved_for_view(yr, w, for_admin=False):
                            weeks_list.append(str(w))
                    self.cb_week_user['values'] = weeks_list
                    if str(self.week_no.get()).strip() not in weeks_list and weeks_list:
                        self.week_no.set(weeks_list[0])
            except Exception:
                pass
        
            # Guard: if selected week is fully approved -> message & return
            yr = int(self.week_year.get()); wk = int(self.week_no.get())
            if self._is_week_fully_approved_for_view(yr, wk, for_admin=False):
                ttk.Label(parent,
                          text=f"Week {wk} is fully approved; hidden by policy. Please choose another ISO week.",
                          foreground="gray").grid(row=3, column=0, columnspan=12, padx=8, pady=12, sticky="w")
                print(f"[GUARD] My Timesheet: Week {wk} hidden (fully approved).")
                print("[CONCLUSION] Weekly grid suppressed only for fully approved weeks; drafts/submitted/rejected remain visible.")
                return




            # === [Cell 8 · ADD] User Toolbar – Weekly/Monthly toggle + ISO Week selectors ===
            # Title: User Toolbar – Add Weekly/Monthly toggle + Week selectors

            m = ttk.Frame(parent); m.pack(fill="x", padx=6, pady=0)
            ttk.Label(m, text="Mode:").pack(side="left", padx=(0,6))
            

            # Title: User Timesheet Weekly-only
            self.view_mode.set("Weekly")
            ttk.Label(m, text="Weekly").pack(side="left")
            print("[UI] User Timesheet forced to Weekly mode.")
            print("[CONCLUSION] Monthly toggle removed from user page.")

            
            # Week selectors
            ttk.Label(m, text="ISO Year").pack(side="left")
            ttk.Combobox(m, textvariable=self.week_year,
                         values=[str(y) for y in range(date.today().year-1, date.today().year+2)],
                         width=6, state="readonly").pack(side="left", padx=6)
            
            ttk.Label(m, text="Week").pack(side="left")

            # Dynamic weeks: hide fully approved weeks for the logged-in user (Admin Self & User)
            def _available_weeks_user():
                yr = int(self.week_year.get())
                vals = []
                for w in range(1,54):
                    if not self._is_week_fully_approved_for_view(yr, w, for_admin=False):
                        vals.append(str(w))
                return vals
            

            # Optimized: compute approved weeks once and filter
            self._approved_weeks_cache = self._compute_fully_approved_weeks(int(self.week_year.get()), for_admin=False)
            weeks_list = [str(w) for w in range(1,54) if w not in self._approved_weeks_cache]
            
            self.cb_week_user = ttk.Combobox(m, textvariable=self.week_no,
                                             values=weeks_list, width=4, state="readonly")

            self.cb_week_user.pack(side="left", padx=6)

            
            ttk.Button(m, text="Load Week", command=self.user_times_refresh_grid).pack(side="left", padx=6)
            
            print("[UI] Weekly/Monthly toggles wired on User toolbar")
            print("[CONCLUSION] User can switch modes and choose an ISO week; grid rebuilds accordingly.")



        try:
            # your UI build code
           pass
        except Exception as e:
            ttk.Label(parent, text=f"[Year/Month build failed] {e}", foreground="red") \
                .pack(anchor="w", padx=10, pady=4)

        # ===== 2) Toolbar (Save/Submit + selection + CSV/XLSX ops) =====
        try:
            b = ttk.Frame(parent); b.pack(anchor="e", padx=6, pady=6)
        
            btn_save = ttk.Button(b, text="Save Draft",
                                  command=lambda: self.user_save_timesheet("draft"))
            btn_save.pack(side="left", padx=5)
        
            btn_submit = ttk.Button(b, text="Submit for Approval", style="Accent.TButton",
                                    command=lambda: self.user_save_timesheet("submitted"))
            btn_submit.pack(side="left", padx=5)
        
            # Expose submit button for other blocks
            self.btn_submit = btn_submit
        
            # Row-selection controls
            ttk.Button(b, text="Select All Rows", command=self._rows_check_all).pack(side="left", padx=5)
            ttk.Button(b, text="Clear Row Selection", command=self._rows_uncheck_all).pack(side="left", padx=5)
            ttk.Button(b, text="Refresh", command=self.user_times_refresh_grid).pack(side="left", padx=5)
            ttk.Button(b, text="Download Template (CSV/XLSX)", command=self.download_my_ts_template).pack(side="left", padx=5)
            ttk.Button(b, text="Import from File (CSV/XLSX)", command=self.import_my_ts_file).pack(side="left", padx=5)
            ttk.Button(b, text="Export Current View (CSV/XLSX)", command=self.export_my_ts_view).pack(side="left", padx=5)
        
            # Keep Submit as the default button globally
            if hasattr(self, "set_default_button"):
                self.set_default_button(self.btn_submit)
        
        except Exception as e:
            ttk.Label(parent, text=f"[Toolbar build failed] {e}", foreground="red") \
                .pack(anchor="w", padx=10, pady=4)        
   
        
        # --- PATCH: Task Picker (create self.e_search and bind Enter + focus handlers) ---
        # ===== 3) Task Picker (searchable multi-select) =====
        try:
            picker = ttk.LabelFrame(parent, text="Pick tasks to fill for this month (multi-select)")
            picker.pack(fill="x", padx=6, pady=(0,6))
        
            self.ts_search = tk.StringVar(value="")
            ttk.Label(picker, text="Search").pack(side="left", padx=(8,4))
        
            # Create Search entry and bind events
            self.e_search = ttk.Entry(picker, textvariable=self.ts_search, width=30)
            self.e_search.pack(side="left")
        
            # Block Enter from triggering Submit
            self.e_search.bind("<Return>", lambda event: "break")
        
            # Disable default button while Search has focus; restore on leave
            self.e_search.bind("<FocusIn>",  lambda event: self.set_default_button(None))
            self.e_search.bind("<FocusOut>", lambda event: self.set_default_button(getattr(self, "btn_submit", None)))
        
            ttk.Button(picker, text="Refresh", command=self.load_my_task_picker).pack(side="left", padx=6)
            ttk.Button(picker, text="Select All", command=lambda: self._ts_pick_select(True)).pack(side="left")
            ttk.Button(picker, text="Clear", command=lambda: self._ts_pick_select(False)).pack(side="left")
            ttk.Button(picker, text="Load Selected to Grid", style="Accent.TButton",
                       command=self._ts_pick_apply).pack(side="left", padx=8)
        
            wrap = ttk.Frame(parent); wrap.pack(fill="x", padx=6, pady=(0,6))
            self.lb_tasks = tk.Listbox(wrap, selectmode="extended", height=6, exportselection=False)
            self.lb_tasks.pack(side="left", fill="both", expand=True)
            ys = ttk.Scrollbar(wrap, orient="vertical", command=self.lb_tasks.yview)
            ys.pack(side="left", fill="y")
            self.lb_tasks.configure(yscrollcommand=ys.set)
        
            if not hasattr(self, "ts_selected_tids"):
                self.ts_selected_tids = set()
        
            self.load_my_task_picker()
        except Exception as e:
            ttk.Label(parent, text=f"[Task Picker failed] {e}", foreground="red") \
                .pack(anchor="w", padx=10, pady=4)

        
        
        # --- OUTPUT / CONCLUSION ---
        print("\n[CONCLUSION] Enter in Search will no longer submit. Submit remains default elsewhere. Scope errors fixed.")
        # ===== 4) Grid area (ScrollableFrame) =====
        try:
                self.ts_area = ScrollableFrame(parent)
                self.ts_area.pack(fill="both", expand=True, padx=6, pady=6)
                # Build grid now
                self.user_times_refresh_grid()
        except Exception as e:
                ttk.Label(parent, text=f"[Grid area failed] {e}", foreground="red") \
                    .pack(anchor="w", padx=10, pady=4)

        self._disable_enter_global()
        
        print("[User Timesheet] Page built (toolbar + task-picker + grid).")
        print("[CONCLUSION] If any sub-block fails, an inline red note will appear instead of a blank page.")
 

    # === [Cell 8 · ADD] My Timesheet day-wise remarks via right-click popup ===
    # Title: Right-click on a day Entry opens a popup to edit the remark for that day & task  
   
    def _remark_popup(self, tid: str, dd_col: str, initial_text: str = "") -> None:
        """
        Open a custom modal popup to edit the remark for (task_id, day).
        - Enter/Numpad-Enter are disabled (do nothing).
        - Only clicking 'Save' commits the remark.
        - 'Esc' or 'Cancel' closes without saving.
        Result is stored in: self.ts_vars[tid]['day_remarks'][dd_col]
        """
        top = tk.Toplevel(self.root)
        top.title("Day Remark")
        top.transient(self.root)
        top.resizable(False, False)
        top.configure(padx=12, pady=10)
    
        # Make modal
        try:
            top.grab_set()
        except Exception:
            pass
    
        # Position near mouse pointer (fallback: center on root)
        try:
            x = max(20, self.root.winfo_pointerx() - 180)
            y = max(20, self.root.winfo_pointery() - 80)
            top.geometry(f"+{x}+{y}")
        except Exception:
            top.update_idletasks()
            W = self.root.winfo_rootx() + (self.root.winfo_width() // 2) - 220
            H = self.root.winfo_rooty() + (self.root.winfo_height() // 2) - 80
            top.geometry(f"+{max(20,W)}+{max(20,H)}")
    
        # --- Block Enter/Numpad-Enter inside this popup ---
        def _sink(_ev=None):
            return "break"
        top.bind("<Return>", _sink)
        top.bind("<KP_Enter>", _sink)
    
        # UI
        lbl = ttk.Label(top, text=f"Task: {tid}   Date: {dd_col}")
        lbl.grid(row=0, column=0, columnspan=2, sticky="w", pady=(0,6))
    
        ttk.Label(top, text="Remark").grid(row=1, column=0, sticky="e", padx=(0,8))
        var_txt = tk.StringVar(value=(initial_text or ""))
        ent = ttk.Entry(top, textvariable=var_txt, width=60)
        ent.grid(row=1, column=1, sticky="we")
        top.grid_columnconfigure(1, weight=1)
    
        # Also block Enter on the Entry field
        ent.bind("<Return>", _sink)
        ent.bind("<KP_Enter>", _sink)
    
        # Buttons
        btns = ttk.Frame(top)
        btns.grid(row=2, column=0, columnspan=2, sticky="e", pady=(10,0))
    
        def on_save():
            txt = (var_txt.get() or "").strip()
            row_state = self.ts_vars.setdefault(tid, {"day_remarks": {}})
            if "day_remarks" not in row_state:
                row_state["day_remarks"] = {}
            row_state["day_remarks"][dd_col] = txt
            try:
                top.grab_release()
            except Exception:
                pass
            top.destroy()
            print(f"[REMARK] Saved for {tid} @ {dd_col}: {txt[:60]}")
    
        def on_cancel(_ev=None):
            try:
                top.grab_release()
            except Exception:
                pass
            top.destroy()
    
        ttk.Button(btns, text="Cancel", command=on_cancel).pack(side="right", padx=(0,6))
        ttk.Button(btns, text="Save",   command=on_save,  style="Accent.TButton").pack(side="right")
    
        # Esc closes (no save)
        top.bind("<Escape>", on_cancel)
    
        # Focus into the Entry
        ent.focus_set()
    
        # Optional: if your app globally binds Enter to a default button, suppress while popup is active
        if hasattr(self, "set_default_button"):
            self.set_default_button(None)
    
        print("[CONCLUSION] Day-wise remark popup opened with Enter disabled; use Save/Cancel buttons.")
        
    print("[OK] My Timesheet: popup helper added.")
    print("[CONCLUSION] Users can enter per-day remarks via a right-click popup.")


    # === [Cell 8 · REPLACE] Helper: compute weekly status using the task's assigned user ===
    def _compute_weekly_status(self, tid: str, date_cols: list[str], weekly_span: dict[tuple[int,int], list[str]]) -> str:
        """
        Return 'approved' / 'rejected' / 'submitted' / 'draft' by inspecting state::<dd>
        for each day in the selected ISO week, reading from the corresponding monthly file(s).
    
        IMPORTANT CHANGE:
        - Resolve the username using TASKS.csv (assigned_user for this task_id), falling back to self.user.
        - This ensures Admin Approvals show the correct weekly status for the selected row's user.
        """
    
        # 1) Resolve the correct username for this task_id (from tasks.csv)
        username = ""
        try:
            tdf = load_df(TASKS_CSV, TASKS_COLUMNS)
        except Exception:
            tdf = pd.DataFrame()
    
        if not tdf.empty:
            m = tdf["task_id"].astype(str).str.upper() == str(tid).upper()
            if m.any():
                username = str(tdf.loc[m, "assigned_user"].iloc[0]).strip()
    
        if not username:
            # Fallback (User page keeps working; Admin page gets the right username when tasks.csv has it)
            username = str(self.user.get("username", "")).strip()
    
        # 2) Preload month files for the ISO week into a cache
        states = []
        if not hasattr(self, "_weekly_cache"):
            self._weekly_cache = {}
    
        for (yy, mm), dlist in weekly_span.items():
            if (yy, mm) not in self._weekly_cache:
                self._weekly_cache[(yy, mm)] = load_timesheet_wide(yy, mm)
    
        # 3) Read state::<dd> for each day from the correct month bucket, filtered by (username, task_id)
        for dd in date_cols:
            # Find which month file contains this dd column
            bucket = None
            for (yy, mm), dlist in weekly_span.items():
                if dd in dlist:
                    bucket = (yy, mm)
                    break
    
            s = ""
            part = self._weekly_cache.get(bucket, pd.DataFrame())
            if not part.empty:
                mask = (
                    part.get("username", pd.Series(dtype=str)).astype(str).str.strip().str.lower() == username.lower()
                ) & (
                    part.get("task_id", pd.Series(dtype=str)).astype(str).str.upper() == str(tid).upper()
                )
                col = f"state::{dd}"
                if mask.any() and col in part.columns:
                    s = str(part.loc[mask, col].iloc[0]).strip().lower()
    
            if s:
                states.append(s)
    
        # 4) Decide final weekly status
        if states and all(s == "approved" for s in states):
            result = "approved"
        elif any(s == "rejected" for s in states):
            result = "rejected"
        elif any(s == "submitted" for s in states):
            result = "submitted"
        else:
            result = "draft"
    
        print(f"[WEEKLY STATUS] user={username} tid={tid} states={states} -> {result}")
        print("[CONCLUSION] Weekly Status now computed against the correct (assigned) user; Admin/User views aligned.")
        return result


    
    def _compute_fully_approved_weeks(self, iso_year: int, for_admin: bool) -> set[int]:
        """Return a set of ISO weeks fully approved for the current user."""
        approved_weeks = set()
        # Load all monthly files once
        files = sorted(glob.glob(os.path.join(DATA_DIR, "timesheets_????_??.csv")))
        user = self.user["username"]
        df_all = []
        for fp in files:
            df = pd.read_csv(fp, dtype=str).fillna("")
            df = df[df.get("username", "") == user]
            if not df.empty:
                df_all.append(df)
        if not df_all:
            return approved_weeks
        df_all = pd.concat(df_all, ignore_index=True)
        # Group by date columns
        all_dates = []
        for col in df_all.columns:
            if col.startswith("state::"):
                dd = col.split("state::")[1]
                all_dates.append(dd)
        # Map dd->ISO week
        for dd in all_dates:
            iso_date = _dd_mmm_yy_to_iso(dd)
            y, w = iso_week_of(iso_date)
            states = df_all[f"state::{dd}"].str.lower()
            if states.notna().any() and (states == "approved").all():
                approved_weeks.add(w)
        return approved_weeks



    def _is_week_fully_approved_for_view(self, iso_year: int, iso_week: int, for_admin: bool) -> bool:
        """Return True if all days in the ISO week are approved for the current user."""
        if not hasattr(self, "_approved_weeks_cache"):
            self._approved_weeks_cache = self._compute_fully_approved_weeks(iso_year, for_admin)
        return iso_week in self._approved_weeks_cache




    # ===========================================
    # [PATCH] User Timesheet Grid + Save/Submit
    # ===========================================
    # Title: Weekly-mode day locking + Weekly Status + Carry-Forward
    # This patch fixes:
    #  1) Per-day cell disable logic (readonly when submitted/approved) in Weekly mode.
    #  2) Weekly Status column rendering calculated per row from state::<day>.
    #  3) Weekly submit marks state::<day>='submitted' and (optionally) carries rows forward.
    #  4) Hidden per-day remarks are injected on save.
    # Prints a short conclusion after definitions to confirm the patch is loaded.
    
    
    
    # --- Helper references used by these methods must already exist in your file ---
    # month_date_cols(y, m): -> list of 'dd-MMM-yy' strings
    # iso_to_dd_mmm_yy(iso_date): -> 'dd-MMM-yy'
    # load_timesheet_wide(y, m), ensure_month_file(y, m), save_timesheet_wide(y, m, df)
    # remark_col_for_dd(dd): -> 'remark::<dd-MMM-yy>'
    # sync_timesheets_long_from_all_wide()
    # TASKS_CSV, TASKS_COLUMNS, USER_TIMESHEET_VISIBLE_STATUSES
    # Tooltip class (optional; used for hover remarks)
    
    
    
    
    def user_times_refresh_grid(self):
        """
        Build the 'My Timesheet' grid for Monthly/Weekly modes with:
          - Weekly: day cells disabled where state::<day> ∈ {submitted, approved}
          - Weekly Status column computed from the 7 day states
          - Per-day remark tooltip & right-click editor
        """
        # Clear area and init state
        for w in self.ts_area.inner.winfo_children():
            w.destroy()
        self._init_row_checks()
        self.ts_vars = {}

        # --- Lock policy: weekly rows that are submitted/approved are hard-locked ---
        LOCKED_WEEKLY_STATES = {"submitted", "approved"}
        unlocked_rows_count = 0  # counts rows that are editable (not locked)
        
        # --- Refresh dynamic week list & guard for fully-approved weeks ---
        if self.view_mode.get() == "Weekly":
            try:
                if hasattr(self, "cb_week_user"):
                    yr = int(self.week_year.get())
                    weeks_list = []
                    for w in range(1, 54):
                        if not self._is_week_fully_approved_for_view(yr, w, for_admin=False):
                            weeks_list.append(str(w))
                    self.cb_week_user['values'] = weeks_list
                    if str(self.week_no.get()).strip() not in weeks_list and weeks_list:
                        self.week_no.set(weeks_list[0])
            except Exception:
                pass
        
            # Guard: if selected week is fully approved -> message & return
            yr = int(self.week_year.get()); wk = int(self.week_no.get())
            if self._is_week_fully_approved_for_view(yr, wk, for_admin=False):
                ttk.Label(self.ts_area.inner,
                          text=f"Week {wk} is fully approved; hidden by policy. Please choose another ISO week.",
                          foreground="gray").grid(row=3, column=0, columnspan=12, padx=8, pady=12, sticky="w")
                print(f"[GUARD] My Timesheet: Week {wk} hidden (fully approved).")
                print("[CONCLUSION] Weekly grid suppressed only for fully approved weeks; drafts/submitted/rejected remain visible.")
                return



        
        # Determine date columns (Weekly vs Monthly)
        # No monthly mode; use ISO week only
        week_iso = self._current_week_dates()
        weekly_span = None
        if self.view_mode.get() == "Weekly":
            week_iso = self._current_week_dates()  # ['YYYY-MM-DD'] * 7
            dd_cols_all, weekly_span = [], {}
            for iso_d in week_iso:
                yy, mm, _ = map(int, iso_d.split("-"))
                dd = iso_to_dd_mmm_yy(iso_d)
                weekly_span.setdefault((yy, mm), []).append(dd)
                dd_cols_all.append(dd)
            date_cols = dd_cols_all
            dd_cols_all = [iso_to_dd_mmm_yy(d) for d in week_iso]
       

            
        print(f"[GRID] Weekly-only view days={len(date_cols)}")
        print("[CONCLUSION] Monthly date columns removed.")

    
        # Load my tasks (visible statuses only)
        tdf = load_df(TASKS_CSV, TASKS_COLUMNS)
        if tdf.empty:
            ttk.Label(self.ts_area.inner, text="No tasks found.", foreground="gray")\
                .grid(row=3, column=0, columnspan=6+len(date_cols), pady=16, sticky="w")
            print("[GRID] No tasks to display.")
            return
    
        # Filter: only my tasks allowed for timesheet entry
        if "task_status" not in tdf.columns:
            tdf["task_status"] = "Not Started"
        tdf["task_status"] = tdf["task_status"].fillna("").replace("", "Not Started")
        tasks = tdf[(tdf["assigned_user"] == self.user["username"]) &
                    (tdf["task_status"].isin(USER_TIMESHEET_VISIBLE_STATUSES))].copy()
    
        # Respect Task Picker selection (if any)
        if getattr(self, "ts_selected_tids", None):
            sel = {t.upper() for t in self.ts_selected_tids}
            if not tasks.empty:
                task_ids_upper = tasks["task_id"].astype(str).str.upper()
                tasks = tasks.loc[task_ids_upper.isin(sel)].copy()
    
        # Prepare monthly/weekly stitched base
        if self.view_mode.get() == "Weekly":
            parts = []
            for (yy, mm), dd_list in weekly_span.items():
                part = load_timesheet_wide(yy, mm)
                if part.empty:
                    continue
                # keep base + these days + hidden remark columns + tail
                keep = set(TS_BASE_WITH_DESC + dd_list + [remark_col_for_dd(d) for d in dd_list] + TS_TAIL)
                for c in TS_BASE_WITH_DESC + TS_TAIL:
                    if c not in part.columns: part[c] = ""
                part = part[[c for c in part.columns if (c in keep or c in TS_BASE_WITH_DESC or c in TS_TAIL)]]
                parts.append(part)
            tsdf = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
            tsdf = tsdf.fillna("").replace({"nan": "", "NaN": "", "None": ""})
        else:
            week_iso = self._current_week_dates()
            month_map = {}
            for iso_d in week_iso:
                yy, mm, _ = map(int, iso_d.split("-"))
                month_map.setdefault((yy, mm), []).append(iso_d)

    
        # ---- Subtotal rows (top band) ----
        my_df = tsdf[tsdf.get("username", "").astype(str) == self.user["username"]] if not tsdf.empty else pd.DataFrame()
        per_day = []
        for dc in date_cols:
            try:
                vals = pd.to_numeric(my_df[dc], errors="coerce") if (not my_df.empty and dc in my_df.columns) else pd.Series(dtype=float)
                per_day.append(float(vals.fillna(0).sum()))
            except Exception:
                per_day.append(0.0)
        BASELINE = 8.0
        extra_day = [max(v - BASELINE, 0.0) for v in per_day]
        grand_per, grand_extra = sum(per_day), sum(extra_day)
    
        # Row 0: Hrs_Perday (shifted due to Select col at 0)
        ttk.Label(self.ts_area.inner, text="Hrs_Perday", font=("Segoe UI", 10, "bold"))\
            .grid(row=0, column=5, padx=4, pady=4, sticky="nsew")
        for c in range(0, 5):  # placeholders for fixed columns Select..Team
            ttk.Label(self.ts_area.inner, text="").grid(row=0, column=c)
        for j, dc in enumerate(date_cols):
            ttk.Label(self.ts_area.inner, text=f"{per_day[j]:.2f}", relief="groove", anchor="center")\
                .grid(row=0, column=6 + j, padx=2, pady=2, sticky="nsew")
        ttk.Label(self.ts_area.inner, text=f"{grand_per:.2f}", relief="groove", anchor="center")\
            .grid(row=0, column=6 + len(date_cols), padx=2, pady=2, sticky="nsew")
    
        # Row 1: Extra_Hrs
        ttk.Label(self.ts_area.inner, text="Extra_Hrs", font=("Segoe UI", 10, "bold"))\
            .grid(row=1, column=5, padx=4, pady=2, sticky="nsew")
        for c in range(0, 5):
            ttk.Label(self.ts_area.inner, text="").grid(row=1, column=c)
        for j, dc in enumerate(date_cols):
            ttk.Label(self.ts_area.inner, text=f"{extra_day[j]:.2f}", relief="groove", anchor="center")\
                .grid(row=1, column=6 + j, padx=2, pady=2, sticky="nsew")
        ttk.Label(self.ts_area.inner, text=f"{grand_extra:.2f}", relief="groove", anchor="center")\
            .grid(row=1, column=6 + len(date_cols), padx=2, pady=2, sticky="nsew")
    
        # ---- Headers (row=2) ----
        headers = ["Select","Task ID","Billing Code","Task Name","Task Description","Team"] \
                  + date_cols + ["Total","User Remarks","Weekly Status"]
        for j, h in enumerate(headers):
            ttk.Label(self.ts_area.inner, text=h, font=("Segoe UI", 10, "bold"))\
                .grid(row=2, column=j, padx=4, pady=(2,6), sticky="nsew")
    
        # Column widths
        self.ts_area.inner.grid_columnconfigure(0, minsize=70)   # Select
        self.ts_area.inner.grid_columnconfigure(1, minsize=120)  # Task ID
        self.ts_area.inner.grid_columnconfigure(2, minsize=120)  # Billing
        self.ts_area.inner.grid_columnconfigure(3, minsize=220)  # Task Name
        self.ts_area.inner.grid_columnconfigure(4, minsize=280)  # Task Desc
        self.ts_area.inner.grid_columnconfigure(5, minsize=160)  # Team
        for j in range(len(date_cols)):
            self.ts_area.inner.grid_columnconfigure(6 + j, minsize=85)
        self.ts_area.inner.grid_columnconfigure(6 + len(date_cols), minsize=120)  # Total
        self.ts_area.inner.grid_columnconfigure(7 + len(date_cols), minsize=240)  # User Remarks
        self.ts_area.inner.grid_columnconfigure(8 + len(date_cols), minsize=130)  # Weekly Status
    
        # ---- Data rows ----
        row_index = 3
        self.ts_vars = {}
        if not tasks.empty:
            # Build maps for billing/name/desc
            t_bill_map = dict(zip(tdf["task_id"].astype(str).str.upper(), tdf["billing_code"].astype(str))) if not tdf.empty else {}
            tdesc_map = dict(zip(tdf["task_id"].astype(str).str.upper(), tdf["task_description"].astype(str))) if not tdf.empty else {}
    
            for _, r in tasks.iterrows():
                tid = str(r.get("task_id","")).upper()
                bill = t_bill_map.get(tid, "")
                tname = r.get("task_name","")
                tdesc = r.get("task_description","") or tdesc_map.get(tid, "")
                dept = r.get("team","") or self.user["team"]
    
                # Resolve existing wide row (prefer current y/m else any stitched month in weekly)
                exist, status = None, ""
                if not tsdf.empty:
                    # Build existing row map for EACH (year,month) in this ISO week (fix cross-month weeks)
                    exist_map = {}
                    
                    for (yy, mm) in (weekly_span or {}).keys():
                        msk_bucket = (
                            (tsdf.get("username", "").astype(str) == self.user["username"]) &
                            (tsdf.get("task_id", "").astype(str).str.upper() == tid) &
                            (tsdf.get("year", "").astype(str) == str(yy)) &
                            (tsdf.get("month", "").astype(str) == str(mm))
                        )
                        if msk_bucket.any():
                            exist_map[(yy, mm)] = tsdf[msk_bucket].iloc[0].to_dict()
                    
                    # Default 'exist' for non-day fields (prefer the week-start bucket; else fallback)
                    iso_dates = self._current_week_dates()  # ['YYYY-MM-DD'] * 7
                    y, m = map(int, iso_dates[0].split("-")[:2])  # first day of week
                    exist = exist_map.get((y, m))
                    
                    # If week-start bucket has no row, fallback to any available bucket
                    if exist is None and exist_map:
                        exist = exist_map[sorted(exist_map.keys())[-1]]
                    
                    else:
                        msk = ((tsdf.get("username", "").astype(str) == self.user["username"]) &
                                (tsdf.get("task_id", "").astype(str).str.upper() == tid) &
                                (tsdf.get("year", "").astype(str) == str(y)) &
                                (tsdf.get("month", "").astype(str) == str(m))
                            )

                        if msk.any():
                            exist = tsdf[msk].iloc[0].to_dict()
                status = str((exist or {}).get("status",""))
    

                # --- Determine Weekly Status EARLY & lock flag (used by checkbox, cells, remarks)
                if self.view_mode.get() == "Weekly":
                    weekly_status = self._compute_weekly_status(tid, date_cols, weekly_span)
                else:
                    weekly_status = str((exist or {}).get("status", "")).strip().lower() or "draft"
                
                weekly_status = (weekly_status or "").strip().lower()
                row_locked    = weekly_status in LOCKED_WEEKLY_STATES
                
                # Persist lock flag for save/submit guard
                self.ts_vars.setdefault(tid, {})["locked"] = row_locked

                           

                # (0) Select checkbox (disabled when weekly status is submitted/approved)
                vchk = tk.BooleanVar(value=False)
                cb = ttk.Checkbutton(self.ts_area.inner, variable=vchk)
                cb.grid(row=row_index, column=0, padx=3, pady=3)
                if row_locked:
                    try:
                        cb.state(["disabled"])
                    except Exception:
                        cb.configure(state="disabled")
                else:
                    self.ts_row_checks[tid] = vchk
                    unlocked_rows_count += 1

    
                # Fixed columns
                ttk.Label(self.ts_area.inner, text=tid).grid(row=row_index, column=1, padx=3, pady=3, sticky="nsew")
                ttk.Label(self.ts_area.inner, text=str(bill)).grid(row=row_index, column=2, padx=3, pady=3, sticky="nsew")
                ttk.Label(self.ts_area.inner, text=str(tname)).grid(row=row_index, column=3, padx=3, pady=3, sticky="nsew")
                ttk.Label(self.ts_area.inner, text=str(tdesc), wraplength=260, justify="left", anchor="w")\
                    .grid(row=row_index, column=4, padx=3, pady=3, sticky="nsew")
                ttk.Label(self.ts_area.inner, text=str(dept)).grid(row=row_index, column=5, padx=3, pady=3, sticky="nsew")
    
                # Day inputs
                day_vars = []
                # Pre-compute row total for current view
                try:
                    exist_series = pd.Series({dc: (exist or {}).get(dc, "") for dc in date_cols}, dtype=str)
                    total_val = pd.to_numeric(exist_series, errors="coerce").fillna(0).sum()
                except Exception:
                    total_val = 0.0
                total_label = tk.StringVar(value=f"{total_val:.2f}")
    
                # Compute y and m from first ISO date of the selected week
                iso_dates = self._current_week_dates()  # ['YYYY-MM-DD'] * 7
                y, m = map(int, iso_dates[0].split("-")[:2])  # first day of week
                
                row_state = {
                    "vars": day_vars,
                    "year": y,
                    "month": m,
                    "team": dept,
                    "total_var": total_label,
                    "date_cols": date_cols,
                    "day_remarks": {}
                }

    
                for j, dc in enumerate(date_cols):

                    # Pick correct month-row for this day column (dc) using weekly_span buckets
                    bucket = None
                    for (yy, mm), dlist in (weekly_span or {}).items():
                        if dc in dlist:
                            bucket = (yy, mm)
                            break
                    
                    src = (exist_map.get(bucket) if 'exist_map' in locals() else None) or (exist or {})
                    val = str(src.get(dc, ""))

                    v = tk.StringVar(value=val)

                    # Numeric validation: allow empty or number with one decimal
                    def _validate_numeric(P):
                        if P == "" or P.replace(".", "", 1).isdigit():
                            return True
                        return False
                    vcmd = self.root.register(_validate_numeric)
                    e = ttk.Entry(self.ts_area.inner, textvariable=v, width=8,
                                  validate="key", validatecommand=(vcmd, "%P"))

                    e.grid(row=row_index, column=6 + j, padx=2, pady=2)

                    # --- Prevent Enter from submitting; also handle Numpad Enter
                    e.bind("<Return>",    lambda _ev: "break")
                    e.bind("<KP_Enter>",  lambda _ev: "break")
                    
                    # --- While editing a cell, disable the global default button (Submit); restore on blur
                    if hasattr(self, "set_default_button"):
                        e.bind("<FocusIn>",  lambda _ev: self.set_default_button(None))
                        e.bind("<FocusOut>", lambda _ev: self.set_default_button(getattr(self, "btn_submit", None)))

    
                    # --- FIX: disable cells where state::<day> is submitted/approved (Weekly only) ---
                    
                    cur_state = str(src.get(f"state::{dc}", "")).lower()

                    
                    # Disable cell if row weekly_status OR per-day state is submitted/approved
                    if weekly_status in ("submitted","approved") or cur_state in ("submitted","approved"):
                        e.configure(state="readonly")


    
                    # Tooltip: show current day remark (from hidden remark::<dd>)
                    cur_remark = str(src.get(remark_col_for_dd(dc), ""))
                    try:
                        Tooltip(e, lambda _tid=tid, _dc=dc: self.ts_vars.get(_tid, {}).get("day_remarks", {}).get(_dc, cur_remark))
                    except Exception:
                        pass
                    # Right-click opens remark editor
                    e.bind("<Button-3>", lambda _ev, _tid=tid, _dc=dc, _txt=cur_remark: self._remark_popup(_tid, _dc, _txt))
                    day_vars.append(v)
    
                # Persist row state for this tid
                self.ts_vars[tid] = row_state
    
                # Total & User Remarks
                ttk.Label(self.ts_area.inner, textvariable=total_label)\
                    .grid(row=row_index, column=6 + len(date_cols), padx=3, pady=3)
                remarks_val = str((exist or {}).get("user_remarks",""))
                remarks_var = tk.StringVar(value=remarks_val)

                # Create the entry and KEEP a handle
                ent_remarks = ttk.Entry(self.ts_area.inner, textvariable=remarks_var, width=32)
                ent_remarks.grid(row=row_index, column=7 + len(date_cols), padx=3, pady=3, sticky="nsew")
                self.ts_vars[tid]["remarks_var"] = remarks_var

                # Lock remarks when row is submitted/approved
                if row_locked:
                    try:
                        ent_remarks.state(["readonly"])
                    except Exception:
                        ent_remarks.configure(state="readonly")

                
                # --- Prevent Enter from submitting; also handle Numpad Enter
                ent_remarks.bind("<Return>",    lambda _ev: "break")
                ent_remarks.bind("<KP_Enter>",  lambda _ev: "break")
                
                # --- Disable default Submit while remarks entry has focus; restore on blur
                if hasattr(self, "set_default_button"):
                    ent_remarks.bind("<FocusIn>",  lambda _ev: self.set_default_button(None))
                    ent_remarks.bind("<FocusOut>", lambda _ev: self.set_default_button(getattr(self, "btn_submit", None)))

    

                # --- Weekly status computed from actual month segments per day ---
                if self.view_mode.get() == "Weekly":
                    weekly_status = self._compute_weekly_status(tid, date_cols, weekly_span)
                else:
                    # Monthly: use row-level status (submitted/draft/approved/rejected)
                    weekly_status = str((exist or {}).get("status", "")).strip().lower() or "draft"
                
                ttk.Label(self.ts_area.inner, text=weekly_status) \
                   .grid(row=row_index, column=8 + len(date_cols), padx=2, pady=2)
    
                row_index += 1
    
        # No rows message
        if row_index == 3:
            ttk.Label(self.ts_area.inner,
                      text="No pending rows for this view (either submitted/approved or no tasks/selection).",
                      foreground="gray")\
                .grid(row=3, column=0, columnspan=6 + len(date_cols), pady=16, sticky="w")
    
        # Done
        print("[GRID] My Timesheet grid rebuilt. Mode:", self.view_mode.get())
        print("[CONCLUSION] Weekly cells locked where approved/submitted; Weekly Status column now accurate.")
    


    def _inject_day_remarks_into_df(self, tsdf: pd.DataFrame, y: int, m: int):
        """
        Ensure hidden remark::<day> columns exist and write remarks from self.ts_vars into the DataFrame.
        Fallback: if a specific day remark is empty, use the row-level monthly user_remarks editor value.
        """
        day_cols = month_date_cols(int(y), int(m))
        remark_cols = [remark_col_for_dd(d) for d in day_cols]
    
        # Ensure hidden remark columns exist
        for c in remark_cols:
            if c not in tsdf.columns:
                tsdf[c] = ""
    
        for tid, info in (self.ts_vars or {}).items():
            monthly_txt = str(info.get("remarks_var", None).get() if info.get("remarks_var") else "").strip()
            drem = info.get("day_remarks", {}) or {}
            msk = (
                tsdf.get("username", pd.Series(dtype=str)).str.strip().str.lower() == self.user["username"].lower()
            ) & (
                tsdf.get("year", pd.Series(dtype=str)).astype(str) == str(y)
            ) & (
                tsdf.get("month", pd.Series(dtype=str)).astype(str) == str(m)
            ) & (
                tsdf.get("task_id", pd.Series(dtype=str)).astype(str).str.upper() == str(tid).upper()
            )
            if not msk.any():
                continue
            for dd in day_cols:
                key = remark_col_for_dd(dd)
                val = str(drem.get(dd, "").strip()) or monthly_txt
                tsdf.loc[msk, key] = val
    
        print(f"[REMARKS] Injected hidden remark::<day> columns for y/m: {y}/{m}")
    
    
    def _carry_forward_to_next_week(self, y, m, selected_tids):
        """
        Create draft rows for next ISO week in the SAME month file (if week doesn't cross month).
        """
        try:
            next_week = int(self.week_no.get()) + 1
            next_week_dates = week_date_range(int(self.week_year.get()), next_week)  # ['YYYY-MM-DD'] * 7
            dd_cols_next = [iso_to_dd_mmm_yy(d) for d in next_week_dates]
    
            # If next week crosses into a different month, skip
            if any(int(d.split("-")[1]) != m for d in next_week_dates):
                print("[CARRY] Next week spans another month; skip carry-forward.")
                return
    

            week_iso = self._current_week_dates()
            month_map = {}
            for iso_d in week_iso:
                yy, mm, _ = map(int, iso_d.split("-"))
                month_map.setdefault((yy, mm), []).append(iso_d)

            for tid in selected_tids:
                # Avoid duplicate row for same task & user
                if tsdf["task_id"].astype(str).str.upper().eq(tid).any() and \
                   tsdf["username"].astype(str).eq(self.user["username"]).any():
                    continue
                new_row = {
                    "username": self.user["username"],
                    "team": self.user["team"],
                    "year": str(y),
                    "month": str(m),
                    "task_id": tid,
                    "billing_code": "",
                    "task_name": "",
                    "task_description": "",
                    **{d: "" for d in dd_cols_next},
                    "total_hours": "",
                    "user_remarks": "",
                    "status": "draft",
                    "submitted_on": "",
                    "approved_by": "",
                    "approved_on": "",
                    "remarks": ""
                }
                tsdf = pd.concat([tsdf, pd.DataFrame([new_row])], ignore_index=True)
            save_timesheet_wide(yy, mm, tsdf)
            print(f"[CARRY] Added {len(selected_tids)} draft task rows for next week within the same month.")
        except Exception as e:
            print(f"[ERROR] Carry-forward failed: {e}")


    
    def user_save_timesheet(self, status):
        """
        Save timesheet rows for current user.
          Weekly: updates all months in the selected ISO week and marks state::<dd>='submitted' when submitting.
          Monthly: normal save/submit (no per-day state changes).
          Also injects hidden per-day remarks and (optional) carry-forward after weekly submit.
        """
        checked = [tid for tid, var in getattr(self, "ts_row_checks", {}).items() if var.get()]
        if not checked:
            return messagebox.showwarning("Select", "Please select one or more rows first.")

        # Skip rows that are already submitted/approved (locked)
        editable = [tid for tid in checked if not (self.ts_vars.get(tid, {}).get("locked"))]
        
        if not editable:
            return messagebox.showwarning(
                "Locked",
                "No editable rows selected.\nRows with Weekly Status 'submitted' or 'approved' cannot be changed or re-submitted."
            )
        
        # Work only on editable rows from here
        checked = editable


        if self.view_mode.get() == "Weekly":
            # Group week dates by month
            week_iso = self._current_week_dates()  # ['YYYY-MM-DD'] * 7
            month_map = {}
            for iso_d in week_iso:
                yy, mm, _ = map(int, iso_d.split("-"))
                dd = iso_to_dd_mmm_yy(iso_d)
                month_map.setdefault((yy, mm), []).append(dd)
    
            # Upsert values and mark per-day states
            for (yy, mm), dd_list in month_map.items():
                ensure_month_file(yy, mm)
                tsdf = load_timesheet_wide(yy, mm)
    
                for tid in checked:
                    info = self.ts_vars.get(tid)
                    if not info:
                        continue
    
                    # Values only for this month segment of the week
                    vals = {}
                    # Map info['date_cols'] index to day var
                    dc_to_var = dict(zip(info["date_cols"], info["vars"]))
                    for dc in dd_list:
                        var = dc_to_var.get(dc)
                        raw = (var.get() if var else "").strip()
                        vals[dc] = raw if raw else "0"
    
                    # Upsert (one row per user-task-month)
                    msk = (
                        (tsdf.get("username", pd.Series(dtype=str)) == self.user["username"]) &
                        (tsdf.get("task_id", pd.Series(dtype=str)).astype(str).str.upper() == tid) &
                        (tsdf.get("year", pd.Series(dtype=str)).astype(str) == str(yy)) &
                        (tsdf.get("month", pd.Series(dtype=str)).astype(str) == str(mm))
                    )
                    if not msk.any():
                        # Create new row initialized for the month
                        new_row = {
                            "username": self.user["username"],
                            "team": self.user["team"],
                            "year": str(yy),
                            "month": str(mm),
                            "task_id": tid,
                            "billing_code": "",
                            "task_name": "",
                            "task_description": "",
                            **{d: "" for d in month_date_cols(yy, mm)},
                            "total_hours": "",
                            "user_remarks": info.get("remarks_var", tk.StringVar(value="")).get().strip(),
                            "status": "submitted" if status == "submitted" else "draft",
                            "submitted_on": datetime.now().strftime("%Y-%m-%d %H:%M") if status == "submitted" else "",
                            "approved_by": "",
                            "approved_on": "",
                            "remarks": ""
                        }
                        tsdf = pd.concat([tsdf, pd.DataFrame([new_row])], ignore_index=True)
                        # re-evaluate mask
                        msk = (
                            (tsdf.get("username", pd.Series(dtype=str)) == self.user["username"]) &
                            (tsdf.get("task_id", pd.Series(dtype=str)).astype(str).str.upper() == tid) &
                            (tsdf.get("year", pd.Series(dtype=str)).astype(str) == str(yy)) &
                            (tsdf.get("month", pd.Series(dtype=str)).astype(str) == str(mm))
                        )
    
                    # Write values & set per-day state
                    for k, v in vals.items():
                        if k not in tsdf.columns:
                            tsdf[k] = ""
                        tsdf.loc[msk, k] = v
                        state_col = f"state::{k}"
                        if state_col not in tsdf.columns:
                            tsdf[state_col] = ""
                        # Always set per-day state based on status
                        if status == "submitted":
                            tsdf.loc[msk, state_col] = "submitted"
                        else:  # draft
                            tsdf.loc[msk, state_col] = "draft"

                        
    
                    # Row-level meta
                    tsdf.loc[msk, "user_remarks"] = info.get("remarks_var", tk.StringVar(value="")).get().strip()
                    tsdf.loc[msk, "status"] = "submitted" if status == "submitted" else "draft"
                    tsdf.loc[msk, "submitted_on"] = datetime.now().strftime("%Y-%m-%d %H:%M") if status == "submitted" else ""
    
                # Inject hidden remarks and save
                self._inject_day_remarks_into_df(tsdf, yy, mm)
                save_timesheet_wide(yy, mm, tsdf)
    
            # --- FIX: Carry-forward after Weekly submit (same month only) ---
            if status == "submitted":
                y_cur = int(self.week_year.get())
                next_week = int(self.week_no.get()) + 1
                self._carry_forward_to_next_week(y_cur, mm, checked)
    
        else:
            # Title: Monthly save removed (Weekly-only)
            print("[SKIP] Monthly save path removed; Weekly-only save writes to monthly files for the 7 days.")

    
        # Sync normalized backend & notify
        sync_timesheets_long_from_all_wide()
        self._weekly_cache = {}  # Clear cache to force reload
        self.user_times_refresh_grid()  # Refresh grid to show updated weekly status

        messagebox.showinfo("Saved", f"Timesheet saved as {status}.")
        print(f"[SAVE] Mode={self.view_mode.get()} status={status} rows={len(checked)}")
        print("[CONCLUSION] Save complete; weekly day states set on submit; long backend synced.")
    
    # === [Cell 8 · Task Picker helpers & PICKER FILTER FOR TIMESHEET] ==============
    def load_my_task_picker(self):
        """Populate Task Picker with only tasks allowed for timesheet entry."""
        if not hasattr(self, "lb_tasks"):
            return
    
        self.lb_tasks.delete(0, "end")
        df = load_df(TASKS_CSV, TASKS_COLUMNS)
        if df.empty:
            return
    
        # Normalize status
        if "task_status" not in df.columns:
            df["task_status"] = "Not Started"
        df["task_status"] = df["task_status"].fillna("").replace("", "Not Started")
    
        # Only my tasks that are allowed for timesheet entry
        my = df[(df["assigned_user"] == self.user["username"])
               & (df["task_status"].isin(USER_TIMESHEET_VISIBLE_STATUSES))].copy()
    
        # Optional search filter
        q = (self.ts_search.get() or "").strip().lower() if hasattr(self, "ts_search") else ""
        if q:
            mask = (
                my["task_id"].astype(str).str.lower().str.contains(q, na=False)
                | my["task_name"].astype(str).str.lower().str.contains(q, na=False)
                | my["task_description"].astype(str).str.lower().str.contains(q, na=False)
            )
            my = my.loc[mask].copy()

    
        # Show as "TID — Task Name - Task Description"
        for _, r in my.iterrows():
            tid = str(r["task_id"]).strip().upper()
            tname = str(r.get("task_name","")).strip()
            tdesc = str(r.get("task_description", "")).strip()
            display_text = f"{tid} - {tname} - {tdesc}"
            self.lb_tasks.insert("end", display_text)

    
        print(f"[Picker] Listed {len(my)} task(s) with status in {USER_TIMESHEET_VISIBLE_STATUSES}")
        print("[CONCLUSION] Completed/Closed do not appear in the picker.")
    
    def _ts_pick_select(self, all: bool):
        """Select all or clear in the listbox."""
        if all:
            self.lb_tasks.select_set(0, "end")
        else:
            self.lb_tasks.select_clear(0, "end")
    
    def _parse_tid_from_label(self, s: str) -> str:
        """Extract leading Task ID from 'TaskID - Task Name - Task Description'."""
        return (s.split(" - ")[0] if " - " in s else s).strip().upper()

    def _ts_pick_apply(self):
        """
        Capture selected TIDs from Task Picker and ensure they exist in the stitched weekly timesheet grid.
        If not present, add a new draft row for the current user in the correct month file.
        """
        # Get selected items from the listbox
        sel = [self.lb_tasks.get(i) for i in self.lb_tasks.curselection()]
        self.ts_selected_tids = {self._parse_tid_from_label(x) for x in sel}
    
        # Compute ISO week dates and month grouping
        week_iso = self._current_week_dates()  # ['YYYY-MM-DD'] for 7 days
        month_map = {}
        for iso_d in week_iso:
            yy, mm, _ = map(int, iso_d.split("-"))
            month_map.setdefault((yy, mm), []).append(iso_d)
    
        # Stitch weekly timesheet data (same logic as user_times_refresh_grid)
        parts = []
        for (yy, mm), dd_list in month_map.items():
            part = load_timesheet_wide(yy, mm)
            if not part.empty:
                parts.append(part)
    
        tsdf = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        tsdf = tsdf.fillna("").replace({"nan": "", "NaN": "", "None": ""})
    
        # Use first day of the week for y/m fallback
        y, m = map(int, week_iso[0].split("-")[:2])
    
        # Add missing rows for selected tasks
        for tid in self.ts_selected_tids:
            # Defensive check for task_id column
            exists = False
            if "task_id" in tsdf.columns:
                exists = tsdf["task_id"].astype(str).str.upper().eq(tid.upper()).any()
    
            if not exists:
                ensure_month_file(y, m)
                base = load_timesheet_wide(y, m)
    
                new_row = {
                    "username": self.user["username"],
                    "team": self.user["team"],
                    "year": str(y),
                    "month": str(m),
                    "task_id": tid,
                    "billing_code": "",
                    "task_name": "",
                    "task_description": "",
                    **{d: "" for d in month_date_cols(y, m)},
                    "total_hours": "",
                    "user_remarks": "",
                    "status": "draft",
                    "submitted_on": "",
                    "approved_by": "",
                    "approved_on": "",
                    "remarks": ""
                }
    
                base = pd.concat([base, pd.DataFrame([new_row])], ignore_index=True)
                save_timesheet_wide(y, m, base)
    
        # Refresh grid after adding rows
        self.user_times_refresh_grid()


        
    # === [Cell 8 · ADD] CSV/XLSX Template / Import / Export helpers ===
     
    def _df_to_path(self, df: pd.DataFrame, path: str):
        """Write DataFrame to CSV or XLSX based on file extension."""
        ext = os.path.splitext(path)[1].lower()
        try:
            if ext == ".xlsx":
                try:
                    df.to_excel(path, index=False)
                except Exception as e:
                    messagebox.showwarning("Excel Save Failed", f"Falling back to CSV. Reason: {e}")
                    csv_path = os.path.splitext(path)[0] + ".csv"
                    df.to_csv(csv_path, index=False)
            else:
                df.to_csv(path, index=False)
        except Exception as e:
            messagebox.showerror("Save failed", f"Could not write file:\n{e}")
            log.exception("Operation failed")
    
    def _path_to_df(self, path: str) -> pd.DataFrame:
        """Read CSV or XLSX based on extension."""
        ext = os.path.splitext(path)[1].lower()
        try:
            if ext == ".xlsx":
                return pd.read_excel(path, dtype=str).fillna("")
            return pd.read_csv(path, dtype=str).fillna("")
        except Exception as e:
            messagebox.showerror("Read error", f"Unable to read file:\n{e}")
            log.exception("Operation failed")
            return pd.DataFrame()
    

    def _my_month_base(self):
        """
        Compute (year, month, month_days, dept) for template/export in Weekly-only mode.
        Uses the first day of the selected ISO week to determine the month.
        """
        y = int(self.week_year.get())
        w = int(self.week_no.get())
        # Get ISO week dates
        iso_dates = week_date_range(y, w)  # ['YYYY-MM-DD'] for 7 days
        # Use first day of week to determine month
        first_day = iso_dates[0]
        yy, mm, _ = map(int, first_day.split("-"))
        days = month_date_cols(yy, mm)
        return yy, mm, days, self.user["team"]

    
    def _selected_or_all_my_tids(self) -> set:
        """Return selected TIDs (if any) or all assigned TIDs (pending) for the month."""
        if getattr(self, "ts_selected_tids", None):
            return {t.upper() for t in self.ts_selected_tids}
        tdf = load_df(TASKS_CSV, TASKS_COLUMNS)
        if tdf.empty:
            return set()
        mine = tdf[tdf["assigned_user"] == self.user["username"]]
        return set(mine["task_id"].astype(str).str.upper().tolist())
    
    def download_my_ts_template(self):
        """Download an empty template for the selected month for (selected) tasks only."""
        y, m, days, dept = self._my_month_base()
        tids = self._selected_or_all_my_tids()
        if not tids:
            return messagebox.showwarning("No Tasks", "No tasks to include in template.")
        
        # Enrich names & descriptions from TASKS.csv
        tasks = load_df(TASKS_CSV, TASKS_COLUMNS)
        t_bill_map = dict(zip(tasks["task_id"].astype(str).str.upper(), tasks["billing_code"].astype(str))) if not tasks.empty else {}
        tname = dict(zip(tasks["task_id"].astype(str).str.upper(), tasks["task_name"].astype(str))) if not tasks.empty else {}
        tdesc = dict(zip(tasks["task_id"].astype(str).str.upper(), tasks["task_description"].astype(str))) if not tasks.empty else {}
        
        # Build header and rows
        cols = TS_BASE_WITH_DESC + days + TS_TAIL
        rows = []
        for tid in sorted(tids):
            rows.append({
                "username": self.user["username"],
                "team": dept,
                "year": str(y),
                "month": str(m),
                "task_id": tid,
                "billing_code": t_bill_map.get(tid, ""),
                "task_name": tname.get(tid, ""),              
                "task_description": tdesc.get(tid, ""),
                **{d: "" for d in days},
                "total_hours": "",
                "status": "",
                "submitted_on": "",
                "approved_by": "",
                "approved_on": "",
                "remarks": ""
            })

        df = pd.DataFrame(rows, columns=cols)
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            initialfile=f"my_timesheet_template_{y}_{m:02d}.csv",
            filetypes=[("CSV","*.csv"),("Excel","*.xlsx")]
        )
        if not path: return
        self._df_to_path(df, path)
        messagebox.showinfo("Template", f"Template saved:\n{path}")
        log_event("TIMESHEET", "Template downloaded for current month")
        print("[download_my_ts_template] Template created.")
        print("[CONCLUSION] You can fill this file offline and import back.")
    
    def import_my_ts_file(self):
        """Import filled timesheet (CSV/XLSX) into current month for current user; merges rows."""
        y, m, days, dept = self._my_month_base()
        base = load_timesheet_wide(y, m)
        path = filedialog.askopenfilename(filetypes=[("CSV/Excel", "*.csv *.xlsx")])
        if not path: return
        inc = self._path_to_df(path)
        if inc.empty:
            return
    
        # Ensure mandatory columns exist
        for c in TS_BASE_WITH_DESC + TS_TAIL:
            if c not in inc.columns: inc[c] = ""
        # Force current user/month & username/team normalization
        inc["username"] = self.user["username"]
        inc["team"] = dept
        inc["year"] = str(y); inc["month"] = str(m)
        # Keep only current month day columns
        for d in days:
            if d not in inc.columns: inc[d] = ""
        import re as _re
        
        for c in list(inc.columns):
            if _is_dd_mmm_yy(c) and c not in days:
                inc.drop(columns=[c], inplace=True)
    
        # Limit to allowed tasks (selected or all assigned)
        allowed = self._selected_or_all_my_tids()
        inc["task_id"] = inc["task_id"].astype(str).str.upper()
        inc = inc[inc["task_id"].isin(allowed)]
        if inc.empty:
            return messagebox.showwarning("No Matching Tasks", "No rows match your selected/assigned tasks.")
    
        
        # Enrich task_name and task_description if blank
        tasks = load_df(TASKS_CSV, TASKS_COLUMNS)
        tname = dict(zip(tasks["task_id"].astype(str).str.upper(), tasks["task_name"].astype(str))) if not tasks.empty else {}
        tdesc = dict(zip(tasks["task_id"].astype(str).str.upper(), tasks["task_description"].astype(str))) if not tasks.empty else {}
        
        inc["task_id"] = inc["task_id"].astype(str).str.upper()
        inc["task_name"] = inc.apply(
            lambda r: r["task_name"] if str(r["task_name"]).strip() else tname.get(r["task_id"], ""),
            axis=1
        )
        inc["task_description"] = inc.apply(
            lambda r: r["task_description"] if str(r["task_description"]).strip() else tdesc.get(r["task_id"], ""),
            axis=1
        )

        # Do not overwrite locked entries (submitted/approved)
        locked = {"submitted", "approved"}
    
        # Merge
        merged = base.copy()
        if merged.empty:
            merged = inc.copy()
        else:
            for _, r in inc.iterrows():
                msk =   (
                            merged.get("username", pd.Series(dtype=str)).str.strip().str.lower() == str(r["username"]).strip().lower()
                        ) & (
                            merged.get("team", pd.Series(dtype=str)).str.strip() == str(r["team"]).strip()
                        ) & (
                            merged.get("year", pd.Series(dtype=str)).str.strip() == str(r["year"]).strip()
                        ) & (
                            merged.get("month", pd.Series(dtype=str)).str.strip() == str(r["month"]).strip()
                        ) & (
                            merged.get("task_id", pd.Series(dtype=str)).str.strip().str.upper() == str(r["task_id"]).strip().upper()
                        )

                # Skip if existing row is locked
                if msk.any():
                    curr_status = str(merged.loc[msk, "status"].iloc[0]).strip().lower()
                    if curr_status in locked:
                        continue
                    for k, v in r.items():
                        merged.loc[msk, k] = v
                else:
                    merged = pd.concat([merged, pd.DataFrame([r])], ignore_index=True)
        merged = merged.drop_duplicates(subset=["username","year","month","task_id"], keep="last")
        save_timesheet_wide(y, m, merged)
        sync_timesheets_long_from_all_wide()
        self.user_times_refresh_grid()
        messagebox.showinfo("Imported", "Your file was imported into this month.")
        log_event("TIMESHEET", "Imported file into current month (user merge)")
        print("[import_my_ts_file] Import & merge completed.")
        print("[CONCLUSION] Draft rows updated; submitted/approved rows preserved.")
    
    def export_my_ts_view(self):
        """Export your current month rows (filtered by selection if any) to CSV/XLSX."""
        y, m, days, _ = self._my_month_base()
        df = load_timesheet_wide(y, m)
        if df.empty:
            return messagebox.showinfo("No Data", "No rows to export.")
        df = df[(df["username"] == self.user["username"])]
        sel = self._selected_or_all_my_tids()
        if sel:
            df = df[df["task_id"].astype(str).str.upper().isin(sel)]
        if df.empty:
            return messagebox.showinfo("No Data", "No rows to export after filtering.")
        # Reorder to month header order
        cols = TS_BASE_WITH_DESC + days + TS_TAIL
        for c in cols:
            if c not in df.columns: df[c] = ""
        df = df[cols + [c for c in df.columns if c not in cols]]
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            initialfile=f"my_timesheet_{y}_{m:02d}.csv",
            filetypes=[("CSV","*.csv"),("Excel","*.xlsx")]
        )
        if not path: return
        self._df_to_path(df, path)
        messagebox.showinfo("Exported", f"Exported to:\n{path}")
        log_event("TIMESHEET", "Exported current month view")
        
        # === [Cell 8 · ADD] PortalApp: Keyboard Accessibility (focus traversal, Enter on buttons, Up/Down navigation) ===
        # === [Cell 8 · ADD] PortalApp.enable_keyboard_accessibility (Enter, Up/Down, Tab, default button) ===
    def enable_keyboard_accessibility(self):
        """
        Make the whole app keyboard-friendly:
          - Enter on a focused Button invokes it
          - Enter on Entry/Combobox invokes the default button (or moves to next field)
          - Up/Down move focus between Entry/Combobox fields
          - Shift+Tab moves focus backward
          - Esc clears selection in Listbox/Treeview
        """
        root = self.root
        self._default_button = None  # tracked default button
    
        # ---- helpers ----
        def widgets_iter(container):
            for w in container.winfo_children():
                yield w
                yield from widgets_iter(w)
    
        def make_focusable(container):
            focusables = (tk.Entry, ttk.Entry, ttk.Combobox, ttk.Button,
                          tk.Text, tk.Listbox, ttk.Treeview, ttk.Checkbutton,
                          ttk.Radiobutton, ttk.Scale, ttk.Spinbox)
            for w in widgets_iter(container):
                try:
                    if isinstance(w, focusables):
                        w.configure(takefocus=True)
                except Exception:
                    pass
        def focus_next(event):
            event.widget.tk_focusNext().focus_set()
            return "break"
    
        def focus_prev(event):
            event.widget.tk_focusPrev().focus_set()
            return "break"
    
        def entry_return(event):
            # Enter in Entry/Combobox -> default button if available, else move next
            db = getattr(self, "_default_button", None)
            try:
                if db and str(db['state']) != 'disabled':
                    db.invoke()
                    return "break"
            except Exception:
                pass
            return focus_next(event)
    
        def entry_up(event):   return focus_prev(event)
        def entry_down(event): return focus_next(event)
    
        def button_return(event):
            try:
                event.widget.invoke()
            except Exception:
                pass
            return "break"
    
        def global_return(event):
            w = root.focus_get()
            # Keep native behavior for multi-line/list widgets
            if isinstance(w, (tk.Text, tk.Listbox, ttk.Treeview)):
                return
            db = getattr(self, "_default_button", None)
            if db and str(db['state']) != 'disabled':
                try:
                    db.invoke()
                    return "break"
                except Exception:
                    pass
    
        def global_escape(event):
            w = root.focus_get()
            try:
                if isinstance(w, ttk.Treeview):
                    for i in w.selection(): w.selection_remove(i)
                    return "break"
                if isinstance(w, tk.Listbox):
                    w.selection_clear(0, "end")
                    return "break"
            except Exception:
                pass
    
        # ---- apply focusability ----
        make_focusable(root)
    
        # ---- class bindings ----
        # Entry / TEntry
        root.bind_class("Entry",    "<Return>", entry_return)
        root.bind_class("TEntry",   "<Return>", entry_return)
        root.bind_class("TEntry",   "<Up>",     entry_up)
        root.bind_class("TEntry",   "<Down>",   entry_down)
    
        # Combobox
        root.bind_class("TCombobox","<Return>", entry_return)
        root.bind_class("TCombobox","<Up>",     entry_up)
        root.bind_class("TCombobox","<Down>",   entry_down)
    
        # Buttons
        root.bind_class("TButton",  "<Return>", button_return)
        root.bind_class("Button",   "<Return>", button_return)
    
        # Text: Ctrl+Enter submits via default button
        def text_ctrl_enter(event):
            db = getattr(self, "_default_button", None)
            if db and str(db['state']) != 'disabled':
                db.invoke()
                return "break"
        root.bind_class("Text", "<Control-Return>", text_ctrl_enter)
    
        # ---- global accelerators ----
        root.bind_all("<Return>",    global_return, add="+")
        root.bind_all("<Escape>",    global_escape, add="+")
        root.bind_all("<Shift-Tab>", focus_prev,    add="+")
    
        # API: set default button for current view
        def set_default_button(widget):
            try:
                if self._default_button and self._default_button is not widget:
                    try:
                        self._default_button.configure(default="normal")
                    except Exception:
                        pass
                widget.configure(default="active")  # Windows will show bold border
                self._default_button = widget
            except Exception:
                self._default_button = widget
        self.set_default_button = set_default_button
    
        print("[ACCESSIBILITY] Keyboard bindings enabled (Enter/Up/Down/Shift+Tab/Esc).")
        print("[CONCLUSION] Keyboard navigation + default button now active across the app.")


# ---- Quick preview & conclusion ----
try:
    # show presence of month files
    month_files = sorted(glob.glob(os.path.join(DATA_DIR, "timesheets_????_??.csv")))
    print("\n-- Monthly wide files present --")
    for fp in month_files: print(" ", os.path.basename(fp))
    # rebuild long once to be consistent
    _long = sync_timesheets_long_from_all_wide()
    print("\n-- timesheet_entries.csv (top) --")
    print(_long.head(8).to_string(index=False))
except Exception as e:
    print("[CHECK] Preview failed:", e)
print("[CHECK] PortalApp has _remark_popup:", hasattr(PortalApp, "_remark_popup"))
print("\n[CONCLUSION] Wide storage is now per-month (timesheets_YYYY_MM.csv).")


# ---- Optional: Rebuild long now to take effect on existing data --------
try:
    _long = sync_timesheets_long_from_all_wide()
    print("\n-- timesheet_entries.csv (top 10) --")
    print(_long.head(10).to_string(index=False))
    print("\n[CONCLUSION] Long rebuilt with per-day remark fallback applied.")
except Exception as e:
    print("[INFO] Rebuild skipped:", e)



print("No cross-month daily columns will appear in the same file anymore.")
print("Long backend rebuilt from monthly files. Run Cell 9 to launch the app.")


# In[ ]:


# === [Cell 9] Launch Application — Optimized with All CSVs ===
# Title: Launch Tkinter App with CSV Preview (Wide + Long Backend)
print("=== CSV Backend Initialized (Monthly dd-MMM-yy, Tkinter) ===")
print(f"[CONFIG] Shared Data Folder → {DATA_DIR}")

# Preview top rows of all key CSV files
csv_files = [
    ("users.csv", USERS_CSV, USERS_COLUMNS),
    ("tasks.csv", TASKS_CSV, TASKS_COLUMNS),
    ("timesheets.csv", TIMESHEET_CSV, None),
    ("timesheet_entries.csv", TIMESHEET_LONG_CSV, TS_LONG_COLUMNS)
]
for name, path, cols in csv_files:
    try:
        df = load_df(path, cols)
        print(f"\n-- {name} (top) --\n", df.head(10).to_string(index=False))
    except Exception as e:
        print(f"\n-- {name} --\n[ERROR] Could not load: {e}")

# Launch the GUI
try:
    def _tk_report_callback_exception(self, exc, val, tb):
        try:
            log.exception("Tkinter callback exception", exc_info=(exc, val, tb))
        except Exception:
            pass
   
    root = tk.Tk()
    root.report_callback_exception = _tk_report_callback_exception.__get__(root, tk.Tk)
  
    app = PortalApp(root)

    # Enable keyboard accessibility AFTER app is created
    try:
        app.enable_keyboard_accessibility()
        print("[OK] Keyboard accessibility enabled (Enter/Up/Down/Shift+Tab/Esc + default button).")
        print("[CONCLUSION] You can now use the app fully with keyboard and mouse.")
    except Exception as e:
        print("[ERR] Could not enable keyboard accessibility:", e)
        print("[CONCLUSION] Verify enable method exists in PortalApp (Cell 8) and rerun.")

    root.mainloop()
except Exception as e:
    print(f"[ERROR] Application failed to launch: {e}")

# --- OUTPUT / CONCLUSION ---
print("\n[CONCLUSION] Application launched successfully.")
print("All CSVs loaded including normalized backend (timesheet_entries.csv).")
print("Admin features include in-grid subtotals, full button set, and restored Settings tab.")


