"""
greytHR Attendance Fetcher → Zoho Analytics
============================================
- First run (Zoho table empty): fetches from 2025-01-01 → yesterday
- Subsequent runs: fetches last 2 months → upsert into Zoho
- Zoho OAuth token is refreshed at the start of every run
- Uses Zoho Analytics v1 Import API (UPDATEADD) with CSV multipart upload
- Runs via GitHub Actions every 2 hours

Usage:
    python greythr_attendance.py
"""

import requests
import pandas as pd
import json
import time
import os
import sys
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

# ── CONFIG ────────────────────────────────────────────────────────────────────
GREYTHR_TOKEN  = os.environ.get("GREYTHR_TOKEN", "XYZ")
DOMAIN         = "srv-media.greythr.com"

ZOHO_REFRESH_TOKEN  = os.environ.get("ZOHO_REFRESH_TOKEN")
ZOHO_CLIENT_ID      = os.environ.get("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET  = os.environ.get("ZOHO_CLIENT_SECRET")

HISTORY_START = date(2025, 1, 1)
# ─────────────────────────────────────────────────────────────────────────────


# ── ZOHO AUTH ─────────────────────────────────────────────────────────────────

def get_zoho_access_token() -> str:
    """Generate a fresh Zoho access token using the refresh token."""
    print("🔑 Refreshing Zoho access token …")
    url = "https://accounts.zoho.in/oauth/v2/token"
    params = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id":     ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type":    "refresh_token",
    }
    r = requests.post(url, params=params, timeout=30)
    if r.status_code != 200:
        print(f"❌ Token refresh failed: {r.status_code} {r.text}")
        sys.exit(1)

    data = r.json()
    token = data.get("access_token")
    if not token:
        print(f"❌ No access_token in response: {data}")
        sys.exit(1)

    print("  ✅ Zoho access token obtained")
    return token


# ── ZOHO DATA ─────────────────────────────────────────────────────────────────

ZOHO_WORKSPACE_ID = "445405000000352027"
ZOHO_VIEW_ID      = "445405000014385591"
ZOHO_V2_BASE      = "https://analyticsapi.zoho.in/restapi/v2"

def zoho_row_count(access_token: str) -> int:
    """
    Check if Zoho table has any rows using v2 API.
    Returns 0 on any failure (treated as first run).
    """
    url     = f"{ZOHO_V2_BASE}/workspaces/{ZOHO_WORKSPACE_ID}/views/{ZOHO_VIEW_ID}/data"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}", "ZANALYTICS-ORGID": os.environ.get("ZOHO_ORG_ID", "")}
    params  = {"pageSize": 1}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        print(f"  ℹ️  Row count check: HTTP {r.status_code}")
        print(f"  ℹ️  Response: {r.text[:300]}")
        if r.status_code == 200:
            data  = r.json()
            count = (
                data.get("data", {}).get("totalCount")
                or data.get("totalCount")
                or 0
            )
            return int(count)
    except Exception as e:
        print(f"  ⚠️  Could not check row count: {e}")
    return 0


def zoho_upsert(df: pd.DataFrame, access_token: str):
    """
    Push full DataFrame to Zoho Analytics using v2 Import API.
    - Batches of 5000 rows each
    - CONFIG as form field (data=), FILE as multipart
    - Stops immediately on any error
    """
    import io

    base_url = f"{ZOHO_V2_BASE}/workspaces/{ZOHO_WORKSPACE_ID}/views/{ZOHO_VIEW_ID}/data"
    headers  = {
        "Authorization":    f"Zoho-oauthtoken {access_token}",
        "ZANALYTICS-ORGID": os.environ.get("ZOHO_ORG_ID", ""),
    }

    config = {
        "importType":      "updateadd",
        "fileType":        "csv",
        "autoIdentify":    "true",
        "dateFormat":      "yyyy-MM-dd",
        "matchingColumns": ["Employee ID", "Date"],
    }
    config_str = json.dumps(config)

    batch_size = 5000
    total      = len(df)
    batches    = (total + batch_size - 1) // batch_size

    print(f"\n  📤 Pushing {total:,} rows in {batches} batch(es) of {batch_size} …")

    for i in range(batches):
        batch = df.iloc[i * batch_size : (i + 1) * batch_size]

        buf = io.BytesIO()
        batch.to_csv(buf, index=False, encoding="utf-8-sig")
        csv_bytes = buf.getvalue()

        # always updateadd — guarantees no duplicates on every run

        r = requests.post(
            base_url,
            headers=headers,
            data={"CONFIG": config_str},
            files={"FILE": ("data.csv", csv_bytes, "text/csv")},
            timeout=120,
        )

        if r.status_code == 200:
            try:
                resp    = r.json()
                summary = resp.get("data", {}).get("importSummary", {})
                print(f"    ✅ Batch {i+1}/{batches} — "
                      f"rows: {summary.get('successRowCount','?')} | "
                      f"op: {summary.get('importOperation','?')}")
            except Exception:
                print(f"    ✅ Batch {i+1}/{batches} succeeded")
        else:
            print(f"    ❌ Batch {i+1}/{batches} failed: HTTP {r.status_code}")
            print(f"       {r.text[:500]}")
            sys.exit(1)

        time.sleep(1)


# ── GREYTHR HELPERS ───────────────────────────────────────────────────────────

def greythr_headers() -> dict:
    return {
        "ACCESS-TOKEN":     GREYTHR_TOKEN,
        "x-greythr-domain": DOMAIN,
        "Content-Type":     "application/json",
    }


def yesterday() -> date:
    return date.today() - timedelta(days=1)


def date_chunks(start: date, end: date, max_days: int = 30):
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=max_days - 1), end)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def get_request(url: str, params: dict, retries: int = 3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=greythr_headers(), params=params, timeout=30)
            if r.status_code == 429:
                print("    ⚠️  Rate limited — waiting 15 s …")
                time.sleep(15)
                continue
            if r.status_code != 200:
                print(f"    ❌ HTTP {r.status_code}: {r.text[:300]}")
                return None
            return r.json()
        except requests.exceptions.RequestException as e:
            print(f"    ⚠️  Attempt {attempt + 1} failed: {e}")
            time.sleep(3)
    print("    ❌ Max retries reached.")
    return None


def hhmmss_to_minutes(val) -> int:
    if val is None:
        return 0
    if isinstance(val, float) and pd.isna(val):
        return 0
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null", ""):
        return 0
    if ":" in s:
        parts = s.split(":")
        try:
            h   = int(parts[0])
            m   = int(parts[1])
            sec = int(float(parts[2])) if len(parts) > 2 else 0
            return h * 60 + m + (1 if sec >= 30 else 0)
        except (IndexError, ValueError):
            return 0
    try:
        return round(float(s) * 60)
    except ValueError:
        return 0


def extract_time(dt_str) -> str:
    if not dt_str or (isinstance(dt_str, float) and pd.isna(dt_str)):
        return ""
    s = str(dt_str).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return ""
    try:
        return datetime.fromisoformat(s).strftime("%H:%M:%S")
    except ValueError:
        return ""


def build_status(s1, s2) -> str:
    s1 = str(s1).strip() if s1 and not (isinstance(s1, float) and pd.isna(s1)) else ""
    s2 = str(s2).strip() if s2 and not (isinstance(s2, float) and pd.isna(s2)) else ""
    if s1 == s2:
        return s1
    if not s1:
        return s2
    if not s2:
        return s1
    return f"{s1}:{s2}"


# ── EMPLOYEES ─────────────────────────────────────────────────────────────────

def fetch_employees() -> pd.DataFrame:
    url      = "https://api.greythr.com/employee/v2/employees"
    page     = 0
    all_emps = []

    while True:
        print(f"  Fetching employees page {page + 1} …")
        data = get_request(url, {"page": page, "size": 2000})
        if data is None:
            break

        if isinstance(data, dict):
            emps = (data.get("data") or data.get("content")
                    or data.get("employees") or [])
            pages_meta  = data.get("pages", {})
            total_pages = int(
                pages_meta.get("totalPages")
                or pages_meta.get("total_pages")
                or data.get("totalPages")
                or 1
            )
            is_last = (page + 1) >= total_pages
        elif isinstance(data, list):
            emps    = data
            is_last = True
        else:
            break

        all_emps.extend(emps)
        print(f"    ✅ {len(emps)} employees | page {page + 1}/{total_pages}")

        if is_last:
            break
        page += 1
        time.sleep(0.5)

    if not all_emps:
        print("  ⚠️  No employee records returned.")
        return pd.DataFrame()

    return pd.json_normalize(all_emps)


# ── ATTENDANCE ────────────────────────────────────────────────────────────────

def fetch_attendance_range(start: date, end: date) -> list:
    url      = "https://api.greythr.com/attendance/v2/employee/muster"
    all_rows = []

    for chunk_start, chunk_end in date_chunks(start, end, max_days=30):
        page = 0
        print(f"\n  📅 Chunk: {chunk_start} → {chunk_end}")

        while True:
            params = {
                "start": chunk_start.isoformat(),
                "end":   chunk_end.isoformat(),
                "page":  page,
                "size":  1000,
            }
            data = get_request(url, params)
            if data is None:
                break

            if isinstance(data, dict) and "data" in data:
                employees   = data["data"]
                pages_meta  = data.get("pages", {})
                total_pages = int(
                    pages_meta.get("totalPages")
                    or pages_meta.get("total_pages")
                    or 1
                )
                is_last = (page + 1) >= total_pages
            elif isinstance(data, dict) and "content" in data:
                employees   = data["content"]
                total_pages = int(data.get("totalPages", 1))
                is_last     = data.get("last", True)
            elif isinstance(data, list):
                employees   = data
                total_pages = 1
                is_last     = True
            else:
                print(f"    ⚠️  Unexpected structure: "
                      f"{list(data.keys()) if isinstance(data, dict) else type(data)}")
                break

            print(f"    ✅ {len(employees)} employees | page {page + 1}/{total_pages}")

            for emp in employees:
                employee_id = emp.get("employeeId")
                records     = emp.get("records", [])
                if isinstance(records, str):
                    try:
                        records = json.loads(records)
                    except json.JSONDecodeError:
                        records = []

                for day in records:
                    summary = day.get("summary", {}) or {}
                    row = {
                        "employeeId":      employee_id,
                        "date":            summary.get("attendanceDate"),
                        "dayType":         summary.get("dayType"),
                        "firstInTime":     summary.get("firstInTime"),
                        "lastOutTime":     summary.get("lastOutTime"),
                        "totalWorkHrs":    summary.get("totalWorkHrs"),
                        "productionHours": summary.get("productionHours"),
                        "shortFallHrs":    summary.get("shortFallHrs"),
                        "breakHours":      summary.get("breakHours"),
                        "session1Label":   summary.get("session1Label"),
                        "session2Label":   summary.get("session2Label"),
                    }
                    all_rows.append(row)

            if is_last:
                break
            page += 1
            time.sleep(0.8)

        time.sleep(1)

    return all_rows


# ── TRANSFORM & JOIN ──────────────────────────────────────────────────────────

def build_final(df_att: pd.DataFrame, df_emp: pd.DataFrame) -> pd.DataFrame:
    df = df_att.copy()

    # Filter OffDay
    if "dayType" in df.columns:
        before = len(df)
        df = df[df["dayType"].astype(str).str.strip() != "OffDay"].copy()
        print(f"    🗑️  Removed {before - len(df):,} OffDay records")

    # Filter Employee IDs starting with 'G'
    before = len(df)
    df = df[~df["employeeId"].astype(str).str.upper().str.startswith("G")].copy()
    print(f"    🗑️  Removed {before - len(df):,} records with Employee ID starting with G")

    # Time & minute columns
    df["Date"]                   = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["In Time"]                = df["firstInTime"].apply(extract_time)
    df["Out Time"]               = df["lastOutTime"].apply(extract_time)
    df["Work Minutes"]           = df["totalWorkHrs"].apply(hhmmss_to_minutes)
    df["Actual Minutes (floor)"] = df["productionHours"].apply(hhmmss_to_minutes)
    df["Shortfall(min)"]         = df["shortFallHrs"].apply(hhmmss_to_minutes)
    df["Break Minutes"]          = df["breakHours"].apply(hhmmss_to_minutes)

    # Status
    df["Status"] = df.apply(
        lambda r: build_status(r.get("session1Label"), r.get("session2Label")),
        axis=1
    )

    # Join employees
    wanted        = ["employeeId", "name", "employeeNo", "leftorg"]
    emp_col_lower = {c.lower(): c for c in df_emp.columns}

    rename_map = {}
    for col in wanted:
        if col in df_emp.columns:
            rename_map[col] = col
        elif col.lower() in emp_col_lower:
            rename_map[emp_col_lower[col.lower()]] = col

    df_emp_clean = (
        df_emp
        .rename(columns=rename_map)
        [[c for c in wanted if c in rename_map.values()]]
        .drop_duplicates(subset=["employeeId"])
    )

    df["employeeId"]           = df["employeeId"].astype(str).str.strip()
    df_emp_clean["employeeId"] = df_emp_clean["employeeId"].astype(str).str.strip()
    df = df.merge(df_emp_clean, on="employeeId", how="left")

    # Filter: active employees only (leftorg == FALSE)
    if "leftorg" in df.columns:
        before = len(df)
        df = df[df["leftorg"].astype(str).str.upper().str.strip() == "FALSE"].copy()
        print(f"    🗑️  Removed {before - len(df):,} records where leftorg ≠ FALSE")

    df = df.rename(columns={"employeeNo": "Employee ID", "name": "Employee Name"})

    # Final columns
    final_cols = [
        "Employee ID",
        "Employee Name",
        "Date",
        "In Time",
        "Out Time",
        "Work Minutes",
        "Actual Minutes (floor)",
        "Shortfall(min)",
        "Break Minutes",
        "Status",
        "leftorg",
    ]
    final_cols = [c for c in final_cols if c in df.columns]
    df_final   = df[final_cols].copy()

    # Clean up types
    numeric_cols = ["Work Minutes", "Actual Minutes (floor)", "Shortfall(min)", "Break Minutes"]
    for col in numeric_cols:
        if col in df_final.columns:
            df_final[col] = pd.to_numeric(df_final[col], errors="coerce").fillna(0).astype(int)

    # Clean string columns
    str_cols = ["Employee ID", "Employee Name", "In Time", "Out Time", "Status", "leftorg"]
    for col in str_cols:
        if col in df_final.columns:
            df_final[col] = df_final[col].astype(str).str.strip()
            df_final[col] = df_final[col].replace({"nan": "", "None": ""})

    # Remove rows with missing Employee ID or Date
    df_final = df_final[df_final["Employee ID"].str.strip() != ""]
    df_final = df_final.dropna(subset=["Date"])
    df_final = df_final[df_final["Date"].astype(str).str.strip() != ""]

    df_final.sort_values(["Employee ID", "Date"], inplace=True)
    df_final.reset_index(drop=True, inplace=True)

    return df_final


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  greytHR Attendance Fetcher → Zoho Analytics")
    print(f"  Run date : {date.today()}  |  Data up to: {yesterday()}")
    print("=" * 60)

    # 1. Refresh Zoho token
    zoho_token = get_zoho_access_token()

    # 2. Check Zoho table row count → decide fetch range
    print("\n[1/4] Checking Zoho table …")
    row_count = zoho_row_count(zoho_token)
    print(f"  ℹ️  Zoho table currently has {row_count:,} rows")

    if row_count == 0:
        fetch_start = HISTORY_START
        print(f"  🆕 First run — fetching full history from {HISTORY_START}")
    else:
        fetch_start = (date.today().replace(day=1) - relativedelta(months=1)).replace(day=1)
        print(f"  🔄 Incremental run — fetching last 2 months from {fetch_start}")

    # 3. Fetch employees
    print("\n[2/4] Fetching employees …")
    df_emp = fetch_employees()
    if df_emp.empty:
        print("❌ Could not fetch employees. Aborting.")
        sys.exit(1)

    # 4. Fetch attendance
    print("\n[3/4] Fetching attendance …")
    rows   = fetch_attendance_range(fetch_start, yesterday())
    df_att = pd.DataFrame(rows)
    if df_att.empty:
        print("⚠️  No attendance records fetched.")
        sys.exit(0)

    # 5. Transform + join
    print("\n[4/4] Transforming & joining …")
    df_final = build_final(df_att, df_emp)

    print(f"\n  Rows      : {len(df_final):,}")
    print(f"  Employees : {df_final['Employee ID'].nunique()}")
    if not df_final.empty:
        print(f"  Date range: {df_final['Date'].min()}  →  {df_final['Date'].max()}")

    # 6. Push to Zoho
    zoho_upsert(df_final, zoho_token)

    print("\n✅ All done!")


if __name__ == "__main__":
    main()
