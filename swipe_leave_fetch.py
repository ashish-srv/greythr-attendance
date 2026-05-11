"""
greytHR Daily Swipe + Leave Fetcher → Zoho Analytics
======================================================
- Fetches employees (filtered: leftorg=FALSE, employeeNo not starting with G)
- Fetches status LOV and leave category LOV
- Fetches today's and yesterday's swipes (first IN swipe per employee per day)
- Fetches today's and yesterday's leave transactions
- Logic:
    * Has swipe → show swipe + leave if any
    * No swipe + has leave → show leave data
    * No swipe + no leave → Leave Type = 'Absent'
- Pushes to Zoho Analytics with UPDATEADD on Employee ID + attendanceDate
- Runs every hour via GitHub Actions

Usage:
    python swipe_leave_fetch.py
"""

import requests
import pandas as pd
import json
import time
import os
import sys
from datetime import datetime, date, timedelta

# ── CONFIG ────────────────────────────────────────────────────────────────────
ACCESS_TOKEN        = os.environ.get("GREYTHR_TOKEN", "XYZ")
DOMAIN              = "srv-media.greythr.com"
OUTPUT_CSV          = "attendance_swipe_final.csv"

ZOHO_REFRESH_TOKEN  = os.environ.get("ZOHO_REFRESH_TOKEN")
ZOHO_CLIENT_ID      = os.environ.get("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET  = os.environ.get("ZOHO_CLIENT_SECRET")
ZOHO_ORG_ID         = os.environ.get("ZOHO_ORG_ID", "")
ZOHO_WORKSPACE_ID   = "445405000000352027"
ZOHO_VIEW_ID        = os.environ.get("ZOHO_SWIPE_VIEW_ID", "445405000014555002")
ZOHO_V2_BASE        = "https://analyticsapi.zoho.in/restapi/v2"

GREYTHR_HEADERS = {
    "ACCESS-TOKEN":     ACCESS_TOKEN,
    "x-greythr-domain": DOMAIN,
    "Content-Type":     "application/json",
}

TODAY     = date.today()
YESTERDAY = TODAY - timedelta(days=1)
# ─────────────────────────────────────────────────────────────────────────────


# ── ZOHO AUTH ─────────────────────────────────────────────────────────────────

def get_zoho_access_token() -> str:
    print("🔑 Refreshing Zoho access token …")
    r = requests.post(
        "https://accounts.zoho.in/oauth/v2/token",
        params={
            "refresh_token": ZOHO_REFRESH_TOKEN,
            "client_id":     ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "grant_type":    "refresh_token",
        },
        timeout=30,
    )
    if r.status_code != 200:
        print(f"❌ Token refresh failed: {r.status_code} {r.text}")
        sys.exit(1)
    token = r.json().get("access_token")
    if not token:
        print(f"❌ No access_token in response: {r.json()}")
        sys.exit(1)
    print("  ✅ Zoho access token obtained")
    return token


def zoho_delete_all(access_token: str):
    """Delete all existing rows from Zoho table before pushing fresh data."""
    url     = f"{ZOHO_V2_BASE}/workspaces/{ZOHO_WORKSPACE_ID}/views/{ZOHO_VIEW_ID}/rows"
    headers = {
        "Authorization":    f"Zoho-oauthtoken {access_token}",
        "ZANALYTICS-ORGID": ZOHO_ORG_ID,
    }
    print("\n  🗑️  Deleting all existing rows from Zoho table …")
    r = requests.delete(url, headers=headers, timeout=60)
    if r.status_code in (200, 204):
        print("  ✅ All rows deleted")
    else:
        print(f"  ⚠️  Delete returned HTTP {r.status_code}: {r.text[:300]}")
        print("  ⚠️  Continuing with push anyway …")


def zoho_push(df: pd.DataFrame, access_token: str):
    """Push DataFrame to Zoho using APPEND (fresh insert after delete)."""
    import io
    url     = f"{ZOHO_V2_BASE}/workspaces/{ZOHO_WORKSPACE_ID}/views/{ZOHO_VIEW_ID}/data"
    headers = {
        "Authorization":    f"Zoho-oauthtoken {access_token}",
        "ZANALYTICS-ORGID": ZOHO_ORG_ID,
    }
    config = {
        "importType":   "append",
        "fileType":     "csv",
        "autoIdentify": "true",
        "dateFormat":   "yyyy-MM-dd",
    }
    config_str = json.dumps(config)

    batch_size = 500
    total      = len(df)
    batches    = (total + batch_size - 1) // batch_size

    print(f"\n  📤 Pushing {total:,} rows in {batches} batch(es) …")

    for i in range(batches):
        batch = df.iloc[i * batch_size : (i + 1) * batch_size]
        buf   = io.BytesIO()
        batch.to_csv(buf, index=False, encoding="utf-8-sig")
        csv_bytes = buf.getvalue()

        r = requests.post(
            url,
            headers=headers,
            data={"CONFIG": config_str},
            files={"FILE": ("data.csv", csv_bytes, "text/csv")},
            timeout=120,
        )

        if r.status_code == 200:
            print(f"    ✅ Batch {i+1}/{batches} pushed")
        else:
            print(f"    ❌ Batch {i+1}/{batches} failed: HTTP {r.status_code}")
            print(f"       {r.text[:500]}")
            sys.exit(1)

        time.sleep(1)


# ── HELPERS ───────────────────────────────────────────────────────────────────

def get_request(url: str, params: dict = None, retries: int = 3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=GREYTHR_HEADERS, params=params, timeout=30)
            if r.status_code == 429:
                print("    ⚠️  Rate limited — waiting 15s …")
                time.sleep(15)
                continue
            if r.status_code != 200:
                print(f"    ❌ HTTP {r.status_code}: {r.text[:300]}")
                return None
            return r.json()
        except requests.exceptions.RequestException as e:
            print(f"    ⚠️  Attempt {attempt + 1} failed: {e}")
            time.sleep(3)
    return None


def post_request(url: str, body: list, retries: int = 3):
    for attempt in range(retries):
        try:
            r = requests.post(url, headers=GREYTHR_HEADERS, json=body, timeout=30)
            if r.status_code != 200:
                print(f"    ❌ HTTP {r.status_code}: {r.text[:300]}")
                return None
            return r.json()
        except requests.exceptions.RequestException as e:
            print(f"    ⚠️  Attempt {attempt + 1} failed: {e}")
            time.sleep(3)
    return None


# ── LOV ───────────────────────────────────────────────────────────────────────

def fetch_lov(lov_key: str) -> pd.DataFrame:
    print(f"  Fetching LOV: {lov_key} …")
    data = post_request("https://api.greythr.com/hr/v2/lov", [lov_key])
    if not data:
        return pd.DataFrame(columns=["id", "name"])

    rows = []
    if isinstance(data, dict):
        for value in data.values():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, list):
                        rows.append({"id": str(item[0]), "name": item[1] if len(item) > 1 else ""})
                    elif isinstance(item, dict):
                        rows.append({"id": str(item.get("id", "")), "name": item.get("name", "")})

    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["id", "name"])
    df = df[df["id"] != ""].drop_duplicates(subset=["id"])
    print(f"    ✅ {len(df)} entries")
    return df


# ── EMPLOYEES ─────────────────────────────────────────────────────────────────

def fetch_employees() -> pd.DataFrame:
    print("  Fetching employees …")
    url      = "https://api.greythr.com/employee/v2/employees"
    page     = 0
    all_emps = []

    while True:
        data = get_request(url, {"page": page, "size": 2000})
        if data is None:
            break

        if isinstance(data, dict):
            emps        = (data.get("data") or data.get("content") or data.get("employees") or [])
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
        return pd.DataFrame()

    df = pd.json_normalize(all_emps)

    keep     = ["employeeId", "name", "employeeNo", "leftorg", "status", "leavingDate"]
    existing = [c for c in keep if c in df.columns]
    df       = df[existing].copy()

    # Filter leftorg == FALSE
    df["leftorg"] = df["leftorg"].astype(str).str.upper().str.strip()
    df = df[df["leftorg"] == "FALSE"].copy()

    # Filter employeeNo not starting with G
    df = df[~df["employeeNo"].astype(str).str.upper().str.startswith("G")].copy()

    df["employeeId"] = df["employeeId"].astype(str).str.strip()
    df["employeeNo"] = df["employeeNo"].astype(str).str.strip()

    print(f"    ✅ {len(df)} employees after filters")
    return df


# ── SWIPES ────────────────────────────────────────────────────────────────────

def fetch_all_swipes(df_emp: pd.DataFrame) -> pd.DataFrame:
    print(f"  Fetching swipes for {len(df_emp)} employees ({YESTERDAY} → {TODAY}) …")
    all_rows = []

    for _, emp in df_emp.iterrows():
        emp_id = emp["employeeId"]
        url    = f"https://api.greythr.com/attendance/v2/employee/{emp_id}/swipes"
        params = {
            "start":        YESTERDAY.isoformat(),
            "end":          TODAY.isoformat(),
            "systemSwipes": "true",
        }
        data = get_request(url, params)
        if data is None:
            time.sleep(0.2)
            continue

        swipe_list = data.get("list", []) if isinstance(data, dict) else data

        for swipe in swipe_list:
            all_rows.append({
                "employeeId":     emp_id,
                "attendanceDate": swipe.get("attendanceDate"),
                "punchDateTime":  swipe.get("punchDateTime"),
                "inOutIndicator": swipe.get("inOutIndicator"),
                "doorName":       swipe.get("doorName"),
                "systemSwipe":    swipe.get("systemSwipe"),
            })

        time.sleep(0.2)

    if not all_rows:
        return pd.DataFrame(columns=["employeeId", "attendanceDate", "punchDateTime",
                                      "inOutIndicator", "doorName", "systemSwipe"])

    df = pd.DataFrame(all_rows)

    # Keep first IN swipe per employee per day
    df["punchDateTime"] = pd.to_datetime(df["punchDateTime"], errors="coerce")
    df_in = df[df["inOutIndicator"] == "IN"].copy()
    df_in = df_in.sort_values(["employeeId", "attendanceDate", "punchDateTime"])
    df_in = df_in.drop_duplicates(subset=["employeeId", "attendanceDate"], keep="first")

    print(f"    ✅ {len(df_in)} first-IN swipes")
    return df_in


# ── LEAVE ─────────────────────────────────────────────────────────────────────

def fetch_leave() -> pd.DataFrame:
    print(f"  Fetching leave data ({YESTERDAY} → {TODAY}) …")
    url      = "https://api.greythr.com/leave/v2/employee/transactions"
    page     = 0
    all_rows = []

    while True:
        params = {
            "start": YESTERDAY.isoformat(),
            "end":   TODAY.isoformat(),
            "page":  page,
            "size":  1000,
        }
        data = get_request(url, params)
        if data is None:
            break

        if isinstance(data, dict) and "data" in data:
            employees   = data["data"]
            pages_meta  = data.get("pages", {})
            total_pages = int(pages_meta.get("totalPages") or pages_meta.get("total_pages") or 1)
            is_last     = (page + 1) >= total_pages
        elif isinstance(data, dict) and "content" in data:
            employees   = data["content"]
            total_pages = int(data.get("totalPages", 1))
            is_last     = data.get("last", True)
        elif isinstance(data, list):
            employees   = data
            total_pages = 1
            is_last     = True
        else:
            break

        for emp in employees:
            employee_id  = str(emp.get("employeeId", "")).strip()
            transactions = emp.get("list", [])
            for txn in transactions:
                if str(txn.get("cancelled", "")).upper() == "TRUE":
                    continue
                all_rows.append({
                    "employeeId":        employee_id,
                    "leaveTypeCategory": str(txn.get("leaveTypeCategory", "")).strip(),
                    "fromDate":          txn.get("fromDate"),
                    "toDate":            txn.get("toDate"),
                    "fromSession":       txn.get("fromSession"),
                    "toSession":         txn.get("toSession"),
                    "reason":            txn.get("reason"),
                })

        print(f"    ✅ Page {page + 1}/{total_pages}")
        if is_last:
            break
        page += 1
        time.sleep(0.5)

    if not all_rows:
        print("    ⚠️  No leave records found")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["fromDate"] = pd.to_datetime(df["fromDate"], errors="coerce").dt.date
    df["toDate"]   = pd.to_datetime(df["toDate"],   errors="coerce").dt.date
    print(f"    ✅ {len(df)} leave records")
    return df


# ── BUILD FINAL ───────────────────────────────────────────────────────────────

def build_final(df_emp, df_swipes, df_leave, df_status_lov, df_leave_cat_lov) -> pd.DataFrame:

    # 1. Join status name to employees
    if "status" in df_emp.columns and not df_status_lov.empty:
        df_emp["status"]        = df_emp["status"].astype(str).str.strip()
        df_status_lov["id"]     = df_status_lov["id"].astype(str).str.strip()
        df_emp = df_emp.merge(
            df_status_lov.rename(columns={"name": "Employee Status"}),
            left_on="status", right_on="id", how="left"
        ).drop(columns=["id", "status"], errors="ignore")
    else:
        df_emp["Employee Status"] = ""

    # 2. Join leave category name to leave
    if not df_leave.empty and not df_leave_cat_lov.empty:
        df_leave["leaveTypeCategory"] = df_leave["leaveTypeCategory"].astype(str).str.strip()
        df_leave_cat_lov["id"]        = df_leave_cat_lov["id"].astype(str).str.strip()
        df_leave = df_leave.merge(
            df_leave_cat_lov.rename(columns={"name": "Leave Type"}),
            left_on="leaveTypeCategory", right_on="id", how="left"
        ).drop(columns=["id", "leaveTypeCategory"], errors="ignore")

    # 3. Build date spine: one row per employee per day
    dates    = [YESTERDAY.isoformat(), TODAY.isoformat()]
    spine    = []
    for _, emp in df_emp.iterrows():
        for d in dates:
            spine.append({
                "employeeId":      emp["employeeId"],
                "Employee ID":     emp.get("employeeNo", ""),
                "Employee Name":   emp.get("name", ""),
                "Employee Status": emp.get("Employee Status", ""),
                "leftorg":         emp.get("leftorg", ""),
                "leavingDate":     emp.get("leavingDate", ""),
                "attendanceDate":  d,
            })
    df_base = pd.DataFrame(spine)

    # 4. Join swipes
    if not df_swipes.empty:
        df_swipes["employeeId"]     = df_swipes["employeeId"].astype(str).str.strip()
        df_swipes["attendanceDate"] = df_swipes["attendanceDate"].astype(str).str.strip()
        df_swipes["Punch Date"]     = df_swipes["punchDateTime"].dt.strftime("%Y-%m-%d")
        df_swipes["Punch Time"]     = df_swipes["punchDateTime"].dt.strftime("%H:%M:%S")
        df_swipes = df_swipes.rename(columns={
            "inOutIndicator": "In/Out",
            "doorName":       "Door Name",
            "systemSwipe":    "System Swipe",
        })
        df_base = df_base.merge(
            df_swipes[["employeeId", "attendanceDate",
                        "Punch Date", "Punch Time", "In/Out", "Door Name", "System Swipe"]],
            on=["employeeId", "attendanceDate"],
            how="left"
        )
    else:
        for col in ["Punch Date", "Punch Time", "In/Out", "Door Name", "System Swipe"]:
            df_base[col] = ""

    # 5. Join leave — check if attendance date falls within fromDate-toDate
    leave_result = []
    df_base["_att_dt"] = pd.to_datetime(df_base["attendanceDate"]).dt.date

    for _, row in df_base.iterrows():
        emp_id = row["employeeId"]
        att_dt = row["_att_dt"]
        has_swipe = pd.notna(row.get("Punch Time")) and str(row.get("Punch Time", "")).strip() != ""

        leave_match = pd.DataFrame()
        if not df_leave.empty:
            leave_match = df_leave[
                (df_leave["employeeId"] == emp_id) &
                (df_leave["fromDate"]   <= att_dt) &
                (df_leave["toDate"]     >= att_dt)
            ]

        if not leave_match.empty:
            m = leave_match.iloc[0]
            leave_result.append({
                "Leave Type":  m.get("Leave Type", ""),
                "fromDate":    str(m.get("fromDate", "")),
                "toDate":      str(m.get("toDate", "")),
                "fromSession": m.get("fromSession", ""),
                "toSession":   m.get("toSession", ""),
                "reason":      m.get("reason", ""),
            })
        elif not has_swipe:
            # No swipe and no leave → Absent
            leave_result.append({
                "Leave Type":  "Absent",
                "fromDate":    "",
                "toDate":      "",
                "fromSession": "",
                "toSession":   "",
                "reason":      "",
            })
        else:
            # Has swipe, no leave → blank leave fields
            leave_result.append({
                "Leave Type":  "",
                "fromDate":    "",
                "toDate":      "",
                "fromSession": "",
                "toSession":   "",
                "reason":      "",
            })

    df_leave_result = pd.DataFrame(leave_result)
    df_base = pd.concat([df_base.reset_index(drop=True),
                          df_leave_result.reset_index(drop=True)], axis=1)
    df_base = df_base.drop(columns=["_att_dt", "employeeId"], errors="ignore")

    # 6. Final column order
    final_cols = [
        "Employee ID",
        "Employee Name",
        "Employee Status",
        "leftorg",
        "leavingDate",
        "attendanceDate",
        "Punch Date",
        "Punch Time",
        "In/Out",
        "Door Name",
        "System Swipe",
        "Leave Type",
        "fromDate",
        "toDate",
        "fromSession",
        "toSession",
        "reason",
    ]
    final_cols = [c for c in final_cols if c in df_base.columns]
    df_final   = df_base[final_cols].fillna("").copy()
    df_final.sort_values(["Employee ID", "attendanceDate"], inplace=True)
    df_final.reset_index(drop=True, inplace=True)
    return df_final


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  greytHR Swipe + Leave Fetcher → Zoho Analytics")
    print(f"  Date range: {YESTERDAY} → {TODAY}")
    print("=" * 60)

    # 1. Zoho token
    zoho_token = get_zoho_access_token()

    # 2. LOVs
    print("\n[1/5] Fetching LOVs …")
    df_status_lov    = fetch_lov("lov::status")
    df_leave_cat_lov = fetch_lov("lov::leavetypecategory")

    # 3. Employees
    print("\n[2/5] Fetching employees …")
    df_emp = fetch_employees()
    if df_emp.empty:
        print("❌ No employees found. Aborting.")
        sys.exit(1)

    # 4. Swipes
    print("\n[3/5] Fetching swipes …")
    df_swipes = fetch_all_swipes(df_emp)

    # 5. Leave
    print("\n[4/5] Fetching leave …")
    df_leave = fetch_leave()

    # 6. Build final
    print("\n[5/5] Building final output …")
    df_final = build_final(df_emp, df_swipes, df_leave, df_status_lov, df_leave_cat_lov)

    print(f"\n  Rows      : {len(df_final):,}")
    print(f"  Employees : {df_final['Employee ID'].nunique()}")
    print(f"\nSample:")
    print(df_final.head(3).to_string(index=False))

    # 7. Push to Zoho
    zoho_delete_all(zoho_token)
    zoho_push(df_final, zoho_token)

    print("\n✅ All done!")


if __name__ == "__main__":
    main()
