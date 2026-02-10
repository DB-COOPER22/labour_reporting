import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime, date
import pytz
import uuid
import os
import xml.etree.ElementTree as ET
from contextlib import contextmanager

# =========================
# CONFIG
# =========================
st.set_page_config(page_title="Labour Reporting", page_icon="🕒", layout="wide")

EMPLOYEE_FILE = Path("employees.csv")
BASE_DIR = Path("data")
BASE_DIR.mkdir(exist_ok=True)

TIMEZONE = "Australia/Sydney"
COUNTER_START = 300  # per day counter starts at 300

def load_employees() -> pd.DataFrame:
    if not EMPLOYEE_FILE.exists():
        return pd.DataFrame(columns=["name", "pin"])
    df = pd.read_csv(EMPLOYEE_FILE, dtype=str).fillna("")
    if "name" not in df.columns or "pin" not in df.columns:
        return pd.DataFrame(columns=["name", "pin"])
    df["name"] = df["name"].astype(str).str.strip()
    df["pin"]  = df["pin"].astype(str).str.strip()
    return df[df["name"] != ""]

# =========================
# UI CSS (cleaner + bigger comment)
# =========================
st.markdown("""
<style>
.main .block-container { max-width: 1200px; padding-top: 2rem; }
textarea { font-size: 16px !important; line-height: 1.35 !important; }
label, .stTextInput label, .stTextArea label, .stSelectbox label { font-weight: 600 !important; }
div[data-testid="stForm"] {
  border: 1px solid #e6e6e6;
  border-radius: 14px;
  padding: 18px;
  background: white;
}
</style>
""", unsafe_allow_html=True)

# =========================
# TIME + FORMAT HELPERS
# =========================
def now_sydney() -> datetime:
    return datetime.now(pytz.timezone(TIMEZONE))

def fmt_dt(dt: datetime) -> str:
    # XML datetime: 2026-01-08 17:27:36
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def fmt_file_dt(dt: datetime) -> str:
    # filename: 2026-01-08_Thu_17-27-36.xml
    return dt.strftime("%Y-%m-%d_%a_%H-%M-%S")

def sanitize_folder(name: str) -> str:
    cleaned = "".join(ch for ch in name.strip() if ch.isalnum() or ch in ("-", "_"))
    return cleaned or "USER"

def parse_hhmm_to_hours(hhmm: str) -> float | None:
    s = (hhmm or "").strip()
    if not s or ":" not in s:
        return None
    parts = s.split(":")
    if len(parts) != 2:
        return None
    try:
        hh = int(parts[0])
        mm = int(parts[1])
        if hh < 0 or mm < 0 or mm >= 60:
            return None
        return float(hh) + float(mm) / 60.0
    except Exception:
        return None

# =========================
# XML BYTES
# =========================
def xml_to_bytes(elem: ET.Element) -> bytes:
    xml_decl = b'<?xml version="1.0" encoding="UTF-8"?>\n\n'
    body = ET.tostring(elem, encoding="utf-8")
    return xml_decl + body

def atomic_write_bytes(path: Path, content: bytes):
    tmp = path.with_suffix(path.suffix + f".tmp-{uuid.uuid4().hex}")
    tmp.write_bytes(content)
    os.replace(tmp, path)

# =========================
# CROSS-PLATFORM FILE LOCK (FIXED FOR WINDOWS)
# =========================
@contextmanager
def locked_file(path: Path, mode: str):
    """
    Exclusive lock:
    - Windows: msvcrt.locking on 1 byte at offset 0
    - Linux: fcntl.flock
    Important: uses 'acquired' flag to avoid PermissionError on unlock.
    """
    path.parent.mkdir(exist_ok=True)

    # Ensure file exists if mode needs it
    if "r" in mode and "+" in mode and not path.exists():
        path.write_bytes(b"")  # create empty

    f = open(path, mode)
    acquired = False

    try:
        if os.name == "nt":
            import msvcrt
            # Ensure file is at least 1 byte or locking can fail on empty file
            f.seek(0, os.SEEK_END)
            if f.tell() == 0:
                f.write(b" ")  # placeholder
                f.flush()
            # Lock from start
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
            acquired = True
        else:
            import fcntl
            fcntl.flock(f, fcntl.LOCK_EX)
            acquired = True

        yield f

    finally:
        # Unlock only if we successfully locked
        try:
            if acquired:
                if os.name == "nt":
                    import msvcrt
                    f.seek(0)
                    try:
                        msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
                    except OSError:
                        pass
                else:
                    import fcntl
                    try:
                        fcntl.flock(f, fcntl.LOCK_UN)
                    except OSError:
                        pass
        finally:
            try:
                f.close()
            except Exception:
                pass

# =========================
# DAILY COUNTER (300,301,302... PER DAY)
# =========================
def counter_file_for_day(day: date) -> Path:
    syd_midnight = pytz.timezone(TIMEZONE).localize(datetime(day.year, day.month, day.day))
    return BASE_DIR / f"{syd_midnight.strftime('%Y-%m-%d_%a')}_counter.txt"

def get_next_occupation_id_for_day(day: date) -> int:
    cfile = counter_file_for_day(day)
    if not cfile.exists():
        cfile.write_bytes(str(COUNTER_START).encode("utf-8"))

    # Use binary mode to keep locking simple and stable
    with locked_file(cfile, "r+b") as f:
        f.seek(0)
        raw = f.read().decode("utf-8", errors="ignore").strip()
        current = int(raw) if raw.isdigit() else COUNTER_START
        if current < COUNTER_START:
            current = COUNTER_START
        next_value = current + 1

        f.seek(0)
        f.truncate()
        f.write(str(next_value).encode("utf-8"))
        f.flush()
        if hasattr(os, "fsync"):
            os.fsync(f.fileno())

    return current

# =========================
# DAILY ALL EMPLOYEE FILE
# =========================
def daily_all_file_path(day: date) -> Path:
    syd_midnight = pytz.timezone(TIMEZONE).localize(datetime(day.year, day.month, day.day))
    return BASE_DIR / f"{syd_midnight.strftime('%Y-%m-%d_%a')}_allEmployee.xml"

def build_occupation_element(
    occ_id: int,
    technician_code: str,
    occ_dt: datetime,
    duration_hours: float,
    wo_code: str,
    hour_type: str,
    occupation_type: str,
    comments: str
) -> ET.Element:
    occ = ET.Element("occupation", attrib={"id": str(occ_id)})

    ET.SubElement(occ, "occupation_technician", attrib={"code": technician_code})

    od = ET.SubElement(occ, "occupation_occupationDate")
    od.text = fmt_dt(occ_dt)  # REAL Sydney date+time

    dur = ET.SubElement(occ, "occupation_duration")
    dur.text = str(round(float(duration_hours), 6))

    ET.SubElement(occ, "occupation_WO", attrib={"code": wo_code})
    ET.SubElement(occ, "occupation_hourType", attrib={"code": hour_type})

    if (occupation_type or "").strip():
        ET.SubElement(occ, "occupation_type", attrib={"code": occupation_type.strip()})

    comm = ET.SubElement(occ, "occupation_comments")
    desc = ET.SubElement(comm, "description_description")
    desc.text = " ".join((comments or "").splitlines()).strip()

    oid = ET.SubElement(occ, "occupation_id")
    oid.text = str(occ_id)

    return occ

def build_entities_xml_with_one(occupation_elem: ET.Element) -> bytes:
    entities = ET.Element("entities", attrib={
        "exchangeInterface": "OCCUPATION_IN",
        "externalSystem": "",
        "timezone": TIMEZONE
    })
    entities.append(occupation_elem)
    return xml_to_bytes(entities)

def append_to_daily_all(day: date, occupation_elem: ET.Element):
    out_path = daily_all_file_path(day)

    # Open as r+b if exists else w+b (create)
    mode = "r+b" if out_path.exists() else "w+b"

    with locked_file(out_path, mode) as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()

        if size == 0:
            root = ET.Element("entities", attrib={
                "exchangeInterface": "OCCUPATION_IN",
                "externalSystem": "",
                "timezone": TIMEZONE
            })
        else:
            f.seek(0)
            existing = f.read()

            # Remove any leading placeholder bytes/spaces if present
            existing = existing.lstrip(b"\x00").lstrip()

            try:
                root = ET.fromstring(existing)
                if root.tag != "entities":
                    raise ValueError("Root not entities")
            except Exception:
                root = ET.Element("entities", attrib={
                    "exchangeInterface": "OCCUPATION_IN",
                    "externalSystem": "",
                    "timezone": TIMEZONE
                })

        root.append(occupation_elem)

        new_content = xml_to_bytes(root)
        f.seek(0)
        f.write(new_content)
        f.truncate()
        f.flush()
        if hasattr(os, "fsync"):
            os.fsync(f.fileno())

# =========================
# NEW: READ USER XML FILES AND BUILD DAILY SUMMARY (NO XML FORMAT CHANGES)
# =========================
def _safe_float(x: str) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def _safe_text(elem, default="") -> str:
    return (elem.text or default).strip() if elem is not None else default

def _safe_attr(elem, key: str, default="") -> str:
    return (elem.attrib.get(key) or default).strip() if elem is not None else default

def get_user_entries_for_day(user_folder: Path, selected_day: date) -> pd.DataFrame:
    """
    Reads per-user xml files (data/<user>/*.xml) and returns entries where
    occupation_occupationDate is on selected_day (Sydney time).
    Does not modify anything, only reads.
    """
    rows = []
    tz = pytz.timezone(TIMEZONE)

    if not user_folder.exists():
        return pd.DataFrame(columns=["Time", "WO", "Time Type", "Occ Type", "Hours", "Comment"])

    for f in sorted(user_folder.glob("*.xml")):
        try:
            data = f.read_bytes().lstrip(b"\x00").lstrip()
            root = ET.fromstring(data)
            if root.tag != "entities":
                continue
            occ = root.find("occupation")
            if occ is None:
                continue

            dt_txt = _safe_text(occ.find("occupation_occupationDate"), "")
            if not dt_txt:
                continue

            # dt_txt format: "YYYY-MM-DD HH:MM:SS"
            try:
                occ_dt = datetime.strptime(dt_txt, "%Y-%m-%d %H:%M:%S")
                occ_dt = tz.localize(occ_dt)
            except Exception:
                continue

            if occ_dt.date() != selected_day:
                continue

            wo_code = _safe_attr(occ.find("occupation_WO"), "code", "")
            hour_type = _safe_attr(occ.find("occupation_hourType"), "code", "")
            occ_type = _safe_attr(occ.find("occupation_type"), "code", "")
            hours = _safe_float(_safe_text(occ.find("occupation_duration"), "0"))

            comment_elem = occ.find("occupation_comments")
            desc_elem = comment_elem.find("description_description") if comment_elem is not None else None
            comment = _safe_text(desc_elem, "")

            rows.append({
                "Time": occ_dt.strftime("%H:%M:%S"),
                "WO": wo_code,
                "Time Type": hour_type,
                "Occ Type": occ_type,
                "Hours": round(hours, 3),
                "Comment": comment
            })
        except Exception:
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["Time", "WO", "Time Type", "Occ Type", "Hours", "Comment"])
    return df.sort_values(["Time", "WO"], ascending=[True, True], ignore_index=True)

# =========================
# APP UI
# =========================
employees_df = load_employees()

st.title("🕒 Labour Reporting")

if employees_df.empty:
    st.warning("No employees found. Make sure employees.csv has headers `name,pin`.")
    st.stop()

# Login
left_auth, right_auth = st.columns([2, 1])
with left_auth:
    user_name = st.selectbox("I am", employees_df["name"].tolist())
with right_auth:
    user_pin = st.text_input("PIN", type="password")

auth_ok = False
if user_name:
    pin_expected = employees_df.loc[employees_df["name"] == user_name, "pin"].iloc[0]
    auth_ok = (user_pin.strip() == pin_expected.strip())

if not auth_ok:
    st.info("Please select your name, enter your PIN, and then press Enter.")
    st.stop()

st.success(f"Logged in as **{user_name}**")

# user folder
user_folder = BASE_DIR / sanitize_folder(user_name)
user_folder.mkdir(exist_ok=True)

day = st.date_input("Select date", value=date.today(), format="DD/MM/YYYY")

# =========================
# NEW: DAILY SUMMARY VIEW (Total hours + WO list with hours)
# =========================
st.subheader("Today Summary")

entries_df = get_user_entries_for_day(user_folder, day)

total_hours = float(entries_df["Hours"].sum()) if not entries_df.empty else 0.0

cA, cB = st.columns([1, 2])
with cA:
    st.metric("Total Hours (Selected Date)", f"{total_hours:.2f}")
with cB:
    st.caption("This summary updates automatically after you save a new entry.")

if entries_df.empty:
    st.info("No entries found for the selected date yet.")
else:
    st.dataframe(entries_df, use_container_width=True, hide_index=True)

    st.subheader("WO Totals (Selected Date)")
    wo_totals = (
        entries_df.groupby("WO", dropna=False)["Hours"]
        .sum()
        .reset_index()
        .sort_values("Hours", ascending=False, ignore_index=True)
    )
    wo_totals["Hours"] = wo_totals["Hours"].round(3)
    st.dataframe(wo_totals, use_container_width=True, hide_index=True)

st.divider()

# =========================
# Existing Entry Form (UNCHANGED)
# =========================
st.subheader("New Entry")

with st.form("labour_entry", clear_on_submit=True):
    c1, c2, c3, c4, c5 = st.columns([1.2, 1.0, 1.0, 1.2, 1.2])

    technician_code = c1.text_input("TECHNICIAN", value=user_name.upper().replace(" ", ""))
    hour_type = c2.selectbox("TIME TYPE", ["NORMAL", "OVERTIME", "SHIFT"], index=0)
    duration = c3.text_input("DURATION (HH:MM)", value="01:00")
    occupation_type = c4.selectbox("OCCUPATION TYPE", ["", "IN", "OUT"], index=0)
    wo_code = c5.text_input("WO", placeholder="Enter WO code")

    comment = st.text_area(
        "COMMENT",
        placeholder="Write full details here (saved in description_description)",
        height=220
    )

    save = st.form_submit_button("Save")

if save:
    dur_hours = parse_hhmm_to_hours(duration)

    if not technician_code.strip():
        st.error("TECHNICIAN is required.")
    elif not wo_code.strip():
        st.error("WO is required.")
    elif dur_hours is None:
        st.error("DURATION must be HH:MM (example 00:45, 03:30).")
    else:
        occ_dt = now_sydney()

        # ID: 300,301,302... (per day)
        occ_id = get_next_occupation_id_for_day(day)

        occ_elem = build_occupation_element(
            occ_id=occ_id,
            technician_code=technician_code.strip(),
            occ_dt=occ_dt,
            duration_hours=dur_hours,
            wo_code=wo_code.strip(),
            hour_type=hour_type.strip(),
            occupation_type=(occupation_type or "").strip(),
            comments=comment or ""
        )

        # Save per-user file
        filename = f"{fmt_file_dt(occ_dt)}.xml"
        user_file = user_folder / filename
        atomic_write_bytes(user_file, build_entities_xml_with_one(occ_elem))

        # Append to daily allEmployee (single file per day)
        append_to_daily_all(day, occ_elem)

        st.success(
            f"Saved ✅ Stored in {user_folder.name}/{filename} and updated "
            f"{daily_all_file_path(day).name}"
        )

        # NEW: Refresh page to show updated totals immediately
        st.rerun()

st.caption(f"Timezone fixed: {TIMEZONE} | Base folder: {BASE_DIR.resolve()}")
