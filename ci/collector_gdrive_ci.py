# ci/collector_gdrive_ci.py
# (Sheets export + recursive + robust ID + All Drives + better logs)
import os, io, re, json, argparse
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def log(msg): print(f"[collector] {msg}")

def parse_numeric(value):
    if value is None: return None
    s = str(value)
    m = re.search(r"([-+]?\d+(?:\.\d+)?)\s*%?", s)
    return float(m.group(1)) if m else None

def extract_metrics_from_df(df, filename):
    cols = list(df.columns)
    out = {"timestamp": None, "game": None, "24H": None, "Week": None, "Month": None, "RTP": None,
           "source_file": os.path.basename(filename)}

    # game name guess
    if cols:
        out["game"] = str(cols[0]).strip()
    if not out["game"] or out["game"].lower().startswith("unnamed"):
        out["game"] = os.path.splitext(os.path.basename(filename))[0]

    if len(df) > 0:
        row0 = df.iloc[0].to_dict()
        for key in ["24H", "Week", "Month", "RTP"]:
            for c in cols:
                if key.lower() in str(c).lower():
                    out[key] = parse_numeric(row0.get(c))
                    break
        for c in cols:
            if any(k in str(c).lower() for k in ["time","date","tarih","zaman"]):
                ts = pd.to_datetime(row0.get(c), errors="coerce")
                if pd.notna(ts):
                    out["timestamp"] = ts
                    break
    else:
        # headers may contain values
        for c in cols:
            cl = str(c).lower()
            if "24h" in cl: out["24H"] = parse_numeric(c)
            if "week" in cl: out["Week"] = parse_numeric(c)
            if "month" in cl: out["Month"] = parse_numeric(c)
            if "rtp" in cl: out["RTP"] = parse_numeric(c)
        for c in cols:
            try:
                out["timestamp"] = pd.to_datetime(str(c), errors="raise")
                break
            except Exception:
                pass

    if out["timestamp"] is None:
        out["timestamp"] = pd.Timestamp.utcnow()

    return out

def normalize_folder_id(raw):
    s = (raw or "").strip()
    m = re.search(r"/folders/([A-Za-z0-9_-]+)", s)
    return m.group(1) if m else s

def drive_context(service, folder_id):
    ctx = dict(supportsAllDrives=True, includeItemsFromAllDrives=True)
    try:
        meta = service.files().get(fileId=folder_id, fields="id,name,driveId",
                                   supportsAllDrives=True).execute()
        if meta.get("driveId"):  # Shared Drive
            ctx.update(corpora="drive", driveId=meta["driveId"])
        else:                    # My Drive
            ctx.update(corpora="user")
    except Exception:
        ctx.update(corpora="allDrives")
    return ctx

def list_children(service, parent_id, ctx):
    q = f"'{parent_id}' in parents and trashed = false"
    return service.files().list(q=q, fields="files(id,name,mimeType)", **ctx).execute().get("files", [])

def walk_files(service, folder_id):
    ctx = drive_context(service, folder_id)
    stack = [folder_id]
    out = []
    while stack:
        fid = stack.pop()
        for it in list_children(service, fid, ctx):
            if it["mimeType"] == "application/vnd.google-apps.folder":
                stack.append(it["id"])
            else:
                out.append(it)
    return out

def download_excel_like(service, file_obj, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    name, mime, fid = file_obj["name"], file_obj["mimeType"], file_obj["id"]

    # Google Sheets → export XLSX
    if mime == "application/vnd.google-apps.spreadsheet":
        target = os.path.join(target_dir, name if name.lower().endswith(".xlsx") else f"{name}.xlsx")
        request = service.files().export_media(
            fileId=fid,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    # Real Excel files
    elif mime in (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel",
        "application/vnd.ms-excel.sheet.macroEnabled.12",
    ):
        target = os.path.join(target_dir, name)
        request = service.files().get_media(fileId=fid)
    else:
        return None

    with io.FileIO(target, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return target

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--folder-id", required=True, help="Drive Folder ID or full URL")
    ap.add_argument("--out", default="data/history.csv")
    args = ap.parse_args()

    # Credentials (file path preferred; fallback to JSON blob)
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds_path and os.path.exists(creds_path):
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    elif creds_json:
        creds = service_account.Credentials.from_service_account_info(json.loads(creds_json), scopes=SCOPES)
    else:
        raise RuntimeError("Missing service account credentials (env).")

    service = build("drive", "v3", credentials=creds)

    folder_id = normalize_folder_id(args.folder_id)
    log(f"Folder: {folder_id}")

    items = walk_files(service, folder_id)
    log(f"Found {len(items)} items (including subfolders).")

    os.makedirs("incoming", exist_ok=True)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    downloaded = []
    for it in items:
        p = download_excel_like(service, it, "incoming")
        if p:
            downloaded.append(p)
            log(f"Downloaded: {it['name']} -> {p}")
    log(f"Excel-like files downloaded: {len(downloaded)}")

    rows = []
    for p in downloaded:
        try:
            df = pd.read_excel(p, sheet_name=0, engine="openpyxl")
        except Exception as e:
            log(f"[SKIP read] {p}: {e}")
            continue
        rec = extract_metrics_from_df(df, p)
        if any(rec.get(k) is not None for k in ["24H","Week","Month","RTP"]):
            rows.append(rec)
        else:
            log(f"[SKIP parse] {p}: no metrics parsed")

    if not rows:
        log("No parsed rows.")
        return

    hist_cols = ["timestamp","game","24H","Week","Month","RTP","source_file"]
    hist_df = pd.DataFrame(rows)[hist_cols]
    header = not os.path.exists(args.out)
    hist_df.to_csv(args.out, mode="a", header=header, index=False)
    log(f"Appended {len(hist_df)} rows to {args.out}")

if __name__ == "__main__":
    main()
