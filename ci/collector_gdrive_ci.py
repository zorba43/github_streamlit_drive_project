
import os, io, argparse, csv, re
from datetime import datetime
import pandas as pd

# Google Drive API (Service Account)
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def parse_numeric(value):
    if value is None: return None
    s = str(value)
    m = re.search(r"([-+]?\d+(?:\.\d+)?)\s*%?", s)
    return float(m.group(1)) if m else None

def extract_metrics_from_df(df, filename):
    cols = list(df.columns)
    out = {"timestamp": None, "game": None, "24H": None, "Week": None, "Month": None, "RTP": None, "source_file": os.path.basename(filename)}

    # game name guess: first column header or filename
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
        # values embedded in headers
        for c in cols:
            cl = str(c).lower()
            if "24h" in cl: out["24H"] = parse_numeric(c)
            if "week" in cl: out["Week"] = parse_numeric(c)
            if "month" in cl: out["Month"] = parse_numeric(c)
            if "rtp" in cl: out["RTP"] = parse_numeric(c)
        # timestamp from header if possible
        for c in cols:
            try:
                ts = pd.to_datetime(str(c), errors="raise")
                out["timestamp"] = ts
                break
            except Exception:
                pass

    if out["timestamp"] is None:
        out["timestamp"] = pd.Timestamp.utcnow()

    return out

def list_drive_files(service, folder_id):
    # Query xlsx or xls in folder
    q = f"'{folder_id}' in parents and trashed = false and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel')"
    files = service.files().list(q=q, fields="files(id, name, mimeType)").execute().get("files", [])
    return files

def download_file(service, file_id, target_path):
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(target_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder-id", required=True, help="Google Drive folder ID")
    parser.add_argument("--out", default="data/history.csv")
    args = parser.parse_args()

    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS_JSON secret not found.")
    creds = service_account.Credentials.from_service_account_info(
        json.loads(creds_json), scopes=SCOPES  # type: ignore
    )
    service = build("drive", "v3", credentials=creds)

    files = list_drive_files(service, args.folder_id)
    os.makedirs("incoming", exist_ok=True)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    rows = []
    for f in files:
        local = os.path.join("incoming", f["name"])
        download_file(service, f["id"], local)
        try:
            df = pd.read_excel(local, sheet_name=0, engine="openpyxl")
        except Exception:
            continue
        rec = extract_metrics_from_df(df, local)
        rows.append(rec)

    if not rows:
        print("No parsed rows.")
        return

    hist_cols = ["timestamp","game","24H","Week","Month","RTP","source_file"]
    hist_df = pd.DataFrame(rows)[hist_cols]
    # Append to CSV
    header = not os.path.exists(args.out)
    hist_df.to_csv(args.out, mode="a", header=header, index=False)
    print(f"Appended {len(hist_df)} rows to {args.out}")

if __name__ == "__main__":
    import json
    main()
