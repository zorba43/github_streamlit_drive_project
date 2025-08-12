#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Google Drive --> data/raw  |  normalize --> data/normalized
Robust:
- --folder-id (CLI) veya DRIVE_FOLDER_ID (env) ile klasör id
- Timestamp bulunamazsa SKIP (IndexError yok)
"""

import os
import io
import argparse
from pathlib import Path

import pandas as pd

# ---- Google Drive API ----
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account


RAW_DIR = Path("data/raw")
NORM_DIR = Path("data/normalized")
INCOMING_DIR = Path("incoming")
RAW_DIR.mkdir(parents=True, exist_ok=True)
NORM_DIR.mkdir(parents=True, exist_ok=True)
INCOMING_DIR.mkdir(parents=True, exist_ok=True)

EXCEL_MIMES = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # .xlsx
    "application/vnd.ms-excel",                                           # .xls
}

# ---------------- Timestamp sütunu tespiti ----------------
def detect_timestamp_col(df):
    """
    Timestamp kolonunu bulur; bulunamazsa None döndürür.
    - İsim: time / date / tarih (case-insensitive)
    - İçerik: ilk 10 kolonda datetime oranı >= %30 ise kabul
    """
    if df is None or getattr(df, "empty", True) or df.shape[1] == 0:
        return None

    # İsimden adaylar
    name_candidates = [
        c for c in df.columns
        if isinstance(c, str) and (
            "time" in c.lower() or "date" in c.lower() or "tarih" in c.lower()
        )
    ]
    if name_candidates:
        return name_candidates[0]

    # İçerikten adaylar
    best_col, best_ratio = None, 0.0
    for c in df.columns[:10]:
        try:
            s = pd.to_datetime(df[c], errors="coerce", utc=True)
            ratio = float(s.notna().mean())
            if ratio > best_ratio:
                best_ratio, best_col = ratio, c
        except Exception:
            continue

    if best_col is None or best_ratio < 0.30:
        return None
    return best_col


# ---------------- Normalizasyon ----------------
def normalize_dataframe(df, src_path_for_log=""):
    """
    Girdi DF'yi normalize eder.
    Dönüş: (normalized_df, reason)
      - normalized_df None ise "reason" SKIP sebebidir.
    """
    if df is None or getattr(df, "empty", True) or df.shape[1] == 0:
        return None, "no rows"

    ts_col = detect_timestamp_col(df)
    if not ts_col:
        return None, "no timestamp column"

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])
    if df.empty:
        return None, "no valid timestamps"

    for col in ["RTP", "24H", "Week", "Month"]:
        if col in df.columns:
            df.loc[(df[col] < 0) | (df[col] > 100), col] = pd.NA

    df = df.sort_values("timestamp")

    wanted = ["timestamp", "RTP", "24H", "Week", "Month"]
    present = [c for c in wanted if c in df.columns]
    if present == ["timestamp"]:
        return None, "only timestamp present"

    norm = df[present].copy()
    return norm, "ok"


# ---------------- Excel okuma (sağlam) ----------------
def read_excel_safely(path: Path) -> pd.DataFrame:
    """
    En çok sütun barındıran sheet'i seçerek oku. Okunamazsa boş DF döndür.
    """
    try:
        x = pd.ExcelFile(path, engine="openpyxl")
        best_sheet, max_cols = None, -1
        for s in x.sheet_names:
            try:
                sample = pd.read_excel(x, sheet_name=s, nrows=5)
                if sample.shape[1] > max_cols:
                    max_cols, best_sheet = sample.shape[1], s
            except Exception:
                continue
        if best_sheet is None:
            return pd.read_excel(x, sheet_name=0)
        return pd.read_excel(x, sheet_name=best_sheet)
    except Exception as e:
        print(f"[collector] [READ ERROR] {path}: {e}")
        return pd.DataFrame()


# ---------------- Drive yardımcıları ----------------
def build_drive_service():
    # 1) Dosya yolu
    gac_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if gac_path and Path(gac_path).exists():
        creds = service_account.Credentials.from_service_account_file(
            gac_path,
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
        return build("drive", "v3", credentials=creds)

    # 2) JSON string
    gac_json = os.getenv("GCP_SA_JSON")
    if gac_json:
        tmp = Path("gcp_sa_temp.json")
        tmp.write_text(gac_json, encoding="utf-8")
        creds = service_account.Credentials.from_service_account_file(
            str(tmp),
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
        return build("drive", "v3", credentials=creds)

    raise RuntimeError(
        "Service account credentials not found. "
        "Set GOOGLE_APPLICATION_CREDENTIALS or GCP_SA_JSON."
    )


def list_drive_files(service, folder_id: str):
    query = f"'{folder_id}' in parents and trashed = false"
    page_token = None
    files = []
    while True:
        res = service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token
        ).execute()
        for f in res.get("files", []):
            if f["mimeType"] in EXCEL_MIMES or f["name"].lower().endswith((".xlsx", ".xls")):
                files.append(f)
        page_token = res.get("nextPageToken")
        if not page_token:
            break
    return files


def download_drive_file(service, file_id, out_path: Path):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(fh.getvalue())


# ---------------- main ----------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder-id", dest="folder_id", default=None,
                        help="Google Drive folder id (CLI öncelikli).")
    args = parser.parse_args()

    # CLI > ENV
    folder_id = (args.folder_id or os.getenv("DRIVE_FOLDER_ID", "")).strip()
    if not folder_id:
        raise RuntimeError("Drive folder id is empty. Pass --folder-id or set DRIVE_FOLDER_ID.")

    print("[collector] Cleaning data/raw and data/normalized")
    for p in RAW_DIR.glob("*"):
        p.unlink(missing_ok=True)
    for p in NORM_DIR.glob("*"):
        p.unlink(missing_ok=True)

    service = build_drive_service()
    files = list_drive_files(service, folder_id)
    print(f"[collector] Found {len(files)} items (including subfolders).")

    # 1) Download
    for f in files:
        out = RAW_DIR / f["name"]
        try:
            download_drive_file(service, f["id"], out)
            print(f"[collector] Downloaded: {f['name']} -> {out}")
        except Exception as e:
            print(f"[collector] [DOWNLOAD ERROR] {f['name']}: {e}")

    # 2) Normalize
    excel_files = sorted(RAW_DIR.glob("*.xlsx")) + sorted(RAW_DIR.glob("*.xls"))
    print(f"[collector] Excel-like files downloaded: {len(excel_files)}")

    for p in excel_files:
        df = read_excel_safely(p)
        if df is None or df.empty:
            print(f"[collector] [SKIP normalize] {p}: no rows parsed")
            continue

        norm, reason = normalize_dataframe(df, str(p))
        if norm is None:
            print(f"[collector] [SKIP normalize] {p}: {reason}")
            continue

        out_csv = NORM_DIR / (p.stem + ".csv")
        try:
            norm.to_csv(out_csv, index=False)
            print(f"[collector] Normalized rows: {len(norm)} -> {out_csv}")
        except Exception as e:
            print(f"[collector] [WRITE ERROR] {out_csv}: {e}")

    print("[collector] Done.")


if __name__ == "__main__":
    main()
