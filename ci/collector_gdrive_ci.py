#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import io
import re
import sys
import argparse
from pathlib import Path
from typing import Optional

import pandas as pd

# Google Drive (service account)
try:
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    from google.oauth2.service_account import Credentials
    GOOGLE_OK = True
except Exception:
    GOOGLE_OK = False


# ---- Klasörler ---------------------------------------------------------------
RAW_FOLDER = Path("data/raw")
NORMALIZED_FOLDER = Path("data/normalized")

RAW_FOLDER.mkdir(parents=True, exist_ok=True)
NORMALIZED_FOLDER.mkdir(parents=True, exist_ok=True)


# ---- Yardımcılar -------------------------------------------------------------
def _to_float(s: str | float | int | None) -> Optional[float]:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    ss = str(s).strip().replace(",", ".")
    try:
        return float(ss)
    except Exception:
        return None


def extract_after_label(value, label: str) -> Optional[float]:
    """
    '24H108.03%'  -> label='24H'   => 108.03
    'Week104,12%' -> label='Week' => 104.12
    'Month80.23%' -> label='Month'=> 80.23
    'RTP96.07%'   -> label='RTP'  => 96.07

    1) Etiketten SONRAKİ sayıyı yakalar (virgül/nokta destekli).
    2) Bulamazsa fallback olarak stringdeki SON sayıyı alır.
    """
    if pd.isna(value):
        return None
    s = str(value).strip()

    # Etiketten sonra gelen sayı
    pat = rf"(?i){re.escape(label)}\s*([+-]?\d+(?:[.,]\d+)?)"
    m = re.search(pat, s)
    if m:
        return _to_float(m.group(1))

    # Fallback: stringdeki SON sayı
    all_nums = re.findall(r"([+-]?\d+(?:[.,]\d+)?)", s)
    if all_nums:
        return _to_float(all_nums[-1])

    return None


def normalize_dataframe(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Excel/CSV içeriğini app.py ile uyumlu kolonlara dönüştürür.
    Beklenen mapping:
      Text -> game
      Text1 -> 24h
      Text2 -> week
      Text3 -> month
      Text4 -> rtp
      Current_time -> timestamp
    """
    if df is None or df.empty:
        return None

    # Mapping
    rename_map = {
        "Text": "game",
        "Text1": "24h",
        "Text2": "week",
        "Text3": "month",
        "Text4": "rtp",
        "Current_time": "timestamp",
    }

    # Kolon isimlerini normalize et (yaklaşık eşleşme için aşağıdaki gibi de yapılabilir)
    base_cols = [c.strip() for c in df.columns]
    df.columns = base_cols
    for k, v in rename_map.items():
        if k in df.columns:
            df = df.rename(columns={k: v})

    # Metrikleri etiket SONRASINA göre parse et
    if "24h" in df:
        df["24h"] = df["24h"].apply(lambda v: extract_after_label(v, "24H"))
    if "week" in df:
        df["week"] = df["week"].apply(lambda v: extract_after_label(v, "Week"))
    if "month" in df:
        df["month"] = df["month"].apply(lambda v: extract_after_label(v, "Month"))
    if "rtp" in df:
        df["rtp"] = df["rtp"].apply(lambda v: extract_after_label(v, "RTP"))

    # Zamanı parse et
    if "timestamp" in df:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

    # Nihai kolon sırası
    keep = [c for c in ["timestamp", "game", "24h", "week", "month", "rtp"] if c in df.columns]
    if not keep:
        return None

    out = df[keep].sort_values("timestamp").reset_index(drop=True)
    if out.empty:
        return None
    return out


def save_normalized(df: pd.DataFrame, raw_path: Path) -> None:
    out_name = raw_path.stem.lower().replace(" ", "-") + ".csv"
    out_path = NORMALIZED_FOLDER / out_name
    df.to_csv(out_path, index=False)
    print(f"[collector] Normalized yazıldı -> {out_path}")


def process_local_file(raw_file: Path) -> None:
    print(f"[collector] Normalize ediliyor: {raw_file.name}")
    try:
        if raw_file.suffix.lower() in [".xlsx", ".xls"]:
            df = pd.read_excel(raw_file)
        elif raw_file.suffix.lower() == ".csv":
            df = pd.read_csv(raw_file)
        else:
            print(f"[collector] SKIP: {raw_file.name} (desteklenmeyen uzantı)")
            return

        norm = normalize_dataframe(df)
        if norm is not None:
            save_normalized(norm, raw_file)
        else:
            print(f"[collector] Uyarı: {raw_file.name} normalize edilemedi (boş ya da eksik kolon).")
    except Exception as e:
        print(f"[collector] Hata: {raw_file.name} işlenemedi -> {e}")


# ---- Google Drive indirme ----------------------------------------------------
DRIVE_MIME_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
DRIVE_MIME_XLS = "application/vnd.ms-excel"
DRIVE_MIME_CSV = "text/csv"


def build_drive_service(creds_file: Path | None) -> Optional[object]:
    if not GOOGLE_OK:
        print("[collector] googleapiclient kurulu değil, Drive indirme atlanacak.")
        return None
    if creds_file is None or not creds_file.exists():
        print("[collector] credentials.json bulunamadı, Drive indirme atlanacak.")
        return None
    try:
        scopes = ["https://www.googleapis.com/auth/drive.readonly"]
        creds = Credentials.from_service_account_file(str(creds_file), scopes=scopes)
        service = build("drive", "v3", credentials=creds)
        return service
    except Exception as e:
        print(f"[collector] Drive service oluşturulamadı -> {e}")
        return None


def download_drive_folder(service, folder_id: str) -> None:
    """Drive klasöründeki Excel/CSV dosyalarını RAW_FOLDER'a indirir."""
    if service is None:
        return

    page_token = None
    query = f"'{folder_id}' in parents and trashed=false"
    wanted_mimes = {DRIVE_MIME_XLSX, DRIVE_MIME_XLS, DRIVE_MIME_CSV}

    total = 0
    while True:
        resp = (
            service.files()
            .list(
                q=query,
                pageSize=1000,
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
            )
            .execute()
        )
        files = resp.get("files", [])
        for f in files:
            name = f["name"]
            mime = f.get("mimeType", "")
            if (mime in wanted_mimes) or name.lower().endswith((".xlsx", ".xls", ".csv")):
                print(f"[collector] Download: {name}")
                request = service.files().get_media(fileId=f["id"])
                raw_path = RAW_FOLDER / name
                with io.FileIO(raw_path, "wb") as fh:
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        status, done = downloader.next_chunk()
                total += 1
        page_token = resp.get("nextPageToken", None)
        if not page_token:
            break

    print(f"[collector] Drive indirme tamam: {total} dosya")


# ---- CLI ve ana akış ---------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Drive → data/raw + data/normalized")
    parser.add_argument("--folder-id", help="Google Drive folder id", default=os.environ.get("DRIVE_FOLDER_ID"))
    parser.add_argument("--creds", help="Service account credentials file (JSON)", default="credentials.json")
    args = parser.parse_args()

    # Drive'dan çek (varsa)
    creds_file = Path(args.creds) if args.creds else None
    service = None
    if args.folder_id:
        service = build_drive_service(creds_file)
        if service:
            download_drive_folder(service, args.folder_id)
        else:
            print("[collector] Drive indirme atlandı (service yok).")
    else:
        print("[collector] --folder-id verilmedi, Drive indirme atlandı.")

    # RAW klasöründeki tüm yerel dosyaları normalize et
    raw_files = sorted(RAW_FOLDER.glob("*.*"))
    if not raw_files:
        print(f"[collector] Uyarı: RAW klasöründe dosya yok -> {RAW_FOLDER}")
        sys.exit(0)

    for f in raw_files:
        process_local_file(f)

    print("[collector] Bitti.")


if __name__ == "__main__":
    main()
