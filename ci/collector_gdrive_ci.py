#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Collector - Scraper Data klasöründeki Excel dosyalarını normalleştirir.
- Kaynak: Google Drive (--folder-id) veya yerel yol (--local-dir "Scraper Data")
- Girdi Excel şeması:
    Text          -> Oyun adı (string)
    Text1         -> "24Hxx.xx%" (xx.xx numeriği alınır)
    Text2         -> "Weekxx.xx%"
    Text3         -> "Monthxx.xx%"
    Text4         -> "RTPxx.xx%"
    Current_time  -> Timestamp
- Çıktı: data/normalized/<dosya-adi-veya-oyun-adi>.csv
  Kolonlar: timestamp, game, rtp, 24h, week, month
"""

import argparse
import os
import re
import shutil
import sys
import tempfile
from typing import Optional, List

import pandas as pd

# ---------- Yardımcılar ----------

def log(msg: str):
    print(f"[collector] {msg}")

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def slugify(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "-", name)
    return name.strip("-") or "game"

VALUE_RE = r"([0-9]+(?:[.,][0-9]+)?)\s*%?"

def parse_prefixed_number(text: Optional[str], prefix: str) -> Optional[float]:
    """
    '24H108.03%' gibi bir metinden prefix sonrası sayıyı döndürür (ör. 108.03).
    """
    if not isinstance(text, str):
        return None
    try:
        m = re.search(prefix + r"\s*" + VALUE_RE, text, flags=re.IGNORECASE)
        if not m:
            return None
        val = m.group(1).replace(",", ".")
        return float(val)
    except Exception:
        return None

def detect_game(df: pd.DataFrame) -> Optional[str]:
    if "Text" in df.columns:
        s = df["Text"].dropna().astype(str).str.strip()
        if len(s) > 0:
            return s.iloc[0]
    return None

def normalize_one_excel(xlsx_path: str) -> Optional[pd.DataFrame]:
    """
    Tek Excel'i normalleştir: belirtilen kolonlardan değerleri parse eder.
    Dönüş: normalize DF (timestamp, game, rtp, 24h, week, month) ya da None.
    """
    try:
        df = pd.read_excel(xlsx_path, engine="openpyxl")
    except Exception as e:
        log(f"Uyarı: '{xlsx_path}' okunamadı: {e}")
        return None

    required_cols = ["Text", "Text1", "Text2", "Text3", "Text4", "Current_time"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        log(f"Uyarı: '{xlsx_path}' normalize edilemedi (eksik kolonlar: {missing}).")
        return None

    out = pd.DataFrame()
    out["timestamp"] = pd.to_datetime(df["Current_time"], errors="coerce", utc=True)
    out["game"] = df["Text"].astype(str).str.strip()

    # 24H / Week / Month / RTP doğru parse
    out["24h"] = df["Text1"].apply(lambda x: parse_prefixed_number(x, "24H"))
    out["week"] = df["Text2"].apply(lambda x: parse_prefixed_number(x, "Week"))
    out["month"] = df["Text3"].apply(lambda x: parse_prefixed_number(x, "Month"))
    out["rtp"] = df["Text4"].apply(lambda x: parse_prefixed_number(x, "RTP"))

    # Geçersiz timestamp'ı at
    out = out.dropna(subset=["timestamp"]).copy()
    if out.empty:
        log(f"Uyarı: '{xlsx_path}' dosyasında geçerli satır yok (timestamp/kolonlar boş).")
        return None

    out = out.sort_values("timestamp").reset_index(drop=True)
    return out

# ---------- Drive (opsiyonel) ----------

def fetch_drive_files(folder_id: str, dest_dir: str) -> List[str]:
    """
    Service account ile Google Drive klasöründen .xlsx indirir.
    Çalışması için CREDENTIALS_JSON içerikli GOOGLE_APPLICATION_CREDENTIALS (ya da
    iş akışında dosya yazılması) gerekir.
    """
    from googleapiclient.discovery import build
    from google.oauth2 import service_account

    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path or not os.path.exists(creds_path):
        log("credentials.json bulunamadı, Drive indirme atlanacak.")
        return []

    SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    service = build("drive", "v3", credentials=creds)

    q = f"'{folder_id}' in parents and trashed = false and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    results = service.files().list(q=q, fields="files(id,name)").execute()
    files = results.get("files", [])
    paths = []
    for f in files:
        name = f["name"]
        fid = f["id"]
        out_path = os.path.join(dest_dir, name)
        request = service.files().get_media(fileId=fid)
        from googleapiclient.http import MediaIoBaseDownload
        import io
        fh = io.FileIO(out_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.close()
        log(f"Drive'dan indirildi: {name}")
        paths.append(out_path)
    return paths

# ---------- Ana akış ----------

def collect_and_normalize(folder_id: Optional[str], local_dir: Optional[str]) -> int:
    """
    Drive klasörü (folder_id) veya yerel klasör (local_dir) kaynak alınır,
    tüm .xlsx dosyaları normalize edip data/normalized altına yazılır.
    """
    ensure_dir("data/normalized")
    tmpdir = tempfile.mkdtemp(prefix="scraper_")
    src_files: List[str] = []

    try:
        if local_dir:
            if not os.path.isdir(local_dir):
                log(f"Hata: Yerel klasör bulunamadı: '{local_dir}'")
                return 1
            for fn in os.listdir(local_dir):
                if fn.lower().endswith((".xlsx", ".xls")):
                    src_files.append(os.path.join(local_dir, fn))
            if not src_files:
                log(f"Uyarı: '{local_dir}' içinde Excel bulunamadı.")
        elif folder_id:
            log("Drive'dan indiriliyor...")
            src_files = fetch_drive_files(folder_id, tmpdir)
            if not src_files:
                log("Uyarı: Drive klasöründe indirilecek Excel bulunamadı.")
        else:
            log("Hata: Kaynak seçilmedi. --local-dir ya da --folder-id vermelisiniz.")
            return 1

        for path in src_files:
            df = normalize_one_excel(path)
            if df is None or df.empty:
                continue

            # Dosya adına göre ya da oyun adına göre çıktı adı
            # 1) Oyun adı belirlenebiliyorsa onu kullan
            game_name = detect_game(df) or os.path.splitext(os.path.basename(path))[0]
            out_name = slugify(game_name) + ".csv"
            out_path = os.path.join("data", "normalized", out_name)
            df.to_csv(out_path, index=False)
            log(f"Normalized yazıldı -> {out_path}")

        log("Bitti.")
        return 0

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder-id", type=str, default=None, help="Google Drive klasör ID")
    parser.add_argument("--local-dir", type=str, default=None, help="Yerel klasör (örn. 'Scraper Data')")
    args = parser.parse_args()

    sys.exit(collect_and_normalize(folder_id=args.folder_id, local_dir=args.local_dir))


if __name__ == "__main__":
    main()
