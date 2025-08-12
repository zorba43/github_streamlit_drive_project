#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import io
import re
import sys
import csv
import json
import time
import shutil
import logging
import tempfile
from pathlib import Path
from typing import Iterable, Dict, List, Optional

import pandas as pd

# Google Drive API
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

# -----------------------
# Ayarlar / Sabitler
# -----------------------
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

RAW_DIR = Path("data/raw")
NORM_DIR = Path("data/normalized")

# Excel sütun adları (sizden gelen şema)
COL_GAME = "Text"
COL_24H = "Text1"
COL_WEEK = "Text2"
COL_MONTH = "Text3"
COL_RTP = "Text4"
COL_TIME = "Current_time"

# En dayanıklı sayı yakalama (örn: '24H96.7%','Week104.12%','RTP96.07%')
NUM_RE = re.compile(r"([-+]?\d+(?:\.\d+)?)\s*%")

def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[collector] %(message)s",
        stream=sys.stdout,
    )


def get_service_from_env() -> "Resource":
    """
    GitHub Secret'ına koyduğumuz JSON içeriğinden service account cred yaratır.
    GDRIVE_CREDENTIALS: json string
    """
    creds_json = os.environ.get("GDRIVE_CREDENTIALS")
    if not creds_json:
        logging.info("credentials.json bulunamadı, Drive indirme atlanacak.")
        sys.exit(0)

    info = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    service = build("drive", "v3", credentials=creds)
    return service


def list_all_files_recursive(service, folder_id: str) -> List[Dict]:
    """Verilen Google Drive klasöründeki *tüm alt klasörleri* dolaşır, dosyaları döner."""
    results: List[Dict] = []

    def walk(fid: str):
        page_token = None
        while True:
            resp = (
                service.files()
                .list(
                    q=f"'{fid}' in parents and trashed=false",
                    fields="files(id,name,mimeType),nextPageToken",
                    pageToken=page_token,
                )
                .execute()
            )
            for f in resp.get("files", []):
                if f["mimeType"] == "application/vnd.google-apps.folder":
                    walk(f["id"])
                else:
                    results.append(f)
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    walk(folder_id)
    return results


def download_file(service, file_id: str, dst_path: Path) -> None:
    """Drive dosyasını yerel dosyaya indirir."""
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(dst_path, "wb")
    downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()


def ensure_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    NORM_DIR.mkdir(parents=True, exist_ok=True)


def sanitize_game_name(name: str) -> str:
    """
    Oyun ismini dosya ismine uygun hale getirmek (küçük harf, boşluk -> '-').
    """
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "game"


def parse_pct(cell: Optional[str]) -> Optional[float]:
    """
    '24H96.7%' / 'Week104.12%' / 'RTP96.07%' gibi stringlerden sayıyı çeker.
    İçinde % yoksa None döner.
    """
    if cell is None:
        return None
    if not isinstance(cell, str):
        cell = str(cell)

    m = NUM_RE.search(cell)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def normalize_excel(xlsx_path: Path) -> int:
    """
    Bir Excel dosyasını okur, her satır için normalize edilmiş satırları
    ilgili oyun CSV’sine (append) yazar.
    Döndürdüğü sayı: yazılan satır adedi.
    """
    try:
        df = pd.read_excel(xlsx_path)
    except Exception as e:
        logging.info(f"{xlsx_path.name} okunamadı: {e}")
        return 0

    # Gerekli kolonlar var mı?
    for col in [COL_GAME, COL_24H, COL_WEEK, COL_MONTH, COL_RTP, COL_TIME]:
        if col not in df.columns:
            logging.info(f"{xlsx_path.name} normalize edilemedi (eksik kolon: {col}).")
            return 0

    # Tarih dönüşümü
    try:
        df["timestamp"] = pd.to_datetime(df[COL_TIME], errors="coerce")
    except Exception:
        df["timestamp"] = pd.NaT

    # Hesaplanan alanlar
    df["rtp"] = df[COL_RTP].apply(parse_pct)
    df["24h"] = df[COL_24H].apply(parse_pct)
    df["week"] = df[COL_WEEK].apply(parse_pct)
    df["month"] = df[COL_MONTH].apply(parse_pct)

    # Sadece gerekli alanlar
    out = df[[COL_GAME, "timestamp", "rtp", "24h", "week", "month"]].dropna(
        subset=["timestamp"]
    )
    out = out.rename(columns={COL_GAME: "game"})
    out = out.sort_values("timestamp")

    written = 0
    for game, sub in out.groupby("game"):
        fname = sanitize_game_name(str(game)) + ".csv"
        dst = NORM_DIR / fname

        # Var ise eskiyi al, timestamp'e göre dup drop
        if dst.exists():
            old = pd.read_csv(dst)
            if "timestamp" in old.columns:
                old["timestamp"] = pd.to_datetime(old["timestamp"], errors="coerce")
            all_df = pd.concat([old, sub[["timestamp", "rtp", "24h", "week", "month"]]])
        else:
            all_df = sub[["timestamp", "rtp", "24h", "week", "month"]]

        # de-dup ve sırala
        all_df = (
            all_df.drop_duplicates(subset=["timestamp"])
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
        all_df.to_csv(dst, index=False)
        written += len(sub)

    return written


def collect_and_normalize(folder_id: str) -> None:
    """
    Drive'dan Excel'leri indir, raw'a koy, normalize et.
    """
    ensure_dirs()
    service = get_service_from_env()

    logging.info("Drive’dan indiriliyor…")
    files = list_all_files_recursive(service, folder_id)

    # Sadece Excel’ler
    excel_files = [
        f for f in files
        if f["name"].lower().endswith((".xlsx", ".xls"))
    ]
    if not excel_files:
        logging.info("Klasörde Excel dosyası bulunamadı.")
        return

    # RAW klasörünü temizleyelim (tümünü baştan kuruyoruz)
    for p in RAW_DIR.glob("*"):
        if p.is_file():
            p.unlink()

    for f in excel_files:
        name = f["name"]
        file_id = f["id"]
        dst = RAW_DIR / name
        try:
            download_file(service, file_id, dst)
            logging.info(f"İndirildi: {name}")
        except Exception as e:
            logging.info(f"{name} indirilemedi: {e}")

    # Normalize et
    total = 0
    for x in RAW_DIR.glob("*.xlsx"):
        total += normalize_excel(x)
    for x in RAW_DIR.glob("*.xls"):
        total += normalize_excel(x)

    logging.info(f"Normalize edilen satır sayısı: {total}")


def main():
    setup_logging()

    # GitHub Actions'tan gelecek
    folder_id = os.environ.get("DRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        logging.info("DRIVE_FOLDER_ID eksik; çıkılıyor.")
        sys.exit(0)

    collect_and_normalize(folder_id)
    logging.info("Bitti.")


if __name__ == "__main__":
    main()
