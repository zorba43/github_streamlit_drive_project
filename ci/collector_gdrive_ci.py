#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Google Drive --> data/raw  |  normalize --> data/normalized

Sağlamlaştırmalar:
- --folder-id (CLI) veya ENV (DRIVE_FOLDER_ID / GDRIVE_FOLDER_ID / GOOGLE_DRIVE_FOLDER_ID / FOLDER_ID / INPUT_FOLDER_ID / DRIVE_FOLDER_URL)
  + URL verilirse ID otomatik çıkarılır.
- Service account kimliği dosya yolu (GOOGLE_APPLICATION_CREDENTIALS) veya JSON string (GCP_SA_JSON)
- Excel okuma: en çok sütunlu sheet
- Normalize:
  * Klasik başlık eşleme (RTP/24H/Week/Month başlıkta)
  * Hücre içi token eşleme (ör. "24H108.03%", "Week104.12%", "Month80.23%", "RTP96.07%")
- Yüzde değerleri her formattan parse edilir (75, "75,0", " 75 %", "%75", "108.03%" vb.)
- Anlaşılır loglar, SKIP sebepleri, Drive erişim hatalarında rehber mesaj
"""

from __future__ import annotations

import os
import io
import re
import argparse
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.oauth2 import service_account


RAW_DIR = Path("data/raw")
NORM_DIR = Path("data/normalized")
INCOMING_DIR = Path("incoming")
RAW_DIR.mkdir(parents=True, exist_ok=True)
NORM_DIR.mkdir(parents=True, exist_ok=True)
INCOMING_DIR.mkdir(parents=True, exist_ok=True)

EXCEL_MIMES = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel",
}


# -------------------- Yardımcılar: ID/URL --------------------
def extract_folder_id_from_url(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", text)
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", text)
    if m:
        return m.group(1)
    return None


def resolve_folder_id(cli_value: Optional[str]) -> str:
    """
    Öncelik: CLI -> potansiyel ENV değişkenleri.
    Drive URL'si verilirse ID otomatik çıkarılır.
    """
    candidates = [
        cli_value,
        os.getenv("DRIVE_FOLDER_ID"),
        os.getenv("GDRIVE_FOLDER_ID"),
        os.getenv("GOOGLE_DRIVE_FOLDER_ID"),
        os.getenv("FOLDER_ID"),
        os.getenv("INPUT_FOLDER_ID"),
        os.getenv("DRIVE_FOLDER_URL"),
    ]
    for val in candidates:
        if not val:
            continue
        v = val.strip().strip('"').strip("'")
        if not v:
            continue
        if "drive.google.com" in v:
            fid = extract_folder_id_from_url(v)
            if fid:
                return fid
        else:
            return v
    return ""


# -------------------- Timestamp tespiti --------------------
def detect_timestamp_col(df: pd.DataFrame) -> Optional[str]:
    if df is None or getattr(df, "empty", True) or df.shape[1] == 0:
        return None

    # isimden
    name_candidates = [
        c for c in df.columns
        if isinstance(c, str) and (
            "time" in c.lower() or "date" in c.lower() or "tarih" in c.lower()
            or "current_time" in c.lower() or "timestamp" in c.lower()
        )
    ]
    if name_candidates:
        return name_candidates[0]

    # içerikten (datetime oranı)
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


# -------------------- Metrik başlık eşleme --------------------
_METRIC_SYNONYMS: Dict[str, list[str]] = {
    "24h": [
        r"^24\s*h$", r"^24h$", r"^24\s*saat$", r"^son\s*24\s*saat$", r"^24\s*hour$", r"^24hours?$",
        r"^24\s*-\s*h$", r"^24$"
    ],
    "week": [
        r"^week$", r"^hafta$", r"^weekly$", r"^1w$", r"^son\s*hafta$", r"^haftalik$"
    ],
    "month": [
        r"^month$", r"^ay$", r"^monthly$", r"^1m$", r"^son\s*ay$", r"^aylik$"
    ],
    "rtp": [
        r"^rtp$", r"^return\s*to\s*player$", r"^oyuncuya\s*dön(ü|u)ş$"
    ],
}
_METRIC_ORDER = ["rtp", "24h", "week", "month"]


def _normalize_percent_like(x: Any) -> Any:
    """'%75', '75 %', '75,5', '75.5', '  75  ' -> 75.5 (float); aksi takdirde NA."""
    if pd.isna(x):
        return pd.NA
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace("%", "").replace("‰", "")
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    try:
        if s in {"", ".", "-"}:
            return pd.NA
        return float(s)
    except Exception:
        return pd.NA


def _match_metric_by_header(colname: str) -> Optional[str]:
    if not isinstance(colname, str):
        return None
    name = colname.strip().lower()
    name = re.sub(r"[\s\-_]+", " ", name)
    for key, patterns in _METRIC_SYNONYMS.items():
        for pat in patterns:
            if re.search(pat, name, flags=re.IGNORECASE):
                return key
    return None


def detect_metric_columns(df: pd.DataFrame) -> Dict[str, str]:
    found: Dict[str, str] = {}
    for c in df.columns:
        key = _match_metric_by_header(c)
        if key and key not in found:
            found[key] = c
    return found


# -------------------- Hücre içi token (24H108.03%) --------------------
_CELL_TOKEN_PATTERNS: Dict[str, str] = {
    "24h": r"\b24\s*h\b",
    "week": r"\bweek\b",
    "month": r"\bmonth\b",
    "rtp": r"\brtp\b",
}

def _parse_number_like(text: Any) -> Any:
    """'108.03%', '104,12 %', ' 80.23 ' -> float; aksi halde NA."""
    if pd.isna(text):
        return pd.NA
    s = str(text).lower().strip().replace("%", "")
    s = s.replace(",", ".")
    m = re.search(r"(-?\d+(?:\.\d+)?)", s)
    if not m:
        return pd.NA
    try:
        return float(m.group(1))
    except Exception:
        return pd.NA


def _extract_metrics_from_cells(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Satırdaki tüm hücreleri tarar; '24H/Week/Month/RTP' tokenı geçiyorsa sayıyı çeker.
    Aynı anahtar birden çok hücrede varsa ilkini kullanır.
    """
    out: Dict[str, Any] = {}
    for val in row_dict.values():
        if pd.isna(val):
            continue
        s = str(val).lower()
        for key, pat in _CELL_TOKEN_PATTERNS.items():
            if key in out:
                continue
            if re.search(pat, s, flags=re.IGNORECASE):
                out[key] = _parse_number_like(s)
    return out


# -------------------- Normalizasyon --------------------
def normalize_dataframe(df: pd.DataFrame, src_path_for_log: str = ""):
    """
    Girdi DF'yi normalize eder (başlık eşleme + hücre içi token).
    Dönüş: (normalized_df, reason) — normalized_df None ise SKIP sebebi 'reason'.
    """
    if df is None or getattr(df, "empty", True) or df.shape[1] == 0:
        return None, "no rows"

    df = df.copy()

    # Timestamp
    ts_col = detect_timestamp_col(df)
    if ts_col:
        df["timestamp"] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    else:
        df["timestamp"] = pd.NaT

    rows = []

    # 1) Önce klasik başlığa göre
    metric_cols = detect_metric_columns(df)
    if metric_cols:
        for key, col in metric_cols.items():
            df[col] = df[col].map(_normalize_percent_like)

        if df["timestamp"].notna().any():
            for _, r in df.iterrows():
                row = {"timestamp": r["timestamp"]}
                for key, col in metric_cols.items():
                    row[key] = r[col]
                rows.append(row)
        else:
            row = {"timestamp": pd.NaT}
            for key, col in metric_cols.items():
                row[key] = pd.to_numeric(df[col], errors="coerce").dropna().tail(1).mean()
            rows.append(row)

    # 2) Başlık eşleşmedi: hücre içi token arama (sizin dosya tipi)
    else:
        for _, r in df.iterrows():
            row_dict = r.to_dict()
            metrics = _extract_metrics_from_cells(row_dict)
            if not metrics:
                continue
            row = {"timestamp": r.get("timestamp", pd.NaT)}
            row.update(metrics)
            rows.append(row)

    if not rows:
        return None, "no metric columns"

    out = pd.DataFrame(rows)

    metric_present = [c for c in ["rtp", "24h", "week", "month"] if c in out.columns and out[c].notna().any()]
    if not metric_present:
        return None, "no usable metric values"

    if "timestamp" in out.columns:
        out = out.sort_values("timestamp", na_position="first").reset_index(drop=True)

    # (Opsiyonel) aşırı değer filtresi — 24H/Week 100+ gelebiliyor; bu yüzden kapalı.
    # for c in ["rtp", "24h", "week", "month"]:
    #     if c in out.columns:
    #         out.loc[(out[c] < 0) | (out[c] > 200), c] = pd.NA

    wanted_order = ["timestamp", "rtp", "24h", "week", "month"]
    present = [c for c in wanted_order if c in out.columns]
    return out[present], "ok"


# -------------------- Excel okuma --------------------
def read_excel_safely(path: Path) -> pd.DataFrame:
    """En çok sütunlu sheet'i seç; okunamazsa boş DF döndür."""
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


# -------------------- Drive yardımcıları --------------------
def build_drive_service():
    gac_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if gac_path and Path(gac_path).exists():
        creds = service_account.Credentials.from_service_account_file(
            gac_path,
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
        return build("drive", "v3", credentials=creds)

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


# -------------------- main --------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder-id", dest="folder_id", default=None,
                        help="Google Drive folder id (CLI öncelikli). URL verirseniz ID otomatik çıkarılır.")
    args = parser.parse_args()

    folder_id = resolve_folder_id(args.folder_id)
    if not folder_id:
        raise RuntimeError(
            "Drive folder id bulunamadı. "
            "CLI (--folder-id) veya ENV (DRIVE_FOLDER_ID / GDRIVE_FOLDER_ID / GOOGLE_DRIVE_FOLDER_ID / "
            "FOLDER_ID / INPUT_FOLDER_ID / DRIVE_FOLDER_URL) ile verin."
        )

    masked = f"{folder_id[:4]}...{folder_id[-4:]}" if len(folder_id) > 8 else folder_id
    print(f"[collector] Using Drive folder id: {masked}")

    print("[collector] Cleaning data/raw and data/normalized")
    for p in RAW_DIR.glob("*"):
        p.unlink(missing_ok=True)
    for p in NORM_DIR.glob("*"):
        p.unlink(missing_ok=True)

    service = build_drive_service()

    try:
        files = list_drive_files(service, folder_id)
    except HttpError as e:
        print(f"[collector] Drive API error: {e}")
        print("[collector] Olası nedenler:")
        print("  • Klasör ID yanlış (özellikle linkten ID kopyalarken).")
        print("  • Bu klasör, servis hesabı e-postasıyla paylaşılmadı (credential JSON içindeki client_email).")
        print("  • Drive API yetkisi.")
        raise

    print(f"[collector] Found {len(files)} items (including subfolders).")

    # 1) İndir
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
