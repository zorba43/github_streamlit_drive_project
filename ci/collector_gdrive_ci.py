#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Google Drive --> data/raw  |  normalize --> data/normalized

Güncellemeler:
- --folder-id (CLI) veya ENV (DRIVE_FOLDER_ID / GDRIVE_FOLDER_ID / GOOGLE_DRIVE_FOLDER_ID / FOLDER_ID / INPUT_FOLDER_ID / DRIVE_FOLDER_URL)
  + URL verilirse ID otomatik çıkarılır.
- Timestamp ve METRİK (24H / Week / Month / RTP) kolonlarını esnek şekilde yakalar:
  * Türkçe/İngilizce başlık varyasyonları (24H, 24 H, 24 Saat, Hafta, Week, Ay, Month, RTP, Return To Player)
  * % işaretli/string değerleri normalize eder (0–100)
  * Dikey tablo (key-value) düzenini algılar (1. kolonda metrik adları, 2. kolonda değerler)
- Metrik bulunamazsa anlaşılır sebep ile SKIP (IndexError yok)
"""

import os
import io
import re
import argparse
from pathlib import Path
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

# -------------------- Yardımcı: URL'den ID çıkar --------------------
def extract_folder_id_from_url(text: str) -> str | None:
    if not text:
        return None
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", text)
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", text)
    if m:
        return m.group(1)
    return None


def resolve_folder_id(cli_value: str | None) -> str:
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


# -------------------- Timestamp sütunu tespiti --------------------
def detect_timestamp_col(df: pd.DataFrame) -> str | None:
    if df is None or getattr(df, "empty", True) or df.shape[1] == 0:
        return None

    # İsimden
    name_candidates = [
        c for c in df.columns
        if isinstance(c, str) and (
            "time" in c.lower() or "date" in c.lower() or "tarih" in c.lower()
        )
    ]
    if name_candidates:
        return name_candidates[0]

    # İçerikten (datetime oranı)
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


# -------------------- Metrik sütunu tespiti --------------------
_METRIC_SYNONYMS = {
    "24h": [
        r"^24\s*h$", r"^24h$", r"^24\s*saat$", r"^24\s*-?\s*h\s*$",
        r"^son\s*24\s*saat$", r"^24\s*hour$", r"^24hours?$"
    ],
    "week": [
        r"^week$", r"^hafta$", r"^weekly$", r"^1w$", r"^son\s*hafta$"
    ],
    "month": [
        r"^month$", r"^ay$", r"^monthly$", r"^1m$", r"^son\s*ay$"
    ],
    "rtp": [
        r"^rtp$", r"^return\s*to\s*player$", r"^oyuncuya\s*dönüş$"
    ],
}
_METRIC_ORDER = ["rtp", "24h", "week", "month"]  # normalize ederken yazım sırası


def _normalize_percent_like(x):
    """'%75', '75 %', '75,5', '75.5', '  75  ' -> 75.5 (float)  |  aksi takdirde NaN."""
    if pd.isna(x):
        return pd.NA
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace("%", "").replace("‰", "")
    # virgülü noktaya çevir
    s = s.replace(",", ".")
    # sadece sayı ve nokta kalsın
    s = re.sub(r"[^0-9.\-]", "", s)
    try:
        if s == "" or s == "." or s == "-":
            return pd.NA
        val = float(s)
        # 0–100 aralığına zorla (çok anormal ise NA)
        if not (0.0 <= val <= 100.0):
            return pd.NA
        return val
    except Exception:
        return pd.NA


def _match_metric_by_header(colname: str) -> str | None:
    """Başlık ismine göre metrik anahtarını (rtp/24h/week/month) döndür."""
    if not isinstance(colname, str):
        return None
    name = colname.strip().lower()
    name = re.sub(r"[\s\-_]+", " ", name)  # "24 - H" gibi durumlar
    for key, patterns in _METRIC_SYNONYMS.items():
        for pat in patterns:
            if re.search(pat, name, flags=re.IGNORECASE):
                return key
    # bazı kısa varyasyonlar
    if name in {"24", "24 h", "24-hour"}:
        return "24h"
    if name in {"w", "haftalik"}:
        return "week"
    if name in {"m", "aylik"}:
        return "month"
    return None


def detect_metric_columns(df: pd.DataFrame) -> dict:
    """
    Metrik sütunlarını esnek başlıklara göre yakalar.
    Dönüş: {'rtp': 'RTP_Sütunu', '24h': '24H_Sütunu', 'week': 'Week_Sütunu', 'month': 'Month_Sütunu'}
    Bulamadıklarını eklemez.
    """
    found = {}
    for c in df.columns:
        key = _match_metric_by_header(c)
        if key and key not in found:
            found[key] = c
    return found


def detect_vertical_kv_layout(df: pd.DataFrame) -> dict | None:
    """
    Dikey tablo: ilk kolonda anahtarlar ('24H','Week','Month','RTP' vb.), ikinci kolonda değerler.
    En az iki metrik bulunursa mapping döndürür.
    """
    if df.shape[1] < 2:
        return None
    key_col = df.columns[0]
    val_col = df.columns[1]

    # anahtarları string kabul ediyoruz
    keys = df[key_col].astype(str).str.strip().str.lower()
    mapping = {}
    for i, k in enumerate(keys):
        key = _match_metric_by_header(k)
        if key and key not in mapping:
            mapping[key] = val_col
    if len(mapping) >= 2:
        # sadece tek timestamp olabilir; normalize ederken tek satır üretiriz
        return {"layout": "vertical", "map": mapping, "key_col": key_col, "val_col": val_col}
    return None


# -------------------- Normalizasyon --------------------
def normalize_dataframe(df: pd.DataFrame, src_path_for_log=""):
    """
    Girdi DF'yi normalize eder.
    Dönüş: (normalized_df, reason)
      - normalized_df None ise "reason" SKIP sebebidir.
    """
    if df is None or getattr(df, "empty", True) or df.shape[1] == 0:
        return None, "no rows"

    # 1) Timestamp
    ts_col = detect_timestamp_col(df)
    df = df.copy()
    if ts_col:
        df["timestamp"] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    else:
        # Bazı dosyalarda timestamp yok ama tek ölçüm vardır; yine de tek satır üretilebilir.
        df["timestamp"] = pd.NaT

    # 2) Metrik sütunları doğrudan başlığa göre
    metric_cols = detect_metric_columns(df)

    # 3) Hiç metrik yakalanmadıysa, dikey tabloda olabilir
    vertical = None
    if not metric_cols:
        vertical = detect_vertical_kv_layout(df)

    # 4) Çıkarma
    rows = []
    if vertical:
        # Dikey key-value düzeni: tek satır üretiriz
        row = {"timestamp": pd.NaT}
        for key in _METRIC_ORDER:
            if key in vertical["map"]:
                col = vertical["map"][key]
                val = df[col].iloc[0] if len(df[col]) > 0 else pd.NA
                row[key] = _normalize_percent_like(val)
        rows.append(row)
    else:
        # Yatay (klasik satırlar): mevcut metrik sütunlarını temizle
        # önce yüzde benzeri değerleri float'a çevir
        for key, col in metric_cols.items():
            df[col] = df[col].map(_normalize_percent_like)

        # timestamp varsa satırlarda dizi üret; yoksa tek satırlık ortalama da yapılabilir.
        if df["timestamp"].notna().any():
            for _, r in df.iterrows():
                row = {"timestamp": r["timestamp"]}
                for key, col in metric_cols.items():
                    row[key] = r[col]
                rows.append(row)
        else:
            # timestamp yok ama metrikler var: tek satır üret (ortalama/son değer)
            row = {"timestamp": pd.NaT}
            for key, col in metric_cols.items():
                row[key] = pd.to_numeric(df[col], errors="coerce").dropna().tail(1).mean()
            rows.append(row)

    if not rows:
        return None, "no metric columns"

    out = pd.DataFrame(rows)

    # En az bir metrik olması lazım
    metric_present = [c for c in ["rtp", "24h", "week", "month"] if c in out.columns and out[c].notna().any()]
    if not metric_present:
        return None, "no usable metric values"

    # timestamp'ı sırala; sonra 0–100 aralığı dışındakileri NA yap
    if "timestamp" in out.columns:
        out = out.sort_values("timestamp", na_position="first")
    for c in ["rtp", "24h", "week", "month"]:
        if c in out.columns:
            out.loc[(out[c] < 0) | (out[c] > 100), c] = pd.NA

    # Son çıktı sütadları
    wanted_order = ["timestamp", "rtp", "24h", "week", "month"]
    present = [c for c in wanted_order if c in out.columns]
    return out[present].reset_index(drop=True), "ok"


# -------------------- Excel okuma --------------------
def read_excel_safely(path: Path) -> pd.DataFrame:
    """En çok sütun barındıran sheet'i seçerek oku. Okunamazsa boş DF döndür."""
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
                        help="Google Drive folder id (CLI öncelikli). URL verilirse ID otomatik çıkarılır.")
    args = parser.parse_args()

    folder_id = resolve_folder_id(args.folder_id)
    if not folder_id:
        raise RuntimeError(
            "Drive folder id bulunamadı. "
            "CLI (--folder-id) veya ENV (DRIVE_FOLDER_ID / GDRIVE_FOLDER_ID / GOOGLE_DRIVE_FOLDER_ID / FOLDER_ID / INPUT_FOLDER_ID / DRIVE_FOLDER_URL) ile verin."
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
        print("  • Bu klasör, servis hesabı e-postasıyla paylaşılmadı.")
        print("    Servis hesabı e-postası: JSON içindeki client_email alanı.")
        print("  • Drive API yetkisi eksik.")
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
