# ci/collector_gdrive_ci.py
# Drive klasörünü (alt klasörler dahil) data/raw/ içine indirir,
# sonra her Excel için 24H/Week/Month/RTP + timestamp'ı ayrıştırıp
# data/normalized/<slug>.csv olarak TEK SATIRLIK normalize veriyi yazar.
# Her çalıştırmada raw/ ve normalized/ klasörleri temizlenir.

import os, io, re, json, argparse, shutil, unicodedata
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def log(msg): print(f"[collector] {msg}")

# ---------- Drive Helpers ----------
def normalize_folder_id(raw):
    s = (raw or "").strip()
    m = re.search(r"/folders/([A-Za-z0-9_-]+)", s)
    return m.group(1) if m else s

def drive_context(service, folder_id):
    ctx = dict(supportsAllDrives=True, includeItemsFromAllDrives=True)
    try:
        meta = service.files().get(fileId=folder_id, fields="id,name,driveId",
                                   supportsAllDrives=True).execute()
        if meta.get("driveId"):
            ctx.update(corpora="drive", driveId=meta["driveId"])
        else:
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

    # Google Sheets → XLSX export
    if mime == "application/vnd.google-apps.spreadsheet":
        target = os.path.join(target_dir, name if name.lower().endswith(".xlsx") else f"{name}.xlsx")
        request = service.files().export_media(
            fileId=fid,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    # Native Excel
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

# ---------- Normalization ----------
def slugify(text):
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^A-Za-z0-9._-]+", "-", text).strip("-")
    return text or "file"

def parse_num(s):
    if s is None: return None
    m = re.search(r"([-+]?\d+(?:\.\d+)?)", str(s))
    return float(m.group(1)) if m else None

def extract_metrics(df, filename):
    """
    Bu dosya tiplerinde veri genelde hücrelerde '24H\\n78.5%' gibi geliyor.
    Mantık: timestamp kolonu dolu olan SON satırı al, oradaki hücre değerlerinden sayıları sök.
    """
    cols = [str(c) for c in df.columns]
    # timestamp kolonunu bul (hem header hem hücrelerde tarih olabilir)
    ts_col = None
    for c in cols:
        cl = c.lower()
        if any(k in cl for k in ["time", "date", "tarih", "zaman"]):
            ts_col = c
            break
    if ts_col is None:
        # bazen timestamp bizzat kolon adında oluyor; hiçbirini bulamazsak en sağ kolonu dene
        ts_col = cols[-1]

    # timestamp'ı dolu son satır
    series_ts = pd.to_datetime(df[ts_col], errors="coerce")
    last_idx = series_ts.last_valid_index()
    row = df.loc[last_idx] if last_idx is not None else df.iloc[-1]

    # oyun adı: ilk kolonun başlığı ya da hücresi
    game = None
    if cols:
        game = str(cols[0]).strip()
    if not game or game.lower().startswith("unnamed"):
        game = str(row.iloc[0])

    # metrikler
    def pick(name_candidates):
        # önce hücrelerde ara, bulamazsan kolon adlarında ara
        for c in df.columns:
            if any(k in str(c).lower() for k in name_candidates):
                val = row[c]
                n = parse_num(val)
                if n is not None:
                    return n
        for c in df.columns:
            cl = str(c).lower()
            if any(k in cl for k in name_candidates):
                n = parse_num(c)
                if n is not None:
                    return n
        return None

    m24  = pick(["24h"])
    week = pick(["week"])
    month= pick(["month"])
    rtp  = pick(["rtp"])

    ts = pd.to_datetime(row[ts_col], errors="coerce")
    if pd.isna(ts):
        ts = pd.Timestamp.utcnow()

    return {
        "timestamp": ts.tz_localize("UTC") if ts.tzinfo is None else ts,
        "game": str(game).strip(),
        "24H": m24, "Week": week, "Month": month, "RTP": rtp
    }

# ---------- Pipeline ----------
def clean_dir(p):
    if os.path.isdir(p):
        for f in os.listdir(p):
            fp = os.path.join(p, f)
            if os.path.isfile(fp):
                os.remove(fp)
    else:
        os.makedirs(p, exist_ok=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--folder-id", required=True, help="Drive Folder ID or full URL")
    args = ap.parse_args()

    # Credentials
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds_path and os.path.exists(creds_path):
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    elif creds_json:
        creds = service_account.Credentials.from_service_account_info(json.loads(creds_json), scopes=SCOPES)
    else:
        raise RuntimeError("Missing service account credentials.")

    service = build("drive", "v3", credentials=creds)

    folder_id = normalize_folder_id(args.folder_id)
    log(f"Folder: {folder_id}")

    # 0) data/raw & data/normalized temizle
    clean_dir("data/raw")
    clean_dir("data/normalized")
    log("Cleaned data/raw and data/normalized")

    # 1) indir
    items = walk_files(service, folder_id)
    log(f"Found {len(items)} items (including subfolders).")

    downloaded = []
    for it in items:
        p = download_excel_like(service, it, "data/raw")
        if p:
            downloaded.append(p)
            log(f"Downloaded: {it['name']} -> {p}")
    log(f"Excel-like files downloaded: {len(downloaded)}")

    # 2) normalize & kaydet
    rows = []
    for p in downloaded:
        try:
            df = pd.read_excel(p, sheet_name=0, engine="openpyxl")
        except Exception as e:
            log(f"[SKIP read] {p}: {e}")
            continue
        rec = extract_metrics(df, p)
        rows.append(rec)
        # per-file CSV
        slug = slugify(os.path.splitext(os.path.basename(p))[0])
        out_path = os.path.join("data/normalized", f"{slug}.csv")
        pd.DataFrame([rec]).to_csv(out_path, index=False)
        log(f"Normalized -> {out_path}")

    # 3) İsteğe bağlı: tek snapshot.csv (tüm oyunların aynı anda)
    if rows:
        snap = pd.DataFrame(rows)
        snap.to_csv("data/snapshot.csv", index=False)
        log("Wrote data/snapshot.csv with current snapshot")
    else:
        log("No parsed rows.")

if __name__ == "__main__":
    main()
