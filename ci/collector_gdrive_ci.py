# ci/collector_gdrive_ci.py
# Drive -> data/raw/ indirir; her Excel'i satır satır NORMALIZE edip
# data/normalized/<slug>.csv (çok satır) yazar. Her çalıştırmada raw/ ve
# normalized/ temizlenir. 24H/Week/Month/RTP değerleri ETİKETTEN SONRAKİ
# sayıya göre çıkarılır (header'dan sayı çekilmez).

import os, io, re, json, argparse, unicodedata
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def log(m): print(f"[collector] {m}")

# ---------------- Drive helpers ----------------
def normalize_folder_id(raw):
    s = (raw or "").strip()
    m = re.search(r"/folders/([A-Za-z0-9_-]+)", s)
    return m.group(1) if m else s

def drive_ctx(service, folder_id):
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
    ctx = drive_ctx(service, folder_id)
    stack = [folder_id]; out=[]
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

    if mime == "application/vnd.google-apps.spreadsheet":
        target = os.path.join(target_dir, name if name.lower().endswith(".xlsx") else f"{name}.xlsx")
        request = service.files().export_media(
            fileId=fid,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
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
        dl = MediaIoBaseDownload(fh, request)
        done=False
        while not done:
            _, done = dl.next_chunk()
    return target

# ---------------- Normalization helpers ----------------
def slugify(text):
    text = unicodedata.normalize("NFKD", str(text)).encode("ascii","ignore").decode("ascii")
    text = re.sub(r"[^A-Za-z0-9._-]+","-", text).strip("-")
    return text or "file"

def match_col(cols, *keys):
    """Kolon adlarını boşluksuz/küçük harf karşılaştır; olası varyasyonları yakala."""
    keys = [k.replace(" ", "").lower() for k in keys]
    for c in cols:
        cl = str(c).lower().replace(" ", "")
        if any(k in cl for k in keys):
            return c
    return None

def detect_timestamp_col(df):
    cols = list(df.columns)
    c = match_col(cols, "time","date","tarih","zaman","timestamp")
    if c: return c
    best = None; best_count = -1
    for c in cols:
        s = pd.to_datetime(df[c], errors="coerce", dayfirst=True, utc=False)
        good = s.notna().sum()
        if good > best_count:
            best_count = good; best = c
    return best or cols[-1]

def parse_metric(cell_value, label):
    """
    Hücre metninden 'label' (örn. 24h / week / month / rtp) etiketini temizleyip
    SONRASINDAKİ ilk sayıyı döndürür. Virgül ondalık, % işareti, satır sonu vb. desteklenir.
    """
    if cell_value is None:
        return None
    s = str(cell_value).strip()
    if not s:
        return None

    # normalize decimal commas and whitespace
    s = s.replace(",", ".")
    s = re.sub(r"\s+", " ", s)

    label = label.lower()
    # farklı yazımlar için olası label kalıpları
    patterns = {
        "24h":  [r"24\s*h", r"24hours?", r"24\s*saat"],
        "week": [r"week", r"1w", r"7d", r"last\s*7", r"7\s*g[uü]n"],
        "month":[r"month", r"1m", r"30d", r"last\s*30", r"30\s*g[uü]n"],
        "rtp":  [r"rtp", r"return\s*to\s*player"]
    }.get(label, [re.escape(label)])

    # 1) etiketten SONRA gelen sayı
    for pat in patterns:
        m = re.search(pat + r"[^0-9\-+]*([-+]?\d+(?:\.\d+)?)", s, flags=re.I)
        if m:
            try:
                return float(m.group(1))
            except Exception:
                pass

    # 2) etiket varsa tamamen temizleyip ilk sayıyı al (yine label'daki '24' yakalanmasın diye)
    for pat in patterns:
        s = re.sub(pat, " ", s, flags=re.I)
    m2 = re.search(r"([-+]?\d+(?:\.\d+)?)", s)
    if m2:
        try:
            return float(m2.group(1))
        except Exception:
            pass

    return None

def normalize_dataframe(df, source_path):
    cols = list(df.columns)

    # Oyun adı
    game_name = str(cols[0]).strip() if cols else ""
    if not game_name or game_name.lower().startswith("unnamed"):
        try:
            game_name = str(df.iloc[0,0]).strip()
        except Exception:
            from os.path import basename, splitext
            game_name = splitext(basename(source_path))[0]

    # Kolon eşleştirme
    ts_col  = detect_timestamp_col(df)
    c_24h   = match_col(cols, "24h", "last24h", "24hours", "24saat")
    c_week  = match_col(cols, "week", "1w", "7d", "last7d", "7gun", "7gün")
    c_month = match_col(cols, "month", "1m", "30d", "last30d", "30gun", "30gün")
    c_rtp   = match_col(cols, "rtp", "returntoplayer")

    out_rows = []
    ts_series = pd.to_datetime(df[ts_col], errors="coerce", dayfirst=True, utc=False)

    for i in range(len(df)):
        ts = ts_series.iloc[i]
        if pd.isna(ts):
            continue

        def val(col, lbl):
            return parse_metric(df.iloc[i][col], lbl) if (col in df.columns and col is not None) else None

        rec = {
            "timestamp": ts,
            "game": game_name,
            "24H":   val(c_24h,   "24h"),
            "Week":  val(c_week,  "week"),
            "Month": val(c_month, "month"),
            "RTP":   val(c_rtp,   "rtp"),
        }

        if any(rec[k] is not None for k in ["24H","Week","Month","RTP"]):
            out_rows.append(rec)

    return pd.DataFrame(out_rows)

def clean_dir(path):
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True); return
    for f in os.listdir(path):
        p = os.path.join(path, f)
        if os.path.isfile(p): os.remove(p)

# ---------------- Pipeline ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--folder-id", required=True, help="Drive Folder ID or full URL")
    args = ap.parse_args()

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

    clean_dir("data/raw")
    clean_dir("data/normalized")
    log("Cleaned data/raw and data/normalized")

    items = walk_files(service, folder_id)
    log(f"Found {len(items)} items (including subfolders).")
    downloaded = []
    for it in items:
        p = download_excel_like(service, it, "data/raw")
        if p:
            downloaded.append(p)
            log(f"Downloaded: {it['name']} -> {p}")
    log(f"Excel-like files downloaded: {len(downloaded)}")

    for p in downloaded:
        try:
            df = pd.read_excel(p, sheet_name=0, engine="openpyxl")
        except Exception as e:
            log(f"[SKIP read] {p}: {e}")
            continue
        norm = normalize_dataframe(df, p)
        if norm.empty:
            log(f"[SKIP normalize] {p}: no rows parsed")
            continue
        slug = slugify(os.path.splitext(os.path.basename(p))[0])
        out_path = os.path.join("data/normalized", f"{slug}.csv")
        norm.to_csv(out_path, index=False)
        log(f"Normalized rows: {len(norm)} -> {out_path}")

    log("Done.")

if __name__ == "__main__":
    main()
