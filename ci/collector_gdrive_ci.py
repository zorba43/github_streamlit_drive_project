# ci/collector_gdrive_ci.py
# Drive klasörünü (alt klasörler dahil) data/ içine MIRROR eder.
# Her çalıştırmada data/ içindeki eski xlsx/xls/xlsm dosyalarını siler,
# Drive’dan yeniden indirir. (CSV yok, append yok.)

import os, io, re, json, argparse, shutil
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def log(msg): print(f"[collector] {msg}")

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

def clean_data_folder(path="data"):
    os.makedirs(path, exist_ok=True)
    removed = 0
    for fname in os.listdir(path):
        fp = os.path.join(path, fname)
        if os.path.isfile(fp) and fname.lower().endswith((".xlsx",".xls",".xlsm")):
            os.remove(fp)
            removed += 1
    return removed

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--folder-id", required=True, help="Drive Folder ID or full URL")
    args = ap.parse_args()

    # Credentials: file path preferred; fall back to JSON blob
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

    # 1) data/ klasörünü temizle
    removed = clean_data_folder("data")
    log(f"Cleaned data/: removed {removed} old files")

    # 2) Drive’ı gez ve indir
    items = walk_files(service, folder_id)
    log(f"Found {len(items)} items (including subfolders).")

    downloaded = 0
    for it in items:
        p = download_excel_like(service, it, "data")
        if p:
            downloaded += 1
            log(f"Downloaded: {it['name']} -> {p}")

    log(f"Downloaded {downloaded} Excel-like files into data/")

if __name__ == "__main__":
    main()
