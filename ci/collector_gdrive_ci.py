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
        target = os.path.join(target_dir, name if name.lower().endswith(".xlsx") else f"{name}.xlsx"_
