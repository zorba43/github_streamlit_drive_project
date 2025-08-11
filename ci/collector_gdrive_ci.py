# ci/collector_gdrive_ci.py  (updated: Sheets export + recursive + logs)
import os, io, re, json, argparse
from datetime import datetime
import pandas as pd

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def log(msg): print(f"[collector] {msg}")

def parse_numeric(value):
    if value is None: return None
    s = str(value)
    m = re.search(r"([-+]?\d+(?:\.\d+)?)\s*%?", s)
    return float(m.group(1)) if m else None

def extract_metrics_from_df(df, filename):
    cols = list(df.columns)
    out = {"timestamp": None, "game": None, "24H": None, "Week": None, "Month": None, "RTP": None,
           "source_file": os.path.basename(filename)}

    # game name guess
    if cols:
        out["game"] = str(cols[0]).strip()
    if not out["game"] or out["game"].lower().startswith("unnamed"):
        out["game"] = os.path.splitext(os.path.basename(filename))[0]

    if len(df) > 0:
        row0 = df.iloc[0].to_dict()
        for key in ["24H", "Week", "Month", "RTP"]:
            for c in cols:
                if key.lower() in str(c).lower():
                    out[key] = parse_numeric(row0.get(c))
                    break
        for c in cols:
            if any(k in str(c).lower() for k in ["time","date","tarih","zaman"]):
                ts = pd.to_datetime(row0.get(c), errors="coerce")
                if pd.notna(ts):
                    out["timestamp"] = ts
                    break
    else:
        for c in co
