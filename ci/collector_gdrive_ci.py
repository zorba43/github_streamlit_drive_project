# ci/collector_gdrive_ci.py
import os, io, re, json, argparse
from datetime import datetime
import pandas as pd

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def parse_numeric(value):
    if value is None: 
        return None
    s = str(value)
    m = re.search(r"([-+]?\d+(?:\.\d+)?)\s*%?", s)
    return float(m.group(1)) if m else None

def extract_metrics_from_df(df, filename):
    cols = list(df.columns)
    out = {
        "timestamp": None, "game": None, "24H": None, "Week": None, "Month": None, "RTP": None,
        "source_file": os.path.basename(filename)
    }

    # game name guess: first header or filename
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
        # values embedded in headers
        for c in cols:
            cl = str(c).lower()
            if "24h" in cl: out["24H"] = parse_numeric(c)
            if "week" in cl: out["Week"] = parse_numeric(c)
            if "month" in cl: out["Month"] = parse_numeric(c)
            if "rtp" in cl: out["RTP"] = parse_numeric(c)
        # timestamp from header if possible
        for c in cols:
            try:
                ts = pd.to_datetime(str(c), errors="raise")
                out["timestamp"] = ts
                break
            except Exception:
                pass

    if out["timestamp"] is None:
        out["timestamp"] = pd.Timestamp.utcnow()

    return out

def normalize_folder_id(raw):
    """Accepts either pure ID or a full URL; returns the ID."""
    s = (raw or "").strip
