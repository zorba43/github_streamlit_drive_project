# normalizer.py
import re
import pandas as pd

def _to_float(x):
    if pd.isna(x): return None
    s = str(x).strip().replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def parse_metric_after_label(val: object, label: str) -> float | None:
    if pd.isna(val): return None
    s = str(val).strip()
    pat = rf'(?i){re.escape(label)}\s*([+-]?\d+(?:[.,]\d+)?)'
    m = re.search(pat, s)
    if m: return _to_float(m.group(1))
    m = re.search(r'([+-]?\d+(?:[.,]\d+)?)', s)
    if m: return _to_float(m.group(1))
    return None

def normalize_from_text_columns(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Raw excel sütunları:
      Text -> Game
      Text1 -> 24H
      Text2 -> Week
      Text3 -> Month
      Text4 -> RTP
      Current_time -> timestamp
    """
    cols = {c.lower(): c for c in df_raw.columns}
    def pick(*names):
        for n in names:
            if n in cols:
                return cols[n]
        return None

    c_game  = pick("text", "game", "oyun")
    c_24h   = pick("text1", "24h")
    c_week  = pick("text2", "week", "1w")
    c_month = pick("text3", "month", "1m")
    c_rtp   = pick("text4", "rtp")
    c_time  = pick("current_time", "timestamp", "time", "datetime")

    out = pd.DataFrame()
    if c_time:  out["timestamp"] = pd.to_datetime(df_raw[c_time], errors="coerce", utc=True)
    if c_game:  out["game"]      = df_raw[c_game].astype(str).str.strip()

    if c_24h:   out["24h"]  = df_raw[c_24h].apply(lambda v: parse_metric_after_label(v, "24h"))
    if c_week:  out["week"] = df_raw[c_week].apply(lambda v: parse_metric_after_label(v, "week"))
    if c_month: out["month"]= df_raw[c_month].apply(lambda v: parse_metric_after_label(v, "month"))
    if c_rtp:   out["rtp"]  = df_raw[c_rtp].apply(lambda v: parse_metric_after_label(v, "rtp"))

    keep = [c for c in ["timestamp", "game", "24h", "week", "month", "rtp"] if c in out.columns]
    out = out[keep].sort_values("timestamp").reset_index(drop=True)
    return out
