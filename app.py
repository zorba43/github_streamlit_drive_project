# app.py
import os
from pathlib import Path
from typing import Dict, List
import re

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Normalized Oyun Zaman Serileri + ADioG", layout="wide")

NORM_DIR = Path("data/normalized")
NORM_DIR.mkdir(parents=True, exist_ok=True)

DISPLAY_METRICS = ["24H", "Week", "Month", "RTP"]
METRIC_MAP = {
    "24H": "24h", "24h": "24h",
    "Week": "week", "week": "week", "1W": "week",
    "Month": "month", "month": "month", "1M": "month",
    "RTP": "rtp", "rtp": "rtp",
}

CANDIDATE_COLUMNS: Dict[str, List[str]] = {
    "timestamp": ["timestamp", "time", "datetime", "current_time", "current time", "tarih", "date"],
    "game": ["game", "oyun", "text", "name", "title"],
    "24h": ["24h", "24H", "24 h", "24hour", "24 hours", "text1"],
    "week": ["week", "hafta", "weekly", "1w", "text2"],
    "month": ["month", "ay", "monthly", "1m", "text3"],
    "rtp": ["rtp", "return to player", "oyuncuya dönüş", "text4"],
}

# ---------- robust parser (etiketten SONRAKİ sayıyı çek) ----------
def _to_float(x):
    if pd.isna(x):
        return None
    s = str(x).strip().replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def parse_metric_after_label(val: object, label: str) -> float | None:
    """
    '24H108.03%'  -> label='24h'  => 108.03
    'Week103,18%' -> label='week' => 103.18
    'RTP96.07%'   -> label='rtp'  => 96.07
    """
    if pd.isna(val):
        return None
    s = str(val).strip()
    pat = rf'(?i){re.escape(label)}\s*([+-]?\d+(?:[.,]\d+)?)'
    m = re.search(pat, s)
    if m:
        return _to_float(m.group(1))
    m = re.search(r'([+-]?\d+(?:[.,]\d+)?)', s)
    if m:
        return _to_float(m.group(1))
    return None
# ---------------------------------------------------------------

def _lower(s: str) -> str:
    return s.lower().strip().replace("_", " ")

def coerce_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Hangi isimle gelirse gelsin kolonları standartlaştır:
      timestamp, game, 24h, week, month, rtp
    Sonra metrik kolonları stringse parse et (24H96.7% gibi).
    """
    if df is None or df.empty:
        return df

    rename_map = {}
    lowered = {_lower(c): c for c in df.columns}

    def find_first(cands: List[str]) -> str | None:
        for cand in cands:
            cand_l = _lower(cand)
            for have, orig in lowered.items():
                if cand_l == have:
                    return orig
        return None

    final_map = {}
    ts_col = find_first(CANDIDATE_COLUMNS["timestamp"])
    if ts_col is None:
        best_col, best_ratio = None, 0.0
        for c in df.columns[:10]:
            try:
                s = pd.to_datetime(df[c], errors="coerce", utc=True)
                r = float(s.notna().mean())
                if r > best_ratio:
                    best_ratio, best_col = r, c
            except Exception:
                continue
        ts_col = best_col
    if ts_col is not None:
        final_map[ts_col] = "timestamp"

    g_col = find_first(CANDIDATE_COLUMNS["game"])
    if g_col is not None:
        final_map[g_col] = "game"

    for std, cands in [("24h", CANDIDATE_COLUMNS["24h"]),
                       ("week", CANDIDATE_COLUMNS["week"]),
                       ("month", CANDIDATE_COLUMNS["month"]),
                       ("rtp", CANDIDATE_COLUMNS["rtp"])]:
        col = find_first(cands)
        if col is not None:
            final_map[col] = std

    df2 = df.rename(columns=final_map).copy()

    if "timestamp" in df2.columns:
        df2["timestamp"] = pd.to_datetime(df2["timestamp"], errors="coerce", utc=True)

    for c in df2.columns:
        if c != "timestamp":
            df2.rename(columns={c: c.lower()}, inplace=True)

    # metrik kolonlarını daima parse et (stringse) veya heüristik düzelt
    for mcol, label in [("24h", "24h"), ("week", "week"), ("month", "month"), ("rtp", "rtp")]:
        if mcol in df2.columns:
            if not pd.api.types.is_numeric_dtype(df2[mcol]):
                df2[mcol] = df2[mcol].apply(lambda v: parse_metric_after_label(v, label))
            else:
                bad_ratio = (df2[mcol] < 40).mean() if len(df2[mcol]) else 0
                if bad_ratio > 0.6:
                    df2[mcol] = df2[mcol].apply(lambda v: parse_metric_after_label(v, label))

    keep = [c for c in ["timestamp", "game", "24h", "week", "month", "rtp"] if c in df2.columns]
    df2 = df2[keep].sort_values("timestamp").reset_index(drop=True)
    return df2

@st.cache_data(ttl=300, show_spinner=False)
def list_games() -> List[str]:
    return sorted([p.stem for p in NORM_DIR.glob("*.csv")])

@st.cache_data(ttl=300, show_spinner=True)
def load_game_df(game_name: str) -> pd.DataFrame:
    path = NORM_DIR / f"{game_name}.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    return coerce_columns(df)

def last_n_steps(df: pd.DataFrame, n: int) -> pd.DataFrame:
    if n <= 0 or n >= len(df):
        return df
    return df.tail(n).copy()

def compute_signal(df: pd.DataFrame, min_diff: float) -> pd.Series:
    has = all(c in df.columns for c in ["24h", "week", "month", "rtp"])
    if not has:
        return pd.Series([False] * len(df), index=df.index)
    cond = (df["24h"] > df["week"]) & (df["week"] > df["month"]) & ((df["24h"] - df["rtp"]) >= min_diff)
    return cond.fillna(False)

# ---------------- UI ----------------
st.title("🧪 Normalized Oyun Zaman Serileri + ADioG")

c1, c2, c3, c4, c5 = st.columns([3, 2, 2, 2, 1])

with c1:
    games = list_games()
    if not games:
        st.warning("`data/normalized/` içinde CSV yok. Collector çalışınca otomatik gelecek.")
        st.stop()
    game = st.selectbox("🎰 Oyun", games)

with c2:
    metric_ui = st.selectbox("📏 Metrik (solo)", DISPLAY_METRICS, index=0)
    metric_col = METRIC_MAP.get(metric_ui, metric_ui)

with c3:
    step_options = {"Son 200 adım": 200, "Son 500 adım": 500, "Son 1000 adım": 1000, "Son 2000 adım": 2000, "Tümü": -1}
    step_label = st.selectbox("⏱ Adım penceresi", list(step_options.keys()), index=1)
    step_n = step_options[step_label]

with c4:
    min_diff = st.number_input("ADioG sinyal eşiği (24H - RTP)", min_value=0.0, max_value=50.0, value=1.5, step=0.1)

with c5:
    if st.button("🔄 Yenile", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

df = load_game_df(game)
if df.empty:
    st.warning(f"`{game}` için veri yok.")
    st.stop()

if "timestamp" not in df.columns:
    st.error("Veride `timestamp` kolonu yok.")
    st.stop()

if metric_col not in df.columns:
    st.error(f"Seçilen metrik (‘{metric_ui}’ → ‘{metric_col}’) yok. Kolonlar: {list(df.columns)}")
    st.stop()

view_df = last_n_steps(df, step_n)

# ---- SOLO grafik ----
st.subheader(f"📈 {game} — {metric_ui}")
solo_df = view_df[["timestamp", metric_col]].dropna().copy()
if solo_df.empty:
    st.info("Seçilen aralıkta veri yok.")
else:
    fig_solo = px.line(solo_df, x="timestamp", y=metric_col, markers=True)
    fig_solo.update_layout(xaxis_title="timestamp", yaxis_title=metric_ui,
                           margin=dict(l=40, r=30, t=10, b=40), hovermode="x unified")
    ymin, ymax = float(solo_df[metric_col].min()), float(solo_df[metric_col].max())
    pad = max(0.5, (ymax - ymin) * 0.1)
    fig_solo.update_yaxes(range=[ymin - pad, ymax + pad])
    st.plotly_chart(fig_solo, use_container_width=True)

# ---- ADioG ----
st.subheader("🧪 ADioG — RTP gri, 24H kırmızı, Week lacivert, Month siyah")
cols = [c for c in ["timestamp", "rtp", "24h", "week", "month"] if c in view_df.columns]
adiog_df = view_df[cols].copy()

if len(adiog_df) < 2:
    st.info("ADioG için yeterli veri yok.")
else:
    adiog_df["signal"] = compute_signal(adiog_df, min_diff=min_diff)

    fig = go.Figure()
    if "rtp" in adiog_df:   fig.add_trace(go.Scatter(x=adiog_df["timestamp"], y=adiog_df["rtp"],   mode="lines",         name="RTP",   line=dict(color="#A0A0A0", width=2)))
    if "24h" in adiog_df:  fig.add_trace(go.Scatter(x=adiog_df["timestamp"], y=adiog_df["24h"],  mode="lines+markers", name="24H",   line=dict(color="#E24A33", width=2)))
    if "week" in adiog_df: fig.add_trace(go.Scatter(x=adiog_df["timestamp"], y=adiog_df["week"], mode="lines+markers", name="Week",  line=dict(color="#1F3A93", width=2)))
    if "month" in adiog_df:fig.add_trace(go.Scatter(x=adiog_df["timestamp"], y=adiog_df["month"],mode="lines+markers", name="Month", line=dict(color="#000000", width=2)))

    if adiog_df["signal"].any():
        pts = adiog_df[adiog_df["signal"]]
        ybase = "24h" if "24h" in pts else ("rtp" if "rtp" in pts else None)
        if ybase:
            fig.add_trace(go.Scatter(
                x=pts["timestamp"], y=pts[ybase], mode="markers",
                name="Giriş Sinyali", marker=dict(color="green", size=10)
            ))

    fig.update_layout(
        xaxis_title="timestamp", yaxis_title="RTP / %", hovermode="x unified",
        margin=dict(l=40, r=30, t=10, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    yvals = []
    for c in ["rtp", "24h", "week", "month"]:
        if c in adiog_df:
            yvals += adiog_df[c].dropna().tolist()
    if yvals:
        ymin, ymax = min(yvals), max(yvals)
        pad = max(0.5, (ymax - ymin) * 0.1)
        fig.update_yaxes(range=[ymin - pad, ymax + pad])

    st.plotly_chart(fig, use_container_width=True)

with st.expander("ℹ️ Veri Özeti"):
    st.write(f"Satır sayısı: **{len(view_df)}**")
    if "timestamp" in view_df:
        st.write(f"Aralık: **{view_df['timestamp'].min()} → {view_df['timestamp'].max()}**")
    st.dataframe(view_df.tail(15), use_container_width=True)
