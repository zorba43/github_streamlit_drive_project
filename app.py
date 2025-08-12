# app.py
import os
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


st.set_page_config(page_title="Normalized Oyun Zaman Serileri + ADioG", layout="wide")

NORM_DIR = Path("data/normalized")
NORM_DIR.mkdir(parents=True, exist_ok=True)

# UI’da gösterilecek metrikler (solo grafik)
DISPLAY_METRICS = ["24H", "Week", "Month", "RTP"]

# UI -> normalized kolon adı (solo grafik için)
METRIC_MAP = {
    "24H": "24h", "24h": "24h",
    "Week": "week", "week": "week", "1W": "week",
    "Month": "month", "month": "month", "1M": "month",
    "RTP": "rtp", "rtp": "rtp",
}

# Her isimle gelebilecek kolonları standart isme dönüştür.
# Amaç: CSV/Excel nereden gelirse gelsin 'timestamp, game, 24h, week, month, rtp' elde etmek.
CANDIDATE_COLUMNS: Dict[str, List[str]] = {
    "timestamp": [
        "timestamp", "time", "datetime", "current_time", "current time",
        "tarih", "date"
    ],
    "game": [
        "game", "oyun", "text", "name", "title"
    ],
    "24h": [
        "24h", "24H", "24", "24 h", "24hour", "24 hours", "text1"
    ],
    "week": [
        "week", "hafta", "weekly", "1w", "text2"
    ],
    "month": [
        "month", "ay", "monthly", "1m", "text3"
    ],
    "rtp": [
        "rtp", "return to player", "oyuncuya dönüş", "text4"
    ],
}


# -----------------------------------------------------------------------------
# Yardımcılar
# -----------------------------------------------------------------------------
def _lower(s: str) -> str:
    return s.lower().strip().replace("_", " ")


def coerce_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Hangi isimle gelirse gelsin kolonları standart isimlere dönüştür:
    timestamp, game, 24h, week, month, rtp
    """
    if df is None or df.empty:
        return df

    # Kolonları küçük harfe indir, boşluk/alt çizgi normalize et
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

    # timestamp
    ts_col = find_first(CANDIDATE_COLUMNS["timestamp"])
    if ts_col is None:
        # Yoksa en iyi olasıyı tahmin etmeye çalış:
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

    # game
    g_col = find_first(CANDIDATE_COLUMNS["game"])
    if g_col is not None:
        final_map[g_col] = "game"

    # 24h, week, month, rtp
    for std, cands in [("24h", CANDIDATE_COLUMNS["24h"]),
                       ("week", CANDIDATE_COLUMNS["week"]),
                       ("month", CANDIDATE_COLUMNS["month"]),
                       ("rtp", CANDIDATE_COLUMNS["rtp"])]:
        col = find_first(cands)
        if col is not None:
            final_map[col] = std

    df2 = df.rename(columns=final_map).copy()

    # timestamp datetime
    if "timestamp" in df2.columns:
        df2["timestamp"] = pd.to_datetime(df2["timestamp"], errors="coerce", utc=True)

    # Metrik kolonları küçük harfe sabit kalsın
    for c in df2.columns:
        if c not in ["timestamp", "game"]:
            df2.rename(columns={c: c.lower()}, inplace=True)

    # Sadece mevcut olanları bırak
    keep = [c for c in ["timestamp", "game", "24h", "week", "month", "rtp"] if c in df2.columns]
    df2 = df2[keep].copy()

    # Sıralı
    if "timestamp" in df2.columns:
        df2 = df2.sort_values("timestamp").reset_index(drop=True)

    return df2


@st.cache_data(ttl=300, show_spinner=False)
def list_games() -> List[str]:
    return sorted([p.stem for p in NORM_DIR.glob("*.csv")])


@st.cache_data(ttl=300, show_spinner=True)
def load_game_df(game_name: str) -> pd.DataFrame:
    """
    data/normalized/<game>.csv dosyasını okur. Eğer farklı başlıklarla gelmişse
    coerce_columns ile standartlaştırır.
    """
    csv_path = NORM_DIR / f"{game_name}.csv"
    if not csv_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(csv_path)
    df = coerce_columns(df)
    return df


def last_n_steps(df: pd.DataFrame, n: int) -> pd.DataFrame:
    if n <= 0 or n >= len(df):
        return df
    return df.tail(n).copy()


def compute_signal(df: pd.DataFrame, min_diff: float) -> pd.Series:
    """
    Basit sinyal:
      24h > week > month VE 24h - rtp >= min_diff
    Şartlar için kolonların varlığı kontrol edilir; olmayan kolonlar False varsayılır.
    """
    has_24h = "24h" in df.columns
    has_week = "week" in df.columns
    has_month = "month" in df.columns
    has_rtp = "rtp" in df.columns

    if not (has_24h and has_week and has_month and has_rtp):
        return pd.Series([False] * len(df), index=df.index)

    cond = (df["24h"] > df["week"]) & (df["week"] > df["month"]) & ((df["24h"] - df["rtp"]) >= min_diff)
    cond = cond.fillna(False)
    return cond


# -----------------------------------------------------------------------------
# UI
# -----------------------------------------------------------------------------
st.title("🧪 Normalized Oyun Zaman Serileri + ADioG")

# Üst bar
col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 2, 1])

with col1:
    games = list_games()
    if not games:
        st.warning("`data/normalized/` içinde normalize CSV bulunamadı. Collector çalıştıktan sonra tekrar deneyin.")
        st.stop()
    game = st.selectbox("🎰 Oyun", games, index=0)

with col2:
    metric_ui = st.selectbox("📏 Metrik (solo)", DISPLAY_METRICS, index=0)
    metric_col = METRIC_MAP.get(metric_ui, metric_ui)

with col3:
    step_options = {
        "Son 200 adım": 200,
        "Son 500 adım": 500,
        "Son 1000 adım": 1000,
        "Son 2000 adım": 2000,
        "Tümü": -1,
    }
    step_label = st.selectbox("⏱ Adım penceresi (1 adım ≈ 10–15 dk)", list(step_options.keys()), index=1)
    step_n = step_options[step_label]

with col4:
    # ADioG sinyal eşiği
    min_diff = st.number_input("ADioG sinyal eşiği (24H - RTP)", min_value=0.0, max_value=50.0, value=1.5, step=0.1)

with col5:
    if st.button("🔄 Veriyi Yenile", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# Veri
df = load_game_df(game)
if df.empty:
    st.warning(f"`{game}` için normalize veri bulunamadı.")
    st.stop()

# Zorunlu kolonlar ve güvenlik
if "timestamp" not in df.columns:
    st.error("Veride `timestamp` kolonu bulunamadı.")
    st.stop()

# Solo grafik için seçilen metrik mevcut mu?
if metric_col not in df.columns:
    st.error(
        f"Seçilen metrik (‘{metric_ui}’ → ‘{metric_col}’) veri kümesinde yok.\n"
        f"Mevcut kolonlar: {list(df.columns)}"
    )
    st.stop()

# Son N adım
view_df = last_n_steps(df, step_n)

# -----------------------------------------------------------------------------
# SOLO METRİK GRAFİĞİ
# -----------------------------------------------------------------------------
st.subheader(f"📈 {game} — {metric_ui}")

solo_df = view_df[["timestamp", metric_col]].dropna().copy()
if solo_df.empty:
    st.info("Seçilen aralıkta gösterilecek veri bulunamadı.")
else:
    fig_solo = px.line(
        solo_df,
        x="timestamp",
        y=metric_col,
        title=None,
        markers=True,
    )
    fig_solo.update_layout(
        xaxis_title="timestamp",
        yaxis_title=metric_ui,
        margin=dict(l=40, r=30, t=10, b=40),
        hovermode="x unified",
    )
    # Dinamik y aralığı
    ymin = float(solo_df[metric_col].min())
    ymax = float(solo_df[metric_col].max())
    padding = max(0.5, (ymax - ymin) * 0.1)
    fig_solo.update_yaxes(range=[ymin - padding, ymax + padding])

    st.plotly_chart(fig_solo, use_container_width=True)

# -----------------------------------------------------------------------------
# ADioG GRAFİĞİ
# -----------------------------------------------------------------------------
st.subheader("🧪 ADioG — RTP gri, 24H kırmızı, Week lacivert, Month siyah")

adiog_cols = [c for c in ["timestamp", "rtp", "24h", "week", "month"] if c in view_df.columns]
adiog_df = view_df[adiog_cols].copy()

if len(adiog_df) < 2:
    st.info("ADioG için yeterli veri yok.")
else:
    # Sinyal
    sig = compute_signal(adiog_df, min_diff=min_diff)
    adiog_df["signal"] = sig

    fig = go.Figure()

    # RTP (gri)
    if "rtp" in adiog_df.columns:
        fig.add_trace(go.Scatter(
            x=adiog_df["timestamp"], y=adiog_df["rtp"],
            mode="lines", name="RTP", line=dict(color="#A0A0A0", width=2)
        ))
    # 24H (kırmızı)
    if "24h" in adiog_df.columns:
        fig.add_trace(go.Scatter(
            x=adiog_df["timestamp"], y=adiog_df["24h"],
            mode="lines+markers", name="24H", line=dict(color="#E24A33", width=2)
        ))
    # Week (lacivert)
    if "week" in adiog_df.columns:
        fig.add_trace(go.Scatter(
            x=adiog_df["timestamp"], y=adiog_df["week"],
            mode="lines+markers", name="Week", line=dict(color="#1F3A93", width=2)
        ))
    # Month (siyah)
    if "month" in adiog_df.columns:
        fig.add_trace(go.Scatter(
            x=adiog_df["timestamp"], y=adiog_df["month"],
            mode="lines+markers", name="Month", line=dict(color="#000000", width=2)
        ))

    # Giriş sinyali: yeşil noktalar
    if adiog_df["signal"].any():
        sig_points = adiog_df[adiog_df["signal"]].copy()
        # 24H varsa onun üstüne işaretleyelim, yoksa RTP üstüne
        ybase = "24h" if "24h" in sig_points.columns else ("rtp" if "rtp" in sig_points.columns else None)
        if ybase:
            fig.add_trace(go.Scatter(
                x=sig_points["timestamp"], y=sig_points[ybase],
                mode="markers", name="Giriş Sinyali",
                marker=dict(color="green", size=10, symbol="circle")
            ))

    fig.update_layout(
        xaxis_title="timestamp",
        yaxis_title="RTP / Yüzde",
        margin=dict(l=40, r=30, t=10, b=40),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    # Y ekseni ölçeği
    yvals = []
    for c in ["rtp", "24h", "week", "month"]:
        if c in adiog_df.columns:
            yvals.extend(adiog_df[c].dropna().tolist())
    if yvals:
        ymin, ymax = min(yvals), max(yvals)
        pad = max(0.5, (ymax - ymin) * 0.1)
        fig.update_yaxes(range=[ymin - pad, ymax + pad])

    st.plotly_chart(fig, use_container_width=True)

# -----------------------------------------------------------------------------
# Alt bilgi
# -----------------------------------------------------------------------------
with st.expander("ℹ️ Veri Özeti"):
    st.write(f"Toplam satır: **{len(view_df)}**")
    if "timestamp" in view_df.columns:
        st.write(f"Görünen aralık: **{view_df['timestamp'].min()} → {view_df['timestamp'].max()}**")
    st.dataframe(view_df.tail(15), use_container_width=True)
