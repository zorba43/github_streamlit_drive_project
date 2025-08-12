# app.py
import os
from pathlib import Path
from typing import Dict, List

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ------------------------
# Genel ayarlar
# ------------------------
st.set_page_config(page_title="Normalized Oyun Zaman Serileri (Lite)", layout="wide")

NORM_DIR = Path("data/normalized")
NORM_DIR.mkdir(parents=True, exist_ok=True)

# UI'da gösterilen metrik -> normalized sütun adı
METRIC_MAP = {
    "24H": "24h", "24h": "24h",
    "Week": "week", "week": "week",
    "Month": "month", "month": "month",
    "RTP": "rtp", "rtp": "rtp",
}

DISPLAY_METRICS = ["24H", "Week", "Month", "RTP"]  # UI seçenekleri

# ------------------------
# Veri okuma / önbellek
# ------------------------
@st.cache_data(ttl=300, show_spinner=False)
def list_games() -> List[str]:
    return sorted([p.stem for p in NORM_DIR.glob("*.csv")])

@st.cache_data(ttl=300, show_spinner=True)
def load_game_df(game_name: str) -> pd.DataFrame:
    """
    data/normalized/<game_name>.csv -> DataFrame
    Beklenen sütunlar: timestamp, game, 24h, week, month, rtp
    """
    csv_path = NORM_DIR / f"{game_name}.csv"
    if not csv_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(csv_path)
    # timestamp → datetime (UTC gibi düşün, tz-aware olması gerekmiyor)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    # kolon adlarını normalize et (metric eşleşmeleri için)
    df = df.rename(columns={c: (c.lower() if c != "timestamp" else c) for c in df.columns})
    # zaman sırası
    if "timestamp" in df.columns:
        df = df.sort_values("timestamp").reset_index(drop=True)
    return df

def last_n_steps(df: pd.DataFrame, n: int) -> pd.DataFrame:
    if n <= 0 or n >= len(df):
        return df
    return df.tail(n).copy()

# ------------------------
# UI
# ------------------------
st.title("🧪 Normalized Oyun Zaman Serileri (Lite)")

col_top1, col_top2, col_top3, col_top4 = st.columns([3, 2, 2, 1])

with col_top1:
    games = list_games()
    if not games:
        st.warning("`data/normalized/` içinde normalize CSV bulunamadı. Collector çalıştıktan sonra tekrar deneyin.")
        st.stop()
    game = st.selectbox("🎰 Oyun", games, index=0)

with col_top2:
    metric_ui = st.selectbox("📏 Metrik", DISPLAY_METRICS, index=0)
    metric_col = METRIC_MAP.get(metric_ui, metric_ui)

with col_top3:
    step_options = {
        "Son 200 adım": 200,
        "Son 500 adım": 500,
        "Son 1000 adım": 1000,
        "Son 2000 adım": 2000,
        "Tümü": -1,
    }
    step_label = st.selectbox("⏱ Adım penceresi (1 adım ≈ 10–15 dk)", list(step_options.keys()), index=1)
    step_n = step_options[step_label]

with col_top4:
    if st.button("🔄 Veriyi Yenile", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ------------------------
# Veri ve güvenlik kontrolleri
# ------------------------
df = load_game_df(game)
if df.empty:
    st.warning(f"`{game}` için normalize veri bulunamadı.")
    st.stop()

# normalized kolonlar küçük harfli olsun (timestamp hariç)
df = df.rename(columns={c: (c.lower() if c != "timestamp" else c) for c in df.columns})

# Kolon kontrolü
if "timestamp" not in df.columns:
    st.error("Veride `timestamp` kolonu yok. Collector çıktısını kontrol edin.")
    st.stop()

if metric_col not in df.columns:
    st.error(
        f"Seçtiğiniz metrik (‘{metric_ui}’ → ‘{metric_col}’) veri kümesinde yok.\n"
        f"Mevcut kolonlar: {list(df.columns)}"
    )
    st.stop()

# Son N adım filtresi
view_df = last_n_steps(df[["timestamp", metric_col]].dropna(), step_n)

if view_df.empty:
    st.info("Seçilen aralıkta gösterilecek veri bulunamadı.")
    st.stop()

# ------------------------
# Grafik
# ------------------------
title = f"{game} — {metric_ui}"
y_label = metric_ui  # isterseniz '% ' ekleyebilirsiniz

fig = px.line(
    view_df,
    x="timestamp",
    y=metric_col,
    title=title,
    markers=True,
)
fig.update_layout(
    xaxis_title="timestamp",
    yaxis_title=y_label,
    margin=dict(l=40, r=30, t=60, b=40),
    hovermode="x unified",
)
# Y eksenini verinin min-max'ına yakın tut (tam ekran yayılmasın)
ymin = float(view_df[metric_col].min())
ymax = float(view_df[metric_col].max())
padding = max(0.5, (ymax - ymin) * 0.1)
fig.update_yaxes(range=[ymin - padding, ymax + padding])

st.plotly_chart(fig, use_container_width=True)

# ------------------------
# Alt bilgi
# ------------------------
with st.expander("ℹ️ Veri Özeti"):
    st.write(f"Satır sayısı: **{len(view_df)}**  |  Görünen aralık: **{view_df['timestamp'].min()} → {view_df['timestamp'].max()}**")
    st.dataframe(view_df.tail(15), use_container_width=True)
