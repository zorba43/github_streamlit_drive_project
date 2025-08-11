
import os, glob, io
from pathlib import Path
from datetime import datetime
import hashlib

import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Normalized Time-Series Dashboard", layout="wide")

# ---------- Auth ----------
DEFAULT_USER = os.environ.get("AUTH_USERNAME", "admin")
DEFAULT_HASH = os.environ.get("AUTH_PASSWORD_SHA256", hashlib.sha256("admin123".encode()).hexdigest())

def check_password(username, password):
    return username == DEFAULT_USER and hashlib.sha256(password.encode()).hexdigest() == DEFAULT_HASH

with st.sidebar:
    st.subheader("🔐 Giriş")
    if "auth" not in st.session_state:
        st.session_state["auth"] = False
    if not st.session_state["auth"]:
        u = st.text_input("Kullanıcı adı")
        p = st.text_input("Şifre", type="password")
        if st.button("Giriş"):
            if check_password(u, p):
                st.session_state["auth"] = True
            else:
                st.error("Hatalı bilgiler")
    else:
        st.success("Giriş yapıldı")
        if st.button("Çıkış"):
            st.session_state["auth"] = False

if not st.session_state["auth"]:
    st.stop()

st.title("📈 Normalized Oyun Zaman Serileri")

# Auto-refresh (15 dk)
st.components.v1.html("<meta http-equiv='refresh' content='900'>", height=0)

NORMALIZED_DIR = os.environ.get("NORMALIZED_DIR", "data/normalized")
files = sorted(glob.glob(str(Path(NORMALIZED_DIR) / "*.csv")))

if not files:
    st.warning(f"'{NORMALIZED_DIR}' içinde CSV bulunamadı.")
    st.stop()

# Build game list from CSV contents
catalog = {}
for f in files:
    try:
        df = pd.read_csv(f, parse_dates=["timestamp"])
        if "game" in df.columns and not df.empty:
            game_name = str(df.iloc[0]["game"])
        else:
            game_name = Path(f).stem.replace("_"," ").title()
        catalog[game_name] = f
    except Exception as e:
        st.error(f"{os.path.basename(f)} okunamadı: {e}")

if not catalog:
    st.warning("Geçerli CSV bulunamadı.")
    st.stop()

games = sorted(catalog.keys())

colA, colB, colC = st.columns([2,2,2])
with colA:
    game = st.selectbox("Oyun", games)
with colB:
    metric = st.selectbox("Metrik", ["24H","Week","Month","RTP"], index=0)
with colC:
    resample = st.selectbox("Zaman aralığı", ["(yok)","15T","30T","1H","4H","1D"], index=0)

# Load selected game's CSV
path = catalog[game]
try:
    gdf = pd.read_csv(path, parse_dates=["timestamp"])
except Exception as e:
    st.error(f"Dosya okunamadı: {e}")
    st.stop()

# Clean and sort
gdf = gdf.dropna(subset=["timestamp"]).sort_values("timestamp")

# Date filters
min_ts = gdf["timestamp"].min()
max_ts = gdf["timestamp"].max()
c1, c2 = st.columns(2)
with c1:
    start_date = st.date_input("Başlangıç", value=min_ts.date() if pd.notna(min_ts) else datetime.utcnow().date())
with c2:
    end_date = st.date_input("Bitiş", value=max_ts.date() if pd.notna(max_ts) else datetime.utcnow().date())

mask = (gdf["timestamp"] >= pd.Timestamp(start_date)) & (gdf["timestamp"] <= pd.Timestamp(end_date) + pd.Timedelta(days=1))
plot_df = gdf.loc[mask, ["timestamp", metric]].copy()

# Resample (if selected and numeric)
if resample != "(yok)" and not plot_df.empty:
    plot_df = plot_df.set_index("timestamp").resample(resample).mean().reset_index()

# KPI & chart
kcol1, kcol2, kcol3 = st.columns([2,2,2])
with kcol1:
    st.metric("Kayıt", len(plot_df))
with kcol2:
    st.metric("İlk", plot_df["timestamp"].min().strftime("%Y-%m-%d %H:%M") if not plot_df.empty else "—")
with kcol3:
    st.metric("Son", plot_df["timestamp"].max().strftime("%Y-%m-%d %H:%M") if not plot_df.empty else "—")

st.subheader(f"🎯 {game} — {metric}")
if plot_df.empty or plot_df[metric].dropna().empty:
    st.info("Seçili filtre/metric için veri yok.")
else:
    fig = px.line(plot_df, x="timestamp", y=metric, markers=True)
    st.plotly_chart(fig, use_container_width=True)

    # Export PNG
    if st.button("Grafiği PNG indir"):
        import plotly.io as pio
        img_bytes = pio.to_image(fig, format="png", width=1280, height=720, scale=2)
        st.download_button("PNG indir", data=img_bytes, file_name=f"{game}_{metric}.png", mime="image/png")

st.divider()
st.subheader("🧾 Veri")
st.dataframe(plot_df, use_container_width=True, hide_index=True)

st.sidebar.markdown("---")
st.sidebar.caption("AUTH_USERNAME / AUTH_PASSWORD_SHA256 / NORMALIZED_DIR ortam değişkenleri desteklenir.")
