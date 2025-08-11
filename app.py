# app.py
import os, glob, hashlib
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Normalized Time-Series Dashboard", layout="wide")

# -------------------- Secrets + ENV helpers --------------------
def get_secret(key, default=None):
    try:
        return st.secrets[key]
    except Exception:
        return os.environ.get(key, default)

AUTH_USER = get_secret("AUTH_USERNAME", "admin")
AUTH_HASH = get_secret("AUTH_PASSWORD_SHA256", hashlib.sha256(b"admin123").hexdigest())
NORMALIZED_DIR = get_secret("NORMALIZED_DIR", "data/normalized")

# -------------------- Auth --------------------
def check_password(username, password):
    return (username == AUTH_USER) and (hashlib.sha256(password.encode()).hexdigest() == AUTH_HASH)

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

if not st.session_state.get("auth", False):
    st.stop()

# -------------------- UI --------------------
st.title("📈 Normalized Oyun Zaman Serileri")
# 15 dk auto-refresh (900 sn)
st.components.v1.html("<meta http-equiv='refresh' content='900'>", height=0)

files = sorted(glob.glob(str(Path(NORMALIZED_DIR) / "*.csv")))
if not files:
    st.warning(f"'{NORMALIZED_DIR}' içinde CSV bulunamadı.")
    st.stop()

# Oyun listesi (CSV içindeki 'game' alanından)
catalog = {}
for f in files:
    try:
        df = pd.read_csv(f, parse_dates=["timestamp"])
        if not df.empty and "game" in df.columns:
            catalog[str(df.iloc[0]["game"])] = f
        else:
            catalog[Path(f).stem] = f
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
    metric = st.selectbox("Metrik", ["24H", "Week", "Month", "RTP"], index=0)
with colC:
    resample = st.selectbox("Zaman aralığı (yeniden örnekleme)", ["(yok)", "15T", "30T", "1H", "4H", "1D"], index=0)

# Seçilen oyunun verisini yükle
path = catalog[game]
try:
    gdf = pd.read_csv(path, parse_dates=["timestamp"])
except Exception as e:
    st.error(f"Dosya okunamadı: {e}")
    st.stop()

gdf = gdf.dropna(subset=["timestamp"]).sort_values("timestamp")

# Tarih filtresi
min_ts = gdf["timestamp"].min()
max_ts = gdf["timestamp"].max()
c1, c2 = st.columns(2)
with c1:
    start_date = st.date_input("Başlangıç", value=(min_ts.date() if pd.notna(min_ts) else datetime.utcnow().date()))
with c2:
    end_date = st.date_input("Bitiş", value=(max_ts.date() if pd.notna(max_ts) else datetime.utcnow().date()))

mask = (gdf["timestamp"] >= pd.Timestamp(start_date)) & (gdf["timestamp"] <= pd.Timestamp(end_date) + pd.Timedelta(days=1))
plot_df = gdf.loc[mask, ["timestamp", metric]].copy()

# Resample (varsa)
if resample != "(yok)" and not plot_df.empty:
    plot_df = plot_df.set_index("timestamp").resample(resample).mean().reset_index()

# KPI'lar
k1, k2, k3 = st.columns(3)
with k1:
    st.metric("Kayıt", len(plot_df))
with k2:
    st.metric("İlk", plot_df["timestamp"].min().strftime("%Y-%m-%d %H:%M") if not plot_df.empty else "—")
with k3:
    st.metric("Son", plot_df["timestamp"].max().strftime("%Y-%m-%d %H:%M") if not plot_df.empty else "—")

# Grafik
st.subheader(f"🎯 {game} — {metric}")
if plot_df.empty or plot_df[metric].dropna().empty:
    st.info("Seçili filtre/metric için veri yok.")
else:
    fig = px.line(plot_df, x="timestamp", y=metric, markers=True)
    st.plotly_chart(fig, use_container_width=True)

    # PNG indirme (Plotly + kaleido)
    try:
        import plotly.io as pio
        img_bytes = pio.to_image(fig, format="png", width=1280, height=720, scale=2)
        st.download_button("Grafiği PNG indir", data=img_bytes, file_name=f"{game}_{metric}.png", mime="image/png")
    except Exception:
        st.caption("Not: PNG export için 'kaleido' paketi gerekir.")

st.divider()
st.subheader("🧾 Veri")
st.dataframe(plot_df, use_container_width=True, hide_index=True)

st.sidebar.markdown("---")
st.sidebar.caption("Secrets: AUTH_USERNAME / AUTH_PASSWORD_SHA256 / NORMALIZED_DIR")
