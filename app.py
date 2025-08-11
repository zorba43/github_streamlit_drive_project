# app.py
import os, io, glob, hashlib, requests
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Normalized Time-Series Dashboard", layout="wide")

# -------------------- helper: secrets/env --------------------
def get_secret(key, default=None):
    try:
        return st.secrets[key]
    except Exception:
        return os.environ.get(key, default)

# -------------------- basit login (yalnızca kullanıcı adı) --------------------
AUTH_USER = "mirzam43"

with st.sidebar:
    st.subheader("🔐 Giriş")
    if "auth" not in st.session_state:
        st.session_state["auth"] = False
    if not st.session_state["auth"]:
        u = st.text_input("Kullanıcı adı")
        if st.button("Giriş"):
            if u == AUTH_USER:
                st.session_state["auth"] = True
            else:
                st.error("Hatalı kullanıcı adı")
    else:
        st.success(f"Giriş yapıldı ({AUTH_USER})")
        if st.button("Çıkış"):
            st.session_state["auth"] = False

if not st.session_state.get("auth", False):
    st.stop()

# -------------------- auto-refresh + manuel yenile --------------------
# 120 sn'de bir sayfayı yenile
st.components.v1.html("<meta http-equiv='refresh' content='900'>", height=0)

with st.sidebar:
    if st.button("🔄 Veriyi yenile"):
        st.cache_data.clear()
        st.rerun()

# -------------------- GitHub'tan veri okuma (önerilen) --------------------
OWNER  = get_secret("GH_OWNER",  "zorba43")
REPO   = get_secret("GH_REPO",   "github_streamlit_drive_project")
BRANCH = get_secret("GH_BRANCH", "main")
PATH   = get_secret("GH_PATH",   "data/normalized")
TOKEN  = get_secret("GITHUB_TOKEN", None)
HEADERS = {"Authorization": f"token {TOKEN}"} if TOKEN else {}

@st.cache_data(ttl=60)  # 60 sn cache
def list_csv_urls(owner, repo, path, branch):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={branch}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code == 200:
        items = r.json()
        out = {}
        for it in items:
            if it.get("type") == "file" and it["name"].lower().endswith(".csv"):
                out[it["name"]] = it.get("download_url") or it.get("url")
        return out
    else:
        # GitHub erişimi yoksa boş dönelim; yerelden deneriz
        return {}

@st.cache_data(ttl=60)
def load_csv_from_github(url):
    if url.startswith("https://api.github.com/"):
        # Private repo: içerik base64
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
        import base64
        content = base64.b64decode(data["content"])
        return pd.read_csv(io.BytesIO(content), parse_dates=["timestamp"])
    else:
        # Public repo: direkt indir
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return pd.read_csv(io.BytesIO(r.content), parse_dates=["timestamp"])

# Önce GitHub'tan dene
csv_urls = list_csv_urls(OWNER, REPO, PATH, BRANCH)

catalog = {}
data_source = ""
if csv_urls:
    for fname, url in sorted(csv_urls.items()):
        try:
            df_tmp = load_csv_from_github(url)
            game_name = str(df_tmp.iloc[0]["game"]) if "game" in df_tmp.columns and not df_tmp.empty else fname.rsplit(".",1)[0]
            catalog[game_name] = ("github", url)
        except Exception as e:
            st.error(f"{fname} (GitHub) okunamadı: {e}")
    data_source = "GitHub"
else:
    # GitHub başarısızsa yerelden oku
    LOCAL_DIR = "data/normalized"
    files = sorted(glob.glob(str(Path(LOCAL_DIR) / "*.csv")))
    for f in files:
        try:
            df_tmp = pd.read_csv(f, parse_dates=["timestamp"])
            game_name = str(df_tmp.iloc[0]["game"]) if "game" in df_tmp.columns and not df_tmp.empty else Path(f).stem
            catalog[game_name] = ("local", f)
        except Exception as e:
            st.error(f"{os.path.basename(f)} (local) okunamadı: {e}")
    data_source = "Local"

if not catalog:
    st.warning("CSV bulunamadı (GitHub/Local).")
    st.stop()

st.caption(f"Veri kaynağı: **{data_source}**")

# -------------------- UI --------------------
st.title("📈 Normalized Oyun Zaman Serileri")

games = sorted(catalog.keys())
colA, colB, colC = st.columns([2,2,2])
with colA:
    game = st.selectbox("Oyun", games)
with colB:
    metric = st.selectbox("Metrik", ["24H", "Week", "Month", "RTP"], index=0)
with colC:
    resample = st.selectbox("Zaman aralığı (yeniden örnekleme)", ["(yok)", "15T", "30T", "1H", "4H", "1D"], index=0)

# Veriyi yükle
src, ref = catalog[game]
try:
    if src == "github":
        gdf = load_csv_from_github(ref)
    else:
        gdf = pd.read_csv(ref, parse_dates=["timestamp"])
except Exception as e:
    st.error(f"Dosya okunamadı: {e}")
    st.stop()

# Hazırla
gdf = gdf.dropna(subset=["timestamp"]).sort_values("timestamp")
min_ts = gdf["timestamp"].min()
max_ts = gdf["timestamp"].max()

c1, c2 = st.columns(2)
with c1:
    start_date = st.date_input("Başlangıç", value=(min_ts.date() if pd.notna(min_ts) else datetime.utcnow().date()))
with c2:
    end_date = st.date_input("Bitiş", value=(max_ts.date() if pd.notna(max_ts) else datetime.utcnow().date()))

mask = (gdf["timestamp"] >= pd.Timestamp(start_date)) & (gdf["timestamp"] <= pd.Timestamp(end_date) + pd.Timedelta(days=1))
plot_df = gdf.loc[mask, ["timestamp", metric]].copy()
if resample != "(yok)" and not plot_df.empty:
    plot_df = plot_df.set_index("timestamp").resample(resample).mean().reset_index()

# KPI + Grafik
k1, k2, k3 = st.columns(3)
with k1: st.metric("Kayıt", len(plot_df))
with k2: st.metric("İlk", plot_df["timestamp"].min().strftime("%Y-%m-%d %H:%M") if not plot_df.empty else "—")
with k3: st.metric("Son", plot_df["timestamp"].max().strftime("%Y-%m-%d %H:%M") if not plot_df.empty else "—")

st.subheader(f"🎯 {game} — {metric}")
if plot_df.empty or plot_df[metric].dropna().empty:
    st.info("Seçili filtre/metric için veri yok.")
else:
    fig = px.line(plot_df, x="timestamp", y=metric, markers=True)
    st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("🧾 Veri")
st.dataframe(plot_df, use_container_width=True, hide_index=True)
