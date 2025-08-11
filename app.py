# app.py
import os, io, glob, requests
from pathlib import Path
from datetime import datetime
import base64

import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Normalized Time-Series Dashboard", layout="wide")

# -------------------- helpers --------------------
def get_secret(key, default=None):
    try:
        return st.secrets[key]
    except Exception:
        return os.environ.get(key, default)

# --- Decision helper (24H vs Week/Month + opsiyonel eşik ve eğim) ---
def decide_signal(df_row, slope_24h=None, min_gap=0.3):
    """
    df_row: tek satır (son kayıt) -> sütunlar: '24H','Week','Month'
    slope_24h: son N noktadan 24H eğimi (dakikada değişim) veya None
    min_gap: 24H'nin Week/Month'tan en az bu kadar yüksek/düşük olması (yüzdelik puan)
    """
    r24, r7, r30 = df_row.get("24H"), df_row.get("Week"), df_row.get("Month")
    if any(pd.isna([r24, r7, r30])):
        return "UNKNOWN", "Veri eksik"

    up_ok   = (r24 - max(r7, r30)) >= min_gap
    down_ok = (min(r7, r30) - r24) >= min_gap

    if up_ok and (slope_24h is None or slope_24h > 0):
        return "BUY", f"24H ({r24:.2f}) > Week ({r7:.2f}) & Month ({r30:.2f})"
    if down_ok and (slope_24h is None or slope_24h <= 0):
        return "SELL", f"24H ({r24:.2f}) < Week ({r7:.2f}) & Month ({r30:.2f})"
    return "HOLD", f"24H ({r24:.2f}) ~ Week ({r7:.2f})/Month ({r30:.2f})"

# -------------------- basit login --------------------
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
st.components.v1.html("<meta http-equiv='refresh' content='120'>", height=0)
with st.sidebar:
    if st.button("🔄 Veriyi yenile"):
        st.cache_data.clear()
        st.rerun()

# -------------------- GitHub'tan veri okuma --------------------
OWNER  = get_secret("GH_OWNER",  "zorba43")
REPO   = get_secret("GH_REPO",   "github_streamlit_drive_project")
BRANCH = get_secret("GH_BRANCH", "main")
PATH   = get_secret("GH_PATH",   "data/normalized")
TOKEN  = get_secret("GITHUB_TOKEN", None)
HEADERS = {"Authorization": f"token {TOKEN}"} if TOKEN else {}

@st.cache_data(ttl=60)
def list_csv_urls(owner, repo, path, branch, headers):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={branch}"
    r = requests.get(url, headers=headers, timeout=20)
    if r.status_code != 200:
        return {}
    items = r.json()
    out = {}
    for it in items:
        if it.get("type") == "file" and it["name"].lower().endswith(".csv"):
            out[it["name"]] = it.get("download_url") or it.get("url")
    return out

@st.cache_data(ttl=900)
def load_csv_from_github(url, headers):
    if url.startswith("https://api.github.com/"):
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
        content = base64.b64decode(data["content"])
        return pd.read_csv(io.BytesIO(content), parse_dates=["timestamp"])
    else:
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        return pd.read_csv(io.BytesIO(r.content), parse_dates=["timestamp"])

csv_urls = list_csv_urls(OWNER, REPO, PATH, BRANCH, HEADERS)

catalog = {}
data_source = ""
if csv_urls:
    for fname, url in sorted(csv_urls.items()):
        try:
            df_tmp = load_csv_from_github(url, HEADERS)
            game_name = str(df_tmp.iloc[0]["game"]) if "game" in df_tmp.columns and not df_tmp.empty else fname.rsplit(".",1)[0]
            catalog[game_name] = ("github", url)
        except Exception as e:
            st.error(f"{fname} (GitHub) okunamadı: {e}")
    data_source = "GitHub"
else:
    # fallback: local
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

with st.sidebar:
    st.markdown("### 🎛️ Sinyal Ayarları")
    min_gap = st.number_input("Minimum fark (puan)", 0.0, 5.0, 0.3, 0.1)
    slope_window = st.selectbox("24H eğim penceresi", ["Yok", 3, 5, 9], index=2)

# Veri yükle
src, ref = catalog[game]
try:
    if src == "github":
        gdf = load_csv_from_github(ref, HEADERS)
    else:
        gdf = pd.read_csv(ref, parse_dates=["timestamp"])
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
if resample != "(yok)" and not plot_df.empty:
    plot_df = plot_df.set_index("timestamp").resample(resample).mean().reset_index()

# -------------------- Sinyal hesapla --------------------
base_sorted = gdf.sort_values("timestamp")
last_row = base_sorted.tail(1).iloc[0]

slope_24h = None
if slope_window != "Yok" and "24H" in base_sorted.columns:
    win = int(slope_window)
    tmp = base_sorted[["timestamp","24H"]].dropna().tail(win)
    if len(tmp) >= 2:
        dt_min = (tmp["timestamp"].iloc[-1] - tmp["timestamp"].iloc[0]).total_seconds() / 60.0
        if dt_min > 0:
            slope_24h = (tmp["24H"].iloc[-1] - tmp["24H"].iloc[0]) / dt_min

signal, reason = decide_signal(last_row, slope_24h=slope_24h, min_gap=min_gap)

st.subheader("📡 Anlık Sinyal")
if signal == "BUY":
    st.success(f"✅ GİR — {reason}" + (f" | Eğim: {slope_24h:+.3f}/dk" if slope_24h is not None else ""))
elif signal == "SELL":
    st.error(f"❌ ÇIK — {reason}" + (f" | Eğim: {slope_24h:+.3f}/dk" if slope_24h is not None else ""))
elif signal == "HOLD":
    st.warning(f"⏳ BEKLE — {reason}" + (f" | Eğim: {slope_24h:+.3f}/dk" if slope_24h is not None else ""))
else:
    st.info("Veri yetersiz")

# -------------------- Grafik --------------------
st.subheader(f"🎯 {game} — {metric}")
if plot_df.empty or plot_df[metric].dropna().empty:
    st.info("Seçili filtre/metric için veri yok.")
else:
    fig = px.line(plot_df, x="timestamp", y=metric, markers=True)
    # sinyal anını (en güncel timestamp) işaretle
    last_ts = base_sorted["timestamp"].max()
    if pd.notna(last_ts):
        fig.add_vline(x=last_ts, line_dash="dot")
    st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("🧾 Veri")
st.dataframe(plot_df, use_container_width=True, hide_index=True)
