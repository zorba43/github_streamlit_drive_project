# app.py
import os, io, glob, base64, requests
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Normalized Time-Series Dashboard", layout="wide")

# -------------------- helpers --------------------
def get_secret(key, default=None):
    try:
        return st.secrets[key]
    except Exception:
        return os.environ.get(key, default)

def decide_signal_row(row, slope_val, min_gap):
    """Tek satır için GİR sinyali koşulu."""
    r24 = row.get("24H"); r7 = row.get("Week"); r30 = row.get("Month"); rtp = row.get("RTP")
    if any(pd.isna([r24, r7, r30, rtp])): 
        return False
    up_ok   = (r24 - max(r7, r30)) >= min_gap and (r24 > rtp)
    slope_ok = True if slope_val is None else (slope_val > 0)
    return bool(up_ok and slope_ok)

def compute_slope_series(df, window_points):
    """Basit eğim: (24H_now - 24H_prev) / dakika (prev = window_points-1 önceki ölçüm)."""
    if window_points is None or window_points <= 1 or "24H" not in df.columns:
        return pd.Series([None]*len(df), index=df.index)
    prev = df["24H"].shift(window_points-1)
    dtm  = (df["timestamp"] - df["timestamp"].shift(window_points-1)).dt.total_seconds()/60.0
    slope = (df["24H"] - prev) / dtm
    return slope

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

@st.cache_data(ttl=60)
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
catalog, data_source = {}, ""

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

# Sinyal ayarları
with st.sidebar:
    st.markdown("### 🎛️ Sinyal Ayarları")
    min_gap = st.number_input("Minimum fark (puan)", 0.0, 5.0, 0.3, 0.1)
    slope_window_opt = st.selectbox("24H eğim penceresi", ["Yok", 3, 5, 9], index=2)

# Veriyi yükle
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

# -------------------- Anlık sinyal --------------------
base_sorted = gdf.sort_values("timestamp")
last_row = base_sorted.tail(1).iloc[0]

# anlık slope
slope_window = None if slope_window_opt == "Yok" else int(slope_window_opt)
slope_24h_now = None
if slope_window is not None:
    tmp = base_sorted[["timestamp","24H"]].dropna().tail(slope_window)
    if len(tmp) >= 2:
        dt_min = (tmp["timestamp"].iloc[-1] - tmp["timestamp"].iloc[0]).total_seconds()/60.0
        if dt_min > 0:
            slope_24h_now = (tmp["24H"].iloc[-1] - tmp["24H"].iloc[0]) / dt_min

def decide_signal_now(df_row, slope_24h=None, min_gap=0.3):
    r24, r7, r30 = df_row.get("24H"), df_row.get("Week"), df_row.get("Month")
    if any(pd.isna([r24, r7, r30])): return "UNKNOWN", "Veri eksik"
    up_ok   = (r24 - max(r7, r30)) >= min_gap
    down_ok = (min(r7, r30) - r24) >= min_gap
    if up_ok and (slope_24h is None or slope_24h > 0):
        return "BUY", f"24H ({r24:.2f}) > Week ({r7:.2f}) & Month ({r30:.2f})"
    if down_ok and (slope_24h is None or slope_24h <= 0):
        return "SELL", f"24H ({r24:.2f}) < Week ({r7:.2f}) & Month ({r30:.2f})"
    return "HOLD", f"24H ({r24:.2f}) ~ Week ({r7:.2f})/Month ({r30:.2f})"

signal_now, reason_now = decide_signal_now(last_row, slope_24h=slope_24h_now, min_gap=min_gap)

st.subheader("📡 Anlık Sinyal")
if signal_now == "BUY":
    st.success(f"✅ GİR — {reason_now}" + (f" | Eğim: {slope_24h_now:+.3f}/dk" if slope_24h_now is not None else ""))
elif signal_now == "SELL":
    st.error(f"❌ ÇIK — {reason_now}" + (f" | Eğim: {slope_24h_now:+.3f}/dk" if slope_24h_now is not None else ""))
elif signal_now == "HOLD":
    st.warning(f"⏳ BEKLE — {reason_now}" + (f" | Eğim: {slope_24h_now:+.3f}/dk" if slope_24h_now is not None else ""))
else:
    st.info("Veri yetersiz")

# -------------------- Tek metrik grafik --------------------
st.subheader(f"🎯 {game} — {metric}")
if plot_df.empty or plot_df[metric].dropna().empty:
    st.info("Seçili filtre/metric için veri yok.")
else:
    fig = px.line(plot_df, x="timestamp", y=metric, markers=True)
    # sinyal anını shape ile işaretle (add_vline yerine)
    last_ts = base_sorted["timestamp"].max()
    if pd.notna(last_ts):
        x_val = last_ts.to_pydatetime() if isinstance(last_ts, pd.Timestamp) else last_ts
        fig.add_shape(type="line", x0=x_val, x1=x_val, y0=0, y1=1,
                      xref="x", yref="paper", line=dict(dash="dot", width=1.5))
        fig.add_annotation(x=x_val, y=1, xref="x", yref="paper",
                           text="Sinyal anı", showarrow=False, yshift=10)
    st.plotly_chart(fig, use_container_width=True)

# -------------------- ADioG: Tüm metrikler + geçmiş sinyal işaretleri --------------------
st.subheader(f"🧪 ADioG — {game} (RTP gri, 24H kırmızı, Week lacivert, Month siyah)")

adio_df = gdf.loc[mask, ["timestamp","RTP","24H","Week","Month"]].dropna().copy()
if resample != "(yok)" and not adio_df.empty:
    adio_df = (adio_df.set_index("timestamp")
                      .resample(resample)
                      .mean()
                      .reset_index())

if adio_df.empty:
    st.info("ADioG için seçili tarih aralığında veri yok.")
else:
    # geçmiş eğim serisi (opsiyonel)
    slope_series = compute_slope_series(adio_df, None if slope_window_opt == "Yok" else int(slope_window_opt))

    # kurala göre giriş noktaları
    entries = []
    for i, row in adio_df.iterrows():
        slope_val = None if pd.isna(slope_series.iloc[i]) else slope_series.iloc[i]
        entries.append(decide_signal_row(row, slope_val, min_gap))
    adio_df["ENTRY"] = entries

    # grafik
    fig_all = go.Figure()
    fig_all.add_trace(go.Scatter(x=adio_df["timestamp"], y=adio_df["RTP"],
                                 mode="lines", name="RTP", line=dict(color="#8c8c8c", width=1.5)))
    fig_all.add_trace(go.Scatter(x=adio_df["timestamp"], y=adio_df["24H"],
                                 mode="lines", name="24H", line=dict(color="#d62728", width=2)))
    fig_all.add_trace(go.Scatter(x=adio_df["timestamp"], y=adio_df["Week"],
                                 mode="lines", name="Week", line=dict(color="#1f77b4", width=2)))
    fig_all.add_trace(go.Scatter(x=adio_df["timestamp"], y=adio_df["Month"],
                                 mode="lines", name="Month", line=dict(color="#000000", width=2)))

    sig_pts = adio_df.loc[adio_df["ENTRY"]]
    if not sig_pts.empty:
        fig_all.add_trace(go.Scatter(
            x=sig_pts["timestamp"], y=sig_pts["24H"],
            mode="markers", name="Giriş Sinyali",
            marker=dict(color="#2ca02c", size=9, symbol="circle"),
            hovertemplate="Giriş: %{x|%Y-%m-%d %H:%M}<br>24H=%{y:.2f}<extra></extra>"
        ))

    fig_all.update_layout(
        margin=dict(l=10, r=10, t=30, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis_title="timestamp", yaxis_title="RTP / Yüzde"
    )
    st.plotly_chart(fig_all, use_container_width=True)

    # özet
    total_signals = int(adio_df["ENTRY"].sum())
    last_signal_ts = sig_pts["timestamp"].iloc[-1] if total_signals > 0 else None
    cA, cB = st.columns(2)
    with cA:
        st.metric("Toplam Giriş Sinyali", total_signals)
    with cB:
        st.metric("Son Giriş Sinyali", "-" if last_signal_ts is None else last_signal_ts.strftime("%Y-%m-%d %H:%M"))

st.divider()
st.subheader("🧾 Veri")
st.dataframe(plot_df, use_container_width=True, hide_index=True)
