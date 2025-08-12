# app.py  (LITE – tarih filtresi yok + timestamp normalizasyonu + auto-zoom)
import os, io, base64, time, requests
from pathlib import Path

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Normalized Time-Series (Lite)", layout="wide")

# -------------------- utils --------------------
def get_secret(key, default=None):
    try:
        return st.secrets[key]
    except Exception:
        return os.environ.get(key, default)

def compute_slope_series(df, window_points):
    if window_points is None or window_points <= 1 or "24H" not in df.columns:
        return pd.Series([None]*len(df), index=df.index)
    prev = df["24H"].shift(window_points - 1)
    dtm = (df["timestamp"] - df["timestamp"].shift(window_points - 1)).dt.total_seconds() / 60.0
    return (df["24H"] - prev) / dtm

def decide_signal_row(row, slope_val, min_gap, use_slope=False, require_rtp=False):
    r24 = row.get("24H"); r7 = row.get("Week"); r30 = row.get("Month"); rtp = row.get("RTP")
    if any(pd.isna([r24, r7, r30])):
        return False
    up_ok = (r24 - max(r7, r30)) >= min_gap
    if require_rtp and not pd.isna(rtp):
        up_ok = up_ok and (r24 > rtp)
    if use_slope:
        return bool(up_ok and (slope_val is not None and slope_val > 0))
    return bool(up_ok)

def nice_game_name(filename):
    name = Path(filename).stem
    return name.replace("-", " ").replace("_", " ").title()

def auto_zoom_last_days(fig, last_ts, days=7):
    if pd.notna(last_ts):
        x_start = last_ts - pd.Timedelta(days=days)
        x_end = last_ts + pd.Timedelta(hours=1)
        fig.update_xaxes(range=[x_start, x_end])

# -------------------- auth (yalın) --------------------
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

# -------------------- refresh / cache-buster --------------------
st.components.v1.html("<meta http-equiv='refresh' content='180'>", height=0)
with st.sidebar:
    if "refresh_token" not in st.session_state:
        st.session_state["refresh_token"] = 0
    if st.button("🔄 Veriyi yenile"):
        st.session_state["refresh_token"] += 1
        st.cache_data.clear()
        st.rerun()
refresh_token = st.session_state.get("refresh_token", 0)

# -------------------- GitHub konfig --------------------
OWNER  = get_secret("GH_OWNER",  "zorba43")
REPO   = get_secret("GH_REPO",   "github_streamlit_drive_project")
BRANCH = get_secret("GH_BRANCH", "main")
PATH   = get_secret("GH_PATH",   "data/normalized")
TOKEN  = get_secret("GITHUB_TOKEN", None)
HEADERS = {"Authorization": f"token {TOKEN}"} if TOKEN else {}

# ---- sadece liste çek (hafif) ----
@st.cache_data(ttl=60)
def list_csv_api_urls(owner, repo, path, branch, headers, _ref=0):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={branch}"
    r = requests.get(url, headers=headers, timeout=20)
    if r.status_code != 200:
        return {}
    out = {}
    for it in r.json():
        if it.get("type") == "file" and it["name"].lower().endswith(".csv"):
            out[it["name"]] = it["url"]  # API content URL (raw değil)
    return out

# ---- seçilince tek dosyayı indir ----
@st.cache_data(ttl=60)
def load_csv_from_api_content(url, headers, _ref=0):
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    j = r.json()
    content = base64.b64decode(j["content"])
    return pd.read_csv(io.BytesIO(content), parse_dates=["timestamp"])

# ---- fallback local ----
def list_local_csvs(local_dir="data/normalized"):
    out = {}
    p = Path(local_dir)
    if p.exists():
        for f in p.glob("*.csv"):
            out[f.name] = str(f)
    return out

# -------------------- listeleme (yalnızca isim) --------------------
api_map = list_csv_api_urls(OWNER, REPO, PATH, BRANCH, HEADERS, refresh_token)
source = "GitHub" if api_map else "Local"
files_map = api_map if api_map else list_local_csvs()
if not files_map:
    st.error("CSV bulunamadı.")
    st.stop()

st.title("📈 Normalized Oyun Zaman Serileri (Lite)")
st.caption(f"Veri kaynağı: **{source}** — Yalnızca seçilen oyun dosyası yüklenir. (Tarih filtresi yok, tüm veri)")

options = {nice_game_name(k): k for k in sorted(files_map.keys())}
game_label = st.selectbox("Oyun", list(options.keys()))
selected_file = options[game_label]
selected_url_or_path = files_map[selected_file]

# Metrik ve resampling
c1, c2 = st.columns([2, 2])
with c1:
    metric = st.selectbox("Metrik", ["24H", "Week", "Month", "RTP"], index=0)
with c2:
    resample = st.selectbox("Zaman aralığı (yeniden örnekleme)", ["(yok)", "15T", "30T", "1H", "4H", "1D"], index=0)

# Sinyal ayarları
with st.sidebar:
    st.markdown("### 🎛️ Sinyal Ayarları")
    min_gap = st.number_input("Minimum fark (puan)", 0.0, 5.0, 0.20, 0.05)
    slope_window_opt = st.selectbox("24H eğim penceresi", ["Yok", 3, 5, 9], index=0)
    use_slope = st.checkbox("Eğim şartı (24H artıyor olsun)", value=False)
    require_rtp = st.checkbox("24H > RTP şartı", value=False)

# -------------------- TEK CSV İNDİR/OKU --------------------
with st.spinner("Veri indiriliyor..."):
    if source == "GitHub":
        gdf = load_csv_from_api_content(selected_url_or_path, HEADERS, refresh_token)
    else:
        gdf = pd.read_csv(selected_url_or_path, parse_dates=["timestamp"])

# ---- OKUMA SONRASI NORMALİZASYON ----
gdf = gdf.dropna(subset=["timestamp"]).copy()

# Timestamp'leri UTC-aware parse et, naive UTC'ye çevir
gdf["timestamp"] = pd.to_datetime(gdf["timestamp"], utc=True, errors="coerce") \
                      .dt.tz_convert("UTC").dt.tz_localize(None)

# Yüzdeleri 0-100 aralığında tut
for col in ["RTP", "24H", "Week", "Month"]:
    if col in gdf.columns:
        gdf.loc[(gdf[col] < 0) | (gdf[col] > 100), col] = pd.NA

gdf = gdf.dropna(subset=["timestamp"]).sort_values("timestamp")

# Çok uzak tarihleri at (son 60 gün içinde olanları tut)
now_utc = pd.Timestamp.utcnow().tz_localize(None)
gdf = gdf[(gdf["timestamp"] >= now_utc - pd.Timedelta(days=60)) &
          (gdf["timestamp"] <= now_utc + pd.Timedelta(days=1))]

# -------------------- Görselleştirme için df --------------------
plot_df = gdf.loc[:, ["timestamp", metric]].copy()
if resample != "(yok)" and not plot_df.empty:
    plot_df = plot_df.set_index("timestamp").resample(resample).mean().reset_index()

# -------------------- Anlık sinyal --------------------
base_sorted = gdf.sort_values("timestamp")
last_row = base_sorted.tail(1).iloc[0]
last_ts = base_sorted["timestamp"].max()

slope_window = None if slope_window_opt == "Yok" else int(slope_window_opt)
slope_24h_now = None
if slope_window is not None:
    tmp = base_sorted[["timestamp", "24H"]].dropna().tail(slope_window)
    if len(tmp) >= 2:
        dt = (tmp["timestamp"].iloc[-1] - tmp["timestamp"].iloc[0]).total_seconds() / 60.0
        if dt > 0:
            slope_24h_now = (tmp["24H"].iloc[-1] - tmp["24H"].iloc[0]) / dt

def decide_signal_now(df_row, slope_24h=None, min_gap=0.3, use_slope=False, require_rtp=False):
    r24, r7, r30, rtp = df_row.get("24H"), df_row.get("Week"), df_row.get("Month"), df_row.get("RTP")
    if any(pd.isna([r24, r7, r30])): 
        return "UNKNOWN", "Veri eksik"
    up_ok = (r24 - max(r7, r30)) >= min_gap
    if require_rtp and not pd.isna(rtp):
        up_ok = up_ok and (r24 > rtp)
    slope_ok = True if not use_slope else (slope_24h is not None and slope_24h > 0)
    if up_ok and slope_ok:
        return "BUY", f"24H {r24:.2f} > max(Week,Month) + {min_gap}"
    return "HOLD", "Koşullar sağlanmadı"

signal_now, reason_now = decide_signal_now(
    last_row, slope_24h=slope_24h_now, min_gap=min_gap,
    use_slope=use_slope, require_rtp=require_rtp
)

st.subheader("📡 Anlık Sinyal")
if signal_now == "BUY":
    st.success(f"✅ GİR — {reason_now}" + (f" | Eğim: {slope_24h_now:+.3f}/dk" if slope_24h_now is not None else ""))
else:
    st.warning(f"⏳ BEKLE — {reason_now}")

# -------------------- Tek metrik grafik --------------------
st.subheader(f"🎯 {game_label} — {metric}")
if plot_df.empty or plot_df[metric].dropna().empty:
    st.info("Seçili metric için veri yok.")
else:
    fig = px.line(plot_df, x="timestamp", y=metric, markers=True)
    # Sinyal anı çizgisi (son veri zamanı)
    if pd.notna(last_ts):
        x_val = last_ts.to_pydatetime() if isinstance(last_ts, pd.Timestamp) else last_ts
        fig.add_shape(type="line", x0=x_val, x1=x_val, y0=0, y1=1,
                      xref="x", yref="paper", line=dict(dash="dot", width=1.5))
        fig.add_annotation(x=x_val, y=1, xref="x", yref="paper",
                           text="Sinyal anı", showarrow=False, yshift=10)
        # Auto-zoom: son 7 gün
        auto_zoom_last_days(fig, last_ts, days=7)
    st.plotly_chart(fig, use_container_width=True)

# -------------------- ADioG – tüm seriler + sinyal noktaları --------------------
st.subheader(f"🧪 ADioG — {game_label} (RTP gri, 24H kırmızı, Week lacivert, Month siyah)")
adio_df = gdf.loc[:, ["timestamp", "RTP", "24H", "Week", "Month"]].dropna().copy()
if resample != "(yok)" and not adio_df.empty:
    adio_df = adio_df.set_index("timestamp").resample(resample).mean().reset_index()

if adio_df.empty:
    st.info("ADioG için veri yok.")
else:
    slope_series = compute_slope_series(adio_df, slope_window if use_slope else None)
    adio_df["ENTRY"] = [
        decide_signal_row(
            adio_df.iloc[i],
            None if pd.isna(slope_series.iloc[i]) else slope_series.iloc[i],
            min_gap,
            use_slope=use_slope,
            require_rtp=require_rtp,
        )
        for i in range(len(adio_df))
    ]

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

    # Auto-zoom: son 7 gün
    auto_zoom_last_days(fig_all, last_ts, days=7)

    fig_all.update_layout(
        margin=dict(l=10, r=10, t=30, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis_title="timestamp", yaxis_title="RTP / Yüzde"
    )
    st.plotly_chart(fig_all, use_container_width=True)

# -------------------- tablo --------------------
st.divider()
st.subheader("🧾 Veri (seçilen metric)")
st.dataframe(plot_df, use_container_width=True, hide_index=True)
