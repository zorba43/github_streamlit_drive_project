# app.py
import os, io, glob, requests, base64
from pathlib import Path
from datetime import datetime

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

# 15 dk adımlarla (N=672) Week yakınsaması
def project_week_15m(W_now: float, D_star: float, k_steps: int, N: int = 672):
    decay = ((N - 1) / N) ** k_steps
    return float(D_star + (W_now - D_star) * decay)

# Saatlik yakınsama (basit üstel yaklaşım)
def simulate_convergence(current: float, target: float, hours: int, decay_factor: float):
    vals = []
    val = current
    for _ in range(hours + 1):
        vals.append(val)
        val += (target - val) * decay_factor
    return vals

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
    slope_window = st.selectbox("24H eğim penceresi", ["Yok", 3, 5, 9], index=2)

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

# -------------------- Sinyal --------------------
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

# ==================== ▶ SIMÜLASYON BUTONU (tetiklemeli) ====================
st.markdown("---")
run_sim = st.button("▶ Simülasyonu Çalıştır")

if run_sim:
    # --- Parametreler ---
    base_rtp = float(last_row.get("RTP")) if "RTP" in last_row else None
    curr_24h = float(last_row.get("24H")) if "24H" in last_row else None
    curr_week = float(last_row.get("Week")) if "Week" in last_row else None
    curr_month = float(last_row.get("Month")) if "Month" in last_row else None

    # Güvenlik
    if any(v is None for v in [base_rtp, curr_24h, curr_week, curr_month]):
        st.info("Simülasyon için 24H / Week / Month / RTP sütunları gerekli.")
    else:
        st.subheader("🔮 Simülasyon")

        # ---- A) 15 dk adımlarında Week yakınsaması (D* = son 24H) ----
        st.markdown("**A) 15 dk Adımlarıyla Week Yakınsaması**")
        cA1, cA2, cA3 = st.columns(3)
        with cA1:
            D_star = st.number_input("24H seviyesi (D*)", value=round(curr_24h, 2), step=0.1,
                                     help="Gelecek ölçümlerde korunacağı varsayılan 24H seviyesi")
        with cA2:
            horizon_steps = st.slider("Ufuk (15 dk adım)", min_value=24, max_value=672, value=288,
                                      help="672 adım ≈ 7 gün")
        with cA3:
            gap15 = st.number_input("Giriş eşiği (puan)", min_value=0.0, max_value=5.0,
                                    value=float(min_gap), step=0.1)

        last_ts = base_sorted["timestamp"].max()
        steps = list(range(0, horizon_steps + 1))
        rows = []
        entry_step = None
        direction = "up" if D_star >= curr_week else "down"

        for k in steps:
            Wk = project_week_15m(curr_week, D_star, k, N=672)
            diff = D_star - Wk
            ts_k = (last_ts + pd.Timedelta(minutes=15 * k)) if pd.notna(last_ts) else None
            if entry_step is None:
                if direction == "up" and diff >= gap15:
                    entry_step = k
                if direction == "down" and (-diff) >= gap15:
                    entry_step = k
            rows.append({"Adım (15dk)": k, "Zaman": ts_k, "Proj. Week": round(Wk, 3),
                         "24H (D*)": round(D_star, 3), "Fark (D*-W)": round(diff, 3)})

        sim15_df = pd.DataFrame(rows)
        st.dataframe(sim15_df[["Adım (15dk)", "Zaman", "Proj. Week", "24H (D*)", "Fark (D*-W)"]],
                     use_container_width=True, hide_index=True)

        x_col = "Zaman" if sim15_df["Zaman"].notna().any() else "Adım (15dk)"
        fig_sim15 = px.line(sim15_df, x=x_col, y="Proj. Week", markers=True,
                            title="Week projeksiyonu (15 dk adımlar)")
        fig_sim15.add_hline(y=D_star, line_dash="dot", annotation_text="24H (D*)", annotation_position="top left")
        if entry_step is not None:
            x_val = sim15_df.loc[entry_step, x_col]
            fig_sim15.add_vline(x=x_val, line_dash="dot",
                                annotation_text=f"Giriş: {entry_step} adım", annotation_position="top right")
        st.plotly_chart(fig_sim15, use_container_width=True)

        if entry_step is None:
            st.warning("15 dk simülasyonunda seçilen ufukta giriş eşiği oluşmadı.")
        else:
            eta = sim15_df.loc[entry_step, "Zaman"]
            dur_text = f" (~{entry_step*15//60} saat {entry_step*15%60} dk)"
            when_text = f", zaman: {eta.strftime('%Y-%m-%d %H:%M')}" if pd.notna(eta) else ""
            st.success(
                f"✅ 15 dk simülasyonu: **{entry_step} adım** sonra{dur_text}{when_text} giriş yapılabilir. "
                f"Week ≈ {sim15_df.loc[entry_step, 'Proj. Week']:.2f}, fark ≈ {sim15_df.loc[entry_step, 'Fark (D*-W)']:.2f}."
            )

        st.markdown("---")

        # ---- B) Saatlik yakınsama (24H/Week/Month → Base RTP) ----
        st.markdown("**B) Saatlik Yakınsama (Orijinal RTP'ye doğru)**")
        cB1, cB2 = st.columns(2)
        with cB1:
            decay_pct = st.slider("Saatlik yakınsama oranı (%)", 5, 50, 25, step=5,
                                  help="Her saatte farkın bu kadarı kapanır")
        with cB2:
            horizon_hours = st.slider("Ufuk (saat)", 1, 24, 6)

        decay = decay_pct / 100.0
        sim_24h = simulate_convergence(curr_24h, base_rtp, horizon_hours, decay)
        sim_week = simulate_convergence(curr_week, base_rtp, horizon_hours, decay)
        sim_month = simulate_convergence(curr_month, base_rtp, horizon_hours, decay)

        simH_df = pd.DataFrame({
            "Saat": list(range(horizon_hours + 1)),
            "24H": sim_24h, "Week": sim_week, "Month": sim_month
        })

        figH = px.line(simH_df, x="Saat", y=["24H", "Week", "Month"],
                       title="Saatlik RTP Yakınsama", markers=True)
        figH.add_hline(y=base_rtp, line_dash="dot", annotation_text="Orijinal RTP", annotation_position="top left")
        st.plotly_chart(figH, use_container_width=True)

        # Hızlı yorum
        if curr_24h > curr_week and curr_24h > curr_month and curr_24h > base_rtp:
            st.success("✅ Saatlik model: Şu an verme eğiliminde (kısa vadede oynanabilir).")
        else:
            st.warning("⏳ Saatlik model: Güçlü bir verme eğilimi teyidi yok (beklemek mantıklı).")
