
import os
from datetime import datetime
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="24H / Week / Month Dashboard", layout="wide")
st.title("📈 24H / Week / Month Dashboard (GitHub + Actions)")
st.caption("Bu uygulama, repo içindeki data/history.csv dosyasını okur. GitHub Actions her 15 dakikada Drive'dan veriyi günceller.")

# Auto refresh
st.components.v1.html("<meta http-equiv='refresh' content='900'>", height=0)

HISTORY_CSV = os.environ.get("HISTORY_CSV_PATH", "data/history.csv")

if not os.path.exists(HISTORY_CSV):
    st.warning("data/history.csv bulunamadı. CI ilk çalıştırmayı yaptığında otomatik oluşacak.")
    st.stop()

df = pd.read_csv(HISTORY_CSV, parse_dates=["timestamp"])
if df.empty:
    st.warning("history.csv şu an boş.")
    st.stop()

st.sidebar.header("🔎 Seçimler")
metric = st.sidebar.selectbox("Metrik", ["24H","Week","Month"])
games = sorted(df["game"].dropna().unique().tolist())
game = st.sidebar.selectbox("Oyun", games, index=0 if games else None)

min_date = df["timestamp"].min()
max_date = df["timestamp"].max()
st.sidebar.caption(f"Tarih aralığı: {min_date} → {max_date}")
start_date = st.sidebar.date_input("Başlangıç", value=min_date.date())
end_date = st.sidebar.date_input("Bitiş", value=max_date.date())

mask = (df["game"] == game) & (df["timestamp"] >= pd.Timestamp(start_date)) & (df["timestamp"] <= pd.Timestamp(end_date) + pd.Timedelta(days=1))
plot_df = df.loc[mask, ["timestamp", metric]].sort_values("timestamp")

col1, col2 = st.columns([3,2])
with col1:
    st.subheader(f"🧩 {game} | {metric}")
with col2:
    st.metric("Kayıt", len(plot_df))
    last_ts = plot_df["timestamp"].max() if not plot_df.empty else None
    st.metric("Son güncelleme", last_ts.strftime("%Y-%m-%d %H:%M:%S") if last_ts is not None else "—")

st.divider()

if plot_df.empty:
    st.info("Seçili filtre için veri yok.")
else:
    fig = px.line(plot_df, x="timestamp", y=metric, markers=True)
    st.plotly_chart(fig, use_container_width=True)
    st.subheader("🧾 Veri")
    st.dataframe(plot_df, use_container_width=True, hide_index=True)
