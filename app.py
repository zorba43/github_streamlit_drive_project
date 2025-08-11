# app.py
import os, glob
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Normalized Time-Series Dashboard", layout="wide")

# -------------------- Ayarlar --------------------
AUTH_USER = "mirzam43"
NORMALIZED_DIR = "data/normalized"

# -------------------- Auth --------------------
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

# -------------------- UI --------------------
st.title("📈 Normalized Oyun Zaman Serileri")
st.components.v1.html("<meta http-equiv='refresh' content='900'>", height=0)

files = sorted(glob.glob(str(Path(NORMALIZED_DIR) / "*.csv")))
if not files:
    st.warning(f"'{NORMALIZED_DIR}' içinde CSV bulunamadı.")
    st.stop()

catalog
