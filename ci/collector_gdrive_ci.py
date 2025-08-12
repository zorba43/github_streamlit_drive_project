import pandas as pd
import os
import re
from datetime import datetime

RAW_FOLDER = "raw"
NORMALIZED_FOLDER = "normalized"

os.makedirs(NORMALIZED_FOLDER, exist_ok=True)

def clean_rtp(value):
    """24H96.7% gibi değerlerden sadece sayıyı alır"""
    if pd.isna(value):
        return None
    match = re.search(r"(\d+(\.\d+)?)", str(value))
    return float(match.group(1)) if match else None

def process_excel(file_path):
    try:
        df = pd.read_excel(file_path)

        # Kolon adları senin belirttiğin şekilde eşleniyor
        df = df.rename(columns={
            "Text": "game",
            "Text1": "24h",
            "Text2": "week",
            "Text3": "month",
            "Text4": "rtp",
            "Current_time": "timestamp"
        })

        # RTP kolonlarını temizle
        for col in ["24h", "week", "month", "rtp"]:
            df[col] = df[col].apply(clean_rtp)

        # Timestamp formatını düzenle
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

        return df

    except Exception as e:
        print(f"Hata: {file_path} işlenemedi -> {e}")
        return None

def main():
    for file_name in os.listdir(RAW_FOLDER):
        if file_name.endswith(".xlsx") or file_name.endswith(".xls"):
            file_path = os.path.join(RAW_FOLDER, file_name)
            print(f"İşleniyor: {file_name}")
            df = process_excel(file_path)
            if df is not None and not df.empty:
                out_name = os.path.splitext(file_name)[0] + ".csv"
                out_path = os.path.join(NORMALIZED_FOLDER, out_name)
                df.to_csv(out_path, index=False)
                print(f"Kaydedildi: {out_path}")

if __name__ == "__main__":
    main()
