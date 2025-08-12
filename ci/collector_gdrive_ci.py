import os
import re
from pathlib import Path
import pandas as pd

RAW_FOLDER = Path("raw")
NORMALIZED_FOLDER = Path("data/normalized")  # app.py bu klasörü okuyor
NORMALIZED_FOLDER.mkdir(parents=True, exist_ok=True)


def _to_float(s: str | float | int | None) -> float | None:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    ss = str(s).strip().replace(",", ".")
    try:
        return float(ss)
    except Exception:
        return None


def extract_after_label(value, label: str) -> float | None:
    """
    '24H108.03%'  -> label='24H'   => 108.03
    'Week104,12%' -> label='Week' => 104.12
    'Month80.23%' -> label='Month'=> 80.23
    'RTP96.07%'   -> label='RTP'  => 96.07

    1) Etiketten SONRAKİ sayıyı yakalar.
    2) Bulamazsa fallback olarak stringdeki SON sayıyı alır.
    """
    if pd.isna(value):
        return None
    s = str(value).strip()

    # 1) Etiketten sonra gelen sayı (virgül/nokta destekli)
    pat = rf"(?i){re.escape(label)}\s*([+-]?\d+(?:[.,]\d+)?)"
    m = re.search(pat, s)
    if m:
        return _to_float(m.group(1))

    # 2) Fallback: stringdeki SON sayı (genellikle yüzde işaretinden önceki)
    all_nums = re.findall(r"([+-]?\d+(?:[.,]\d+)?)", s)
    if all_nums:
        return _to_float(all_nums[-1])

    return None


def process_excel(file_path: Path) -> pd.DataFrame | None:
    try:
        df = pd.read_excel(file_path)

        # Belirttiğin mapping
        df = df.rename(
            columns={
                "Text": "game",
                "Text1": "24h",
                "Text2": "week",
                "Text3": "month",
                "Text4": "rtp",
                "Current_time": "timestamp",
            }
        )

        # Metrikleri etiketten sonra gelen değer olarak parse et
        if "24h" in df:
            df["24h"] = df["24h"].apply(lambda v: extract_after_label(v, "24H"))
        if "week" in df:
            df["week"] = df["week"].apply(lambda v: extract_after_label(v, "Week"))
        if "month" in df:
            df["month"] = df["month"].apply(lambda v: extract_after_label(v, "Month"))
        if "rtp" in df:
            df["rtp"] = df["rtp"].apply(lambda v: extract_after_label(v, "RTP"))

        # timestamp'i güvenli şekilde parse et
        if "timestamp" in df:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

        # App ile uyumlu kolon sırası
        keep = [c for c in ["timestamp", "game", "24h", "week", "month", "rtp"] if c in df.columns]
        out = df[keep].sort_values("timestamp").reset_index(drop=True)

        return out

    except Exception as e:
        print(f"Hata: {file_path} işlenemedi -> {e}")
        return None


def main():
    files = sorted([p for p in RAW_FOLDER.glob("*.xls*")])
    if not files:
        print("raw klasöründe Excel bulunamadı.")
        return

    for f in files:
        print(f"İşleniyor: {f.name}")
        norm = process_excel(f)
        if norm is not None and not norm.empty:
            out_name = f.stem.lower().replace(" ", "-") + ".csv"
            out_path = NORMALIZED_FOLDER / out_name
            norm.to_csv(out_path, index=False)
            print(f"Kaydedildi: {out_path}")
        else:
            print(f"Uyarı: {f.name} için normalize edilecek satır yok/boş.")


if __name__ == "__main__":
    main()
