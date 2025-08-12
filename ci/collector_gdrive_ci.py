import os
import pandas as pd
from pathlib import Path

# Google Drive API yükleme kısmı burada olacak (credentials.json kullanıyor)

SCRAPER_FOLDER = "Scraper Data"
NORMALIZED_FOLDER = "data/normalized"

os.makedirs(NORMALIZED_FOLDER, exist_ok=True)

def normalize_excel(file_path):
    df = pd.read_excel(file_path)

    # Kolon adlarını küçük harfe çevir
    df.columns = [c.lower() for c in df.columns]

    # "24H" gibi metriklerden sonraki yüzdeyi ayıklayıp normalize et
    if '24h' in df.columns:
        df['24h'] = df['24h'].astype(str).str.extract(r'(\d+\.?\d*)').astype(float)

    if 'week' in df.columns:
        df['week'] = df['week'].astype(str).str.extract(r'(\d+\.?\d*)').astype(float)

    if 'month' in df.columns:
        df['month'] = df['month'].astype(str).str.extract(r'(\d+\.?\d*)').astype(float)

    # CSV olarak normalized klasörüne kaydet
    out_path = Path(NORMALIZED_FOLDER) / (Path(file_path).stem + ".csv")
    df.to_csv(out_path, index=False)
    print(f"[collector] Normalized yazıldı -> {out_path}")

def main():
    for file_name in os.listdir(SCRAPER_FOLDER):
        if file_name.endswith(".xlsx"):
            file_path = os.path.join(SCRAPER_FOLDER, file_name)
            print(f"[collector] Normalize ediliyor: {file_name}")
            try:
                normalize_excel(file_path)
            except Exception as e:
                print(f"[collector] Hata: {file_name} normalize edilemedi -> {e}")

if __name__ == "__main__":
    main()
