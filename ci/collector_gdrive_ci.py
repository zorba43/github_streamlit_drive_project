import os
import json
import sys
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
import pandas as pd

# Ortam değişkeninden klasör ID'sini al
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
if not DRIVE_FOLDER_ID:
    print("❌ DRIVE_FOLDER_ID tanımlı değil. GitHub Secrets veya .env dosyasını kontrol edin.")
    sys.exit(1)

# Google Drive API servisini oluştur
def get_service():
    creds_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not creds_json:
        print("❌ GOOGLE_SERVICE_ACCOUNT_JSON tanımlı değil.")
        sys.exit(1)
    creds_info = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(creds_info)
    return build('drive', 'v3', credentials=creds)

# Dosyaları indir
def download_files():
    service = get_service()
    query = f"'{DRIVE_FOLDER_ID}' in parents"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    
    if not files:
        print("⚠️ Klasörde dosya bulunamadı.")
        return
    
    os.makedirs("Scraper Data", exist_ok=True)
    
    for file in files:
        print(f"📥 İndiriliyor: {file['name']}")
        request = service.files().get_media(fileId=file['id'])
        filepath = os.path.join("Scraper Data", file['name'])
        with open(filepath, "wb") as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print(f"İndirme durumu: {int(status.progress() * 100)}%")

# Normalize et
def normalize_files():
    os.makedirs("data/normalized", exist_ok=True)
    for filename in os.listdir("Scraper Data"):
        if filename.endswith(".xlsx"):
            df = pd.read_excel(os.path.join("Scraper Data", filename))
            # Kolon isimleri eşleştirme
            df = df.rename(columns={
                "Text": "Oyun İsmi",
                "Text1": "24H RTP",
                "Text2": "1 Week RTP",
                "Text3": "1 Month RTP",
                "Text4": "Orjinal RTP",
                "Current_Time": "Time"
            })
            outname = filename.replace(".xlsx", ".csv")
            df.to_csv(os.path.join("data/normalized", outname), index=False)
            print(f"✅ Normalize edildi: {outname}")

def main():
    download_files()
    normalize_files()

if __name__ == "__main__":
    main()
