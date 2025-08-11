
# GitHub + Google Drive + Streamlit (15 dk)

Bu repo şablonu, Google Drive klasörünüzdeki Excel dosyalarından **24H / Week / Month / RTP** değerlerini alıp `data/history.csv`'ye yazar ve Streamlit dashboard ile grafikleri gösterir.

## Mimarisi
- **GitHub Actions** (schedule: */15 dak.) → Drive'dan Excel indir → `data/history.csv`'ye ekle → commit & push
- **Streamlit app (`app.py`)** → `data/history.csv`'yi okuyup canlı grafik

## Kurulum Adımları
1) Bu şablonu GitHub'a yükleyin (yeni repo).
2) Drive tarafı için **Service Account** oluşturun ve Drive klasörünüzü bu hesabın e-postasıyla **paylaşın (Viewer)**.
3) Service Account JSON içeriğini GitHub Secrets’a ekleyin: **`GOOGLE_APPLICATION_CREDENTIALS_JSON`**
4) Drive klasör ID’sini Secrets’a ekleyin: **`DRIVE_FOLDER_ID`**
5) `Actions` sekmesinde workflow’u bir kez **Run workflow** ile çalıştırın ya da 15 dakikalık cron’u bekleyin.
6) `data/history.csv` oluşunca:
```
pip install -r requirements.txt
streamlit run app.py
```
> İpucu: Streamlit Community Cloud'a bağlarsanız, repo push’larında otomatik güncellenir.

## Notlar
- Excel’lerde değerler başlıklarda gömülü ise extractor heuristics ile ayrıştırılır.
- Zaman damgası yoksa UTC zamanı kullanılır.
- `data/history.csv` şeması: `timestamp, game, 24H, Week, Month, RTP, source_file`.
