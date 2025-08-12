env:
  DRIVE_FOLDER_ID: 1UCWuJZc2NGf0Jaynr6DKpcm7I1rfwvAM
  GDRIVE_CREDENTIALS: ${{ secrets.GDRIVE_CREDENTIALS }}

steps:
  - uses: actions/checkout@v4

  - name: Set up Python
    uses: actions/setup-python@v5
    with:
      python-version: '3.11'

  - name: Install dependencies
    run: |
      python -m pip install --upgrade pip
      pip install google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib pandas openpyxl

  # (İsterseniz ayrıca yazabilirsiniz ama yukarıdaki script de yazar)
  - name: Write service account file (optional)
    run: |
      echo "$GDRIVE_CREDENTIALS" > credentials.json

  - name: Run collector (Drive → data/raw + data/normalized)
    run: |
      python ci/collector_gdrive_ci.py --folder-id "$DRIVE_FOLDER_ID"
