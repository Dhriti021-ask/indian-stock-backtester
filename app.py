import streamlit as st
import yfinance as yf
import pandas as pd
import ta
import os
import io
import concurrent.futures
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from datetime import datetime, timedelta

# --- CONFIG ---
st.set_page_config(page_title="Nifty 500 Cloud Scanner", layout="wide")
FOLDER_ID = "1mx3Fr-MX1bno7-n0p066OVTmtYK6-E8R"

# --- GOOGLE DRIVE CONNECTION ---
def get_gdrive_service():
    # This looks for the block you pasted into Streamlit Secrets
    creds_info = st.secrets["gdrive_service_account"]
    creds = service_account.Credentials.from_service_account_info(creds_info)
    return build('drive', 'v3', credentials=creds)

def upload_to_drive(file_name, dataframe):
    service = get_gdrive_service()
    
    # Convert dataframe to Parquet in memory
    buffer = io.BytesIO()
    dataframe.to_parquet(buffer)
    buffer.seek(0)
    
    # Check if file exists to update instead of create
    query = f"name = '{file_name}' and '{FOLDER_ID}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])

    media = MediaIoBaseUpload(buffer, mimetype='application/octet-stream', resumable=True)
    
    if files:
        file_id = files[0]['id']
        service.files().update(fileId=file_id, media_body=media).execute()
    else:
        file_metadata = {'name': file_name, 'parents': [FOLDER_ID]}
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()

# --- DATA ENGINE ---
def sync_market_to_drive():
    # Starting with a core list; you can expand this to the full Nifty 500
    tickers = ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "TATASTEEL.NS", "INFY.NS", "ICICIBANK.NS", "SBIN.NS"] 
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, ticker in enumerate(tickers):
        status_text.text(f"Processing {ticker}...")
        df = yf.download(ticker, period="2y", progress=False)
        if not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            upload_to_drive(f"{ticker}.parquet", df)
        progress_bar.progress((i + 1) / len(tickers))
    
    status_text.text("Sync Complete!")
    st.success("All data successfully synced to Google Drive!")

# --- UI ---
st.title("🎯 Nifty 500 Drive-Synced Scanner")

# THIS IS THE SIDEBAR SECTION YOU WERE LOOKING FOR
with st.sidebar:
    st.header("Storage Control")
    if st.button("🔄 Sync Market to Google Drive"):
        sync_market_to_drive()

    st.markdown("---")
    st.header("Analysis")
    logic_input = st.text_area("Scanner Logic", "Close > SMA_20 and RSI > 50")
    run_scan = st.button("🚀 Run Scan from Drive")

if run_scan:
    st.info("Scanner logic will appear here once data is synced.")
