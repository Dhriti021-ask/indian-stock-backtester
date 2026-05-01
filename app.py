import streamlit as st
import yfinance as yf
import pandas as pd
import ta
import os
import io
import googleapiclient.errors
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from datetime import datetime, timedelta

# --- CONFIG ---
st.set_page_config(page_title="Nifty 500 Cloud Scanner", layout="wide")
# Ensure this matches your shared folder exactly
FOLDER_ID = "1mx3Fr-MX1bno7-n0p066OVTmtYK6-E8R"

# --- GOOGLE DRIVE CONNECTION ---
def get_gdrive_service():
    # REQUIRED: Scopes for full Drive access
    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    try:
        creds_info = st.secrets["gdrive_service_account"]
        creds = service_account.Credentials.from_service_account_info(
            creds_info, 
            scopes=SCOPES
        )
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        st.error(f"Authentication Error: {e}")
        st.stop()

def upload_to_drive(file_name, dataframe):
    try:
        service = get_gdrive_service()
        
        # Convert to Parquet in memory[cite: 1]
        buffer = io.BytesIO()
        dataframe.to_parquet(buffer)
        buffer.seek(0)
        
        # Check if file exists (includeItemsFromAllDrives allows scanning Shared Drives)[cite: 1]
        query = f"name = '{file_name}' and '{FOLDER_ID}' in parents and trashed = false"
        results = service.files().list(
            q=query, 
            fields="files(id)",
            supportsAllDrives=True, 
            includeItemsFromAllDrives=True
        ).execute()
        files = results.get('files', [])

        media = MediaIoBaseUpload(buffer, mimetype='application/octet-stream', resumable=True)
        
        if files:
            # Update existing file[cite: 1]
            file_id = files[0]['id']
            service.files().update(
                fileId=file_id, 
                media_body=media,
                supportsAllDrives=True
            ).execute()
        else:
            # Create new file[cite: 1]
            file_metadata = {'name': file_name, 'parents': [FOLDER_ID]}
            service.files().create(
                body=file_metadata, 
                media_body=media, 
                fields='id',
                supportsAllDrives=True
            ).execute()
            
    except googleapiclient.errors.HttpError as error:
        # Catch the specific Quota Error and explain it[cite: 1]
        st.error(f"Google Drive Quota/Permission Error: {error.resp.status}")
        st.write(f"Details: {error.content.decode('utf-8')}")
        st.info("NOTE: If you are using a personal Gmail, Service Accounts have 0GB quota. "
                "You MUST use a 'Shared Drive' or a smaller data sample.")
        st.stop()

# --- DATA ENGINE ---
def sync_market_to_drive():
    # Tickers to sync[cite: 1]
    tickers = ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "TATASTEEL.NS", "INFY.NS", "ICICIBANK.NS", "SBIN.NS"] 
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, ticker in enumerate(tickers):
        status_text.text(f"Processing {ticker}...")
        # Download 2 years of daily data[cite: 1]
        df = yf.download(ticker, period="2y", progress=False)
        if not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            upload_to_drive(f"{ticker}.parquet", df)
        progress_bar.progress((i + 1) / len(tickers))
    
    status_text.text("Sync Complete!")
    st.success("All data successfully synced to Google Drive![cite: 1]")

# --- UI ---
st.title("🎯 Nifty 500 Drive-Synced Scanner")

with st.sidebar:
    st.header("Storage Control")
    if st.button("🔄 Sync Market to Google Drive"):
        sync_market_to_drive()

    st.markdown("---")
    st.header("Analysis")
    logic_input = st.text_area("Scanner Logic", "Close > SMA_20 and RSI > 50")
    run_scan = st.button("🚀 Run Scan from Drive")

if run_scan:
    st.info("Scanner logic is ready. Ensure your Drive data is synced first.[cite: 1]")
