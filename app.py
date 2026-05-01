import streamlit as st
import yfinance as yf
import pandas as pd
import ta
import io
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import google.oauth2.credentials

# --- CONFIG ---
st.set_page_config(page_title="Nifty Personal Drive Scanner", layout="wide")
FOLDER_ID = "1mx3Fr-MX1bno7-n0p066OVTmtYK6-E8R" # Ensure this folder is in your Personal Drive
SCOPES = ['https://www.googleapis.com/auth/drive.file']

# --- AUTHENTICATION ENGINE ---
def get_user_creds():
    client_config = {
        "web": {
            "client_id": st.secrets["G_CLIENT_ID"],
            "client_secret": st.secrets["G_CLIENT_SECRET"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [st.secrets["G_REDIRECT_URI"]]
        }
    }
    
    flow = Flow.from_client_config(
        client_config, 
        scopes=SCOPES, 
        redirect_uri=st.secrets["G_REDIRECT_URI"]
    )
    
    # Handle the redirect from Google
    if "code" in st.query_params:
        flow.fetch_token(code=st.query_params["code"])
        st.session_state.creds = flow.credentials
        # Clear the code from the URL for a clean UI
        st.query_params.clear()
        return st.session_state.creds
    
    if "creds" in st.session_state:
        return st.session_state.creds

    # Show Login Link if no creds
    auth_url, _ = flow.authorization_url(prompt='consent', access_type='offline')
    st.warning("Action Required: Please authorize access to your personal Google Drive.")
    st.link_button("🔑 Login with Google", auth_url)
    st.stop()

# --- DRIVE UPLOADER ---
def upload_to_drive(file_name, dataframe, creds):
    service = build('drive', 'v3', credentials=creds)
    
    buffer = io.BytesIO()
    dataframe.to_parquet(buffer)
    buffer.seek(0)
    
    # Logic to check if file exists
    query = f"name = '{file_name}' and '{FOLDER_ID}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])

    media = MediaIoBaseUpload(buffer, mimetype='application/octet-stream', resumable=True)
    
    if files:
        # Update existing file
        service.files().update(fileId=files[0]['id'], media_body=media).execute()
    else:
        # Create new file[cite: 1]
        file_metadata = {'name': file_name, 'parents': [FOLDER_ID]}
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()

# --- SYNC FUNCTION ---
def sync_data():
    creds = get_user_creds()
    tickers = ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "TATASTEEL.NS", "INFY.NS"] 
    
    progress = st.progress(0)
    status = st.empty()
    
    for i, ticker in enumerate(tickers):
        status.text(f"Syncing {ticker} to your personal Drive...")
        df = yf.download(ticker, period="2y", progress=False)
        if not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            upload_to_drive(f"{ticker}.parquet", df, creds)
        progress.progress((i + 1) / len(tickers))
    
    status.text("Done!")
    st.success("Successfully synced to your personal storage!")

# --- UI ---
st.title("🎯 Nifty 500 Personal Drive Scanner")

with st.sidebar:
    st.header("Control Panel")
    if st.button("🔄 Sync Market to My Drive"):
        sync_data()
    
    st.markdown("---")
    logic_input = st.text_area("Strategy Logic", "Close > SMA_20 and RSI > 50")
    run_scan = st.button("🚀 Run Analysis")

if run_scan:
    st.info("Reading your personal Drive data... Results will appear here.")
