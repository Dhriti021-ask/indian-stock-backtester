```python
import streamlit as st
import yfinance as yf
import pandas as pd
import pandas_ta as ta
import os
from datetime import datetime, timedelta

# --- CONFIG ---
st.set_page_config(page_title="FinBacktest India", layout="wide")
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# --- ENGINE ---
def get_data(ticker):
    path = f"{DATA_DIR}/{ticker}.parquet"
    
    if os.path.exists(path):
        df = pd.read_parquet(path)
        last_date = df.index.max()
        # Check if we need incremental update (if last date is not yesterday/today)
        if last_date.date() < (datetime.now() - timedelta(days=1)).date():
            st.info(f"Updating data for {ticker}...")
            new_data = yf.download(ticker, start=last_date + timedelta(days=1))
            if not new_data.empty:
                df = pd.concat([df, new_data])
                df = df[~df.index.duplicated(keep='last')]
                df.to_parquet(path)
        return df
    else:
        st.warning(f"Downloading 2 years of history for {ticker}...")
        df = yf.download(ticker, period="2y")
        df.to_parquet(path)
        return df

def run_backtest(df, logic):
    # Prepare Indicators for Screener Logic
    df['SMA_20'] = ta.sma(df['Close'], length=20)
    df['SMA_50'] = ta.sma(df['Close'], length=50)
    df['RSI'] = ta.rsi(df['Close'], length=14)
    
    # Execute "Screener" Logic
    try:
        # Convert user logic to Python-readable query
        df['Signal'] = df.eval(logic)
    except Exception as e:
        st.error(f"Logic Error: {e}")
        return None

    # Calculate Returns
    df['Market_Ret'] = df['Close'].pct_change()
    df['Strategy_Ret'] = df['Signal'].shift(1) * df['Market_Ret']
    df['Cum_Market'] = (1 + df['Market_Ret'].fillna(0)).cumprod()
    df['Cum_Strategy'] = (1 + df['Strategy_Ret'].fillna(0)).cumprod()
    return df

# --- UI ---
st.title("🇮🇳 Indian Market Backtester")
st.markdown("---")

with st.sidebar:
    ticker = st.text_input("NSE Ticker", "TATASTEEL.NS")
    logic = st.text_area("Screener Logic", "Close > SMA_20 and RSI > 40")
    st.info("Available: Close, SMA_20, SMA_50, RSI")
    
    if st.button("🚀 Sync & Run Backtest"):
        data = get_data(ticker)
        result = run_backtest(data, logic)
        
        if result is not None:
            st.session_state['result'] = result

# --- DISPLAY ---
if 'result' in st.session_state:
    res = st.session_state['result']
    
    # Stats Table
    m1, m2, m3 = st.columns(3)
    final_ret = (res['Cum_Strategy'].iloc[-1] - 1) * 100
    m1.metric("Strategy Return", f"{final_ret:.2f}%")
    m2.metric("Market Return", f"{(res['Cum_Market'].iloc[-1]-1)*100:.2f}%")
    m3.metric("Trades Found", res['Signal'].sum())
    
    st.line_chart(res[['Cum_Strategy', 'Cum_Market']])
    st.dataframe(res.tail(20))
