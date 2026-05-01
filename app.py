import streamlit as st
import yfinance as yf
import pandas as pd
import ta
import os
from datetime import datetime, timedelta

# --- CONFIGURATION ---
st.set_page_config(page_title="FinBacktest India", layout="wide")
DATA_DIR = "market_data"

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# --- ENGINE: DATA MANAGEMENT ---
def get_data(ticker):
    """Handles 2-year download and incremental daily updates"""
    path = os.path.join(DATA_DIR, f"{ticker}.parquet")
    
    if os.path.exists(path):
        df = pd.read_parquet(path)
        last_date = df.index.max()
        # If the last data point is older than yesterday, fetch the gap
        if last_date.date() < (datetime.now() - timedelta(days=1)).date():
            st.info(f"Checking for new data for {ticker}...")
            new_data = yf.download(ticker, start=last_date + timedelta(days=1))
            if not new_data.empty:
                # Standardize columns to avoid MultiIndex issues in 2026 yfinance
                if isinstance(new_data.columns, pd.MultiIndex):
                    new_data.columns = new_data.columns.get_level_values(0)
                
                df = pd.concat([df, new_data])
                df = df[~df.index.duplicated(keep='last')]
                df.to_parquet(path)
        return df
    else:
        st.warning(f"Downloading 2-year history for {ticker}...")
        df = yf.download(ticker, period="2y")
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.to_parquet(path)
        return df

# --- ENGINE: BACKTEST LOGIC ---
def run_backtest(df, logic):
    """Calculates indicators and evaluates screener-style logic"""
    # Create a copy to avoid modifying the original cached data
    test_df = df.copy()
    
    # Calculate Indicators using the 'ta' library
    test_df['SMA_20'] = ta.trend.sma_indicator(test_df['Close'], window=20)
    test_df['SMA_50'] = ta.trend.sma_indicator(test_df['Close'], window=50)
    test_df['RSI'] = ta.momentum.rsi(test_df['Close'], window=14)
    
    # Drop rows where indicators are NaN (the first 50 rows)
    test_df = test_df.dropna()
    
    try:
        # Evaluate "Screener" Logic (e.g., "Close > SMA_20 and RSI > 50")
        test_df['Signal'] = test_df.eval(logic)
        
        # Performance Calculation
        test_df['Market_Ret'] = test_df['Close'].pct_change()
        # Shift signal by 1 day because you enter the day AFTER the signal
        test_df['Strategy_Ret'] = test_df['Signal'].shift(1) * test_df['Market_Ret']
        
        test_df['Cum_Market'] = (1 + test_df['Market_Ret'].fillna(0)).cumprod()
        test_df['Cum_Strategy'] = (1 + test_df['Strategy_Ret'].fillna(0)).cumprod()
        
        return test_df
    except Exception as e:
        st.error(f"Logic Error: Check your syntax. {e}")
        return None

# --- USER INTERFACE ---
st.title("📈 Indian Stock Market Backtester")
st.caption("Custom Backtesting Tool - Zero Cost Infrastructure")
st.markdown("---")

with st.sidebar:
    st.header("Parameters")
    ticker_input = st.text_input("NSE Ticker (with .NS)", "TATASTEEL.NS").upper()
    
    st.subheader("Strategy Logic")
    logic_input = st.text_area(
        "Enter Logic (Screener Style)", 
        value="Close > SMA_20 and RSI > 40",
        help="Variables: Close, SMA_20, SMA_50, RSI"
    )
    
    st.info("💡 Hint: Use 'and', 'or', '>', '<', '=='")
    
    run_btn = st.button("🚀 Sync & Run Backtest", use_container_width=True)

# --- EXECUTION & DISPLAY ---
if run_btn:
    try:
        # 1. Fetch/Update Data
        data = get_data(ticker_input)
        
        # 2. Run Analysis
        result = run_backtest(data, logic_input)
        
        if result is not None:
            # Metrics Row
            m1, m2, m3 = st.columns(3)
            strategy_pct = (result['Cum_Strategy'].iloc[-1] - 1) * 100
            market_pct = (result['Cum_Market'].iloc[-1] - 1) * 100
            
            m1.metric("Strategy Return", f"{strategy_pct:.2f}%", 
                      delta=f"{strategy_pct - market_pct:.2f}% vs Market")
            m2.metric("Market Return", f"{market_pct:.2f}%")
            m3.metric("Trades Found", int(result['Signal'].sum()))
            
            # Charting
            st.subheader("Equity Curve: Strategy vs Market")
            st.line_chart(result[['Cum_Strategy', 'Cum_Market']])
            
            # Data Table
            st.subheader("Recent Signals")
            st.dataframe(result[['Close', 'SMA_20', 'RSI', 'Signal']].tail(20), use_container_width=True)
            
            # Backup Option
            csv = result.to_csv().encode('utf-8')
            st.download_button("📥 Download Backtest Results (CSV)", csv, f"backtest_{ticker_input}.csv")
            
    except Exception as e:
        st.error(f"Execution Error: {e}")
else:
    st.write("👈 Configure your strategy and ticker in the sidebar, then click **Run**.")
