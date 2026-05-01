import streamlit as st
import yfinance as yf
import pandas as pd
import ta
import os
from datetime import datetime, timedelta

# --- INITIAL CONFIGURATION ---
st.set_page_config(page_title="India Backtest Pro", layout="wide")
DATA_DIR = "market_data"

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# --- DATA ENGINE ---
def get_data(ticker):
    """Handles initial 2-year download and incremental updates"""
    path = os.path.join(DATA_DIR, f"{ticker}.parquet")
    
    if os.path.exists(path):
        df = pd.read_parquet(path)
        last_date = df.index.max()
        
        # If the local data is older than yesterday, fetch only the missing piece
        if last_date.date() < (datetime.now() - timedelta(days=1)).date():
            st.info(f"Syncing new data for {ticker}...")
            new_data = yf.download(ticker, start=last_date + timedelta(days=1))
            
            if not new_data.empty:
                # Handle potential MultiIndex columns in 2026 yfinance
                if isinstance(new_data.columns, pd.MultiIndex):
                    new_data.columns = new_data.columns.get_level_values(0)
                
                df = pd.concat([df, new_data])
                df = df[~df.index.duplicated(keep='last')]
                df.to_parquet(path)
        return df
    else:
        st.warning(f"No local data. Downloading 2-year history for {ticker}...")
        df = yf.download(ticker, period="2y")
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.to_parquet(path)
        return df

# --- BACKTEST ENGINE ---
def run_backtest(df, logic):
    """Applies indicators and evaluates logic string"""
    test_df = df.copy()
    
    # Calculate Pre-defined Indicators
    test_df['SMA_20'] = ta.trend.sma_indicator(test_df['Close'], window=20)
    test_df['SMA_50'] = ta.trend.sma_indicator(test_df['Close'], window=50)
    test_df['RSI'] = ta.momentum.rsi(test_df['Close'], window=14)
    
    # Clean data (Remove NaNs before logic check)
    test_df = test_df.dropna()
    
    try:
        # Step 1: Generate Signals
        test_df['Signal'] = test_df.eval(logic)
        
        # Step 2: Calculate Strategy Returns
        test_df['Market_Ret'] = test_df['Close'].pct_change()
        # Shift signal by 1 so we enter the trade on the NEXT day's price
        test_df['Strategy_Ret'] = test_df['Signal'].shift(1).fillna(False) * test_df['Market_Ret']
        
        # Step 3: Calculate Cumulative Wealth
        test_df['Cum_Market'] = (1 + test_df['Market_Ret'].fillna(0)).cumprod()
        test_df['Cum_Strategy'] = (1 + test_df['Strategy_Ret'].fillna(0)).cumprod()
        
        return test_df
    except Exception as e:
        st.error(f"Logic Error: Check your syntax. {e}")
        return None

# --- APP INTERFACE ---
st.title("📈 Indian Stock Backtester")
st.caption("Powered by Streamlit & Parquet - 100% Free Infrastructure")
st.markdown("---")

with st.sidebar:
    st.header("Settings")
    ticker = st.text_input("Ticker (e.g., RELIANCE.NS)", "TATASTEEL.NS").upper()
    
    st.subheader("Strategy Builder")
    logic_input = st.text_area(
        "Screener Logic", 
        value="Close > SMA_20 and RSI > 40",
        help="Use: Close, SMA_20, SMA_50, RSI"
    )
    
    st.markdown("---")
    run_btn = st.button("🚀 Sync & Run", use_container_width=True)

# --- RESULTS DISPLAY ---
if run_btn:
    try:
        # Load and Backtest
        raw_data = get_data(ticker)
        results = run_backtest(raw_data, logic_input)
        
        if results is not None:
            # Final data cleaning for the charts (Forces numeric types)
            chart_df = results[['Cum_Strategy', 'Cum_Market']].astype(float)
            
            # 1. Summary Metrics
            c1, c2, c3 = st.columns(3)
            strat_perf = (chart_df['Cum_Strategy'].iloc[-1] - 1) * 100
            mkt_perf = (chart_df['Cum_Market'].iloc[-1] - 1) * 100
            
            c1.metric("Strategy Return", f"{strat_perf:.2f}%", delta=f"{strat_perf - mkt_perf:.2f}% vs Mkt")
            c2.metric("Market Return", f"{mkt_perf:.2f}%")
            c3.metric("Trades Count", int(results['Signal'].sum()))
            
            # 2. The Equity Curve
            st.subheader("Performance vs Market")
            st.line_chart(chart_df)
            
            # 3. Signals Table
            st.subheader("Recent Trade Signals")
            # Convert Boolean to String for better table display
            table_df = results[['Close', 'SMA_20', 'RSI', 'Signal']].tail(15).copy()
            table_df['Signal'] = table_df['Signal'].astype(str)
            st.dataframe(table_df, use_container_width=True)
            
            # 4. Export
            csv_data = results.to_csv().encode('utf-8')
            st.download_button("📥 Download Full CSV", csv_data, f"{ticker}_backtest.csv")
            
    except Exception as e:
        st.error(f"Something went wrong: {e}")
else:
    st.info("Enter your ticker and logic on the left, then click 'Sync & Run'.")
