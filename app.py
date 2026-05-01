import streamlit as st
import yfinance as yf
import pandas as pd
import ta
import os
import numpy as np
import itertools
from datetime import datetime, timedelta

# --- CONFIG ---
st.set_page_config(page_title="GoalSeeker India", layout="wide")
DATA_DIR = "market_data"
if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

# --- DATA ENGINE ---
def get_data(ticker):
    path = os.path.join(DATA_DIR, f"{ticker}.parquet")
    if os.path.exists(path):
        df = pd.read_parquet(path)
        if df.index.max().date() < (datetime.now() - timedelta(days=1)).date():
            new = yf.download(ticker, start=df.index.max() + timedelta(days=1))
            if not new.empty:
                if isinstance(new.columns, pd.MultiIndex): new.columns = new.columns.get_level_values(0)
                df = pd.concat([df, new])
                df = df[~df.index.duplicated(keep='last')]
                df.to_parquet(path)
        return df
    else:
        df = yf.download(ticker, period="2y")
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        df.to_parquet(path)
        return df

# --- OPTIMIZER ENGINE ---
def find_best_strategy(df, target_return_pct):
    target_decimal = target_return_pct / 100
    
    # Define Search Space
    fast_mas = [5, 10, 15, 20]
    slow_mas = [40, 50, 60, 80, 100]
    rsi_levels = [30, 40, 50, 60]
    
    best_strategy = None
    best_return = -999
    
    # Progress bar for the UI
    progress_bar = st.progress(0)
    total_combos = len(fast_mas) * len(slow_mas) * len(rsi_levels)
    count = 0

    for f, s, r in itertools.product(fast_mas, slow_mas, rsi_levels):
        count += 1
        progress_bar.progress(count / total_combos)
        
        temp_df = df.copy()
        temp_df['SMA_F'] = ta.trend.sma_indicator(temp_df['Close'], window=f)
        temp_df['SMA_S'] = ta.trend.sma_indicator(temp_df['Close'], window=s)
        temp_df['RSI'] = ta.momentum.rsi(temp_df['Close'], window=r)
        temp_df = temp_df.dropna()
        
        # Logic: Price > Fast SMA AND Fast SMA > Slow SMA AND RSI > Level
        temp_df['Signal'] = (temp_df['Close'] > temp_df['SMA_F']) & \
                            (temp_df['SMA_F'] > temp_df['SMA_S']) & \
                            (temp_df['RSI'] > r)
        
        ret = temp_df['Close'].pct_change()
        strat_ret = temp_df['Signal'].shift(1).fillna(False) * ret
        cum_ret = (1 + strat_ret.fillna(0)).cumprod().iloc[-1] - 1
        
        # Keep track of the best one found
        if cum_ret > best_return:
            best_return = cum_ret
            best_strategy = {"Fast": f, "Slow": s, "RSI": r, "Return": cum_ret}
            # If we hit the goal, we can stop or keep looking for even better
            if best_return >= target_decimal:
                pass 

    progress_bar.empty()
    return best_strategy

# --- UI ---
st.title("🎯 Strategy Goal-Seeker (India)")
st.caption("Tell the tool what you want, and it will find the logic.")

with st.sidebar:
    ticker = st.text_input("Ticker", "TATASTEEL.NS").upper()
    target_return = st.number_input("Target 2-Year Return (%)", value=25.0)
    st.markdown("---")
    find_btn = st.button("🔍 Find Winning Strategy")

if find_btn:
    data = get_data(ticker)
    with st.spinner("Analyzing hundreds of combinations..."):
        best = find_best_strategy(data, target_return)
    
    if best:
        # 1. Show the Winning Strategy
        st.success(f"### Found a strategy! It generated **{best['Return']*100:.2f}%**")
        
        col1, col2, col3 = st.columns(3)
        col1.code(f"Fast SMA: {best['Fast']}")
        col2.code(f"Slow SMA: {best['Slow']}")
        col3.code(f"RSI Level: {best['RSI']}")
        
        # 2. Run the full backtest for the winner to show the chart
        data['SMA_F'] = ta.trend.sma_indicator(data['Close'], window=best['Fast'])
        data['SMA_S'] = ta.trend.sma_indicator(data['Close'], window=best['Slow'])
        data['RSI'] = ta.momentum.rsi(data['Close'], window=best['RSI'])
        data = data.dropna()
        data['Signal'] = (data['Close'] > data['SMA_F']) & (data['SMA_F'] > data['SMA_S']) & (data['RSI'] > best['RSI'])
        
        data['Market_Ret'] = data['Close'].pct_change()
        data['Strategy_Ret'] = data['Signal'].shift(1).fillna(False) * data['Market_Ret']
        data['Cum_Market'] = (1 + data['Market_Ret'].fillna(0)).cumprod()
        data['Cum_Strategy'] = (1 + data['Strategy_Ret'].fillna(0)).cumprod()
        
        st.subheader("Backtest of the Found Strategy")
        st.line_chart(data[['Cum_Strategy', 'Cum_Market']])
        
        st.info(f"**Calculated Logic:** `Close > SMA_{best['Fast']} and SMA_{best['Fast']} > SMA_{best['Slow']} and RSI > {best['RSI']}`")
    else:
        st.error("Could not find a strategy meeting that return in the current search space.")
