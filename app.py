import streamlit as st
import yfinance as yf
import pandas as pd
import ta
import os
import numpy as np
import itertools
from datetime import datetime, timedelta

# --- CONFIG ---
st.set_page_config(page_title="QuantGoal India", layout="wide")
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
    
    # Expanded Search Space for better accuracy
    fast_mas = [5, 9, 13, 21]
    slow_mas = [34, 50, 89, 144, 200]
    rsi_levels = [30, 40, 45, 50, 55, 60]
    
    best_strategy = None
    best_return = -999
    
    progress_bar = st.progress(0)
    total_combos = len(fast_mas) * len(slow_mas) * len(rsi_levels)
    count = 0

    for f, s, r in itertools.product(fast_mas, slow_mas, rsi_levels):
        count += 1
        if count % 10 == 0: progress_bar.progress(count / total_combos)
        
        temp_df = df.copy()
        temp_df['SMA_F'] = ta.trend.sma_indicator(temp_df['Close'], window=f)
        temp_df['SMA_S'] = ta.trend.sma_indicator(temp_df['Close'], window=s)
        temp_df['RSI'] = ta.momentum.rsi(temp_df['Close'], window=r)
        temp_df = temp_df.dropna()
        
        if temp_df.empty: continue

        # Logic: Bullish Alignment
        temp_df['Signal'] = (temp_df['Close'] > temp_df['SMA_F']) & \
                            (temp_df['SMA_F'] > temp_df['SMA_S']) & \
                            (temp_df['RSI'] > r)
        
        ret = temp_df['Close'].pct_change()
        strat_ret = temp_df['Signal'].shift(1).fillna(False) * ret
        cum_ret = (1 + strat_ret.fillna(0)).cumprod().iloc[-1] - 1
        
        # Calculate extra metrics for the winner
        if cum_ret > best_return:
            # Win Rate Calculation
            trades = strat_ret[strat_ret != 0]
            win_rate = (trades > 0).sum() / len(trades) if len(trades) > 0 else 0
            
            # Max Drawdown Calculation
            cum_wealth = (1 + strat_ret.fillna(0)).cumprod()
            peak = cum_wealth.cummax()
            drawdown = (cum_wealth - peak) / peak
            max_dd = drawdown.min()

            best_return = cum_ret
            best_strategy = {
                "Fast": f, "Slow": s, "RSI": r, 
                "Return": cum_ret, "WinRate": win_rate, "MaxDD": max_dd
            }

    progress_bar.empty()
    return best_strategy

# --- UI ---
st.title("🎯 QuantGoal: Strategy Optimizer")
st.markdown("Enter your profit goal, and the engine will reverse-engineer the indicators required to hit it.")

with st.sidebar:
    st.header("Setup")
    ticker = st.text_input("NSE Ticker", "RELIANCE.NS").upper()
    target_return = st.slider("Target 2-Year Return (%)", 10.0, 200.0, 40.0)
    st.info("The engine will scan Fibonacci and Standard intervals to find your goal.")
    st.markdown("---")
    find_btn = st.button("🔍 Solve for Strategy", use_container_width=True)

if find_btn:
    data = get_data(ticker)
    with st.spinner(f"Simulating market scenarios for {ticker}..."):
        best = find_best_strategy(data, target_return)
    
    if best:
        # 1. Performance Overview
        if best['Return'] >= (target_return/100):
            st.success(f"### Goal Achieved! Found a strategy yielding **{best['Return']*100:.2f}%**")
        else:
            st.warning(f"### Goal Partially Met: Best found was **{best['Return']*100:.2f}%**")
        
        # 2. Metric Cards
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Return", f"{best['Return']*100:.1f}%")
        m2.metric("Win Rate", f"{best['WinRate']*100:.1f}%")
        m3.metric("Max Drawdown", f"{best['MaxDD']*100:.1f}%")
        m4.metric("Strategy Type", "Trend Following")

        # 3. Parameter Display
        st.markdown("#### Found Parameters")
        c1, c2, c3 = st.columns(3)
        c1.info(f"**Fast SMA:** {best['Fast']}")
        c2.info(f"**Slow SMA:** {best['Slow']}")
        c3.info(f"**RSI Threshold:** {best['RSI']}")
        
        # 4. Detailed Backtest
        # Re-run winning logic for the chart
        data['SMA_F'] = ta.trend.sma_indicator(data['Close'], window=best['Fast'])
        data['SMA_S'] = ta.trend.sma_indicator(data['Close'], window=best['Slow'])
        data['RSI'] = ta.momentum.rsi(data['Close'], window=best['RSI'])
        data = data.dropna()
        data['Signal'] = (data['Close'] > data['SMA_F']) & (data['SMA_F'] > data['SMA_S']) & (data['RSI'] > best['RSI'])
        
        data['Market_Ret'] = data['Close'].pct_change()
        data['Strategy_Ret'] = data['Signal'].shift(1).fillna(False) * data['Market_Ret']
        data['Cum_Market'] = (1 + data['Market_Ret'].fillna(0)).cumprod()
        data['Cum_Strategy'] = (1 + data['Strategy_Ret'].fillna(0)).cumprod()
        
        st.subheader("Strategy vs. Market (Cumulative Growth)")
        st.line_chart(data[['Cum_Strategy', 'Cum_Market']])
        
        # 5. Logic Export
        st.code(f"""
# Strategy Logic for {ticker}:
# IF Price > SMA({best['Fast']}) 
# AND SMA({best['Fast']}) > SMA({best['Slow']}) 
# AND RSI({best['RSI']}) > {best['RSI']}
# THEN BUY
        """, language="python")
    else:
        st.error("No profitable strategy found in the current search space. Try a different ticker.")
