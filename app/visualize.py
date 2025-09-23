import pandas as pd
import numpy as np
import mplfinance as mpf
import logging
from typing import List

logger = logging.getLogger(__name__)

def plot_with_trades(df_input: pd.DataFrame, trades: pd.DataFrame, symbol: str, save_path: str):
    """
    Generate and save a candlestick chart with indicators, ATR bands, and trade signals.
    Limits to the last 150 bars for visual clarity. Displays indicators (gauss, kijun, smma, adx, atr)
    and marks trades with long (^, lime), short (v, red), partial exit (o, orange, 40%), and full exit (o, orange).
    Supports both: - transactions df (columns: date, price, side) - trades_detailed df
    (columns: entry_date, exit_date, entry_price, exit_price). Called by backtest.py to
    visualize the strategy's performance as the final step in the ETL pipeline.
    """
    df = df_input.copy()
    df.columns = [c.lower() for c in df.columns]
    df = df.tail(150).set_index('date')

    add_plots: List[dict] = []

    # Indicators
    indicator_colors = {'gauss': 'blue', 'kijun': 'orange', 'smma': 'purple'}
    for name, color in indicator_colors.items():
        if name in df.columns:
            add_plots.append(mpf.make_addplot(df[name], color=color, panel=0))

    if 'atr' in df.columns and 'close' in df.columns:
        atr_mult = 3.0
        df['atr_upper'] = df['close'] + df['atr'] * atr_mult
        df['atr_lower'] = df['close'] - df['atr'] * atr_mult
        add_plots.append(mpf.make_addplot(df['atr_upper'], color='red', alpha=0.3, panel=0))
        add_plots.append(mpf.make_addplot(df['atr_lower'], color='lime', alpha=0.3, panel=0))
        logger.info("Added ATR bands to plot (trailing stop levels)")

    if 'adx' in df.columns:
        add_plots.append(mpf.make_addplot(df['adx'], color='cyan', panel=2, ylabel='ADX'))

    # Markers
    df['long_entry'] = np.nan
    df['short_entry'] = np.nan
    df['partial_exit'] = np.nan
    df['full_exit'] = np.nan

    if 'entry_date' in trades.columns and 'exit_date' in trades.columns:
        # trades_detailed-format
        trades = trades.copy()
        trades['entry_date'] = pd.to_datetime(trades['entry_date'])
        trades['exit_date'] = pd.to_datetime(trades['exit_date'])
        visible_start, visible_end = df.index.min(), df.index.max()
        trades = trades[(trades['entry_date'] >= visible_start) & (trades['entry_date'] <= visible_end)]

        for _, row in trades.iterrows():
            entry_dt, exit_dt = row['entry_date'], row['exit_date']
            is_long = row['entry_price'] < row['exit_price']

            if entry_dt in df.index:
                df.at[entry_dt, 'long_entry' if is_long else 'short_entry'] = (
                    df.at[entry_dt, 'low']*0.99 if is_long else df.at[entry_dt, 'high']*1.01
                )
            mid_dt = entry_dt + (exit_dt - entry_dt)/2
            if mid_dt in df.index:
                df.at[mid_dt, 'partial_exit'] = df.at[mid_dt, 'close']
            if exit_dt in df.index:
                df.at[exit_dt, 'full_exit'] = df.at[exit_dt, 'close']

    elif 'date' in trades.columns and 'side' in trades.columns:
        # transactions-format
        trades = trades.copy()
        trades['date'] = pd.to_datetime(trades['date'])
        trades = trades[trades['date'].between(df.index.min(), df.index.max())]

        current_pos = 0
        for _, row in trades.iterrows():
            dt, side = row['date'], row['side']
            if dt not in df.index:
                continue
            if side == 'buy':
                if current_pos == 0:
                    df.at[dt, 'long_entry'] = df.at[dt, 'low']*0.99
                else:
                    df.at[dt, 'partial_exit'] = df.at[dt, 'close']
                current_pos += 1
            else:
                current_pos -= 1
                if current_pos == 0:
                    df.at[dt, 'full_exit'] = df.at[dt, 'close']
                else:
                    df.at[dt, 'partial_exit'] = df.at[dt, 'close']

    # Addplots
    if not pd.isna(df['long_entry']).all():
        add_plots.append(mpf.make_addplot(df['long_entry'], type='scatter', marker='^', color='lime', markersize=100))
    if not pd.isna(df['short_entry']).all():
        add_plots.append(mpf.make_addplot(df['short_entry'], type='scatter', marker='v', color='red', markersize=100))
    if not pd.isna(df['partial_exit']).all():
        add_plots.append(mpf.make_addplot(df['partial_exit'], type='scatter', marker='o', color='orange', markersize=40, alpha=0.6))
    if not pd.isna(df['full_exit']).all():
        add_plots.append(mpf.make_addplot(df['full_exit'], type='scatter', marker='o', color='orange', markersize=120))

    try:
        mpf.plot(
            df, type='candle', style='charles',
            title=f"{symbol} - Backtest (Last 3 Days) - ATR Bands",
            ylabel='Price ($)', volume=True, panel_ratios=(3, 1, 1),
            addplot=add_plots, figscale=1.5,
            savefig=dict(fname=save_path, dpi=150)
        )
        logger.info(f"Trade plot saved to: {save_path} (Long^/Shortv/TP1/ExitO)")
    except Exception as e:
        logger.error(f"Failed to generate plot: {e}")
