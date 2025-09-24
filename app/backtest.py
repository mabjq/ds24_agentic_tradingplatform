from typing import Optional, Dict, Any
import backtrader as bt
import pandas as pd
import logging
from backtrader.utils import num2date
from config.config import AppConfig
from app.strategies import GaussianKijunStrategy
from app.visualize import plot_with_trades
from pathlib import Path

logger = logging.getLogger(__name__)


class TradeLogger(bt.Analyzer):
    """
    Custom analyzer to log detailed trade information (entry/exit, reason, PnL).
    Uses transaction history to extract actual entry and exit prices.
    Stores trade data in a list with fields like trade_id, entry_date, exit_date, PnL,
    and close_reason, handling both open and closed trades via notify_trade.
    """
    def __init__(self):
        self.trades: list[dict] = []
        self.trade_counter = 0
        self._open_trades: dict[int, dict] = {}

    def notify_trade(self, trade: bt.Trade) -> None:
        if trade.isopen and trade.ref not in self._open_trades:
            entry_date = num2date(trade.dtopen)
            self._open_trades[trade.ref] = {
                'entry_date': entry_date,
                'entry_price': trade.price,
                'size': abs(trade.size),
            }

        if trade.isclosed:
            self.trade_counter += 1
            entry_info = self._open_trades.pop(trade.ref, {})
            entry_price = entry_info.get('entry_price', trade.price)
            size = entry_info.get('size', abs(trade.size))
            entry_date = entry_info.get('entry_date', num2date(trade.dtopen))
            exit_date = num2date(trade.dtclose)
            exit_price = entry_price + (trade.pnl / size if size else 0)

            self.trades.append({
                'trade_id': self.trade_counter,
                'entry_date': entry_date,
                'exit_date': exit_date,
                'duration_bars': trade.barlen,
                'duration_hours': trade.barlen * 0.5,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'size': size,
                'pnl': trade.pnl,
                'pnl_comm': trade.pnlcomm,
                'pnl_percent': (trade.pnl / (entry_price * size)) * 100 if size else 0,
                'is_winner': trade.pnl > 0,
                'close_reason': getattr(self.strategy, 'close_reason', 'Unknown')
            })

    def get_analysis(self):
        return self.trades


class PandasDataExtended(bt.feeds.PandasData):
    """"Custom PandasData feed extending backtrader's feed to include indicator lines
    (gauss, kijun, vapi, smma, adx, atr, swing_high, swing_low) for use with
    GaussianKijunStrategy."""
    lines = ('gauss', 'kijun', 'vapi', 'smma', 'adx', 'atr', 'swing_high', 'swing_low')
    params = (
        ('datetime', None),
        ('open', 'Open'),
        ('high', 'High'),
        ('low', 'Low'),
        ('close', 'Close'),
        ('volume', 'Volume'),
        ('openinterest', None),
        ('gauss', 'gauss'),
        ('kijun', 'kijun'),
        ('vapi', 'vapi'),
        ('smma', 'smma'),
        ('adx', 'adx'),
        ('atr', 'atr'),
        ('swing_high', 'swing_high'),
        ('swing_low', 'swing_low'),
    )


def run_backtest(df: pd.DataFrame, config: Optional[AppConfig] = None) -> Dict[str, Any]:
    """
    Runs a backtest using a DataFrame with OHLCV and indicators.
    Takes a DataFrame `df` and optional `config` (AppConfig) to set parameters.
    Saves input to 'backtest_input.csv', generates a plot, and exports summary/trade
    details to CSV. Returns a dict with metrics like final_value, pnl, and winrate.
    """
    if config is None:
        config = AppConfig()

    # Save transformed data to CSV
    csv_path = "results/reports/backtest_input.csv"
    Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    logger.info(f"Saved backtest input to {csv_path} ({len(df)} rows)")

    # Prepare DataFrame
    df_in = df.copy()
    if 'Date' in df_in.columns:
        df_in = df_in.set_index('Date')
    if not isinstance(df_in.index, pd.DatetimeIndex):
        df_in.index = pd.to_datetime(df_in.index)

    # Validate required columns
    required = ['Open', 'High', 'Low', 'Close', 'Volume', 'gauss', 'kijun', 'vapi', 'smma', 'adx', 'atr', 'swing_high', 'swing_low']
    missing = [c for c in required if c not in df_in.columns]
    if missing:
        raise ValueError(f"Missing required columns for backtest: {missing}")

    # Setup Backtrader
    cerebro = bt.Cerebro(stdstats=True)
    cerebro.broker.setcash(config.trading.starting_equity)
    cerebro.broker.setcommission(commission=0.0)

    # Add data feed
    feed = PandasDataExtended(dataname=df_in)
    cerebro.adddata(feed)

    # Add strategy and analyzers
    cerebro.addstrategy(GaussianKijunStrategy, app_config=config)
    cerebro.addanalyzer(bt.analyzers.Transactions, _name='transactions')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trade_analyzer')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(TradeLogger, _name='trade_logger')

    # Run backtest
    logger.info("Starting backtest...")
    results = cerebro.run()
    strat = results[0]

    # Extract metrics
    final_value = cerebro.broker.getvalue()
    pnl = final_value - config.trading.starting_equity
    pnl_percent = (pnl / config.trading.starting_equity) * 100

    drawdown = strat.analyzers.drawdown.get_analysis()
    max_drawdown_percent = drawdown.get('max', {}).get('drawdown', 0.0)

    trade_analyzer = strat.analyzers.trade_analyzer.get_analysis()
    total_trades = trade_analyzer.get('total', {}).get('closed', 0)
    won_trades = trade_analyzer.get('won', {}).get('total', 0)
    percent_profitable = (won_trades / total_trades * 100) if total_trades > 0 else 0.0
    profit_factor = (trade_analyzer.get('won', {}).get('pnl', {}).get('total', 0) /
                     abs(trade_analyzer.get('lost', {}).get('pnl', {}).get('total', 0))) if trade_analyzer.get('lost', {}).get('pnl', {}).get('total', 0) != 0 else float('inf')

    summary = {
        "final_value": final_value,
        "pnl": pnl,
        "pnl_percent": pnl_percent,
        "max_drawdown_percent": max_drawdown_percent,
        "total_trades": total_trades,
        "percent_profitable": percent_profitable,
        "profit_factor": profit_factor,
    }

    # Save summary to CSV
    summary_df = pd.DataFrame([summary])
    summary_csv_path = "results/reports/backtest_summary.csv"
    summary_df.to_csv(summary_csv_path, index=False)

    # Detailed trades
    trade_logger = strat.analyzers.trade_logger.get_analysis()
    if trade_logger:
        trades_detailed_df = pd.DataFrame(trade_logger)
        trades_detailed_path = "results/reports/trades_detailed.csv"
        trades_detailed_df.to_csv(trades_detailed_path, index=False)

    # Transactions for plot (filtered to entry/exit pairs)
    transactions = strat.analyzers.transactions.get_analysis()
    trade_list = []
    for dt, trans in transactions.items():
        dt_converted = num2date(dt) if isinstance(dt, (int, float)) else pd.to_datetime(dt)
        for t in trans:
            price, size = t[0], t[1]
            trade_list.append({'date': dt_converted, 'price': price, 'side': 'buy' if size > 0 else 'sell'})
    trades_df = pd.DataFrame(trade_list)

    if not trades_df.empty:
        trades_df = trades_df.sort_values("date")
        clean_rows = []
        position = 0
        for _, row in trades_df.iterrows():
            side = row["side"]
            if side == "buy":
                if position == 0:
                    clean_rows.append(row)
                position += 1
            else:  # sell
                position -= 1
                if position == 0:
                    clean_rows.append(row)
        trades_df = pd.DataFrame(clean_rows)

    if not trades_df.empty:
        plot_with_trades(
            df_input=df,
            trades=trades_df,
            symbol=config.trading.ticker,
            save_path="results/plots/backtest_chart.png"
        )

    return summary


if __name__ == "__main__":
    import argparse
    from app.logger import setup_logging

    parser = argparse.ArgumentParser(description="Run backtest on a CSV file with indicators")
    parser.add_argument("--csv", "-c", required=True, help="CSV file path with indicator columns")
    args = parser.parse_args()
    config = AppConfig()
    setup_logging(log_path=config.logging.app_log_path, level=config.logging.log_level)

    df = pd.read_csv(args.csv, parse_dates=['Date'])
    summary = run_backtest(df, config=config)
    print(summary)
