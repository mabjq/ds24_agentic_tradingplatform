from typing import Optional
import backtrader as bt
import datetime
import logging
import math
import pandas as pd
from config.config import AppConfig

logger = logging.getLogger(__name__)

class GaussianKijunStrategy(bt.Strategy):
    """
    Gaussian + Kijun + VAPI + ATR + SMMA200 strategy for Backtrader.
    Uses either fixed USD position sizing (config.trading.fixed_position_size)
    or risk-based sizing (config.trading.risk_pct, 0.9% per trade) if fixed size is 0.
    Includes custom breakeven (+0.4R), ATR trailing stop (ATR*3), TP1 partial exit (40% at 0.75R),
    and trendbreak exit logic. Relies on indicators from indicators.py, executed in
    backtest.py as part of the analysis step in the ETL pipeline.
    """

    params = (
        ("app_config", AppConfig()),
    )

    def __init__(self) -> None:
        """Initiate and prepare variables."""
        cfg: AppConfig = self.p.app_config
        self.cfg = cfg.trading

        # Data feed with extra lines for indicators
        self.data_extras = self.datas[0]

        # Trade management
        self.today = None
        self.trades_today = 0
        self.entry_price: Optional[float] = None
        self.stop_price: Optional[float] = None
        self.initial_atr: Optional[float] = None
        self.entry_risk: Optional[float] = None
        self.tp_price: Optional[float] = None
        self.breakeven_active = False
        self.highest_since_entry: Optional[float] = None
        self.lowest_since_entry: Optional[float] = None
        self.close_reason = ""
        self.entry_order = None
        self.exit_order = None

    def log(self, txt: str, dt: Optional[datetime.datetime] = None) -> None:
        dt = dt or self.data.datetime.datetime(0)
        logger.debug(f"{dt.isoformat()} - {txt}")

    def next(self) -> None:
        """Execute strategy logic on each bar.
        Evaluates entry/exit conditions using indicators (gauss, kijun, vapi, adx, smma)
        and manages trade limits (max_trades_per_day) from config.
        """
        dt = self.data.datetime.datetime(0)
        if self.today is None or dt.date() != self.today:
            self.today = dt.date()
            self.trades_today = 0

        if len(self.data) < self.cfg.min_bars:
            return

        # Get indicators
        try:
            close = float(self.data.close[0])
            high = float(self.data.high[0])
            low = float(self.data.low[0])
            gauss = float(self.data_extras.gauss[0])
            gauss_prev = float(self.data_extras.gauss[-1])
            kijun = float(self.data_extras.kijun[0])
            vapi = float(self.data_extras.vapi[0])
            vapi_prev = float(self.data_extras.vapi[-1])
            adx = float(self.data_extras.adx[0])
            smma = float(self.data_extras.smma[0])
            atr = float(self.data_extras.atr[0])
            swing_low = float(self.data_extras.swing_low[0])
            swing_high = float(self.data_extras.swing_high[0])
        except (ValueError, TypeError):
            return

        if pd.isna(adx) or adx <= self.cfg.adx_threshold:
            return

        if self.trades_today >= self.cfg.max_trades_per_day:
            return

        gauss_up = gauss > gauss_prev
        vapi_up = vapi > vapi_prev

        if not self.position:
            self.exit_order = None
            self.close_reason = ""

            # LONG entry
            if gauss_up and vapi_up and close > smma and close > kijun and swing_low < close:
                size = self._determine_size(close, swing_low)
                if size > 0:
                    self._enter_long(close, size, swing_low, atr)
                    return

            # SHORT entry
            if not gauss_up and not vapi_up and close < smma and close < kijun and swing_high > close:
                size = self._determine_size(close, swing_high, short=True)
                if size > 0:
                    self._enter_short(close, size, swing_high, atr)
                    return
        else:
            self._update_position_management(close, high, low, kijun)

            if self.position.size > 0 and close < kijun and not self.close_reason:
                self.close()
                self.close_reason = "Trendbreak LONG (close under Kijun)"
                self.log(self.close_reason)
            elif self.position.size < 0 and close > kijun and not self.close_reason:
                self.close()
                self.close_reason = "Trendbreak SHORT (close over Kijun)"
                self.log(self.close_reason)

    def _determine_size(self, entry: float, stop: float, short: bool = False) -> int:
        """Return contract size using fixed USD size or risk-based sizing."""
        if self.cfg.fixed_position_size > 0:
            usd = self.cfg.fixed_position_size
            return max(1, int(usd / entry))
        return self.calculate_size(entry, stop, short)

    def _enter_long(self, close: float, size: int, stop: float, atr: float) -> None:
        """Initiate a long position with specified size, stop-loss, and take-profit.
        Sets custom TP1 (0.75R) and tracks initial ATR for trailing.
        """
        self.entry_order = self.buy(size=size)
        self.entry_price = close
        self.stop_price = stop
        self.initial_atr = atr
        self.entry_risk = close - stop
        self.tp_price = close + self.cfg.tp_r_multiple * self.entry_risk
        self.breakeven_active = False
        self.highest_since_entry = close
        self.trades_today += 1
        self.log(f"LONG ENTRY: {size}@{close:.2f} SL={stop:.2f} TP={self.tp_price:.2f}")

    def _enter_short(self, close: float, size: int, stop: float, atr: float) -> None:
        """Initiate a short position with specified size, stop-loss, and take-profit.
        Sets custom TP1 (0.5R) and tracks initial ATR for trailing.
        """
        self.entry_order = self.sell(size=size)
        self.entry_price = close
        self.stop_price = stop
        self.initial_atr = atr
        self.entry_risk = stop - close
        self.tp_price = close - 0.5 * self.entry_risk
        self.breakeven_active = False
        self.lowest_since_entry = close
        self.trades_today += 1
        self.log(f"SHORT ENTRY: {size}@{close:.2f} SL={stop:.2f} TP={self.tp_price:.2f}")

    def _update_position_management(self, close: float, high: float, low: float, kijun_v: float) -> None:
        """Updates stop for breakeven and trailing.
        Implements custom breakeven (+0.4R), ATR trailing stop (ATR*3), and TP1 (40% partial).
        """
        if self.entry_price is None or self.stop_price is None or self.entry_risk is None:
            return

        if self.position.size > 0:  # Long position
            # Update highest since entry
            self.highest_since_entry = max(self.highest_since_entry or self.entry_price, high)
            
            # Breakeven at +0.4R
            be_price = self.entry_price + 0.4 * self.entry_risk
            if close >= be_price and not self.breakeven_active:
                self.stop_price = self.entry_price
                self.breakeven_active = True
                self.log(f"Breakeven activated for LONG at {self.stop_price:.2f}")

            # Trailing stop: highest_since_entry - ATR * 3
            if self.initial_atr is not None:
                trail_stop = self.highest_since_entry - self.initial_atr * self.cfg.trailing_atr_mult
                if trail_stop > self.stop_price:
                    self.stop_price = trail_stop
                    self.log(f"Trailing stop updated for LONG to {self.stop_price:.2f} (high={self.highest_since_entry:.2f})")

            # Check TP (0.75R) - use limit order if not already placed
            if self.tp_price is not None and high >= self.tp_price and self.exit_order is None:
                # Partial close: 40% at TP
                tp_size = int(math.floor(abs(self.position.size) * 0.4))
                if tp_size > 0:
                    self.exit_order = self.sell(size=tp_size, exectype=bt.Order.Limit, price=self.tp_price)
                    self.log(f"TP1 order placed for LONG: {tp_size} contracts at {self.tp_price:.2f}")

            # Stop loss check
            if close <= self.stop_price and self.close_reason == "":
                self.close()
                self.close_reason = f"Stop loss LONG at {self.stop_price:.2f}"
                self.log(self.close_reason)

        else:  # Short position
            # Update lowest since entry
            self.lowest_since_entry = min(self.lowest_since_entry or self.entry_price, low)
            
            # Breakeven at -0.4R
            be_price = self.entry_price - 0.4 * self.entry_risk
            if close <= be_price and not self.breakeven_active:
                self.stop_price = self.entry_price
                self.breakeven_active = True
                self.log(f"Breakeven activated for SHORT at {self.stop_price:.2f}")

            # Trailing stop: lowest_since_entry + ATR * 3
            if self.initial_atr is not None:
                trail_stop = self.lowest_since_entry + self.initial_atr * self.cfg.trailing_atr_mult
                if trail_stop < self.stop_price:
                    self.stop_price = trail_stop
                    self.log(f"Trailing stop updated for SHORT to {self.stop_price:.2f} (low={self.lowest_since_entry:.2f})")

            # Check TP (0.5R for short)
            if self.tp_price is not None and low <= self.tp_price and self.exit_order is None:
                # Partial close: 40% at TP
                tp_size = int(math.floor(abs(self.position.size) * 0.4))
                if tp_size > 0:
                    self.exit_order = self.buy(size=tp_size, exectype=bt.Order.Limit, price=self.tp_price)
                    self.log(f"TP1 order placed for SHORT: {tp_size} contracts at {self.tp_price:.2f}")

            # Stop loss check
            if close >= self.stop_price and self.close_reason == "":
                self.close()
                self.close_reason = f"Stop loss SHORT at {self.stop_price:.2f}"
                self.log(self.close_reason)

    def notify_order(self, order) -> None:
        if order.status == order.Completed:
            if order.isbuy():
                self.log(f"BUY EXECUTED, Price: {order.executed.price:.2f}, Size: {order.executed.size}")
            elif order.issell():
                self.log(f"SELL EXECUTED, Price: {order.executed.price:.2f}, Size: {order.executed.size}")

    def notify_trade(self, trade) -> None:
        if trade.isclosed:
            self.log(f"TRADE CLOSED: PnL Gross {trade.pnl:.2f}, Net {trade.pnlcomm:.2f}, Reason: {self.close_reason if self.close_reason else 'Unknown'}")
            self.close_reason = ""  # Reset for next trade

    def calculate_size(self, entry: float, stop: float, short: bool = False) -> int:
        """Calculate position size based on risk."""
        equity = self.broker.getvalue()
        risk_amount = equity * self.cfg.risk_pct
        distance = abs(entry - stop)
        if distance <= 0:
            return 0
        raw_size = risk_amount / (distance * self.cfg.contract_multiplier)
        return max(0, int(math.floor(raw_size)))
