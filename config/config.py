from pathlib import Path
from typing import Optional, Tuple
from pydantic import BaseModel, ConfigDict, FilePath

class TradingConfig(BaseModel):
    """Configuration for trading parameters."""
    ticker: str = "KC=F"  # Coffee futures
    timeframe: str = "30m"  # 30-minute timeframe
    gaussian_period: int = 34
    kijun_period: int = 125
    vapi_period: int = 13
    adx_period: int = 14
    atr_period: int = 14
    smma_period: int = 200
    tp_r_multiple: float = 0.75  # For TP1 (long)
    trailing_atr_mult: float = 3.0  # For TP2
    lookback_days: int = 60  # Days for data fetch
    adx_threshold: int = 25  # ADX threshold 
    swing_order: int = 55  # Lookback for swing high/low (initial Stop Loss)
    risk_pct: float = 0.009  # 0.9% risk per trade
    max_trades_per_day: int = 5
    min_bars: int = 200  # Minimum bars before trading in backtest
    contract_multiplier: float = 1.0  # Value per price point per contract
    starting_equity: float = 100000.0
    fixed_position_size: float = 20000.0

    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        arbitrary_types_allowed=True
    )

class DatabaseConfig(BaseModel):
    """Configuration for database settings."""
    db_path: FilePath = Path("data/trading.db")

    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        arbitrary_types_allowed=True
    )

class APIConfig(BaseModel):
    """Configuration for external APIs."""
    xai_api_key: Optional[str] = None
    twitter_api_key: Optional[str] = None

    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        arbitrary_types_allowed=True
    )

class LoggingConfig(BaseModel):
    """Configuration for logging."""
    app_log_path: FilePath = Path("logs/app.log")
    log_level: str = "INFO"  

    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        arbitrary_types_allowed=True
    )

class AppConfig(BaseModel):
    """Main application configuration."""
    trading: TradingConfig = TradingConfig()
    database: DatabaseConfig = DatabaseConfig()
    api: APIConfig = APIConfig()
    logging: LoggingConfig = LoggingConfig()

    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        arbitrary_types_allowed=True
    )

def load_config() -> AppConfig:
    """Load and validate application configuration."""
    return AppConfig()