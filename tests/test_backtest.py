""" tests/test_backtest.py: not finished."""

import pytest
import pandas as pd
from pathlib import Path
from app.backtest import run_backtest
from config.config import AppConfig
from unittest.mock import patch, MagicMock

@pytest.fixture
def mock_df() -> pd.DataFrame:
    """Fixture for mock OHLCV + indicators."""
    dates = pd.date_range(start='2025-01-01', periods=10, freq='30min')
    return pd.DataFrame({
        'Date': dates,
        'Open': [100.0] * 10,
        'High': [105.0] * 10,
        'Low': [95.0] * 10,
        'Close': [102.0] * 10,
        'Volume': [1000] * 10,
        'gauss': [101.0] * 10,
        'kijun': [100.5] * 10,
        'vapi': [101.5] * 10,
        'smma': [100.0] * 10,
        'adx': [30.0] * 10,
        'atr': [2.0] * 10,
        'swing_high': [105.0] * 10,
        'swing_low': [95.0] * 10
    })

@pytest.mark.skip(reason="Backtest function is not testable yet.")
@patch('app.backtest.plot_with_trades')
def test_run_backtest_success_isolated(mock_savefig: MagicMock, mock_plot: MagicMock, mock_df: pd.DataFrame, tmp_path: Path) -> None:
    """Test run_backtest completes without affecting production files."""
    config = AppConfig()

    output_dir = tmp_path / "backtest_results"
    
    with patch('app.backtest.save_results') as mock_save_results:
        mock_save_results.return_value = None
        
        summary = run_backtest(mock_df, config)

        assert 'final_value' in summary
        assert summary['total_trades'] >= 0
        assert summary['pnl_percent'] is not None

        mock_plot.assert_called_once()
        mock_save_results.assert_called_once()