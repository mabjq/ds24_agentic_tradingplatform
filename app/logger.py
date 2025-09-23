import logging
from pathlib import Path
from typing import Optional

def setup_logging(log_path: Path, level: str = "INFO") -> logging.Logger:
    """Set up centralized logging configuration."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path, mode="a"),
            logging.StreamHandler(),
        ],
        force=True,
    )
    return logging.getLogger(__name__)