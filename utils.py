"""Utility functions for FPL dataset builder."""

import time
from datetime import UTC, date, datetime
from pathlib import Path

import requests


def http_get(url: str, retries: int = 3, timeout: int = 30) -> bytes:
    """HTTP GET with retries."""
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise
            print(f"Retry {attempt + 1}/{retries} for {url}: {e}")
            time.sleep(2 ** attempt)
    return b""

def now_utc() -> datetime:
    """Current UTC datetime."""
    return datetime.now(UTC)

def ensure_data_dir():
    """Create data directory if it doesn't exist."""
    Path("data").mkdir(exist_ok=True)

def infer_last_completed_season() -> str:
    """Infer last completed season from current date."""
    today = date.today()

    # Premier League season typically runs Aug-May
    if today.month >= 8:  # Aug-Dec, current season is starting/ongoing
        end_year = today.year
        start_year = today.year - 1
    else:  # Jan-July, previous season just ended
        end_year = today.year
        start_year = today.year - 1

    return f"{start_year}-{end_year}"
