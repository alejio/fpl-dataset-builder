"""Utility functions for FPL dataset builder."""

import os
import time
from datetime import UTC, datetime
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
            time.sleep(2**attempt)
    return b""


def now_utc() -> datetime:
    """Current UTC datetime."""
    return datetime.now(UTC)


def ensure_data_dir():
    """Create data directory if it doesn't exist."""
    Path("data").mkdir(exist_ok=True)


def get_my_manager_id() -> int | None:
    """Get MY_MANAGER_ID from environment variable or return default (4233026)."""
    manager_id = os.getenv("MY_MANAGER_ID")
    if manager_id:
        try:
            return int(manager_id)
        except ValueError:
            print(f"Warning: MY_MANAGER_ID '{manager_id}' is not a valid integer, using default")
            return 4233026
    return 4233026
