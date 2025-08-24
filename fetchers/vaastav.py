"""Vaastav GitHub data fetching functions."""

import pandas as pd

from utils import http_get


def download_vaastav_merged_gw(season_folder: str) -> pd.DataFrame:
    """Download historical GW data from vaastav repo and return as DataFrame."""
    print(f"Downloading historical GW data for {season_folder}...")

    base_url = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"
    url = f"{base_url}/{season_folder}/gws/merged_gw.csv"

    try:
        from io import StringIO

        data = http_get(url)
        # Parse CSV data directly into DataFrame
        df = pd.read_csv(StringIO(data.decode("utf-8")))
        print(f"Downloaded vaastav historical data: {len(df)} rows")
        return df
    except Exception as e:
        print(f"Error downloading vaastav data: {e}")
        # Return empty DataFrame with basic columns
        return pd.DataFrame(columns=["name", "position", "team", "GW", "total_points"])
