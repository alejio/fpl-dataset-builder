"""Vaastav GitHub data fetching functions."""

import pandas as pd

from utils import http_get


def download_vaastav_merged_gw(season_folder: str):
    """Download historical GW data from vaastav repo."""
    print(f"Downloading historical GW data for {season_folder}...")

    base_url = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"
    url = f"{base_url}/{season_folder}/gws/merged_gw.csv"

    try:
        data = http_get(url)
        with open("data/fpl_historical_gameweek_data.csv", "wb") as f:
            f.write(data)
        print("Downloaded fpl_historical_gameweek_data.csv")
    except Exception as e:
        print(f"Error downloading vaastav data: {e}")
        # Create empty CSV with some basic columns
        pd.DataFrame(columns=["name", "position", "team", "GW", "total_points"]).to_csv(
            "data/fpl_historical_gameweek_data.csv", index=False
        )
