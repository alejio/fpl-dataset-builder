"""FPL API data fetching functions."""

import json

from utils import http_get


def fetch_fpl_bootstrap() -> dict:
    """Fetch FPL bootstrap data and save raw JSON."""
    print("Fetching FPL bootstrap data...")
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    data = http_get(url)

    bootstrap = json.loads(data)

    # Save raw JSON
    with open("data/fpl_raw_bootstrap.json", "w") as f:
        json.dump(bootstrap, f, indent=2)

    return bootstrap


def fetch_fpl_fixtures() -> list[dict]:
    """Fetch FPL fixtures and save raw JSON."""
    print("Fetching FPL fixtures...")
    url = "https://fantasy.premierleague.com/api/fixtures/"
    data = http_get(url)

    fixtures = json.loads(data)

    # Save raw JSON
    with open("data/fpl_raw_fixtures.json", "w") as f:
        json.dump(fixtures, f, indent=2)

    return fixtures
