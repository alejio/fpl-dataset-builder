"""Live FPL data fetching functions."""


def get_current_gameweek(bootstrap_data: dict) -> tuple[int, bool]:
    """Extract current gameweek number and whether it's finished from bootstrap data."""
    current_event = None
    is_finished = True

    for event in bootstrap_data.get("events", []):
        if event.get("is_current", False):
            current_event = event["id"]
            is_finished = event.get("finished", True)
            break

    return current_event or 1, is_finished
