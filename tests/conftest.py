"""Pytest configuration and fixtures for FPL dataset builder tests."""


def pytest_addoption(parser):
    """Add custom command line options for tests."""
    parser.addoption(
        "--last-gw",
        action="store",
        type=int,
        default=None,
        help="Override last completed gameweek for completeness tests (default: auto-detect)",
    )
