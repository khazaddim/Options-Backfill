from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add opt-in live Massive test options."""
    parser.addoption(
        "--massive-live",
        action="store_true",
        default=False,
        help="Run live Massive integration tests.",
    )
    parser.addoption(
        "--massive-token-file",
        action="store",
        default="",
        help="Full path to JSON file containing the Massive API token.",
    )
    parser.addoption(
        "--massive-token-key",
        action="store",
        default="",
        help="Exact top-level JSON key in --massive-token-file for Massive API token.",
    )
