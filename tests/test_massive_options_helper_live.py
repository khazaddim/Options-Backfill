from __future__ import annotations

import os

import pandas as pd
import pytest

import massive_options_helper as options_helper


def _require_live_massive_tests() -> str:
    """Return Massive token if live tests are explicitly enabled, else skip."""
    if os.getenv("MASSIVE_LIVE_TESTS") != "1":
        pytest.skip("Set MASSIVE_LIVE_TESTS=1 to run live Massive integration tests.")

    token = os.getenv("MASSIVE_API_TOKEN")
    if not token:
        pytest.skip("Set MASSIVE_API_TOKEN to run live Massive integration tests.")

    return token


def test_live_download_options_contracts_returns_shape() -> None:
    token = _require_live_massive_tests()

    frame = options_helper.download_options_contracts(
        underlying_symbol="SPY",
        option_type="put",
        page_limit=25,
        api_token=token,
    )

    assert isinstance(frame, pd.DataFrame)
    assert "contract" in frame.columns
    assert "underlying_symbol" in frame.columns
    assert "option_type" in frame.columns


def test_live_download_options_time_series_accepts_real_contract() -> None:
    token = _require_live_massive_tests()

    contracts = options_helper.download_options_contracts(
        underlying_symbol="SPY",
        option_type="put",
        page_limit=10,
        api_token=token,
    )
    assert not contracts.empty

    contract = str(contracts.iloc[0]["contract"])
    bars = options_helper.download_options_time_series(
        contract=contract,
        range_from="2025-01-01",
        range_to="2025-01-10",
        multiplier=15,
        timespan="minute",
        api_token=token,
    )

    assert isinstance(bars, pd.DataFrame)
    # Some contracts/time windows can legitimately have no bars.
    # Validate shape compatibility rather than forcing non-empty results.
    expected_columns = {"contract", "tradetime", "open", "high", "low", "last", "volume"}
    assert expected_columns.issubset(set(bars.columns)) or bars.empty
