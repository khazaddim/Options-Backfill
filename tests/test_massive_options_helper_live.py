from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import pytest

import massive_options_helper as options_helper


@dataclass(frozen=True)
class LiveMassiveAuth:
    """Credential reference for live Massive tests."""

    token_file: str
    token_key: str


def _require_live_massive_tests(request: pytest.FixtureRequest) -> LiveMassiveAuth:
    """Return explicit file/key auth inputs for live tests, else skip."""
    enabled = bool(request.config.getoption("--massive-live", default=False))
    if not enabled:
        pytest.skip("Pass --massive-live to run live Massive integration tests.")

    token_file = request.config.getoption("--massive-token-file", default="")
    token_key = request.config.getoption("--massive-token-key", default="")
    if not token_file or not token_key:
        pytest.skip("Pass both --massive-token-file and --massive-token-key for live tests.")

    return LiveMassiveAuth(token_file=str(token_file), token_key=str(token_key))


def test_live_download_options_contracts_returns_shape(request: pytest.FixtureRequest) -> None:
    auth = _require_live_massive_tests(request)

    frame = options_helper.download_options_contracts(
        underlying_symbol="SPY",
        option_type="put",
        page_limit=25,
        api_token_file=auth.token_file,
        api_token_key=auth.token_key,
    )

    assert isinstance(frame, pd.DataFrame)
    assert "contract" in frame.columns
    assert "underlying_symbol" in frame.columns
    assert "option_type" in frame.columns


def test_live_download_options_time_series_accepts_real_contract(request: pytest.FixtureRequest) -> None:
    auth = _require_live_massive_tests(request)

    contracts = options_helper.download_options_contracts(
        underlying_symbol="SPY",
        option_type="put",
        page_limit=10,
        api_token_file=auth.token_file,
        api_token_key=auth.token_key,
    )
    assert not contracts.empty

    contract = str(contracts.iloc[0]["contract"])
    bars = options_helper.download_options_time_series(
        contract=contract,
        range_from="2025-01-01",
        range_to="2025-01-10",
        multiplier=15,
        timespan="minute",
        api_token_file=auth.token_file,
        api_token_key=auth.token_key,
    )

    assert isinstance(bars, pd.DataFrame)
    # Some contracts/time windows can legitimately have no bars.
    # Validate shape compatibility rather than forcing non-empty results.
    expected_columns = {"contract", "tradetime", "open", "high", "low", "last", "volume"}
    assert expected_columns.issubset(set(bars.columns)) or bars.empty
