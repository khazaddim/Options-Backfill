from __future__ import annotations

from pathlib import Path

import pandas as pd

import massive_options_helper as options_helper


def test_download_options_eod_uses_duckdb_cache_for_repeat_query(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(options_helper, "OPTIONS_CACHE_DB_PATH", tmp_path / "options_cache.duckdb")

    call_counter = {"count": 0}

    def fake_http_get_json(url: str, timeout: int = 30) -> dict[str, object]:
        call_counter["count"] += 1
        return {
            "data": [
                {
                    "id": "SPY250117P00450000-2025-01-03",
                    "type": "options-eod",
                    "attributes": {
                        "contract": "SPY250117P00450000",
                        "underlying_symbol": "SPY",
                        "exp_date": "2025-01-17",
                        "type": "put",
                        "strike": 450.0,
                        "bid": 1.1,
                        "ask": 1.3,
                        "midpoint": 1.2,
                        "tradetime": "2025-01-03",
                    },
                }
            ],
            "links": {"next": None},
        }

    monkeypatch.setattr(options_helper, "_http_get_json", fake_http_get_json)

    first = options_helper.download_options_eod(
        underlying_symbol="SPY",
        option_type="put",
        exp_date_eq="2025-01-17",
        tradetime_eq="2025-01-03",
        api_token="demo",
    )
    second = options_helper.download_options_eod(
        underlying_symbol="SPY",
        option_type="put",
        exp_date_eq="2025-01-17",
        tradetime_eq="2025-01-03",
        api_token="demo",
    )

    assert call_counter["count"] == 1
    assert len(first) == 1
    assert second.equals(first)
    assert second.loc[0, "contract"] == "SPY250117P00450000"


def test_download_options_eod_uses_massive_snapshot_shape_and_caches_repeat_query(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(options_helper, "OPTIONS_CACHE_DB_PATH", tmp_path / "options_cache.duckdb")

    requested_urls: list[str] = []

    def fake_http_get_json(url: str, timeout: int = 30) -> dict[str, object]:
        requested_urls.append(url)
        return {
            "results": [
                {
                    "details": {
                        "ticker": "SPY250117P00450000",
                        "contract_type": "put",
                        "expiration_date": "2025-01-17",
                        "strike_price": 450.0,
                    },
                    "underlying_asset": {"ticker": "SPY"},
                    "day": {"last_updated": 1735862400000000000},
                    "last_quote": {"midpoint": 1.2},
                },
                {
                    "details": {
                        "ticker": "SPY250117P00450000",
                        "contract_type": "put",
                        "expiration_date": "2025-01-17",
                        "strike_price": 450.0,
                    },
                    "underlying_asset": {"ticker": "SPY"},
                    "day": {"last_updated": 1736467200000000000},
                    "last_quote": {"midpoint": 0.9},
                },
            ],
            "next_url": None,
        }

    monkeypatch.setattr(options_helper, "_http_get_json", fake_http_get_json)

    initial = options_helper.download_options_eod(
        underlying_symbol="SPY",
        option_type="put",
        exp_date_eq="2025-01-17",
        tradetime_eq="2025-01-03",
        api_token="demo",
    )
    expanded = options_helper.download_options_eod(
        underlying_symbol="SPY",
        option_type="put",
        exp_date_eq="2025-01-17",
        tradetime_from="2025-01-03",
        tradetime_to="2025-01-10",
        api_token="demo",
    )
    cached = options_helper.download_options_eod(
        underlying_symbol="SPY",
        option_type="put",
        exp_date_eq="2025-01-17",
        tradetime_from="2025-01-03",
        tradetime_to="2025-01-10",
        api_token="demo",
    )

    assert len(initial) == 1
    assert len(expanded) == 2
    assert len(cached) == 2
    assert len(requested_urls) == 1
    assert cached["tradetime"].dt.strftime("%Y-%m-%d").tolist() == ["2025-01-03", "2025-01-10"]


def test_download_options_underlying_symbols_flattens_compact_response(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(options_helper, "OPTIONS_CACHE_DB_PATH", tmp_path / "options_cache.duckdb")

    def fake_http_get_json(url: str, timeout: int = 30) -> dict[str, object]:
        return {
            "data": ["AAPL", "MSFT", "SPY"],
            "links": {"next": None},
        }

    monkeypatch.setattr(options_helper, "_http_get_json", fake_http_get_json)

    result = options_helper.download_options_underlying_symbols(api_token="demo")

    assert result["underlying_symbol"].tolist() == ["AAPL", "MSFT", "SPY"]


def test_download_options_requires_api_token(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(options_helper, "OPTIONS_CACHE_DB_PATH", tmp_path / "options_cache.duckdb")

    try:
        options_helper.download_options_underlying_symbols()
    except ValueError as exc:
        assert "Massive API token is required" in str(exc)
    else:
        raise AssertionError("Expected ValueError when no API token is configured.")


def test_download_options_uses_token_from_explicit_json_file_and_key(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(options_helper, "OPTIONS_CACHE_DB_PATH", tmp_path / "options_cache.duckdb")

    token_file = tmp_path / "massive_key.json"
    token_file.write_text('{"my_massive_key":"demo_from_file"}', encoding="utf-8")

    requested_urls: list[str] = []

    def fake_http_get_json(url: str, timeout: int = 30) -> dict[str, object]:
        requested_urls.append(url)
        return {
            "data": ["AAPL"],
            "links": {"next": None},
        }

    monkeypatch.setattr(options_helper, "_http_get_json", fake_http_get_json)

    result = options_helper.download_options_underlying_symbols(
        api_token_file=str(token_file),
        api_token_key="my_massive_key",
    )

    assert result["underlying_symbol"].tolist() == ["AAPL"]
    assert len(requested_urls) == 1
    assert "apiKey=demo_from_file" in requested_urls[0]


def test_download_options_time_series_uses_massive_bars_endpoint_and_cache(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(options_helper, "OPTIONS_CACHE_DB_PATH", tmp_path / "options_cache.duckdb")

    requested_urls: list[str] = []

    def fake_http_get_json(url: str, timeout: int = 30) -> dict[str, object]:
        requested_urls.append(url)
        return {
            "ticker": "O:SPY250117P00450000",
            "results": [
                {
                    "o": 1.10,
                    "h": 1.20,
                    "l": 1.05,
                    "c": 1.15,
                    "t": 1735914600000,
                    "v": 120,
                    "vw": 1.14,
                },
                {
                    "o": 1.15,
                    "h": 1.18,
                    "l": 1.08,
                    "c": 1.10,
                    "t": 1735915500000,
                    "v": 90,
                    "vw": 1.12,
                },
            ],
            "next_url": None,
        }

    monkeypatch.setattr(options_helper, "_http_get_json", fake_http_get_json)

    first = options_helper.download_options_time_series(
        contract="O:SPY250117P00450000",
        range_from="2025-01-03",
        range_to="2025-01-03",
        multiplier=15,
        timespan="minute",
        api_token="demo",
    )
    second = options_helper.download_options_time_series(
        contract="O:SPY250117P00450000",
        range_from="2025-01-03",
        range_to="2025-01-03",
        multiplier=15,
        timespan="minute",
        api_token="demo",
    )

    assert len(requested_urls) == 1
    assert "/v2/aggs/ticker/O%3ASPY250117P00450000/range/15/minute/2025-01-03/2025-01-03" in requested_urls[0]
    assert "apiKey=demo" in requested_urls[0]
    assert len(first) == 2
    assert second.equals(first)
    assert first["contract"].unique().tolist() == ["O:SPY250117P00450000"]
    assert first["tradetime"].notna().all()


def test_oop_helper_resolves_json_token_once_and_reuses_it(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(options_helper, "OPTIONS_CACHE_DB_PATH", tmp_path / "options_cache.duckdb")

    load_counter = {"count": 0}

    def fake_load_api_token_from_file(file_path: Path, token_key: str) -> str | None:
        load_counter["count"] += 1
        assert str(file_path).endswith("massive_key.json")
        assert token_key == "my_massive_key"
        return "demo_from_file"

    requested_urls: list[str] = []

    def fake_http_get_json(url: str, timeout: int = 30) -> dict[str, object]:
        requested_urls.append(url)
        return {
            "data": ["AAPL"],
            "links": {"next": None},
        }

    monkeypatch.setattr(options_helper, "_load_api_token_from_file", fake_load_api_token_from_file)
    monkeypatch.setattr(options_helper, "_http_get_json", fake_http_get_json)

    helper = options_helper.MassiveOptionsHelper(
        api_token_file=str(tmp_path / "massive_key.json"),
        api_token_key="my_massive_key",
    )

    first = helper.download_options_underlying_symbols()
    second = helper.download_options_underlying_symbols()

    assert first["underlying_symbol"].tolist() == ["AAPL"]
    assert second.equals(first)
    assert load_counter["count"] == 1
    assert len(requested_urls) == 1
    assert "apiKey=demo_from_file" in requested_urls[0]
