from __future__ import annotations

"""Massive historical options adapter with DuckDB-backed query caching.

Public functions:
- download_options_underlying_symbols: Return the list of supported optionable
    underlying symbols from the Massive marketplace API.
- download_options_contracts: Return filtered option contract metadata and
    latest contract-level fields for a symbol, expiry, strike range, or type.
- download_options_eod: Return historical end-of-day option records, including
    prices, midpoint, IV, Greeks, and related contract fields.

Internal helpers are grouped by responsibility:
- date normalization helpers keep cache checks and API filter handling
    consistent.
- DuckDB cache helpers read, merge, and persist query results locally.
- query and HTTP helpers shape endpoint-specific parameters and fetch paged
    JSON responses.
- _download_collection_cached provides the shared read-through caching flow
    used by each public endpoint wrapper.

TODO before first live production use:
- Add a client-side rate limiter, likely with a process-wide semaphore or token
    bucket, so paginated backfills cannot burst through vendor minute limits.
- Add retry handling for transient failures, especially HTTP 429 and temporary
    network errors.
- Add exponential backoff with jitter so retries do not hammer the API.
- Add request logging and basic counters so session-level quota usage is
    visible during research runs.
- Add an optional max_pages guardrail to stop unexpectedly large paginated
    pulls before they consume too much quota.
"""

import hashlib
import json
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

from pandas import Timedelta, Timestamp

import pandas as pd

try:
    import duckdb
except ImportError:
    duckdb = None


OPTIONS_API_BASE_URL = "https://api.massive.com"
OPTIONS_CACHE_DB_PATH = Path(__file__).with_name("options_api_cache.duckdb")
DuckDBConnection = Any
DEFAULT_PAGE_LIMIT = 1000
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30
DATE_COLUMNS = {"exp_date", "tradetime", "bid_date", "ask_date", "previous_date"}
DATE_FILTER_KEYS = {"filter[tradetime_eq]", "filter[tradetime_from]", "filter[tradetime_to]"}


def _normalize_date(value: Optional[str], label: str) -> Optional[Timestamp]:
    """Parse an optional date string into a normalized pandas timestamp."""
    if value is None:
        return None

    try:
        return pd.to_datetime(value).normalize()
    except (ValueError, TypeError) as exc:
        raise ValueError(f"{label} must be a valid date string (YYYY-MM-DD).") from exc


def _normalize_any_timestamp(value: object) -> Optional[Timestamp]:
    """Parse date strings or epoch milliseconds into normalized timestamps."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = pd.to_datetime(value, unit="ms", errors="coerce")
    else:
        ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return _normalize_timestamp_for_compare(ts)


def _normalize_timestamp_for_compare(value: Optional[Timestamp]) -> Optional[Timestamp]:
    """Convert a timestamp into a normalized tz-naive value for comparisons."""
    if value is None:
        return None

    if value.tzinfo is not None:
        value = value.tz_convert("UTC").tz_localize(None)

    return value.normalize()


def _normalize_date_series_for_compare(values: pd.Series) -> pd.Series:
    """Normalize a datetime-like series into tz-naive midnight timestamps."""
    normalized = pd.to_datetime(values)
    if getattr(normalized.dt, "tz", None) is not None:
        normalized = normalized.dt.tz_convert("UTC").dt.tz_localize(None)
    return normalized.dt.normalize()


def _quote_identifier(identifier: str) -> str:
    """Quote a DuckDB identifier so cache table names are safe in SQL."""
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _table_exists(connection: DuckDBConnection, table_name: str) -> bool:
    """Return whether a DuckDB cache table exists in the local database."""
    result = connection.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return result is not None


def _read_cached_table(connection: DuckDBConnection, table_name: str) -> pd.DataFrame:
    """Load a cached query result table from DuckDB into a pandas DataFrame."""
    quoted_table_name = _quote_identifier(table_name)
    data = connection.execute(f"SELECT * FROM {quoted_table_name}").fetchdf()
    for column in DATE_COLUMNS | {"__cache_date"}:
        if column in data.columns:
            data[column] = pd.to_datetime(data[column], errors="coerce")
    return data


def _store_cached_table(connection: DuckDBConnection, table_name: str, data: pd.DataFrame) -> None:
    """Persist a normalized query result DataFrame into DuckDB."""
    quoted_table_name = _quote_identifier(table_name)
    prepared = data.copy()
    for column in DATE_COLUMNS | {"__cache_date"}:
        if column in prepared.columns:
            prepared[column] = pd.to_datetime(prepared[column], errors="coerce")

    connection.register("cache_frame", prepared)
    try:
        connection.execute(f"CREATE OR REPLACE TABLE {quoted_table_name} AS SELECT * FROM cache_frame")
    finally:
        connection.unregister("cache_frame")


def _merge_cached_data(cached_data: pd.DataFrame, fresh_data: pd.DataFrame) -> pd.DataFrame:
    """Merge cached and freshly fetched rows into one de-duplicated frame."""
    if cached_data.empty:
        return fresh_data.reset_index(drop=True)
    if fresh_data.empty:
        return cached_data.reset_index(drop=True)

    combined = pd.concat([cached_data, fresh_data], ignore_index=True, sort=False)
    dedupe_columns = [column for column in ["__cache_row_id"] if column in combined.columns]
    if dedupe_columns:
        combined = combined.drop_duplicates(subset=dedupe_columns, keep="last")
    else:
        combined = combined.drop_duplicates(keep="last")

    if "__cache_date" in combined.columns:
        combined = combined.sort_values(["__cache_date", "__cache_row_id"], na_position="last")
    return combined.reset_index(drop=True)


def _resolve_api_token(
    api_token: Optional[str],
    api_token_file: Optional[str | Path],
    api_token_key: Optional[str],
) -> str:
    """Resolve the Massive API token from explicit function arguments only."""
    if api_token:
        return api_token

    if api_token_file or api_token_key:
        if not api_token_file or not api_token_key:
            raise ValueError("Both api_token_file and api_token_key are required together.")
        resolved_token = _load_api_token_from_file(Path(api_token_file), api_token_key)
        if resolved_token:
            return resolved_token

    raise ValueError(
        "A Massive API token is required. Pass api_token=... or pass both "
        "api_token_file=... and api_token_key=...."
    )


def _load_api_token_from_file(file_path: Path, token_key: str) -> Optional[str]:
    """Load a Massive API token from one explicit JSON file and key."""
    if not file_path.exists() or not file_path.is_file():
        return None

    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    if not isinstance(payload, dict):
        return None

    token_value = payload.get(token_key)
    if isinstance(token_value, str) and token_value.strip():
        return token_value.strip()
    return None


def _normalize_fields(fields: Optional[list[str] | tuple[str, ...] | str], field_name: str) -> Optional[str]:
    """Normalize a field selection input into the comma-separated API format."""
    if fields is None:
        return None
    if isinstance(fields, str):
        return fields
    cleaned_fields = [str(field).strip() for field in fields if str(field).strip()]
    if not cleaned_fields:
        return None
    return ",".join(cleaned_fields)


def _build_contract_query_params(
    *,
    contract: Optional[str] = None,
    underlying_symbol: Optional[str] = None,
    exp_date_eq: Optional[str] = None,
    exp_date_from: Optional[str] = None,
    exp_date_to: Optional[str] = None,
    tradetime_eq: Optional[str] = None,
    tradetime_from: Optional[str] = None,
    tradetime_to: Optional[str] = None,
    option_type: Optional[str] = None,
    strike_eq: Optional[float] = None,
    strike_from: Optional[float] = None,
    strike_to: Optional[float] = None,
    sort: Optional[str] = None,
    fields: Optional[list[str] | tuple[str, ...] | str] = None,
    compact: bool = False,
    page_limit: int = DEFAULT_PAGE_LIMIT,
) -> dict[str, Any]:
    """Build generic helper query parameters before endpoint-specific translation."""
    params: dict[str, Any] = {
        "filter[contract]": contract,
        "filter[underlying_symbol]": underlying_symbol,
        "filter[exp_date_eq]": exp_date_eq,
        "filter[exp_date_from]": exp_date_from,
        "filter[exp_date_to]": exp_date_to,
        "filter[tradetime_eq]": tradetime_eq,
        "filter[tradetime_from]": tradetime_from,
        "filter[tradetime_to]": tradetime_to,
        "filter[type]": option_type,
        "filter[strike_eq]": strike_eq,
        "filter[strike_from]": strike_from,
        "filter[strike_to]": strike_to,
        "sort": sort,
        "compact": int(compact),
        "page[limit]": min(max(int(page_limit), 1), DEFAULT_PAGE_LIMIT),
    }

    normalized_fields = _normalize_fields(fields, "fields")
    if normalized_fields is not None:
        params["fields"] = normalized_fields
    return {key: value for key, value in params.items() if value is not None}


def _translate_params_for_endpoint(endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
    """Translate helper-style params into Massive endpoint query parameters."""
    translated: dict[str, Any] = {}

    for key, value in params.items():
        if key in {"page[offset]", "compact"}:
            continue

        if endpoint == "underlying-symbols":
            # Massive does not have an options-underlying index endpoint. We use
            # the reference ticker index and return active US stock tickers.
            if key == "page[limit]":
                translated["limit"] = value
            continue

        if endpoint == "contracts":
            mapping = {
                "filter[contract]": "ticker",
                "filter[underlying_symbol]": "underlying_ticker",
                "filter[exp_date_eq]": "expiration_date",
                "filter[exp_date_from]": "expiration_date.gte",
                "filter[exp_date_to]": "expiration_date.lte",
                "filter[type]": "contract_type",
                "filter[strike_eq]": "strike_price",
                "filter[strike_from]": "strike_price.gte",
                "filter[strike_to]": "strike_price.lte",
                "page[limit]": "limit",
            }
            mapped_key = mapping.get(key)
            if mapped_key is None:
                if key in {"sort", "order", "fields"}:
                    translated[key] = value
                continue
            translated[mapped_key] = value
            continue

        if endpoint == "eod":
            mapping = {
                "filter[exp_date_eq]": "expiration_date",
                "filter[exp_date_from]": "expiration_date.gte",
                "filter[exp_date_to]": "expiration_date.lte",
                "filter[type]": "contract_type",
                "filter[strike_eq]": "strike_price",
                "filter[strike_from]": "strike_price.gte",
                "filter[strike_to]": "strike_price.lte",
                "page[limit]": "limit",
            }
            mapped_key = mapping.get(key)
            if mapped_key is None:
                if key in {"sort", "order", "fields"}:
                    translated[key] = value
                continue
            translated[mapped_key] = value

        if endpoint == "bars":
            if key in {"contract", "range_from", "range_to", "multiplier", "timespan"}:
                translated[key] = value
                continue

            mapping = {
                "sort": "sort",
                "page[limit]": "limit",
                "limit": "limit",
                "adjusted": "adjusted",
            }
            mapped_key = mapping.get(key)
            if mapped_key is None:
                continue
            translated[mapped_key] = value

    if endpoint == "underlying-symbols":
        translated.setdefault("market", "stocks")
        translated.setdefault("active", "true")
        translated.setdefault("sort", "ticker")
        translated.setdefault("order", "asc")

    # Massive uses `sort` + `order` instead of `-field` notation.
    sort_value = translated.get("sort")
    if isinstance(sort_value, str) and sort_value.startswith("-"):
        translated["sort"] = sort_value[1:]
        translated["order"] = "desc"
    elif isinstance(sort_value, str) and sort_value and "order" not in translated:
        translated["order"] = "asc"

    return translated


def _clean_query_params(params: dict[str, Any]) -> dict[str, Any]:
    """Drop null query values and normalize booleans into API-friendly ints."""
    cleaned: dict[str, Any] = {}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, bool):
            cleaned[key] = int(value)
        else:
            cleaned[key] = value
    return cleaned


def _cache_signature(endpoint: str, params: dict[str, Any], include_date_filters: bool) -> str:
    """Return a stable hash for a query shape used to name cache tables."""
    relevant_params = {}
    for key, value in _clean_query_params(params).items():
        if key in {"page[offset]", "page[limit]"}:
            continue
        if not include_date_filters and key in DATE_FILTER_KEYS:
            continue
        relevant_params[key] = value

    signature_payload = json.dumps(
        {"endpoint": endpoint, "params": relevant_params},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha1(signature_payload.encode("utf-8")).hexdigest()


def _build_cache_table_name(endpoint: str, params: dict[str, Any]) -> str:
    """Build the DuckDB table name for a cached endpoint query."""
    signature = _cache_signature(endpoint, params, include_date_filters=False)
    return f"massive_{endpoint}_{signature[:24]}"


def _extract_tradetime_window(params: dict[str, Any]) -> tuple[Optional[Timestamp], Optional[Timestamp]]:
    """Extract the tradetime filter window used for cache coverage checks."""
    # Bars requests express range boundaries directly rather than filter[...] keys.
    range_start = _normalize_any_timestamp(params.get("range_from"))
    range_end = _normalize_any_timestamp(params.get("range_to"))
    if range_start is not None or range_end is not None:
        return range_start, range_end

    exact_date = _normalize_date(params.get("filter[tradetime_eq]"), "filter[tradetime_eq]")
    if exact_date is not None:
        exact_date = _normalize_timestamp_for_compare(exact_date)
        return exact_date, exact_date

    start_ts = _normalize_timestamp_for_compare(
        _normalize_date(params.get("filter[tradetime_from]"), "filter[tradetime_from]")
    )
    end_ts = _normalize_timestamp_for_compare(
        _normalize_date(params.get("filter[tradetime_to]"), "filter[tradetime_to]")
    )
    return start_ts, end_ts


def _filter_cached_data(
    data: pd.DataFrame,
    start_ts: Optional[Timestamp],
    end_ts: Optional[Timestamp],
) -> pd.DataFrame:
    """Trim cached data to the requested tradetime window when present."""
    if data.empty or "__cache_date" not in data.columns:
        return data.reset_index(drop=True)

    filtered = data
    normalized_dates = _normalize_date_series_for_compare(filtered["__cache_date"])
    if start_ts is not None:
        filtered = filtered[normalized_dates >= start_ts]
        normalized_dates = normalized_dates[normalized_dates >= start_ts]
    if end_ts is not None:
        filtered = filtered[normalized_dates <= end_ts]
    return filtered.reset_index(drop=True)


def _cache_covers_request(
    data: pd.DataFrame,
    start_ts: Optional[Timestamp],
    end_ts: Optional[Timestamp],
    tolerance: Timedelta,
) -> bool:
    """Return whether cached rows fully cover the requested tradetime window."""
    if data.empty:
        return False

    if start_ts is None and end_ts is None:
        return True

    if "__cache_date" not in data.columns:
        return False

    date_series = _normalize_date_series_for_compare(data["__cache_date"])
    first_date = date_series.min()
    last_date = date_series.max()

    if start_ts is not None and (pd.isna(first_date) or first_date > start_ts + tolerance):
        return False
    if end_ts is not None and (pd.isna(last_date) or last_date < end_ts - tolerance):
        return False

    return True


def _finalize_frame(data: pd.DataFrame, sort: Optional[str]) -> pd.DataFrame:
    """Drop cache-only columns and apply the requested output sort order."""
    if data.empty:
        return data.copy()

    finalized = data.drop(
        columns=[column for column in data.columns if column.startswith("__cache_")],
        errors="ignore",
    )

    if sort in {"exp_date", "-exp_date"} and "exp_date" in finalized.columns:
        finalized = finalized.sort_values("exp_date", ascending=not sort.startswith("-"))
    elif sort in {"strike", "-strike"} and "strike" in finalized.columns:
        finalized = finalized.sort_values("strike", ascending=not sort.startswith("-"))
    elif "tradetime" in finalized.columns:
        finalized = finalized.sort_values("tradetime")

    return finalized.reset_index(drop=True)


def _build_cache_row_ids(frame: pd.DataFrame) -> pd.Series:
    """Construct stable per-row identifiers used for cache deduplication."""
    if "ticker" in frame.columns and "tradetime" in frame.columns:
        return frame["ticker"].astype(str) + "|" + frame["tradetime"].astype(str)
    if "response_id" in frame.columns and "tradetime" in frame.columns:
        return frame["response_id"].astype(str) + "|" + frame["tradetime"].astype(str)
    if "contract" in frame.columns and "tradetime" in frame.columns:
        return frame["contract"].astype(str) + "|" + frame["tradetime"].astype(str)
    if "response_id" in frame.columns:
        return frame["response_id"].astype(str)
    if "underlying_symbol" in frame.columns:
        return frame["underlying_symbol"].astype(str)
    return pd.Series(
        [hashlib.sha1(json.dumps(row, sort_keys=True, default=str).encode("utf-8")).hexdigest() for row in frame.to_dict("records")]
    )


def _normalize_records(endpoint: str, records: list[Any]) -> pd.DataFrame:
    """Flatten endpoint JSON payload records into a normalized DataFrame."""
    rows: list[dict[str, Any]] = []
    for record in records:
        if isinstance(record, dict):
            row: dict[str, Any] = {}

            # Legacy adapter compatibility path (`data[*].attributes`).
            attributes = record.get("attributes")
            if isinstance(attributes, dict):
                if "id" in record:
                    row["response_id"] = record["id"]
                if "type" in record:
                    row["response_type"] = record["type"]
                row.update(attributes)
                rows.append(row)
                continue

            # Massive-style contracts payloads are already flat records.
            if endpoint == "contracts":
                row.update(record)
                if "ticker" in row and "contract" not in row:
                    row["contract"] = row.get("ticker")
                if "underlying_ticker" in row and "underlying_symbol" not in row:
                    row["underlying_symbol"] = row.get("underlying_ticker")
                if "contract_type" in row and "option_type" not in row:
                    row["option_type"] = row.get("contract_type")
                if "expiration_date" in row and "exp_date" not in row:
                    row["exp_date"] = row.get("expiration_date")
                if "strike_price" in row and "strike" not in row:
                    row["strike"] = row.get("strike_price")
                rows.append(row)
                continue

            # Massive option-chain snapshot payloads include nested details/day/greeks objects.
            if endpoint == "eod":
                details = record.get("details") if isinstance(record.get("details"), dict) else {}
                day = record.get("day") if isinstance(record.get("day"), dict) else {}
                greeks = record.get("greeks") if isinstance(record.get("greeks"), dict) else {}
                quote = record.get("last_quote") if isinstance(record.get("last_quote"), dict) else {}
                underlying = record.get("underlying_asset") if isinstance(record.get("underlying_asset"), dict) else {}

                row.update(record)
                row.update(details)
                row.update(greeks)

                if "ticker" in details:
                    row["contract"] = details.get("ticker")
                if "contract_type" in details:
                    row["option_type"] = details.get("contract_type")
                if "expiration_date" in details:
                    row["exp_date"] = details.get("expiration_date")
                if "strike_price" in details:
                    row["strike"] = details.get("strike_price")

                if "ticker" in underlying:
                    row["underlying_symbol"] = underlying.get("ticker")

                if "open" in day:
                    row["open"] = day.get("open")
                if "high" in day:
                    row["high"] = day.get("high")
                if "low" in day:
                    row["low"] = day.get("low")
                if "close" in day:
                    row["last"] = day.get("close")
                if "volume" in day:
                    row["volume"] = day.get("volume")
                if "last_updated" in day:
                    row["tradetime"] = pd.to_datetime(day.get("last_updated"), unit="ns", errors="coerce")

                if "bid" in quote:
                    row["bid"] = quote.get("bid")
                if "ask" in quote:
                    row["ask"] = quote.get("ask")
                if "midpoint" in quote:
                    row["midpoint"] = quote.get("midpoint")
                if "last_updated" in quote:
                    quote_ts = pd.to_datetime(quote.get("last_updated"), unit="ns", errors="coerce")
                    row["bid_date"] = quote_ts
                    row["ask_date"] = quote_ts

                if "implied_volatility" in record:
                    row["volatility"] = record.get("implied_volatility")
                rows.append(row)
                continue

            if endpoint == "bars":
                row.update(record)
                if "ticker" in row and "contract" not in row:
                    row["contract"] = row.get("ticker")
                if "t" in row:
                    row["tradetime"] = pd.to_datetime(row.get("t"), unit="ms", errors="coerce")
                if "o" in row:
                    row["open"] = row.get("o")
                if "h" in row:
                    row["high"] = row.get("h")
                if "l" in row:
                    row["low"] = row.get("l")
                if "c" in row:
                    row["last"] = row.get("c")
                if "v" in row:
                    row["volume"] = row.get("v")
                if "vw" in row:
                    row["vwap"] = row.get("vw")
                rows.append(row)
                continue

            row.update({key: value for key, value in record.items() if key not in {"attributes", "links"}})
            rows.append(row)
            continue

        if endpoint == "underlying-symbols":
            rows.append({"underlying_symbol": record})
        else:
            rows.append({"value": record})

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    for column in DATE_COLUMNS & set(frame.columns):
        frame[column] = pd.to_datetime(frame[column], errors="coerce")

    frame["__cache_row_id"] = _build_cache_row_ids(frame)
    if "tradetime" in frame.columns:
        frame["__cache_date"] = pd.to_datetime(frame["tradetime"], errors="coerce")
    elif "exp_date" in frame.columns:
        frame["__cache_date"] = pd.to_datetime(frame["exp_date"], errors="coerce")
    else:
        frame["__cache_date"] = pd.NaT

    return frame


def _build_url(endpoint: str, params: dict[str, Any], api_token: str) -> str:
    """Build a fully qualified API URL for a Massive endpoint request."""
    query_params = _clean_query_params(_translate_params_for_endpoint(endpoint, params))
    query_params["apiKey"] = api_token

    if endpoint == "underlying-symbols":
        path = "/v3/reference/tickers"
    elif endpoint == "contracts":
        path = "/v3/reference/options/contracts"
    elif endpoint == "eod":
        underlying_symbol = params.get("filter[underlying_symbol]")
        if not underlying_symbol:
            raise ValueError(
                "Massive EOD snapshot requests require underlying_symbol because the endpoint path is /v3/snapshot/options/{underlyingAsset}."
            )
        path = f"/v3/snapshot/options/{underlying_symbol}"
    elif endpoint == "bars":
        contract = params.get("contract")
        range_from = params.get("range_from")
        range_to = params.get("range_to")
        multiplier = int(params.get("multiplier", 15) or 15)
        timespan = str(params.get("timespan", "minute") or "minute")
        if not contract:
            raise ValueError("Massive bars requests require contract.")
        if range_from is None or range_to is None:
            raise ValueError("Massive bars requests require both range_from and range_to.")

        path = (
            f"/v2/aggs/ticker/{quote(str(contract), safe='')}/"
            f"range/{multiplier}/{timespan}/{range_from}/{range_to}"
        )
    else:
        raise ValueError(f"Unsupported endpoint: {endpoint}")

    return f"{OPTIONS_API_BASE_URL}{path}?{urlencode(query_params)}"


def _append_api_key(url: str, api_token: str) -> str:
    """Ensure Massive pagination URLs include apiKey for authenticated follow-up requests."""
    parsed = urlparse(url)
    query_pairs = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query_pairs.setdefault("apiKey", api_token)
    return urlunparse(parsed._replace(query=urlencode(query_pairs)))


def _extract_page_records(payload: dict[str, Any]) -> list[Any]:
    """Return page records from Massive (results) or legacy (data) payload shapes."""
    if isinstance(payload.get("results"), list):
        return payload["results"]
    if isinstance(payload.get("results"), dict):
        return [payload["results"]]
    if isinstance(payload.get("data"), list):
        return payload["data"]
    return []


def _extract_next_link(payload: dict[str, Any]) -> str | None:
    """Return next-page URL from Massive (`next_url`) or legacy (`links.next`) payloads."""
    if isinstance(payload.get("next_url"), str):
        return payload["next_url"]
    links = payload.get("links")
    if isinstance(links, dict) and isinstance(links.get("next"), str):
        return links["next"]
    return None


def _http_get_json(url: str, timeout: int = DEFAULT_REQUEST_TIMEOUT_SECONDS) -> dict[str, Any]:
    """Execute an HTTP GET request and decode the JSON payload."""
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "Macro_Ideas options adapter/1.0",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        message = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Massive request failed with HTTP {exc.code}: {message}") from exc
    except URLError as exc:
        raise RuntimeError(f"Massive request failed: {exc.reason}") from exc


def _fetch_endpoint_data(
    endpoint: str,
    params: dict[str, Any],
    api_token: str,
    timeout: int,
) -> pd.DataFrame:
    """Fetch and paginate one endpoint query into a single normalized frame."""
    records: list[Any] = []
    request_url = _build_url(endpoint, params, api_token)

    while request_url:
        payload = _http_get_json(request_url, timeout=timeout)
        page_records = _extract_page_records(payload)
        if endpoint == "bars" and isinstance(payload.get("ticker"), str):
            ticker = payload.get("ticker")
            for record in page_records:
                if isinstance(record, dict):
                    record.setdefault("ticker", ticker)
        records.extend(page_records)
        next_link = _extract_next_link(payload)
        request_url = _append_api_key(next_link, api_token) if next_link else ""

    return _normalize_records(endpoint, records)


def _download_collection_cached(
    endpoint: str,
    params: dict[str, Any],
    *,
    api_token: Optional[str] = None,
    api_token_file: Optional[str | Path] = None,
    api_token_key: Optional[str] = None,
    date_tolerance_days: int = 0,
    timeout: int = DEFAULT_REQUEST_TIMEOUT_SECONDS,
) -> pd.DataFrame:
    """Fetch one endpoint query with DuckDB-backed read-through caching."""
    resolved_token = _resolve_api_token(api_token, api_token_file, api_token_key)
    cleaned_params = _clean_query_params(params)
    sort = cleaned_params.get("sort")
    start_ts, end_ts = _extract_tradetime_window(cleaned_params)
    tolerance = Timedelta(days=max(int(date_tolerance_days), 0))
    cache_table_name = _build_cache_table_name(endpoint, cleaned_params)

    if duckdb is None:
        print(f"[massive_options_helper] DuckDB not available; requesting {endpoint} live.")
        return _finalize_frame(
            _fetch_endpoint_data(endpoint, cleaned_params, resolved_token, timeout=timeout),
            sort=sort,
        )

    with duckdb.connect(str(OPTIONS_CACHE_DB_PATH)) as connection:
        if _table_exists(connection, cache_table_name):
            cached_data = _read_cached_table(connection, cache_table_name)
            if _cache_covers_request(cached_data, start_ts, end_ts, tolerance):
                print(f"[massive_options_helper] Cache hit for {endpoint}; returning DuckDB data.")
                return _finalize_frame(_filter_cached_data(cached_data, start_ts, end_ts), sort=sort)
            print(f"[massive_options_helper] Cache partial/miss for {endpoint}; downloading fresh data.")
        else:
            cached_data = pd.DataFrame()
            print(f"[massive_options_helper] No cache table for {endpoint}; downloading fresh data.")

        fresh_data = _fetch_endpoint_data(endpoint, cleaned_params, resolved_token, timeout=timeout)
        merged_data = _merge_cached_data(cached_data, fresh_data)
        _store_cached_table(connection, cache_table_name, merged_data)
        return _finalize_frame(_filter_cached_data(merged_data, start_ts, end_ts), sort=sort)


def download_options_underlying_symbols(
    *,
    api_token: Optional[str] = None,
    api_token_file: Optional[str | Path] = None,
    api_token_key: Optional[str] = None,
    timeout: int = DEFAULT_REQUEST_TIMEOUT_SECONDS,
) -> pd.DataFrame:
    """Return the list of supported underlying symbols with options coverage."""
    return _download_collection_cached(
        "underlying-symbols",
        {},
        api_token=api_token,
        api_token_file=api_token_file,
        api_token_key=api_token_key,
        timeout=timeout,
    )


def download_options_contracts(
    *,
    contract: Optional[str] = None,
    underlying_symbol: Optional[str] = None,
    exp_date_eq: Optional[str] = None,
    exp_date_from: Optional[str] = None,
    exp_date_to: Optional[str] = None,
    tradetime_eq: Optional[str] = None,
    tradetime_from: Optional[str] = None,
    tradetime_to: Optional[str] = None,
    option_type: Optional[str] = None,
    strike_eq: Optional[float] = None,
    strike_from: Optional[float] = None,
    strike_to: Optional[float] = None,
    sort: Optional[str] = None,
    fields: Optional[list[str] | tuple[str, ...] | str] = None,
    compact: bool = False,
    page_limit: int = DEFAULT_PAGE_LIMIT,
    api_token: Optional[str] = None,
    api_token_file: Optional[str | Path] = None,
    api_token_key: Optional[str] = None,
    date_tolerance_days: int = 0,
    timeout: int = DEFAULT_REQUEST_TIMEOUT_SECONDS,
) -> pd.DataFrame:
    """Return filtered option contract records for the requested query slice."""
    params = _build_contract_query_params(
        contract=contract,
        underlying_symbol=underlying_symbol,
        exp_date_eq=exp_date_eq,
        exp_date_from=exp_date_from,
        exp_date_to=exp_date_to,
        tradetime_eq=tradetime_eq,
        tradetime_from=tradetime_from,
        tradetime_to=tradetime_to,
        option_type=option_type,
        strike_eq=strike_eq,
        strike_from=strike_from,
        strike_to=strike_to,
        sort=sort,
        fields=fields,
        compact=compact,
        page_limit=page_limit,
    )
    return _download_collection_cached(
        "contracts",
        params,
        api_token=api_token,
        api_token_file=api_token_file,
        api_token_key=api_token_key,
        date_tolerance_days=date_tolerance_days,
        timeout=timeout,
    )


def download_options_eod(
    *,
    contract: Optional[str] = None,
    underlying_symbol: Optional[str] = None,
    exp_date_eq: Optional[str] = None,
    exp_date_from: Optional[str] = None,
    exp_date_to: Optional[str] = None,
    tradetime_eq: Optional[str] = None,
    tradetime_from: Optional[str] = None,
    tradetime_to: Optional[str] = None,
    option_type: Optional[str] = None,
    strike_eq: Optional[float] = None,
    strike_from: Optional[float] = None,
    strike_to: Optional[float] = None,
    sort: Optional[str] = None,
    fields: Optional[list[str] | tuple[str, ...] | str] = None,
    compact: bool = False,
    page_limit: int = DEFAULT_PAGE_LIMIT,
    api_token: Optional[str] = None,
    api_token_file: Optional[str | Path] = None,
    api_token_key: Optional[str] = None,
    date_tolerance_days: int = 0,
    timeout: int = DEFAULT_REQUEST_TIMEOUT_SECONDS,
) -> pd.DataFrame:
    """Return historical end-of-day options records for the requested query slice."""
    params = _build_contract_query_params(
        contract=contract,
        underlying_symbol=underlying_symbol,
        exp_date_eq=exp_date_eq,
        exp_date_from=exp_date_from,
        exp_date_to=exp_date_to,
        tradetime_eq=tradetime_eq,
        tradetime_from=tradetime_from,
        tradetime_to=tradetime_to,
        option_type=option_type,
        strike_eq=strike_eq,
        strike_from=strike_from,
        strike_to=strike_to,
        sort=sort,
        fields=fields,
        compact=compact,
        page_limit=page_limit,
    )
    return _download_collection_cached(
        "eod",
        params,
        api_token=api_token,
        api_token_file=api_token_file,
        api_token_key=api_token_key,
        date_tolerance_days=date_tolerance_days,
        timeout=timeout,
    )


def download_options_time_series(
    *,
    contract: str,
    range_from: str | int,
    range_to: str | int,
    multiplier: int = 15,
    timespan: str = "minute",
    adjusted: bool = True,
    sort: str = "asc",
    limit: int = 50000,
    api_token: Optional[str] = None,
    api_token_file: Optional[str | Path] = None,
    api_token_key: Optional[str] = None,
    date_tolerance_days: int = 0,
    timeout: int = DEFAULT_REQUEST_TIMEOUT_SECONDS,
) -> pd.DataFrame:
    """Return Massive options aggregate bars for a contract over a time window.

    This is the primary helper path for intraday options use cases such as
    15-minute bar backfills.
    """
    params = {
        "contract": contract,
        "range_from": range_from,
        "range_to": range_to,
        "multiplier": int(multiplier),
        "timespan": timespan,
        "adjusted": adjusted,
        "sort": sort,
        "limit": int(limit),
    }
    return _download_collection_cached(
        "bars",
        params,
        api_token=api_token,
        api_token_file=api_token_file,
        api_token_key=api_token_key,
        date_tolerance_days=date_tolerance_days,
        timeout=timeout,
    )
