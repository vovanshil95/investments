"""Fetch the full bond ticker list from the TBank invest API."""

import time

import pandas as pd
import requests

BASE_URL = "https://api.tinkoff.ru/trading/bonds/list"
HEADERS = {"User-Agent": "Mozilla/5.0"}
PAGE_SIZE = 100


def fetch_page(start: int, end: int) -> dict:
    """Fetch one page of bonds.

    Args:
        start: offset index
        end: end index (exclusive)

    Returns:
        API payload dict with 'values' and 'total'
    """
    params = {
        "start": start,
        "end": end,
        "country": "All",
        "orderType": "Desc",
        "sortType": "ByRateAndYieldToClient",
    }
    resp = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()["payload"]


def fetch_tickers() -> pd.DataFrame:
    """Fetch every bond ticker currently listed on the exchange.

    Returns:
        DataFrame with columns ticker, isin, name, currency, sector, country

    Example:
        >>> df = fetch_tickers()
        >>> df.columns.tolist()
        ['ticker', 'isin', 'name', 'currency', 'sector', 'country']
    """
    first = fetch_page(0, 1)
    total = first["total"]

    rows = []
    for start in range(0, total, PAGE_SIZE):
        end = min(start + PAGE_SIZE, total)
        payload = fetch_page(start, end)
        for item in payload["values"]:
            sym = item["symbol"]
            rows.append({
                "ticker": sym["ticker"],
                "isin": sym.get("isin", ""),
                "name": sym.get("showName", ""),
                "currency": sym.get("currency", ""),
                "sector": sym.get("sector", ""),
                "country": sym.get("countryOfRiskBriefName", ""),
            })
        time.sleep(0.2)

    return pd.DataFrame(rows)
