"""Fetch all bond tickers from TBank invest API and save to CSV."""

import csv
import time
import requests

BASE_URL = "https://api.tinkoff.ru/trading/bonds/list"
HEADERS = {"User-Agent": "Mozilla/5.0"}
PAGE_SIZE = 100
OUTPUT = "bonds_tickers.csv"


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


def main() -> None:
    """Fetch all bonds and write tickers to CSV."""
    first = fetch_page(0, 1)
    total = first["total"]
    print(f"Total bonds: {total}")

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
        print(f"  fetched {end}/{total}")
        time.sleep(0.2)

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "isin", "name", "currency", "sector", "country"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nSaved {len(rows)} bonds to {OUTPUT}")


if __name__ == "__main__":
    main()
