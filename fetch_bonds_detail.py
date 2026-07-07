"""Fetch detailed info for each bond from TBank invest API and save to CSV.

Reads tickers from bonds_tickers.csv, fetches detail per ticker,
saves to bonds_detail.csv. Supports resume: already-fetched tickers are skipped.
Random delay ~1s between requests to avoid rate limiting.
"""

import csv
import random
import time
from pathlib import Path

import requests
from tqdm import tqdm

TICKERS_CSV = "bonds_tickers.csv"
OUTPUT_CSV = "bonds_detail.csv"
API_URL = "https://api.tinkoff.ru/trading/bonds/get"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

FIELDS = [
    "ticker", "isin", "name", "currency", "sector", "country",
    "exchange", "lot_size",
    "face_value", "face_value_original", "face_value_currency",
    "price_buy", "price_sell", "price_last", "price_close",
    "price_pct",
    "nkd", "coupon_value", "coupon_pct", "coupon_period_days",
    "coupon_qty_per_year", "coupon_type", "floating_coupon",
    "next_coupon_date", "maturity_date", "date_to_client",
    "total_yield", "yield_to_maturity", "yield_to_client",
    "duration", "rate",
    "earnings_abs", "earnings_rel",
    "liquidity", "is_low_liquid", "tax_free",
    "subordinated", "perpetual", "reliable",
    "securitization",
    "risk_category", "qual_investor",
    "is_tinkoff_bond", "indexed_nominal",
    "hist_price_1y",
]


def parse_bond(payload: dict) -> dict:
    """Extract flat row from raw API payload.

    Args:
        payload: raw dict from API response['payload']

    Returns:
        flat dict with keys matching FIELDS
    """
    sym = payload.get("symbol", {})
    prices = payload.get("prices", {})
    earnings = payload.get("earnings", {})
    hist = payload.get("historicalPrices", [])

    return {
        "ticker": sym.get("ticker"),
        "isin": sym.get("isin"),
        "name": sym.get("showName"),
        "currency": sym.get("currency"),
        "sector": sym.get("sector"),
        "country": sym.get("countryOfRiskBriefName"),
        "exchange": sym.get("exchangeShowName"),
        "lot_size": sym.get("lotSize"),

        "face_value": payload.get("faceValue"),
        "face_value_original": payload.get("faceValueOriginal"),
        "face_value_currency": payload.get("faceValueOriginalCurrency"),

        "price_buy": (prices.get("buy") or {}).get("value"),
        "price_sell": (prices.get("sell") or {}).get("value"),
        "price_last": (prices.get("last") or {}).get("value"),
        "price_close": (prices.get("close") or {}).get("value"),
        "price_pct": (payload.get("pricesRelative") or {}).get("last", {}).get("value"),

        "nkd": payload.get("nkd"),
        "coupon_value": payload.get("couponValue"),
        "coupon_pct": payload.get("couponPercent"),
        "coupon_period_days": payload.get("couponPeriodDays"),
        "coupon_qty_per_year": payload.get("couponQuantityPerYear"),
        "coupon_type": payload.get("couponType"),
        "floating_coupon": payload.get("floatingCoupon"),

        "next_coupon_date": payload.get("endDate"),
        "maturity_date": payload.get("matDate"),
        "date_to_client": payload.get("dateToClient"),

        "total_yield": payload.get("totalYield"),
        "yield_to_maturity": payload.get("yieldToMaturity"),
        "yield_to_client": payload.get("yieldToClient"),

        "duration": payload.get("duration"),
        "rate": payload.get("rate"),

        "earnings_abs": (earnings.get("absolute") or {}).get("value"),
        "earnings_rel": earnings.get("relative"),

        "liquidity": payload.get("liquidity"),
        "is_low_liquid": payload.get("isLowLiquid"),
        "tax_free": payload.get("taxFree"),
        "subordinated": payload.get("subordinated"),
        "perpetual": payload.get("perpetual"),
        "reliable": payload.get("reliable"),
        "securitization": payload.get("securitizationFlag"),
        "risk_category": payload.get("riskCategory"),
        "qual_investor": sym.get("qualInvestorFlag"),
        "is_tinkoff_bond": payload.get("isTinkoffInvestBond"),
        "indexed_nominal": payload.get("indexedNominalFlag"),

        "hist_price_1y": hist[0]["amount"] if hist else None,
    }


def load_tickers() -> list[str]:
    """Load tickers from bonds_tickers.csv."""
    with open(TICKERS_CSV, encoding="utf-8") as f:
        return [row["ticker"] for row in csv.DictReader(f)]


def load_done() -> set[str]:
    """Load already-fetched tickers from output CSV (for resume)."""
    path = Path(OUTPUT_CSV)
    if not path.exists():
        return set()
    with open(path, encoding="utf-8") as f:
        return {row["ticker"] for row in csv.DictReader(f)}


def fetch_detail(ticker: str, session: requests.Session) -> dict | None:
    """Fetch bond detail from API.

    Args:
        ticker: bond ticker string
        session: requests Session

    Returns:
        parsed flat dict or None on error
    """
    try:
        resp = session.get(API_URL, params={"ticker": ticker}, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") == "Error" or "payload" not in data:
            print(f"  [warn] {ticker}: API error — {data.get('payload', {}).get('message', '?')}")
            return None
        return parse_bond(data["payload"])
    except Exception as e:
        print(f"  [error] {ticker}: {e}")
        return None


def main() -> None:
    tickers = load_tickers()
    done = load_done()
    remaining = [t for t in tickers if t not in done]

    print(f"Total: {len(tickers)} | Already done: {len(done)} | To fetch: {len(remaining)}")

    write_header = not Path(OUTPUT_CSV).exists()
    errors: list[str] = []

    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if write_header:
            writer.writeheader()

        session = requests.Session()

        for ticker in tqdm(remaining, desc="Fetching bonds", unit="bond"):
            row = fetch_detail(ticker, session)
            if row:
                writer.writerow(row)
                f.flush()
            else:
                errors.append(ticker)

            # random delay: uniform 0.5–1.5s → mean 1s
            time.sleep(random.uniform(0.5, 1.5))

    print(f"\n\nDone. Saved to {OUTPUT_CSV}")
    if errors:
        print(f"Errors ({len(errors)}): {', '.join(errors)}")
        with open("bonds_errors.txt", "w") as ef:
            ef.write("\n".join(errors))


if __name__ == "__main__":
    main()
