"""Filter detailed bonds into a monthly-coupon shortlist.

Reads bonds_detail.csv, keeps bonds matching the strategy, enriches them
with a `securitization` flag and writes bonds_filtered.csv. Strategy:
  - 12 coupon payments per year (monthly coupon)
  - coupon value >= 2% of last price
  - last price >= 97% of face value
  - non-qualified-investor only
  - fixed (non-floating) coupon

The `securitization` column marks collateral-backed bonds (securitization,
SPV issuers "СФО"). The API `securitizationFlag` is incomplete, so it is
combined with an issuer-name check for "СФО".
"""

import random
import time

import pandas as pd
import requests

INPUT_CSV = "bonds_detail.csv"
OUTPUT_CSV = "bonds_filtered.csv"
API_URL = "https://api.tinkoff.ru/trading/bonds/get"
HEADERS = {"User-Agent": "Mozilla/5.0"}

NUMERIC = ["coupon_qty_per_year", "coupon_value", "price_last", "face_value"]


def base_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the monthly-coupon strategy filter.

    Args:
        df: detailed bonds dataframe (columns of bonds_detail.csv)

    Returns:
        filtered dataframe (reset index)
    """
    for col in NUMERIC:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    mask = (
        (df["coupon_qty_per_year"] == 12)
        & (df["coupon_value"] >= 0.02 * df["price_last"])
        & (df["price_last"] >= 0.97 * df["face_value"])
        & (df["qual_investor"] == False)  # noqa: E712 — pandas boolean mask
        & (df["floating_coupon"] == False)  # noqa: E712
    )
    return df[mask].reset_index(drop=True)


def fetch_securitization_flag(ticker: str, session: requests.Session) -> bool:
    """Fetch the API securitizationFlag for one bond.

    Args:
        ticker: bond ticker/ISIN
        session: requests Session

    Returns:
        True if the API marks the bond as securitized, else False
    """
    try:
        resp = session.get(API_URL, params={"ticker": ticker}, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        payload = resp.json().get("payload", {})
        return bool(payload.get("securitizationFlag"))
    except Exception:
        return False


def add_securitization(df: pd.DataFrame) -> pd.DataFrame:
    """Add a `securitization` column: API flag OR issuer name starts with 'СФО'.

    Reuses the flag already present in the input if available, otherwise
    fetches it from the API for the (small) shortlist.

    Args:
        df: shortlist dataframe

    Returns:
        dataframe with a boolean `securitization` column
    """
    if "securitization" in df.columns:
        flag = df["securitization"] == True  # noqa: E712
    else:
        session = requests.Session()
        flags = []
        for ticker in df["ticker"]:
            flags.append(fetch_securitization_flag(ticker, session))
            time.sleep(random.uniform(0.2, 0.4))
        flag = pd.Series(flags, index=df.index)

    is_sfo = df["name"].str.strip().str.startswith("СФО")
    df["securitization"] = (flag | is_sfo).fillna(False)
    return df


def main() -> None:
    """Read detail CSV, filter, enrich with securitization, write shortlist CSV."""
    df = pd.read_csv(INPUT_CSV)
    out = add_securitization(base_filter(df))
    out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    n_sec = int(out["securitization"].sum())
    print(f"Filtered {len(df)} -> {len(out)} bonds ({n_sec} с залоговым обеспечением), saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
