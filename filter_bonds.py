"""Filter detailed bonds into a monthly-coupon shortlist.

Reads bonds_detail.csv, keeps bonds matching the strategy and writes
bonds_filtered.csv. Strategy:
  - 12 coupon payments per year (monthly coupon)
  - coupon value >= 2% of last price
  - last price >= 97% of face value
  - non-qualified-investor only
  - fixed (non-floating) coupon
"""

import pandas as pd

INPUT_CSV = "bonds_detail.csv"
OUTPUT_CSV = "bonds_filtered.csv"

NUMERIC = ["coupon_qty_per_year", "coupon_value", "price_last", "face_value"]


def filter_bonds(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the monthly-coupon strategy filter.

    Args:
        df: detailed bonds dataframe (columns of bonds_detail.csv)

    Returns:
        filtered dataframe
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


def main() -> None:
    """Read detail CSV, filter, write shortlist CSV."""
    df = pd.read_csv(INPUT_CSV)
    out = filter_bonds(df)
    out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Filtered {len(df)} -> {len(out)} bonds, saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
