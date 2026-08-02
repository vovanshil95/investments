"""Filter detailed bonds into a monthly-coupon shortlist.

Given the detailed bonds DataFrame (from fetch_bonds_detail.fetch_details),
keeps bonds matching the strategy and tags collateral-backed ones.
"""

import pandas as pd

NUMERIC = ["coupon_qty_per_year", "coupon_value", "price_last", "face_value"]


def apply_filter(
    df: pd.DataFrame,
    *,
    min_coupon_qty_per_year: int = 12,
    min_coupon_to_price_pct: float = 0.02,
    min_price_to_face_pct: float = 0.97,
    exclude_qual_investor: bool = True,
    exclude_floating_coupon: bool = True,
) -> pd.DataFrame:
    """Apply the monthly-coupon strategy filter.

    Args:
        df: detailed bonds dataframe (columns from fetch_bonds_detail.FIELDS)
        min_coupon_qty_per_year: required coupon payments per year (exact match)
        min_coupon_to_price_pct: minimum coupon value as a fraction of last price
        min_price_to_face_pct: minimum last price as a fraction of face value
        exclude_qual_investor: drop bonds restricted to qualified investors
        exclude_floating_coupon: drop bonds with a floating coupon

    Returns:
        filtered dataframe (reset index)

    Example:
        >>> filtered = apply_filter(detail_df, min_coupon_to_price_pct=0.015)
    """
    df = df.copy()
    for col in NUMERIC:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    mask = (
        (df["coupon_qty_per_year"] == min_coupon_qty_per_year)
        & (df["coupon_value"] >= min_coupon_to_price_pct * df["price_last"])
        & (df["price_last"] >= min_price_to_face_pct * df["face_value"])
    )
    if exclude_qual_investor:
        mask &= df["qual_investor"] == False  # noqa: E712 — pandas boolean mask
    if exclude_floating_coupon:
        mask &= df["floating_coupon"] == False  # noqa: E712

    return df[mask].reset_index(drop=True)


def add_securitization(df: pd.DataFrame) -> pd.DataFrame:
    """Add a `securitization` column: API flag OR issuer name starts with 'СФО'.

    The API's `securitizationFlag` misses some actual SPV ("СФО") issuers,
    so it is combined with a name-based check.

    Args:
        df: bonds dataframe with `securitization` and `name` columns

    Returns:
        dataframe with an authoritative boolean `securitization` column
    """
    df = df.copy()
    flag = df["securitization"] == True  # noqa: E712
    is_sfo = df["name"].str.strip().str.startswith("СФО")
    df["securitization"] = (flag | is_sfo).fillna(False)
    return df
