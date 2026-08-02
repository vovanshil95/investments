"""Bond realized P&L report: sales/redemptions vs coupon/accrued-interest income.

Walks the full operations history (GetOperationsByCursor, bond instruments
only) to keep a running weighted-average purchase cost per instrument
(clean price + commission, no accrued interest). For a chosen date range it
then reports two separate numbers:
  - capital P&L: (sale/redemption proceeds + commission) - avg cost of the
    units sold/redeemed
  - coupon/accrued-interest (НКД) income: coupon payments + accrued interest
    received on sale minus accrued interest paid on purchase, plus any
    coupon-related tax

Purchases always use the full history to compute the average cost; only
sales, redemptions, commissions and coupons dated inside the period are
counted toward the period's totals. Partial redemptions (amortisation)
reduce the average cost pro-rata to the fraction of face value repaid,
looked up via InstrumentsService.BondBy.
"""

import argparse
import os
from calendar import monthrange
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from tinkoff.invest import Client
from tinkoff.invest.schemas import GetOperationsByCursorRequest, InstrumentIdType, OperationType

load_dotenv()

OUTPUT_CSV = "bond_pnl.csv"


def money(value) -> float:
    """Convert a MoneyValue protobuf field to a float in currency units.

    Args:
        value: protobuf MoneyValue (or None)

    Returns:
        amount as a plain float, 0.0 if value is None
    """
    if value is None:
        return 0.0
    return value.units + value.nano / 1e9


def fetch_bond_operations(client: Client, account_id: str) -> list:
    """Fetch all operations for the account, bond instruments only, ascending by date.

    Args:
        client: connected T-Invest API client
        account_id: brokerage account id

    Returns:
        operations sorted ascending by date
    """
    resp = client.operations.get_operations_by_cursor(
        GetOperationsByCursorRequest(account_id=account_id, limit=1000)
    )
    rows = [op for op in resp.items if op.instrument_type == "bond"]
    rows.sort(key=lambda op: op.date)
    return rows


def get_nominal(client: Client, instrument_uid: str, cache: dict) -> float:
    """Fetch (and cache) a bond's original face value per unit.

    Args:
        client: connected T-Invest API client
        instrument_uid: bond instrument uid
        cache: dict used to memoize lookups across calls

    Returns:
        face value per unit in currency units
    """
    if instrument_uid not in cache:
        bond = client.instruments.bond_by(
            id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_UID, id=instrument_uid
        ).instrument
        cache[instrument_uid] = money(bond.nominal)
    return cache[instrument_uid]


def compute_pnl(client: Client, rows: list, date_from: datetime, date_to: datetime) -> pd.DataFrame:
    """Walk operations chronologically and compute per-instrument realized P&L.

    Args:
        client: connected T-Invest API client (used to look up bond nominal
            for partial redemptions)
        rows: bond operations sorted ascending by date
        date_from: period start (inclusive, timezone-aware)
        date_to: period end (inclusive, timezone-aware)

    Returns:
        one row per instrument with capital_pnl / nkd_pnl for the period,
        sorted by total_pnl descending

    Example:
        >>> report = compute_pnl(client, rows, date_from, date_to)
        >>> report.columns.tolist()
        ['name', 'capital_pnl', 'nkd_pnl', 'total_pnl']
    """
    qty_held: dict = defaultdict(float)
    avg_cost: dict = defaultdict(float)
    names: dict = {}
    nominal_cache: dict = {}
    capital_pnl: dict = defaultdict(float)
    nkd_pnl: dict = defaultdict(float)

    for op in rows:
        uid = op.instrument_uid
        names[uid] = op.name
        in_period = date_from <= op.date <= date_to
        price = money(op.price)
        qty = op.quantity
        commission = money(op.commission)
        payment = money(op.payment)
        accrued = money(op.accrued_int)

        if op.type == OperationType.OPERATION_TYPE_BUY:
            cost = price * qty - commission
            total_cost = avg_cost[uid] * qty_held[uid] + cost
            qty_held[uid] += qty
            avg_cost[uid] = total_cost / qty_held[uid] if qty_held[uid] else 0.0
            if in_period:
                nkd_pnl[uid] -= accrued

        elif op.type == OperationType.OPERATION_TYPE_SELL:
            net_proceeds = price * qty + commission
            if in_period:
                capital_pnl[uid] += net_proceeds - avg_cost[uid] * qty
                nkd_pnl[uid] += accrued
            qty_held[uid] -= qty

        elif op.type == OperationType.OPERATION_TYPE_BOND_REPAYMENT_FULL:
            net_proceeds = payment + commission
            if in_period:
                capital_pnl[uid] += net_proceeds - avg_cost[uid] * qty_held[uid]
                nkd_pnl[uid] += accrued
            qty_held[uid] = 0.0
            avg_cost[uid] = 0.0

        elif op.type == OperationType.OPERATION_TYPE_BOND_REPAYMENT:
            if qty_held[uid] > 0:
                nominal = get_nominal(client, uid, nominal_cache)
                fraction = payment / (qty_held[uid] * nominal) if nominal else 0.0
                cost_consumed = avg_cost[uid] * fraction * qty_held[uid]
                if in_period:
                    capital_pnl[uid] += payment - cost_consumed
                    nkd_pnl[uid] += accrued
                avg_cost[uid] -= avg_cost[uid] * fraction
            elif in_period:
                capital_pnl[uid] += payment

        elif op.type == OperationType.OPERATION_TYPE_COUPON:
            if in_period:
                nkd_pnl[uid] += payment

        elif op.type in (
            OperationType.OPERATION_TYPE_BOND_TAX,
            OperationType.OPERATION_TYPE_TAX_CORRECTION_COUPON,
        ):
            if in_period:
                nkd_pnl[uid] += payment

        # OPERATION_TYPE_BROKER_FEE (standalone) is skipped on purpose: it
        # duplicates the `commission` already folded into the paired
        # BUY/SELL row above, counting it again would double the fee.

    instruments = set(capital_pnl) | set(nkd_pnl)
    report = pd.DataFrame(
        [
            {
                "облигация": names.get(uid, uid),
                "доход_без_нкд": round(capital_pnl.get(uid, 0.0), 2),
                "нкд_и_купоны": round(nkd_pnl.get(uid, 0.0), 2),
                "итого": round(capital_pnl.get(uid, 0.0) + nkd_pnl.get(uid, 0.0), 2),
            }
            for uid in instruments
        ]
    )
    return report.sort_values("итого", ascending=False).reset_index(drop=True)


def parse_date(s: str, end_of_day: bool = False) -> datetime:
    """Parse a YYYY-MM-DD string into a timezone-aware UTC datetime.

    Args:
        s: date string, e.g. "2026-07-01"
        end_of_day: if True, set time to 23:59:59 instead of 00:00:00

    Returns:
        timezone-aware UTC datetime
    """
    dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59)
    return dt


def month_range(year: int, month: int) -> tuple:
    """Return the (first, last) datetime of the given month, UTC, inclusive.

    Args:
        year: e.g. 2026
        month: 1-12

    Returns:
        (first_day_00:00:00, last_day_23:59:59) as timezone-aware datetimes
    """
    first = datetime(year, month, 1, tzinfo=timezone.utc)
    last_day = monthrange(year, month)[1]
    last = datetime(year, month, last_day, 23, 59, 59, tzinfo=timezone.utc)
    return first, last


def main() -> None:
    """CLI: report realized bond P&L for a date range (default: current month)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="date_from", help="YYYY-MM-DD, включительно")
    parser.add_argument("--to", dest="date_to", help="YYYY-MM-DD, включительно")
    parser.add_argument("--month", help="YYYY-MM, шорткат вместо --from/--to")
    args = parser.parse_args()

    if args.month:
        year, month = map(int, args.month.split("-"))
        date_from, date_to = month_range(year, month)
    elif args.date_from and args.date_to:
        date_from = parse_date(args.date_from)
        date_to = parse_date(args.date_to, end_of_day=True)
    else:
        now = datetime.now(timezone.utc)
        date_from, date_to = month_range(now.year, now.month)

    token = os.getenv("TINVEST_TOKEN")
    if not token:
        raise RuntimeError("TINVEST_TOKEN не найден в .env")

    with Client(token) as client:
        account_id = client.users.get_accounts().accounts[0].id
        rows = fetch_bond_operations(client, account_id)
        report = compute_pnl(client, rows, date_from, date_to)

    report.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

    print(f"Период: {date_from.date()} — {date_to.date()}\n")
    print(report.to_string(index=False))
    print()
    print(f"Доход/расход без НКД (продажи + погашения):  {report['доход_без_нкд'].sum():,.2f} руб.")
    print(f"НКД и купоны:                                {report['нкд_и_купоны'].sum():,.2f} руб.")
    print(f"Итого:                                       {report['итого'].sum():,.2f} руб.")
    print(f"\nСохранено в {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
