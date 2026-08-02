"""Fetch account operations history via the official T-Invest API.

Reads TINVEST_TOKEN from .env, lists all accounts, pulls the full executed
operations history for each and writes operations.csv.
"""

import os

import pandas as pd
from dotenv import load_dotenv
from tinkoff.invest import Client, OperationState

load_dotenv()

OUTPUT_CSV = "operations.csv"

FIELDS = [
    "account_id",
    "date",
    "type",
    "operation_type",
    "state",
    "instrument_uid",
    "figi",
    "quantity",
    "price",
    "payment",
    "currency",
]


def money(value) -> float:
    """Convert a MoneyValue protobuf field to a float in currency units."""
    if value is None:
        return 0.0
    return value.units + value.nano / 1e9


def parse_operation(account_id: str, op) -> dict:
    """Flatten one Operation protobuf message into a plain dict row."""
    return {
        "account_id": account_id,
        "date": op.date,
        "type": op.type,
        "operation_type": op.operation_type.name,
        "state": op.state.name,
        "instrument_uid": op.instrument_uid,
        "figi": op.figi,
        "quantity": op.quantity,
        "price": money(op.price),
        "payment": money(op.payment),
        "currency": op.currency,
    }


def main() -> None:
    """Fetch all accounts' executed operations and write them to CSV."""
    token = os.getenv("TINVEST_TOKEN")
    if not token:
        raise RuntimeError("TINVEST_TOKEN не найден в .env")

    rows = []
    with Client(token) as client:
        accounts = client.users.get_accounts().accounts
        for acc in accounts:
            ops = client.operations.get_operations(
                account_id=acc.id,
                state=OperationState.OPERATION_STATE_EXECUTED,
            )
            for op in ops.operations:
                rows.append(parse_operation(acc.id, op))

    df = pd.DataFrame(rows, columns=FIELDS)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Выгружено {len(df)} операций по {df['account_id'].nunique()} счёт(ам) -> {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
