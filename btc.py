from datetime import datetime

import yfinance as yf
import pandas as pd
import duckdb

PREFIX = "crypto"


def extract(start: str = "2010-01-01", symbol: str = "BTC-EUR") -> pd.DataFrame:
    data = yf.Ticker(symbol)
    df = data.history(start=start, interval="1d")
    return df


def transform(df: pd.DataFrame, symbol: str = "BTC-EUR") -> pd.DataFrame:
    df = df.reset_index()
    df = df.set_index("Date")
    df["symbol"] = symbol
    return df


def load(df: pd.DataFrame) -> None:
    df.to_csv(f"{PREFIX}.csv")
    df.to_parquet(f"{PREFIX}.parquet")

    with duckdb.connect(f"{PREFIX}.duckdb") as connection:
        connection.sql(f"CREATE TABLE IF NOT EXISTS {PREFIX} AS SELECT * FROM df")


def main() -> None:
    df = extract(start="2010-01-01")
    df = transform(df)
    print(df)
    load(df)


if __name__ == "__main__":
    main()
