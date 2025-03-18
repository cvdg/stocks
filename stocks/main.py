from datetime import datetime, timedelta
import logging
import os

import yfinance as yf
from sqlalchemy import create_engine

from stocks.utils.db_value import (
    db_value_setup,
    db_values_exist,
    db_values_get,
    db_values_set,
)
from stocks.utils.db_journal import db_journal_setup, db_journal_info
from stocks.utils.db_stock import db_stock_setup, insert_or_do_nothing_on_conflict


logger = logging.getLogger(__name__)


def extract(engine, symbol: str, yfinance_symbol: str, start: str, end: str) -> bool:
    if start == end:
        return True

    key_end = f"{symbol}-end-date"
    data = yf.Ticker(yfinance_symbol)
    df = data.history(start=start, end=end, interval="1d")
    df.drop(columns=['Capital Gains'], errors='ignore', inplace=True)
    # print(df)
    # return True
    rows = len(df)
    if rows:
        df.reset_index(inplace=True)
        df.rename(columns={"Date": "day", "Stock Splits": "splits"}, inplace=True)
        df.rename(str.lower, axis="columns", inplace=True)
        df["symbol"] = symbol
        df.to_sql(
            name="daily",
            con=engine,
            if_exists="append",
            index=False,
            method=insert_or_do_nothing_on_conflict,
        )
        db_values_set(key_end, end)
        db_journal_info(f"Fetched {symbol}: {end} - rows: {rows}")
        logger.info(f"Fetched {symbol}: {end} - rows: {rows}")
    else:
        db_journal_info(f"Fetched {symbol}: no rows")
        logger.info(f"Fetched {symbol}: no rows")
    return rows <= 0


def stock(enfine, symbol: str, yfinance_symbol: str) -> None:
    stop = False
    key_start = f"{symbol}-start-date"
    key_end = f"{symbol}-end-date"

    if not db_values_exist(key_end):
        string_start = "2010-01-01"
        db_values_set(key_start, string_start)
        db_values_set(key_end, string_start)

    while not stop:
        string_start = db_values_get(key_end)
        date_end = datetime.fromisoformat(string_start).date() + timedelta(days=180)
        date_now = datetime.now().date()

        if date_end > date_now:
            date_end = date_now

        string_end = date_end.isoformat()
        stop = extract(engine, symbol, yfinance_symbol, string_start, string_end)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        logger.info("Started")

        uri = os.getenv("STOCKS_DATABASE_URI")

        # For: psycopg3
        db_journal_setup(f"postgresql://{uri}")
        db_value_setup(f"postgresql://{uri}")
        db_stock_setup(f"postgresql://{uri}")

        # For: SQLAlchemy
        engine = create_engine(f"postgresql+psycopg://{uri}")

        stock(engine, "aex", "^AEX")
        stock(engine, "EURgovernment", "IBGS.AS")
        stock(engine, "rheinmetal", "RHM.DE")
        stock(engine, "airbus", "AIR.PA")
        stock(engine, "thales", "HO.PA")
        
        stock(engine, "btc", "BTC-EUR")

        logger.info("Finished")
    except Exception as e:
        logger.exception(e)
