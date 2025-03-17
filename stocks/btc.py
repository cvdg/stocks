from datetime import datetime, timedelta
import logging

import yfinance as yf
from sqlalchemy import create_engine

from stocks.utils.db_value import db_values_exist, db_values_get, db_values_set
from stocks.utils.db_journal import db_journal_info


def extract_btc(connection, start: str, end: str) -> bool:
    key_end = "btc-end-date"
    data = yf.Ticker("BTC-EUR")
    df = data.history(start=start, end=end, interval="1h")
    rows = len(df)
    if rows:
        df.reset_index(inplace=True)
        df.rename(
            columns={"Datetime": "datetime", "Stock Splits": "splits"}, inplace=True
        )
        df.rename(str.lower, axis="columns", inplace=True)
        df.set_index("datetime", inplace=True)
        df["symbol"] = "btc"
        df.to_sql(name="stocks", con=connection, if_exists="append")

        db_values_set(key_end, end)
        db_journal_info(f"Fetched BTC to: {end} - rows: {rows}")
        logger.info(f"Fetched BTC to: {end} - rows: {rows}")
    else:
        db_journal_info(f"Fetched no rows")
        logger.info(f"Fetched no rows")
    return rows > 0


def btc(connection) -> None:
    stop = False
    key_start = "btc-start-date"
    key_end = "btc-end-date"

    if db_values_exist(key_end):
        string_start = db_values_get(key_end)
    else:
        string_start = "2023-04-01"
        db_values_set(key_start, string_start)

    date_end = datetime.fromisoformat(string_start).date() + timedelta(days=30)
    date_now = datetime.now().date()

    if date_end > date_now:
        date_end = date_now
        stop = True
    string_end = date_end.isoformat()

    while not stop:
        stop = extract_btc(connection, string_start, string_end)


if __name__ == "__main__":
    import os
    from stocks.utils import db_journal_setup, db_value_setup, db_stock_setup

    logger = logging.getLogger(__name__)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s  %(message)s",
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
        btc(engine)
        logger.info("Finished")
    except Exception as e:
        logger.exception(e)
