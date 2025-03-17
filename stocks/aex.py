from datetime import datetime, timedelta
import logging

import yfinance as yf
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert

from stocks.utils.db_value import db_values_exist, db_values_get, db_values_set
from stocks.utils.db_journal import db_journal_info


def insert_or_do_nothing_on_conflict(table, conn, keys, data_iter):
    insert_stmt = insert(table.table).values(list(data_iter))
    # you need to specify column(s) name(s) used to infer unique index
    on_duplicate_key_stmt = insert_stmt.on_conflict_do_nothing(
        index_elements=["datetime", "symbol"]
    )
    conn.execute(on_duplicate_key_stmt)


def extract_aex(connection, start: str, end: str) -> bool:
    if start == end:
        return True

    key_end = "aex-end-date"
    data = yf.Ticker("^AEX")    
    df = data.history(start=start, end=end, interval="1h")
    # print(df)
    # return True
    rows = len(df)
    if rows:
        df.reset_index(inplace=True)
        df.rename(
            columns={"Datetime": "datetime", "Stock Splits": "splits"}, inplace=True
        )
        df.rename(str.lower, axis="columns", inplace=True)
        # df.set_index("datetime", inplace=True)
        df["symbol"] = "aex"
        # df.to_sql(name="stocks", con=connection, if_exists="append")
        df.to_sql(
            name="stocks",
            con=connection,
            if_exists="append",
            index=False,
            method=insert_or_do_nothing_on_conflict,
        )
        db_values_set(key_end, end)
        db_journal_info(f"Fetched AEX to: {end} - rows: {rows}")
        logger.info(f"Fetched AEX to: {end} - rows: {rows}")
    else:
        db_journal_info(f"Fetched AEX no rows")
        logger.info(f"Fetched AEX no rows")
    return rows <= 0


def aex(connection) -> None:
    stop = False
    key_start = "aex-start-date"
    key_end = "aex-end-date"

    if not db_values_exist(key_end):
        string_start = "2023-04-01"
        db_values_set(key_start, string_start)
        db_values_set(key_end, string_start)

    while not stop:
        string_start = db_values_get(key_end)
        date_end = datetime.fromisoformat(string_start).date() + timedelta(days=90)
        date_now = datetime.now().date()

        if date_end > date_now:
            date_end = date_now

        string_end = date_end.isoformat()
        stop = extract_aex(connection, string_start, string_end)


if __name__ == "__main__":
    import os
    from stocks.utils import db_journal_setup, db_value_setup, db_stock_setup

    logger = logging.getLogger(__name__)

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
        aex(engine)
        
        logger.info("Finished")
    except Exception as e:
        logger.exception(e)
