import logging
import os

import duckdb
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def extract(engine) -> pd.DataFrame:
    """Extract dataframe from PostgrSQL database."""
    with engine.connect() as conn, conn.begin():
        df = pd.read_sql(
            sql="SELECT day, symbol, close FROM daily ORDER BY day, symbol",
            con=conn,
            index_col=["day", "symbol"],
        )
        logger.info(f"Extracted dataframe - {len(df)} rows")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Include index columns 'day' and 'symbol' in the dataframe."""
    df.reset_index(inplace=True)
    df.rename(columns={"close": "value"}, inplace=True)
    logger.info(f"Transformed dataframe - {len(df)} rows")
    return df


def load_duckdb(df: pd.DataFrame, filename: str) -> None:
    """Write dataframe to a DuckDB file."""
    if os.path.exists(filename):
        os.remove(filename)
        logger.warning(f"Removed: {filename}")
    with duckdb.connect(filename) as conn:
        conn.sql("CREATE TABLE daily AS SELECT * FROM df")
        conn.commit()
        logger.info(f"Loaded dataframe: {filename} - {len(df)} rows")


def load_parquet(df: pd.DataFrame, filename: str) -> None:
    """Write dataframe to a Parquet file."""
    if os.path.exists(filename):
        os.remove(filename)
        logger.warning(f"Removed: {filename}")
    with duckdb.connect() as conn:
        conn.sql("CREATE TABLE daily AS SELECT * FROM df")
        conn.sql(f"COPY (SELECT * FROM daily) TO '{filename}' (FORMAT parquet)")
        conn.commit()
        logger.info(f"Loaded dataframe: {filename} - {len(df)} rows")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        logger.info("Started")

        if not os.path.exists("data"):
            os.makedirs("data")
            logger.warning("Created directory: data/")

        df = extract(
            create_engine(f"postgresql+psycopg://{os.getenv("STOCKS_DATABASE_URI")}")
        )
        df = transform(df)
        load_duckdb(df, "data/stocks.duckdb")
        load_parquet(df, "data/stocks.parquet")

        logger.info("Finished")
    except Exception as e:
        logger.exception(e)
