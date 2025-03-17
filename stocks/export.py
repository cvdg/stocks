"""
Export PostgreSQL database table stocks.daily to a parquet file.
"""

import logging
import os

import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def export_parquet(engine) -> None:
    with engine.connect() as conn, conn.begin():  
        df = pd.read_sql(
            sql="SELECT day, symbol, close FROM daily ORDER BY day, symbol",
            con=conn,
            index_col=['day', 'symbol'],
        )
        # print(df)
        rows = len(df)
        df.to_parquet(path="data/stocks.parquet", compression="zstd")
        logger.info(f"Wrote data to: data/stocks.parquet - {rows} rows")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        logger.info("Started")

        uri = os.getenv("STOCKS_DATABASE_URI")
        engine = create_engine(f"postgresql+psycopg://{uri}")
        export_parquet(engine)

        logger.info("Finished")
    except Exception as e:
        logger.exception(e)
