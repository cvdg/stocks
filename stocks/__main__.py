import datetime
import logging
import os

import psycopg

__VERSION__ = "0.0.1"

logger = logging.getLogger(__name__)

CREATE_VALUES_TABLE = """
CREATE TABLE IF NOT EXISTS values (
  key   VARCHAR(128) NOT NULL PRIMARY KEY,
  value VARCHAR(256) NOT NULL 
);
"""

SELECT_VALUE_BY_KEY = "SELECT value FROM values WHERE key = %s"
INSERT_KEY_VALUE = "INSERT INTO values (key, value) VALUES(%s, %s)"


def stocks(uri: str) -> None:
    with psycopg.connect(uri) as conn:
        with conn.cursor() as curr:
            logger.info("Connected to the database")
            curr.execute(CREATE_VALUES_TABLE)
            result = curr.execute(SELECT_VALUE_BY_KEY, ('Created', ))
            value = result.fetchone()
            
            if not value:
                curr.execute(INSERT_KEY_VALUE, ('Created', datetime.datetime.now(), ))
                curr.execute(INSERT_KEY_VALUE, ('Version', __VERSION__, ))
                logger.info("Created table values")
        conn.commit()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger.info("Started")

    uri = os.getenv('STOCKS_DATABASE_URI')

    stocks(uri)

    logger.info("Finished")
