import psycopg

from sqlalchemy.dialects.postgresql import insert


__VERSION__ = "0.0.1"

__stocks_uri: str = None


CREATE_STOCKS_TABLE = """
CREATE TABLE IF NOT EXISTS daily (
    day          DATE         NOT NULL,
    symbol       VARCHAR(128) NOT NULL,
    open         REAL         NOT NULL,
    high         REAL         NOT NULL,
    low          REAL         NOT NULL,
    close        REAL         NOT NULL,
    volume       REAL         NOT NULL,
    dividends    REAL         NOT NULL,
    splits       REAL         NOT NULL,
    UNIQUE (day, symbol)
);
"""


def db_stock_setup(uri: str) -> None:
    """Save the PostgreSQL database URI and creates the tables if they do not exist."""
    global __stocks_uri

    with psycopg.connect(uri) as conn:
        with conn.cursor() as curr:
            curr.execute(CREATE_STOCKS_TABLE)
        conn.commit()
    __stocks_uri = uri


def insert_or_do_nothing_on_conflict(table, conn, keys, data_iter):
    insert_stmt = insert(table.table).values(list(data_iter))
    on_duplicate_key_stmt = insert_stmt.on_conflict_do_nothing(
        index_elements=["day", "symbol"]
    )
    conn.execute(on_duplicate_key_stmt)
