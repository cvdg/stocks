from datetime import date, timedelta
import logging
from typing import List

import psycopg
import yfinance as yf


# URL = "postgresql+psycopg://test:test@database01.griend.dev:5432/test"
URL = "postgresql://test:test@database01.griend.dev:5432/test"

logger = logging.getLogger(__name__)


def stocks(url: str) -> List[str]:
    symbols = []
    with psycopg.connect(URL) as conn:
        with conn.cursor() as curr:
            curr.execute(
                "SELECT stock_symbol FROM stocks WHERE stock_active = TRUE ORDER BY stock_id"
            )
            rows = curr.fetchall()
            for row in rows:
                symbol = row[0]
                # print(f"{symbol=}")
                symbols.append(symbol)
            curr.close()
        conn.commit()
        conn.close()
    return symbols


def __get_stock_id(url: str, symbol: str) -> None:
    with psycopg.connect(URL) as conn:
        with conn.cursor() as curr:
            curr.execute(
                "SELECT stock_id FROM stocks WHERE stock_symbol = %s", (symbol,)
            )
            result = curr.fetchone()
            if not result:
                logger.error(f"stock_id for symbol: {symbol} not found")
            else:
                stock_id = result[0]
        conn.commit()
    return stock_id


def __tickers_upsert(row, url: str, symbol: str, stock_id: int) -> None:
    """
    Slow!
    For each row a new connection is made and a commit is done.
    """
    with psycopg.connect(URL) as conn:
        with conn.cursor() as curr:
            day = row["Date"].date().isoformat()
            open = float(row["Open"])
            high = float(row["High"])
            low = float(row["Low"])
            close = float(row["Close"])
            volume = float(row["Volume"])
            dividends = float(row["Dividends"])
            splits = float(row["Stock Splits"])
            curr.execute(
                """
INSERT INTO tickers (
    stock_id,
    ticker_date,
    ticker_open,
    ticker_high,
    ticker_low,
    ticker_close,
    ticker_volume,
    ticker_dividends,
    ticker_splits
) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (stock_id, ticker_date) DO UPDATE
SET ticker_open      = excluded.ticker_open,
    ticker_high      = excluded.ticker_high,
    ticker_low       = excluded.ticker_low,
    ticker_close     = excluded.ticker_close,
    ticker_volume    = excluded.ticker_volume,
    ticker_dividends = excluded.ticker_dividends,
    ticker_splits    = excluded.ticker_splits
""",
                (
                    stock_id,
                    day,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    dividends,
                    splits,
                ),
            )
            logger.info(f"Updated: date: {day}, symbol: {symbol}")
        conn.commit()


def tickers(url: str, symbol: str, start: str = "2020-01-01") -> None:
    """Fecth financial data from Yahoo Finance."""
    stock_id = __get_stock_id(url, symbol)
    data = yf.Ticker(symbol)
    df = data.history(start=start, interval="1d")
    df.reset_index(inplace=True)
    df.apply(__tickers_upsert, axis=1, url=url, symbol=symbol, stock_id=stock_id)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        logger.info("Started")

        start = (date.today() - timedelta(days=7)).isoformat()
        symbols = stocks(url=URL)

        for symbol in symbols:
            tickers(url=URL, start=start, symbol=symbol)

        logger.info("Finished")
    except Exception as e:
        logger.exception(e)
