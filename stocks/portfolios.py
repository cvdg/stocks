from datetime import date, timedelta
import logging
import os

import psycopg


logger = logging.getLogger(__name__)


SELECT_CURRENT_PORTFOLIO = """
  SELECT stock_id,
         SUM(transaction_share) AS transaction_share,
         SUM(transaction_price) AS transaction_price
    FROM transactions
   WHERE transaction_date <= %s
GROUP BY stock_id
ORDER BY stock_id
"""

SELECT_LAST_TICKER = """
  SELECT stock_id,
         ticker_id,
         ticker_date
    FROM tickers
   WHERE stock_id = %s
     AND ticker_date <= %s
ORDER BY ticker_date DESC
   LIMIT 1
"""

SELECT_TICKER_CLOSE = """
  SELECT ticker_close
    FROM tickers
   WHERE ticker_id = %s
"""

UPSERT_PORTFOLIO = """
INSERT INTO portfolios (
    stock_id,
    ticker_id,
    portfolio_date,
    portfolio_share,
    portfolio_price,
    portfolio_value
) VALUES(%s, %s, %s, %s, %s, %s)
ON CONFLICT (stock_id, ticker_id) DO UPDATE
SET portfolio_date  = excluded.portfolio_date,
    portfolio_share = excluded.portfolio_share,
    portfolio_price = excluded.portfolio_price,
    portfolio_value = excluded.portfolio_value
"""


def portfolios(url: str, start: str = "2025-02-18") -> None:
    current_date = date.fromisoformat(start)
    today_date = date.today()
    with psycopg.connect(url) as conn:
        while current_date <= today_date:
            day = current_date.isoformat()

            with conn.cursor() as curr:
                curr.execute(SELECT_CURRENT_PORTFOLIO, (day,))
                result = curr.fetchall()
                for row in result:
                    stock_id = row[0]
                    share = row[1]
                    price = row[2]

                    logger.debug(f"{day=} {stock_id=} {share=} {price=}")

                    curr.execute(
                        SELECT_LAST_TICKER,
                        (
                            stock_id,
                            day,
                        ),
                    )
                    ticker_id = curr.fetchone()[1]
                    logger.debug(f"{ticker_id=}")

                    curr.execute(SELECT_TICKER_CLOSE, (ticker_id,))
                    ticker_close = curr.fetchone()[0]
                    logger.debug(f"{ticker_close=}")

                    value = ticker_close * share

                    logger.debug(
                        f"{stock_id=} {ticker_id=} {day=} {share=:.2f} {price=:.2f} {value=:.2f}"
                    )

                    curr.execute(
                        UPSERT_PORTFOLIO,
                        (
                            stock_id,
                            ticker_id,
                            day,
                            share,
                            price,
                            value,
                        ),
                    )
                    logger.info(f"Update portfolio day: {day } stock_id: {stock_id}")
            current_date += timedelta(days=1)
        conn.commit()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        logger.info("Started")

        url = os.getenv(
            "STOCKS_DATABASE_URL",
            "postgresql://test:test@database01.griend.dev:5432/test",
        )
        portfolios(url=url)

        logger.info("Finished")
    except Exception as e:
        logger.exception(e)
