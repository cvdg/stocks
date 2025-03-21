from datetime import date, timedelta
import logging
import os

from stocks.tickers import stocks, tickers
from stocks.portfolios import portfolios


logger = logging.getLogger(__name__)



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
        start = (date.today() - timedelta(days=7)).isoformat()
        symbols = stocks(url=url)
        for symbol in symbols:
            tickers(url=url, start=start, symbol=symbol)
        portfolios(url=url, start=start)

        logger.info("Finished")
    except Exception as e:
        logger.exception(e)
