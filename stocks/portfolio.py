from datetime import datetime, date
import logging
import os

import pandas as pd
import psycopg


from stocks.utils import db_stock_setup


logger = logging.getLogger(__name__)


INSERT_PORTFOLIO = """
INSERT INTO portfolio (
    day,
    action,
    symbol,
    shares,
    price,
    costs
) VALUES (%s, %s, %s, %s, %s, %s)
"""


def portfolio_insert(
    uri: str,
    day: date,
    action: str,
    symbol: str,
    shares: float,
    price: float,
    costs: float,
) -> None:
    with psycopg.connect(uri) as conn:
        with conn.cursor() as curr:
            curr.execute(
                INSERT_PORTFOLIO,
                (
                    day,
                    action,
                    symbol,
                    shares,
                    price,
                    costs,
                ),
            )
        conn.commit()
        logger.info(f"{action} {shares} of {symbol} for {price}")


def portfolio_buy(
    uri: str, day: date, symbol: str, shares: float, price: float, costs: float
) -> None:
    portfolio_insert(uri, day, "buy", symbol, shares, price, costs)


def portfolio_sell(
    uri: str, day: date, symbol: str, shares: float, price: float, costs: float
) -> None:
    portfolio_insert(uri, day, "sell", symbol, -shares, price, costs)


def portfolio_start_date(uri: str) -> date:
    with psycopg.connect(uri) as conn:
        with conn.cursor() as curr:
            curr.execute("SELECT MIN(day) FROM transactions")
            result = curr.fetchone()
            start_date = result[0]
            curr.execute("SELECT MAX(day) FROM portfolio")
            result = curr.fetchone()
            if result:
                current_date = result[0]
        conn.commit()
        if current_date > start_date:
            start_date = current_date
    return start_date


def portfolio(uri: str) -> None:
    start_date = portfolio_start_date(uri)
    today_date = datetime.today().date()
    date_range = pd.date_range(start_date, today_date, freq="D")
    for d in date_range:
        total_value = 0.0
        total_costs = 0.0
        day = d.strftime("%Y-%m-%d")
        with psycopg.connect(uri) as conn:
            with conn.cursor() as curr:
                # For each day calculate the number of stocks
                curr.execute(
                    "SELECT symbol, SUM(shares), SUM(price) FROM transactions WHERE day <= %s GROUP BY symbol ORDER BY symbol",
                    (day,),
                )
                rows = curr.fetchall()

                for row in rows:
                    symbol = row[0]
                    shares = row[1]
                    costs = row[2]
                    # Find the last known close price
                    curr.execute(
                        "SELECT close FROM daily WHERE day <= %s AND symbol = %s ORDER BY day DESC LIMIT 1",
                        (
                            day,
                            symbol,
                        ),
                    )
                    result = curr.fetchone()
                    if result:
                        value = result[0] * shares
                        print(
                            f"{day} - {symbol}: {shares} - {value:.2f} ({100 * value / costs:.1f}%)"
                        )
                        total_value += value
                        total_costs += costs
                        # Does record exists?
                        curr.execute(
                            "SELECT day, symbol FROM portfolio WHERE day = %s AND symbol = %s",
                            (
                                day,
                                symbol,
                            ),
                        )
                        exists = curr.fetchone()
                        if exists:
                            curr.execute(
                                "UPDATE portfolio SET shares = %s, value = %s, costs = %s WHERE day = %s AND symbol = %s",
                                (
                                    shares,
                                    value,
                                    costs,
                                    day,
                                    symbol,
                                ),
                            )
                        else:
                            curr.execute(
                                "INSERT INTO portfolio (day, symbol, shares, value, costs) VALUES (%s, %s, %s, %s, %s)",
                                (
                                    day,
                                    symbol,
                                    shares,
                                    value,
                                    costs,
                                ),
                            )
                    else:
                        print(f"{day} - {symbol}: {shares}")
            conn.commit()
        print(
            f"{day} - TOTAL: {total_value:0.1f} ({100 * total_value / total_costs:.1f}%)"
        )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        logger.info("Started")
        uri = f"postgresql://{os.getenv("STOCKS_DATABASE_URI")}"
        db_stock_setup(uri)
        portfolio(uri)
        logger.info("Finished")
    except Exception as e:
        logger.exception(e)
