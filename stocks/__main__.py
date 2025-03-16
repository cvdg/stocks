import logging
import os

from stocks.utils import db_journal_setup, db_value_setup


logger = logging.getLogger(__name__)


def main(uri: str) -> None:
    pass


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger.info("Started")

    uri = os.getenv("STOCKS_DATABASE_URI")

    db_journal_setup(uri)
    db_value_setup(uri)

    main(uri)

    logger.info("Finished")
