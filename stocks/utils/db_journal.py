import datetime
import psycopg

__VERSION__ = "0.0.1"

__journal_uri: str = None
__execution_id: int = 0

CREATE_JOURNALS_TABLE = """
CREATE TABLE IF NOT EXISTS executions (
    id           SERIAL                   NOT NULL PRIMARY KEY,
    datetime     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS journals (
    id           SERIAL                   NOT NULL PRIMARY KEY,
    datetime     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    execution_id INTEGER                  NOT NULL,
    level        INTEGER                  NOT NULL DEFAULT 0,
    message      VARCHAR(256)             NOT NULL,
    
    FOREIGN KEY (execution_id) REFERENCES executions(id)
);
"""

INSERT_MESSAGE = """
INSERT INTO journals (
    execution_id,
    level,
    message
) VALUES (%s, %s, %s)
"""


def db_journal_setup(uri: str) -> None:
    """Save the PostgreSQL database URI and creates the tables if they do not exist."""
    global __journal_uri
    global __execution_id

    with psycopg.connect(uri) as conn:
        with conn.cursor() as curr:
            curr.execute(CREATE_JOURNALS_TABLE)
            curr.execute("SELECT nextval('executions_id_seq')")
            result = curr.fetchone()
            __execution_id = result[0]
            curr.execute("INSERT INTO executions (id) VALUES (%s)", (__execution_id,))
        conn.commit()
    __journal_uri = uri


def __db_journal_message(level: int, message: str) -> None:
    with psycopg.connect(__journal_uri) as conn:
        with conn.cursor() as curr:
            curr.execute(
                INSERT_MESSAGE,
                (
                    __execution_id,
                    level,
                    message,
                ),
            )
        conn.commit()


def db_journal_fatal(message: str) -> None:
    """Insert a message with level fatal (0)."""
    __db_journal_message(0, message)


def db_journal_error(message: str) -> None:
    """Insert a message with level error (1)."""
    __db_journal_message(1, message)


def db_journal_warning(message: str) -> None:
    """Insert a message with level warning (2)."""
    __db_journal_message(2, message)


def db_journal_info(message: str) -> None:
    """Insert a message with level info (3)."""
    __db_journal_message(3, message)


def db_journal_debug(message: str) -> None:
    """Insert a message with level debug (4)."""
    __db_journal_message(4, message)


def db_journal_trace(message: str) -> None:
    """Insert a message with level trace (5)."""
    __db_journal_message(5, message)
