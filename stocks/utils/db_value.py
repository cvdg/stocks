import datetime
import psycopg

__VERSION__ = "0.0.1"

__values_uri: str = None

CREATE_VALUES_TABLE = "CREATE TABLE IF NOT EXISTS values (key VARCHAR(128) NOT NULL PRIMARY KEY, value VARCHAR(256) NOT NULL)"
SELECT_VALUE_BY_KEY = "SELECT value FROM values WHERE key = %s"
UPSERT_KEY_VALUE = "INSERT INTO values (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s"
DELETE_KEY_VALUE = "DELETE FROM values WHERE key = %s"


def db_value_setup(uri: str) -> None:
    """Save the PostgreSQL database URI and creates the table if it does not exists."""
    global __values_uri

    with psycopg.connect(uri) as conn:
        with conn.cursor() as curr:
            curr.execute(CREATE_VALUES_TABLE)
            result = curr.execute(SELECT_VALUE_BY_KEY, ("Created",))
            row = result.fetchone()

            if not row:
                curr.execute(
                    UPSERT_KEY_VALUE,
                    (
                        "Created",
                        datetime.datetime.now(),
                        datetime.datetime.now(),
                    ),
                )
                curr.execute(
                    UPSERT_KEY_VALUE,
                    (
                        "Version",
                        __VERSION__,
                        __VERSION__,
                    ),
                )
        conn.commit()
    __values_uri = uri


def db_values_exist(key: str) -> bool:
    """Checks if the key exists."""
    with psycopg.connect(__values_uri) as conn:
        with conn.cursor() as curr:
            curr.execute(CREATE_VALUES_TABLE)
            result = curr.execute(SELECT_VALUE_BY_KEY, (key,))
            row = result.fetchone()

            if row == None:
                exist = False
            else:
                exist = True
        conn.commit()

    return exist


def db_values_get(key: str) -> str:
    """Gets the value for the key."""
    with psycopg.connect(__values_uri) as conn:
        with conn.cursor() as curr:
            curr.execute(CREATE_VALUES_TABLE)
            result = curr.execute(SELECT_VALUE_BY_KEY, (key,))
            row = result.fetchone()

            if row:
                value = row[0]
        conn.commit()

    return value


def db_values_set(key: str, value: str) -> None:
    """Sets the value for the key."""
    with psycopg.connect(__values_uri) as conn:
        with conn.cursor() as curr:
            curr.execute(
                UPSERT_KEY_VALUE,
                (
                    key,
                    value,
                    value,
                ),
            )
        conn.commit()


def db_values_delete(key: str) -> None:
    """Deletes the value for the key."""
    with psycopg.connect(__values_uri) as conn:
        with conn.cursor() as curr:
            curr.execute(DELETE_KEY_VALUE, (key,))
        conn.commit()
