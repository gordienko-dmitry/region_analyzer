import logging
import asyncpg

from api.settings import USER, PASSWORD, DATABASE, HOST, PORT, SQL_TIMES_RECONNECT


class SQLExecuteError(Exception):
    pass


def reconnect(func):
    async def wrapper(*args, **kwargs):
        for num in range(SQL_TIMES_RECONNECT):
            try:
                return await func(*args, **kwargs)
            except Exception as ex:
                if num == SQL_TIMES_RECONNECT-1:
                    logging.exception('Error in sql query {}'.format(args[1]))
                pass
        else:
            raise SQLExecuteError
    return wrapper


@reconnect
async def connect():
    """
    Connecting and return connection
    """
    return await asyncpg.connect(user=USER, password=PASSWORD,
                                             database=DATABASE, host=HOST, port=PORT, timeout=10)

@reconnect
async def fetch_query(connection, query, row=False):
    """
    Fetching query, if row=True, then return row (one record), else - return list of record objects
    """
    if row:
        return await connection.fetchrow(query, timeout=3)
    else:
        return await connection.fetch(query, timeout=3)


@reconnect
async def execute_query(connection, query, *args):
    """
    Executing query
    """
    return await connection.execute(query, *args, timeout=3)


@reconnect
async def copy_records_to_table(connection, table, records, columns):
    """
    Mass insert values to DB
    """
    return await connection.copy_records_to_table(table, records=records, columns=columns)


async def close(connection):
    """
    Close connection
    """
    await connection.close()
