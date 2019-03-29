import os
from contextlib import contextmanager
from functools import wraps
from sqlite3 import dbapi2 as sqlite


@contextmanager
def immediate_transaction_wrapper(connection):
    try:
        connection.execute("BEGIN IMMEDIATE TRANSACTION")
        yield connection
    except:
        connection.rollback()
        raise
    else:
        connection.commit()


@contextmanager
def transaction_wrapper(connection):
    try:
        connection.execute("BEGIN DEFERRED TRANSACTION")
        yield connection
    except:
        connection.rollback()
        raise
    else:
        connection.commit()


@contextmanager
def cursor_manager(connection):
    cursor = connection.cursor()
    yield cursor
    cursor.close()


def get_table_count(connection):
    with cursor_manager(connection) as cursor:
        cursor.execute("SELECT count(*) as tableCount FROM sqlite_master "
                       "WHERE type = 'table' AND name != 'sqlite_sequence';")
        return cursor.fetchone()['tableCount']


def optimize_db(connection):
    with cursor_manager(connection) as cursor:
        cursor.execute("PRAGMA optimize;")


def vacuum_db(connection):
    with cursor_manager(connection) as cursor:
        cursor.execute("VACUUM")


@wraps(sqlite.connect)
def create_connection(*args, **kwargs):
    conn = sqlite.connect(*args, **kwargs)
    conn.row_factory = sqlite.Row
    return conn


def load_raw_file(filename, folder):
    with open(os.path.join(folder, filename), 'r') as f:
        return f.read()
