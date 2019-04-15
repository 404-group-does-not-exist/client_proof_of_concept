import os
import math
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


class WeightedAverage(object):
    def __init__(self):
        self.running_sum = 0.0
        self.running_weight = 0.0

    def step(self, avg, weight):
        if avg is not None and weight is not None:
            self.running_sum += avg*weight*1.0
            self.running_weight += weight

    def finalize(self):
        if self.running_weight > 0.0:
            return self.running_sum / self.running_weight
        else:
            return None


class WeightedStdDev(object):
    def __init__(self):
        self.running_variance = 0.0
        self.running_weight = 0.0

    def step(self, std_dev, weight):
        if std_dev is not None and weight is not None:
            self.running_variance += (std_dev * std_dev * weight * 1.0)
            self.running_weight += weight

    def finalize(self):
        if self.running_weight > 0.0:
            return math.sqrt(self.running_variance / self.running_weight)
        else:
            return None


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
    conn.create_aggregate("weighted_avg", 2, WeightedAverage)
    conn.create_aggregate("weighted_std_dev", 2, WeightedStdDev)
    return conn


def load_raw_file(filename, folder):
    with open(os.path.join(folder, filename), 'r') as f:
        return f.read()
