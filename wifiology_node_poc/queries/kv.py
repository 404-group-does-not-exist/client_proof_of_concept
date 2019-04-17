from bottle import json_loads, json_dumps

from wifiology_node_poc.core_sqlite import cursor_manager
from wifiology_node_poc.queries import limit_offset_helper


def kv_store_get(connection, key_name, default=None):
    assert isinstance(key_name, str)
    with cursor_manager(connection) as c:
        c.execute(
            """
            SELECT value FROM keyValueStore WHERE keyName=?
            """,
            (key_name,)
        )
        row = c.fetchone()
        if row is None:
            return default
        else:
            return json_loads(row['value'])


def kv_store_get_prefix(connection, prefix_name, limit=None, offset=None):
    assert isinstance(prefix_name, str)
    clause, params = limit_offset_helper(
        limit, offset, order_by="keyName",
        extra_params={"prefix": prefix_name}
    )
    with cursor_manager(connection) as c:
        c.execute(
            """
            SELECT keyName, value FROM keyValueStore WHERE keyName LIKE :prefix || '%'
            """ + clause,
            params
        )
        return [(r['keyName'], json_loads(r['value'])) for r in c.fetchall()]


def kv_store_get_all(connection, limit=None, offset=None):
    clause, params = limit_offset_helper(
        limit, offset, order_by="keyName"
    )
    with cursor_manager(connection) as c:
        c.execute(
            "SELECT keyName, value FROM keyValueStore " + clause,
            params
        )
        return [(r['keyName'], json_loads(r['value'])) for r in c.fetchall()]


def kv_store_set(transaction, key_name, value):
    assert isinstance(key_name, str)
    with cursor_manager(transaction) as c:
        c.execute(
            """
            REPLACE INTO keyValueStore(keyName, value) VALUES(?, ?)
            """,
            (key_name, json_dumps(value))
        )


def kv_store_del(transaction, key_name):
    assert isinstance(key_name, str)
    with cursor_manager(transaction) as c:
        c.execute(
            "DELETE FROM keyValueStore WHERE keyName=?", (key_name,)
        )