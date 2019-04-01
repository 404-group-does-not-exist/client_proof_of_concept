from wifiology_node_poc.core_sqlite import cursor_manager, load_raw_file
from wifiology_node_poc.models import Measurement, ServiceSet, Station

import os
from bottle import json_dumps, json_loads


SQL_FOLDER = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'sql'
)


def write_schema(connection):
    schema = load_raw_file("schema.sql", SQL_FOLDER)
    connection.executescript(schema)


#helps grab rows after a and before b
def limit_offset_helper(limit, offset, order_by=None, extra_params=None):
    params = extra_params or {}
    clause = ""

    if limit is not None:
        clause += " LIMIT :limit"
        params['limit'] = limit
    if limit is not None and offset is not None:
        clause += " OFFSET :offset"
        params['offset'] = offset
    if order_by is not None:
        # NOTE; Order by should NEVER be user specified, as this would be
        # a SQL injection vulnerability.
        clause += " ORDER BY {0}".format(order_by)
    return clause, params


def insert_measurement(transaction, new_measurement):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT INTO measurement(
               measurementStartTime, measurementEndTime, 
               measurementDuration, channel, managementFrameCount,
               controlFrameCount, dataFrameCount, extraJSONData
            ) VALUES (
               :measurementStartTime, :measurementEndTime,
               :measurementDuration, :channel, :managementFrameCount,
               :controlFrameCount, :dataFrameCount, :extraJSONData
            )
            
            """,
            new_measurement.to_row()
        )
        return c.lastrowid


def select_measurement_by_id(connection, measurement_id):
    with cursor_manager(connection) as c:
        c.execute(
            """
            SELECT * FROM measurement WHERE measurementID=?
            """,
            (measurement_id,)
        )
        return Measurement.from_row(c.fetchone())


def select_all_measurements(connection, limit=None, offset=None):
    clause, params = limit_offset_helper(limit, offset)

    with cursor_manager(connection) as c:
        c.execute("SELECT * FROM measurement " + clause, params)
        return [Measurement.from_row(r) for r in c.fetchall()]


def select_latest_channel_measurements(connection, channel_num, limit=None, offset=None):
    clause, params = limit_offset_helper(
        limit, offset, order_by="measurementStartTime DESC",
        extra_params={"channelNum": channel_num}
    )

    with cursor_manager(connection) as c:
        c.execute(
            """
            SELECT * FROM measurement WHERE channel=:channelNum
            """ + clause,
            params
        )
        return [Measurement.from_row(r) for r in c.fetchall()]


def insert_station(transaction, new_radio_device):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT INTO station(
              macAddress, extraJSONData
            ) VALUES (
              :macAddress, :extraJSONData
            )
            """,
            new_radio_device.to_row()
        )
        return c.lastrowid


def select_all_stations(connection, limit=None, offset=None):
    clause, params = limit_offset_helper(limit, offset)

    with cursor_manager(connection) as c:
        c.execute("SELECT * FROM station " + clause, params)
        return [Station.from_row(r) for r in c.fetchall()]


def select_station_by_id(connection, station_id):
    with cursor_manager(connection) as c:
        c.execute("SELECT * FROM station WHERE stationID = ?", (station_id,))
        return Station.from_row(c.fetchone())


def select_station_by_mac_address(connection, mac_address):
    with cursor_manager(connection) as c:
        c.execute("SELECT * FROM station WHERE macAddress = ?", (mac_address,))
        return Station.from_row(c.fetchone())


def insert_service_set(transaction, new_ssid):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT INTO serviceSet(
              networkName, extraJSONData
            ) VALUES (
              :networkName, :extraJSONData
            )
            """,
            new_ssid.to_row()
        )
        return c.lastrowid


def select_all_service_sets(connection, limit=None, offset=None):
    clause, params = limit_offset_helper(limit, offset)
    with cursor_manager(connection) as c:
        c.execute("SELECT * FROM serviceSet " + clause, params)
        return [ServiceSet.from_row(r) for r in c.fetchall()]


def select_service_set_by_id(connection, service_set_id):
    with cursor_manager(connection) as c:
        c.execute("SELECT * FROM serviceSet WHERE serviceSetID=?", (service_set_id,))
        return ServiceSet.from_row(c.fetchone())


def select_service_set_by_network_name(connection, network_name):
    with cursor_manager(connection) as c:
        c.execute("SELECT * FROM serviceSet WHERE networkName=?", (network_name,))
        return ServiceSet.from_row(c.fetchone())


def insert_service_set_station(transaction, network_name, station_mac):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT OR IGNORE INTO infrastructureStationServiceSetMap(
               mapStationID, mapServiceSetID
            ) SELECT s.stationID, ss.serviceSetID 
            FROM station AS S, serviceSet AS ss
            WHERE s.macAddress = :stationMac
              AND ss.networkName = :networkName            
            """,
            {"networkName": network_name, "stationMac": station_mac}
        )


def select_stations_for_service_set(connection, service_set_id):
    with cursor_manager(connection) as c:
        c.execute(
            """
            SELECT s.* FROM station AS s
            WHERE s.stationID IN (
              SELECT mapStationID FROM infrastructureStationServiceSetMap AS m
              JOIN serviceSet AS ss ON ss.serviceSetID = m.mapServiceSetID
              WHERE serviceSetID = :serviceSetID
            )
            """,
            {"serviceSetID": service_set_id}
        )
        return [Station.from_row(r) for r in c.fetchall()]


def insert_measurement_station(transaction, measurement_id, station_mac):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT OR IGNORE INTO measurementStationMap(
               mapMeasurementID, mapStationID
            ) SELECT :measurementID, s.stationID 
            FROM station AS s
            WHERE s.macAddress = :stationMac            
            """,
            {"measurementID": measurement_id, "stationMac": station_mac}
        )     


def select_stations_for_measurement(connection, measurement_id):
    with cursor_manager(connection) as c:
        c.execute(
          """
          SELECT * FROM station as s
          WHERE s.stationID in (
            SELECT mapStationID FROM measurementStationMap
            WHERE mapMeasurementID = :measurement_id
          )
          """,
          {"measurement_id": measurement_id}
        )
        return [Station.from_row(r) for r in c.fetchall()]


def select_service_sets_for_measurement(connection, measurement_id):
    with cursor_manager(connection) as c:
        c.execute(
          """
          SELECT * FROM serviceSet as s
          WHERE s.serviceSetID in (
            SELECT mapServiceSetID FROM measurementServiceSetMap
            WHERE mapMeasurementID = :measurement_id
          )
          """,
          {"measurement_id": measurement_id}
        )
        return [ServiceSet.from_row(r) for r in c.fetchall()]


def insert_measurement_service_set(transaction, measurement_id, network_name):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT OR IGNORE INTO measurementServiceSetMap(
               mapMeasurementID, mapServiceSetID
            ) SELECT :measurementID, ss.serviceSetID
            FROM serviceSet AS ss
            WHERE ss.networkName = :network_name            
            """,
            {"measurementID": measurement_id, "network_name": network_name}
        )


def select_service_sets_by_channel(connection, channel_num, limit=None, offset=None):
    clause, params = limit_offset_helper(
        limit, offset, extra_params={
            'channelNum': channel_num
        }
    )

    with cursor_manager(connection) as c:
        c.execute(
            """
            SELECT DISTINCT ss.* FROM serviceSet AS ss
            JOIN measurementServiceSetMap AS map
            ON map.mapServiceSetID = ss.serviceSetID
            WHERE map.mapMeasurementID IN (
              SELECT measurementID FROM measurement WHERE channel=:channelNum
            )
            """ + clause,
            params
        ),
        return [ServiceSet.from_row(r) for r in c.fetchall()]


def select_stations_by_channel(connection, channel_num, limit=None, offset=None):
    clause, params = limit_offset_helper(
        limit, offset, extra_params={
            'channelNum': channel_num
        }
    )

    with cursor_manager(connection) as c:
        c.execute(
            """
            SELECT DISTINCT s.* FROM station AS s
            JOIN measurementStationMap AS map
            ON map.mapStationID = s.stationID
            WHERE map.mapMeasurementID IN (
              SELECT measurementID FROM measurement WHERE channel=:channelNum
            )
            """ + clause,
            params
        ),
        return [Station.from_row(r) for r in c.fetchall()]


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
            "SELECT keyName, value FROM keyValueStore" + clause,
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
