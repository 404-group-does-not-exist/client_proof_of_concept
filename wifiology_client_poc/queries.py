from wifiology_client_poc.core_sqlite import cursor_manager, load_raw_file
from wifiology_client_poc.models import Measurement, ServiceSet, Station

import os

SQL_FOLDER = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'sql'
)


def write_schema(connection):
    schema = load_raw_file("schema.sql", SQL_FOLDER)
    connection.executescript(schema)


def limit_offset_helper(limit, offset, extra_params=None):
    params = extra_params or {}
    clause = ""

    if limit is not None:
        clause += " LIMIT :limit"
        params['limit'] = limit
    if limit is not None and offset is not None:
        clause += " OFFSET :offset"
        params['offset'] = offset
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
            INSERT INTO infrastructureStationServiceSetMap(
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