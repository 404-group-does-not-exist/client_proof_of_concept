from wifiology_node_poc.core_sqlite import cursor_manager, load_raw_file
from wifiology_node_poc.models import ServiceSet, Station, Measurement
from wifiology_node_poc.queries import limit_offset_helper, SQL_FOLDER, place_holder_generator


def select_all_service_sets(connection, limit=None, offset=None):
    clause, params = limit_offset_helper(limit, offset)
    with cursor_manager(connection) as c:
        c.execute("SELECT * FROM serviceSet " + clause, params)
        return [ServiceSet.from_row(r) for r in c.fetchall()]


def select_service_set_by_id(connection, service_set_id):
    with cursor_manager(connection) as c:
        c.execute("SELECT * FROM serviceSet WHERE serviceSetID=?", (service_set_id,))
        return ServiceSet.from_row(c.fetchone())


def select_service_set_by_bssid(connection, bssid):
    with cursor_manager(connection) as c:
        c.execute("SELECT * FROM serviceSet WHERE bssid=?", (bssid,))
        return ServiceSet.from_row(c.fetchone())


def insert_service_set_infrastructure_station(transaction, service_set_bssid, station_mac):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT OR IGNORE INTO infrastructureStationServiceSetMap(
               mapStationID, mapServiceSetID
            ) SELECT s.stationID, ss.serviceSetID
            FROM station AS s, serviceSet AS ss
            WHERE s.macAddress=? AND ss.bssid = ?       
            """,
            (station_mac, service_set_bssid)
        )


def insert_service_set_associated_station(transaction, service_set_bssid, station_mac):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT OR IGNORE INTO associationStationServiceSetMap(
               associatedStationID, associatedServiceSetID
            ) SELECT s.stationID, ss.serviceSetID
            FROM station AS s, serviceSet AS ss
            WHERE s.macAddress=? AND ss.bssid = ?    
            """,
            (station_mac, service_set_bssid)
        )


def update_service_set_network_name(transaction, service_set_bssid, network_name):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            UPDATE serviceSet
            SET networkName = :networkName
            WHERE bssid = :bssid AND networkName != :networkName
            """,
            {"bssid": service_set_bssid, "networkName": network_name}
        )


def select_infrastructure_stations_for_service_set(connection, service_set_id):
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


def insert_measurement_station(transaction, measurement_id, station_id, frame_counts):
    params = {"measurementID": measurement_id, "stationID": station_id}
    params.update(frame_counts.to_row())
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT INTO measurementStationMap(
               mapMeasurementID, mapStationID, managementFrameCount,
               controlFrameCount, rtsFrameCount, ctsFrameCount,
               ackFrameCount, dataFrameCount, dataThroughputIn, dataThroughputOut
            ) VALUES (:measurementID, :stationID, :managementFrameCount, :controlFrameCount, 
               :rtsFrameCount, :ctsFrameCount, :ackFrameCount, :dataFrameCount, 
               :dataThroughputIn, :dataThroughputOut
            )         
            """,
            params
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


def insert_measurement_service_set(transaction, measurement_id, service_set_id):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT INTO measurementServiceSetMap(
               mapMeasurementID, mapServiceSetID
            ) VALUES (
               ?, ?
            )         
            """,
            (measurement_id, service_set_id)
        )


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


def write_schema(connection):
    schema = load_raw_file("schema.sql", SQL_FOLDER)
    connection.executescript(schema)


def insert_measurement(transaction, new_measurement):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT INTO measurement(
               measurementStartTime, measurementEndTime, 
               measurementDuration, channel, averageNoise, stdDevNoise, 
               extraJSONData
            ) VALUES (
               :measurementStartTime, :measurementEndTime,
               :measurementDuration, :channel, :averageNoise, :stdDevNoise,
                :extraJSONData
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


def select_data_counters_for_measurements(connection, measurement_ids):
    with cursor_manager(connection) as c:
        c.execute(
            """
            SELECT 
              SUM(m.managementFrameCount) AS managementFrameCount,
              SUM(m.associationFrameCount) AS associationFrameCount,
              SUM(m.reassociationFrameCount) AS reassociationFrameCount,
              SUM(m.disassociationFrameCount) AS disassociationFrameCount,
              SUM(m.controlFrameCount) AS controlFrameCount,
              SUM(m.rtsFrameCount) AS rtsFrameCount,
              SUM(m.ctsFrameCount) AS ctsFrameCount,
              SUM(m.ackFrameCount) AS ackFrameCount,
              SUM(m.dataFrameCount) AS dataFrameCount,
              SUM(m.dataThroughputIn) AS dataThroughputIn,
              SUM(m.dataThroughputOut) AS dataThroughputOut,
              SUM(m.retryFrameCount) AS retryFrameCount,
              weighted_avg(m.averagePower, m.managementFrameCount + m.controlFrameCount + m.dataFrameCount) AS averagePower,
              weighted_std_dev(m.stdDevPower, m.managementFrameCount + m.controlFrameCount + m.dataFrameCount) AS stdDevPower,
              MIN(m.lowestRate) AS lowestRate,
              MAX(m.highestRate) AS highestRate,
              SUM(m.failedFCSCount) AS failedFCSCount
            FROM measurementStationMap AS m
            GROUP BY m.mapMeasurementID
            HAVING m.mapMeasurementID IN
            """ + place_holder_generator(measurement_ids),
            measurement_ids
        )


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


def select_latest_channel_device_counts(connection, channel_num, limit=None, offset=None):
    clause, params = limit_offset_helper(
        limit, offset, order_by="m.measurementStartTime DESC",
        extra_params={"channelNum": channel_num}
    )

    with cursor_manager(connection) as c:
        c.execute(
            """
            SELECT m.measurementID, m.measurementStartTime, m.measurementEndTime, 
              m.measurementDuration, COUNT(DISTINCT map.mapStationID) AS stationCount
            FROM measurement AS m 
            JOIN measurementStationMap AS map
            ON m.measurementID = map.mapMeasurementID
            WHERE channel=:channelNum
            GROUP BY m.measurementID
            """ + clause,
            params
        )
        return [dict(r) for r in c.fetchall()]


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
              bssid, networkName, extraJSONData
            ) VALUES (
              :bssid, :networkName, :extraJSONData
            )
            """,
            new_ssid.to_row()
        )
        return c.lastrowid
