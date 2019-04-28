from wifiology_node_poc.core_sqlite import cursor_manager, load_raw_file
from wifiology_node_poc.models import ServiceSet, Station, Measurement, DataCounters, ServiceSetJitterMeasurement
from wifiology_node_poc.queries import limit_offset_helper, SQL_FOLDER, place_holder_generator


import time

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


def insert_service_set_infrastructure_station(transaction, measurement_id, service_set_bssid, station_mac):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT INTO infrastructureStationServiceSetMap(
               mapStationID, mapServiceSetID, measurementID
            ) SELECT s.stationID, ss.serviceSetID, :measurementID
            FROM station AS s, serviceSet AS ss
            WHERE s.macAddress=:mac AND ss.bssid=:bssid       
            """,
            {"measurementID": measurement_id, "mac": station_mac, "bssid": service_set_bssid}
        )


def insert_service_set_associated_station(transaction, measurement_id, service_set_bssid, station_mac):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT INTO associationStationServiceSetMap(
               associatedStationID, associatedServiceSetID, measurementID
            ) SELECT s.stationID, ss.serviceSetID, :measurementID
            FROM station AS s, serviceSet AS ss
            WHERE s.macAddress=:mac AND ss.bssid=:bssid    
            """,
            {"measurementID": measurement_id, "mac": station_mac, "bssid": service_set_bssid}
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
            SELECT DISTINCT s.* 
            FROM station AS s
            JOIN infrastructureStationServiceSetMap AS m
            ON m.mapStationID = s.stationID
            WHERE m.mapServiceSetID = :serviceSetID
            """,
            {"serviceSetID": service_set_id}
        )
        return [Station.from_row(r) for r in c.fetchall()]


def select_associated_stations_for_service_set(connection, service_set_id):
    with cursor_manager(connection) as c:
        c.execute(
            """
            SELECT DISTINCT s.* 
            FROM station AS s
            JOIN associationStationServiceSetMap AS m
            ON m.associatedStationID = s.stationID
            WHERE m.associatedServiceSetID = :serviceSetID
            """,
            {"serviceSetID": service_set_id}
        )
        return [Station.from_row(r) for r in c.fetchall()]


def insert_measurement_station(transaction, measurement_id, station_id, data_counters):
    params = {"measurementID": measurement_id, "stationID": station_id}
    params.update(data_counters.to_row())
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT INTO measurementStationMap(
               mapMeasurementID, mapStationID, managementFrameCount,
               associationFrameCount, reassociationFrameCount, disassociationFrameCount,
               controlFrameCount, rtsFrameCount, ctsFrameCount,
               ackFrameCount, dataFrameCount, dataThroughputIn, dataThroughputOut,
               retryFrameCount, averagePower, stdDevPower, lowestRate, highestRate,
               failedFCSCount
            ) VALUES (
               :measurementID, :stationID, :managementFrameCount, 
               :associationFrameCount, :reassociationFrameCount, :disassociationFrameCount,
               :controlFrameCount,  :rtsFrameCount, :ctsFrameCount, :ackFrameCount, 
               :dataFrameCount, :dataThroughputIn, :dataThroughputOut,
               :retryFrameCount, :averagePower, :stdDevPower, :lowestRate, :highestRate,
               :failedFCSCount
            )         
            """,
            params
        )


def select_stations_for_measurement(connection, measurement_id):
    with cursor_manager(connection) as c:
        c.execute(
          """
          SELECT s.stationID, s.macAddress, s.extraJSONData, 
            map.* 
          FROM measurementStationMap AS map
          JOIN station AS s ON s.stationID = map.mapStationID
          WHERE mapMeasurementID = :measurement_id
          """,
          {"measurement_id": measurement_id}
        )

        return [
            Station.from_row(r, data_counters=DataCounters.from_row(r))
            for r in c.fetchall()
        ]


def select_service_sets_for_measurement(connection, measurement_id):
    with cursor_manager(connection) as c:
        c.execute(
          """
          SELECT DISTINCT s.* 
          FROM serviceSet as s
          WHERE EXISTS (
            SELECT 1 FROM associationStationServiceSetMap AS a 
            WHERE a.measurementID = :measurementID AND s.serviceSetID = a.associatedServiceSetID UNION ALL
            SELECT 1 FROM infrastructureStationServiceSetMap AS m
            WHERE m.measurementID = :measurementID AND s.serviceSetID = m.mapServiceSetID
          )
          """,
          {"measurementID": measurement_id}
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
            LEFT JOIN associationStationServiceSetMap AS a ON ss.serviceSetID = a.associatedServiceSetID
            LEFT JOIN infrastructureStationServiceSetMap AS m ON ss.serviceSetID = m.mapServiceSetID
            JOIN measurement AS m2 ON a.measurementID = m2.measurementID OR m2.measurementID = m.measurementID
            WHERE m2.channel =  :channelNum
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
            JOIN measurementStationMap AS map ON map.mapStationID = s.stationID
            JOIN measurement AS m ON m.measurementID = map.mapMeasurementID
            WHERE channel = :channelNum 
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
               hasBeenUploaded, extraJSONData
            ) VALUES (
               :measurementStartTime, :measurementEndTime,
               :measurementDuration, :channel, :averageNoise, :stdDevNoise,
               :hasBeenUploaded, :extraJSONData
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
              m.mapMeasurementID AS measurementID,
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
        return {row["measurementID"]: DataCounters.from_row(row) for row in c.fetchall()}


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


def insert_jitter_measurement(transaction, new_jitter_measurement):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT INTO serviceSetJitterMeasurement(
                measurementID, serviceSetID, minJitter, maxJitter, avgJitter, stdDevJitter, 
                jitterHistogram, jitterHistogramOffset, interval, extraJSONData
            ) VALUES (
                :measurementID, :serviceSetID, :minJitter, :maxJitter, :avgJitter, :stdDevJitter,
                :jitterHistogram, :jitterHistogramOffset, :interval, :extraJSONData
            )
            """,
            new_jitter_measurement.to_row()
        )


def select_jitter_measurements_by_measurement_id(connection, measurement_id):
    with cursor_manager(connection) as c:
        c.execute(
            """
            SELECT * FROM serviceSetJitterMeasurement WHERE measurementID = :measurementID
            """,
            {"measurementID": measurement_id}
        )
        return [ServiceSetJitterMeasurement.from_row(row) for row in c.fetchall()]


def select_jitter_measurement_by_measurement_id_and_service_set_id(connection, measurement_id, service_set_id):
    with cursor_manager(connection) as c:
        c.execute(
            """
            SELECT * FROM serviceSetJitterMeasurement
            WHERE measurementID = :measurementID AND serviceSetID = :serviceSetID
            """,
            {"measurementID": measurement_id, "serviceSetID": service_set_id}
        )
        row = c.fetchone()
        if row is not None:
            return ServiceSetJitterMeasurement.from_row(row)
        else:
            return None


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


def select_measurements_that_need_upload(connection, limit):
    clause, params = limit_offset_helper(
        limit, None, order_by="measurementStartTime"
    )
    with cursor_manager(connection) as c:
        c.execute(
            """
            SELECT m.* 
            FROM measurement AS m
            WHERE hasBeenUploaded=0
            """ + clause,
            params
        )
        return [Measurement.from_row(r) for r in c.fetchall()]


def update_measurements_upload_status(transaction, measurement_ids, new_status):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            UPDATE measurement
            SET hasBeenUploaded=?
            WHERE measurementID IN
            """ + place_holder_generator(measurement_ids),
            [1 if new_status else 0] + list(measurement_ids)
        )


def select_infrastructure_mac_addresses_for_measurement_service_set(connection, measurement_id, service_set_id):
    with cursor_manager(connection) as c:
        c.execute(
            """
            SELECT s.macAddress
            FROM station AS s
            JOIN infrastructureStationServiceSetMap AS m
            ON s.stationID = m.mapStationID
            WHERE m.measurementID = :measurementID AND m.mapServiceSetID = :serviceSetID
            """,
            {"measurementID": measurement_id, "serviceSetID": service_set_id}
        )
        return [r["macAddress"] for r in c.fetchall()]


def select_associated_mac_addresses_for_measurement_service_set(connection, measurement_id, service_set_id):
    with cursor_manager(connection) as c:
        c.execute(
            """
            SELECT s.macAddress
            FROM station AS s
            JOIN associationStationServiceSetMap AS a 
            ON s.stationID = a.associatedStationID
            WHERE a.measurementID = :measurementID AND a.associatedServiceSetID = :serviceSetID
            """,
            {"measurementID": measurement_id, "serviceSetID": service_set_id}
        )
        return [r["macAddress"] for r in c.fetchall()]


def delete_old_measurements(transaction, days_old):
    start_time = time.time() - (60*60*24*days_old)
    with cursor_manager(transaction) as c:
        c.execute(
            """
            DELETE FROM measurement WHERE measurementStartTime < ?
            """,
            [start_time]
        )
        return c.rowcount
