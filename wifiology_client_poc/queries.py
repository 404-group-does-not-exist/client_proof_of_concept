from wifiology_client_poc.core_sqlite import cursor_manager, load_raw_file
import os

SQL_FOLDER = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'sql'
)


def write_schema(connection):
    schema = load_raw_file("schema.sql", SQL_FOLDER)
    connection.executescript(schema)


def insert_measurement(transaction, new_measurement):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT INTO measurement(
               measurementStartTime, measurementEndTime, 
               measurementDuration, channel, managementFrameCount,
               controlFrameCount, dataFrameCount, extraJSONData
            ) VALUES (
               :measurementStartTime,
               :measurementEndTime, :channel, :managementFrameCount,
               :controlFrameCount, :dataFrameCount, :extraJSONData
            )
            
            """,
            new_measurement.to_row()
        )
        return c.lastrowid


def insert_radio_device(transaction, new_radio_device):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT INTO radioDevice(
              macAddress, extraJSONData
            ) VALUES (
              :macAddress, :extraJSONData
            )
            """,
            new_radio_device.to_row()
        )
        return c.lastrowid


def insert_ssid(transaction, new_ssid):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT INTO ssid(
              networkName, extraJSONData
            ) VALUES (
              :networkName, :extraJSONData
            )
            """,
            new_ssid.to_row()
        )
        return c.lastrowid


def insert_ssid_radio_device(transaction, network_name, radio_mac):
    with cursor_manager(transaction) as c:
        c.execute(
            """
            INSERT INTO radioServiceSetMap(
               mapRadioDeviceID, mapServiceSetIdentifier
            ) SELECT rd.radioDeviceID, s.ssidID 
            FROM radioDevice AS rd, ssid AS s
            WHERE rd.macAddress = :radioMac
              AND s.networkName = :networkName            
            """,
            {"networkName": network_name, "radioMac": radio_mac}
        )
