import inspect
from bottle import json_dumps, json_loads


class RecordObject(object):
    """
    All record types that match rows in the database must provides methods for:
    * Deserializing from a database row (from_row)
    * Serializing to a database row compatible dict (to_row)
    * Generating something user acceptable and JSON serializable for the API (to_api_response)
    * A copy constructor (copy)
    * A convenience constructor for new items (new)

    The object initialization method should do as little as possible (usually just assign attributes
    directly from parameters). All methods on record objects should be strictly pure (outside of object
    attributes) and strictly side-effect free (i.e NO I/O ON ANY METHOD).
    """

    @classmethod
    def from_row(cls, row, prefix=""):
        raise NotImplementedError

    @classmethod
    def new(cls, *args, **kwargs):
        raise NotImplementedError

    def copy(self, **kwargs):
        raise NotImplementedError

    def to_row(self, prefix=""):
        raise NotImplementedError

    def to_api_response(self):
        raise NotImplementedError

    def _check_init_keys(self, kwargs):
        return set(kwargs.keys()) - set(inspect.signature(self.__init__).parameters.keys())

    @classmethod
    def _json_dumps(cls, *args, **kwargs):
        return json_dumps(*args, **kwargs)

    @staticmethod
    def _json_loads(*args, **kwargs):
        return json_loads(*args, **kwargs)


class Measurement(RecordObject):
    def __init__(self, measurement_id, measurement_start_time, measurement_end_time, measurement_duration,
                 channel, management_frame_count, control_frame_count, data_frame_count, extra_data):
        self.measurement_id = measurement_id
        self.measurement_start_time = measurement_start_time
        self.measurement_end_time = measurement_end_time
        self.measurement_duration = measurement_duration
        self.channel = channel
        self.management_frame_count = management_frame_count
        self.control_frame_count = control_frame_count
        self.data_frame_count = data_frame_count
        self.extra_data = extra_data

    @classmethod
    def from_row(cls, row, prefix=""):
        if row is None:
            return None
        else:
            return cls(
                row[prefix + "measurementID"],
                row[prefix + "measurementStartTime"],
                row[prefix + "measurementEndTime"],
                row[prefix + "measurementDuration"],
                row[prefix + "channel"],
                row[prefix + "managementFrameCount"],
                row[prefix + "controlFrameCount"],
                row[prefix + "dataFrameCount"],
                cls._json_loads(row[prefix + "extraJSONData"])
            )

    @classmethod
    def new(cls, start_time, end_time, duration, channel, management_frame_count, control_frame_count,
            data_frame_count, extra_data=None):
        return cls(
            None,
            start_time,
            end_time,
            duration,
            channel,
            management_frame_count,
            control_frame_count,
            data_frame_count,
            extra_data or {}
        )

    def to_row(self, prefix=""):
        return {
            prefix + 'measurementID': self.measurement_id,
            prefix + 'measurementStartTime': self.measurement_start_time,
            prefix + 'measurementEndTime': self.measurement_end_time,
            prefix + 'measurementDuration': self.measurement_duration,
            prefix + 'channel': self.channel,
            prefix + 'managementFrameCount': self.management_frame_count,
            prefix + 'controlFrameCount': self.control_frame_count,
            prefix + 'dataFrameCount': self.data_frame_count,
            prefix + 'extraJSONData': self._json_dumps(self.extra_data)
        }

    def to_api_response(self):
        return {
            'measurementID': self.measurement_id,
            'measurementStartTime': self.measurement_start_time,
            'measurementEndTime': self.measurement_end_time,
            'measurementDuration': self.measurement_duration,
            'channel': self.channel,
            'managementFrameCount': self.management_frame_count,
            'controlFrameCount': self.control_frame_count,
            'dataFrameCount': self.data_frame_count,
            'extraData': self._json_dumps(self.extra_data)
        }


class RadioDevice(RecordObject):
    def __init__(self, radio_device_id, mac_address, extra_data):
        self.radio_device_id = radio_device_id
        self.mac_address = mac_address
        self.extra_data = extra_data

    @classmethod
    def from_row(cls, row, prefix=""):
        if row is None:
            return None
        else:
            return cls(
                row[prefix + "radioDeviceID"],
                row[prefix + "macAddress"],
                cls._json_loads(row[prefix + "extraJSONData"])
            )

    @classmethod
    def new(cls, mac_address, extra_data=None):
        return cls(
            None, mac_address, extra_data or {}
        )

    def to_row(self, prefix=""):
        return {
            prefix + 'radioDeviceID': self.radio_device_id,
            prefix + 'macAddress': self.mac_address,
            prefix + 'extraJSONData': self._json_dumps(self.extra_data)
        }

    def to_api_response(self):
        return {
            'radioDeviceID': self.radio_device_id,
            'macAddress': self.mac_address,
            'extraData': self.extra_data
        }


class SSID(RecordObject):
    def __init__(self, ssid_id, network_name, extra_data):
        self.ssid_id = ssid_id
        self.network_name = network_name
        self.extra_data = extra_data

    @classmethod
    def from_row(cls, row, prefix=""):
        if row is None:
            return None
        else:
            return cls(
                row[prefix + "ssidID"],
                row[prefix + "networkName"],
                cls._json_loads(row[prefix + "extraJSONData"])
            )

    @classmethod
    def new(cls, network_name, extra_data=None):
        return cls(
            None, network_name, extra_data or {}
        )

    def to_row(self, prefix=""):
        return {
            prefix + 'ssidID': self.ssid_id,
            prefix + 'networkName': self.network_name,
            prefix + 'extraJSONData': self._json_dumps(self.extra_data)
        }

    def to_api_response(self):
        return {
            'ssidID': self.ssid_id,
            'networkName': self.network_name,
            'extraData': self.extra_data
        }

