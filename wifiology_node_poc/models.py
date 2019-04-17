import inspect
import statistics
import math
from bottle import json_dumps, json_loads


def altered_mean(data):
    if not data:
        return None
    elif len(data) == 1:
        return data[0]
    else:
        return statistics.mean(data)


def altered_stddev(data):
    if not data:
        return None
    elif len(data) == 1:
        return 0.0
    else:
        return statistics.stdev(data)


def bytes_to_str(b):
    if isinstance(b, (bytes, bytearray)):
        result = repr(b)[2:-1]
    else:
        result = b
    return result


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
                 channel, average_noise, std_dev_noise, has_been_uploaded, extra_data, data_counters=None):
        self.measurement_id = measurement_id
        self.measurement_start_time = measurement_start_time
        self.measurement_end_time = measurement_end_time
        self.measurement_duration = measurement_duration
        self.channel = channel
        self.average_noise = average_noise
        self.std_dev_noise = std_dev_noise
        self.has_been_uploaded = has_been_uploaded
        self.extra_data = extra_data
        self.data_counters = data_counters

    def __repr__(self):
        return "Measurement(measurementID={id}, startTime={st}, endTime={et}, duration={d}, channel={c}, " \
               "dataCounters={dc})".format(
                    id=self.measurement_id, st=self.measurement_start_time, et=self.measurement_end_time,
                    d=self.measurement_duration, c=self.channel, dc=repr(self.data_counters)
               )

    @classmethod
    def from_row(cls, row, prefix="", data_counters=None):
        if row is None:
            return None
        else:
            return cls(
                row[prefix + "measurementID"],
                row[prefix + "measurementStartTime"],
                row[prefix + "measurementEndTime"],
                row[prefix + "measurementDuration"],
                row[prefix + "channel"],
                row[prefix + "averageNoise"],
                row[prefix + "stdDevNoise"],
                row[prefix + "hasBeenUploaded"],
                cls._json_loads(row[prefix + "extraJSONData"]),
                data_counters=data_counters
            )

    @classmethod
    def new(cls, start_time, end_time, duration, channel, noise_measurements, has_been_uploaded=False,
            extra_data=None, data_counters=None):
        return cls(
            None,
            start_time,
            end_time,
            duration,
            channel,
            altered_mean(noise_measurements),
            altered_stddev(noise_measurements),
            has_been_uploaded,
            extra_data or {},
            data_counters=data_counters
        )

    def to_row(self, prefix=""):
        base_row = {
            prefix + 'measurementID': self.measurement_id,
            prefix + 'measurementStartTime': self.measurement_start_time,
            prefix + 'measurementEndTime': self.measurement_end_time,
            prefix + 'measurementDuration': self.measurement_duration,
            prefix + 'channel': self.channel,
            prefix + 'averageNoise': self.average_noise,
            prefix + 'stdDevNoise': self.std_dev_noise,
            prefix + 'hasBeenUploaded': 1 if self.has_been_uploaded else 0,
            prefix + 'extraJSONData': self._json_dumps(self.extra_data)
        }
        return base_row

    def to_api_response(self):
        base_response = {
            'measurementID': self.measurement_id,
            'measurementStartTime': self.measurement_start_time,
            'measurementEndTime': self.measurement_end_time,
            'measurementDuration': self.measurement_duration,
            'channel': self.channel,
            'averageNoise': self.average_noise,
            'stdDevNoise': self.std_dev_noise,
            'hasBeenUploaded': self.has_been_uploaded,
            'extraData': self.extra_data
        }
        if self.data_counters:
            base_response.update(self.data_counters.to_api_response())
        return base_response

    def to_api_upload_payload(self, stations_data=None, service_sets_data=None, bssid_to_network_name_map=None):
        base_response = {
            'measurementID': self.measurement_id,
            'measurementStartTime': self.measurement_start_time,
            'measurementEndTime': self.measurement_end_time,
            'measurementDuration': self.measurement_duration,
            'channel': self.channel,
            'extraData': self.extra_data
        }
        if self.average_noise is not None:
            base_response['averageNoise'] = self.average_noise
        if self.std_dev_noise is not None:
            base_response['stdDevNoise'] = self.std_dev_noise
        if stations_data is not None:
            base_response['stations'] = stations_data
        if service_sets_data is not None:
            base_response['serviceSets'] = service_sets_data
        if bssid_to_network_name_map is not None:
            base_response['bssidToNetworkNameMap'] = bssid_to_network_name_map
        return base_response

    @property
    def data_frame_count(self):
        return self.data_counters.data_frame_count

    @property
    def control_frame_count(self):
        return self.data_counters.control_frame_count

    @property
    def management_frame_count(self):
        return self.data_counters.management_frame_count


class Station(RecordObject):
    def __init__(self, station_id, mac_address, extra_data, data_counters=None):
        self.station_id = station_id
        self.mac_address = mac_address
        self.extra_data = extra_data
        self.data_counters = data_counters

    def __repr__(self):
        return "Station(stationID={0}, macAddress={1})".format(self.station_id, self.mac_address)

    @classmethod
    def from_row(cls, row, prefix="", data_counters=None):
        if row is None:
            return None
        else:
            return cls(
                row[prefix + "stationID"],
                row[prefix + "macAddress"],
                cls._json_loads(row[prefix + "extraJSONData"]),
                data_counters=data_counters
            )

    @classmethod
    def new(cls, mac_address, extra_data=None, data_counters=None):
        return cls(
            None, mac_address, extra_data or {}, data_counters=data_counters
        )

    def to_row(self, prefix=""):
        return {
            prefix + 'stationID': self.station_id,
            prefix + 'macAddress': self.mac_address,
            prefix + 'extraJSONData': self._json_dumps(self.extra_data)
        }

    def to_api_response(self):
        base_response = {
            'stationID': self.station_id,
            'macAddress': self.mac_address,
            'extraData': self.extra_data
        }
        if self.data_counters:
            base_response['dataCounters'] = self.data_counters.to_api_upload_payload()
        return base_response

    def to_api_upload_payload(self):
        return self.to_api_response()


class ServiceSet(RecordObject):
    def __init__(self, service_set_id, bssid, network_name, extra_data):
        self.service_set_id = service_set_id
        self.bssid = bssid
        self.network_name = network_name
        self.extra_data = extra_data

    def __repr__(self):
        return "ServiceSet(serviceSetID={0}, networkName={1})".format(self.service_set_id, self.network_name)

    @classmethod
    def from_row(cls, row, prefix=""):
        if row is None:
            return None
        else:
            return cls(
                row[prefix + "serviceSetID"],
                row[prefix + "bssid"],
                row[prefix + "networkName"],
                cls._json_loads(row[prefix + "extraJSONData"])
            )

    @classmethod
    def new(cls, bssid, network_name=None, extra_data=None):
        return cls(
            None, bssid, network_name, extra_data or {}
        )

    @property
    def nice_network_name(self):
        if self.network_name is not None:
            return bytes_to_str(self.network_name)
        else:
            return None

    def to_row(self, prefix=""):
        return {
            prefix + 'serviceSetID': self.service_set_id,
            prefix + 'bssid': self.bssid,
            prefix + 'networkName': self.network_name,
            prefix + 'extraJSONData': self._json_dumps(self.extra_data)
        }

    def to_api_response(self):
        return {
            'serviceSetID': self.service_set_id,
            'bssid': self.bssid,
            'networkName': self.nice_network_name,
            'extraData': self.extra_data
        }

    def to_api_upload_payload(self, infra_mac_addresses=None, associated_mac_addresses=None):
        base_payload = {
            'serviceSetID': self.service_set_id,
            'bssid': self.bssid,
            'extraData': self.extra_data
        }
        if self.network_name is not None:
            base_payload["networkName"] = self.nice_network_name
        if infra_mac_addresses is not None:
            base_payload["infrastructureMacAddresses"] = infra_mac_addresses
        if associated_mac_addresses is not None:
            base_payload["associatedMacAddresses"] = associated_mac_addresses
        return base_payload


class DataCounters(RecordObject):
    def __init__(self, management_frame_count, association_frame_count, reassocation_frame_coumt,
                 disassociation_frame_count, control_frame_count, rts_frame_count, cts_frame_count,
                 ack_frame_count, data_frame_count, data_throughput_in, data_throughput_out,
                 retry_frame_count, average_power=None, std_dev_power=None,
                 lowest_rate=None, higest_rate=None, failed_fcs_count=None,
                 power_measurements=None, rate_measurements=None):
        self.management_frame_count = management_frame_count
        self.association_frame_count = association_frame_count
        self.reassociation_frame_count = reassocation_frame_coumt
        self.disassociation_frame_count = disassociation_frame_count
        self.control_frame_count = control_frame_count
        self.rts_frame_count = rts_frame_count
        self.cts_frame_count = cts_frame_count
        self.ack_frame_count = ack_frame_count
        self.data_frame_count = data_frame_count
        self.data_throughput_in = data_throughput_in
        self.data_throughput_out = data_throughput_out
        self.retry_frame_count = retry_frame_count
        self._average_power = average_power
        self._std_dev_power = std_dev_power
        self._lowest_rate = lowest_rate
        self._highest_rate = higest_rate
        self.power_measurements = power_measurements or []
        self.rate_measurements = rate_measurements or []
        self.failed_fcs_count = failed_fcs_count

    def __repr__(self):
        return """
DataCounters(
  managementFrameCount={mfc}, controlFrameCount={cfc}, dataFrameCount={dfc},
  associationFrameCount={afc}, reassociationFrameCount={rfc}, disassociationFrameCount={disfc}, 
  ctsFrameCount={ctsfc}, rtsFrameCount={rtsfc}, ackFrameCount={ackfc}, 
  dataThroughputIn={din}, dataThroughputOut={dout}, retryCount={retry},
  averagePower={avgpwr}, stdDevPower={stdpwr}, lowestDataRate={lowrate},
  highestDataRate={highrate}, failedFCSCount={ffcs}
)  
        """.format(
            mfc=self.management_frame_count, afc=self.association_frame_count,
            rfc=self.reassociation_frame_count, disfc=self.disassociation_frame_count,
            cfc=self.control_frame_count, rtsfc=self.rts_frame_count, ctsfc=self.cts_frame_count,
            ackfc=self.ack_frame_count, dfc=self.data_frame_count, din=self.data_throughput_in,
            dout=self.data_throughput_out, retry=self.retry_frame_count, avgpwr=self.average_power,
            stdpwr=self.std_dev_power, lowrate=self.lowest_rate, highrate=self.highest_rate,
            ffcs=self.failed_fcs_count
        ).strip()

    @property
    def average_power(self):
        if self.power_measurements:
            return altered_mean(self.power_measurements)
        else:
            return self._average_power

    @property
    def std_dev_power(self):
        if self.power_measurements:
            return altered_stddev(self.power_measurements)
        else:
            return self._std_dev_power

    @property
    def highest_rate(self):
        if self.rate_measurements:
            return max(self.rate_measurements)
        else:
            return self._highest_rate

    @property
    def lowest_rate(self):
        if self.rate_measurements:
            return min(self.rate_measurements)
        else:
            return self._lowest_rate

    @property
    def total_frame_count(self):
        return self.management_frame_count + self.control_frame_count + self.data_frame_count

    @classmethod
    def from_row(cls, row, prefix=""):
        if row is None:
            return None
        else:
            return cls(
                row[prefix + "managementFrameCount"],
                row[prefix + "associationFrameCount"],
                row[prefix + "reassociationFrameCount"],
                row[prefix + "disassociationFrameCount"],
                row[prefix + "controlFrameCount"],
                row[prefix + "rtsFrameCount"],
                row[prefix + "ctsFrameCount"],
                row[prefix + "ackFrameCount"],
                row[prefix + "dataFrameCount"],
                row[prefix + "dataThroughputIn"],
                row[prefix + "dataThroughputOut"],
                row[prefix + "retryFrameCount"],
                average_power=row[prefix + "averagePower"],
                std_dev_power=row[prefix + "stdDevPower"],
                lowest_rate=row[prefix + "lowestRate"],
                higest_rate=row[prefix + "highestRate"],
                failed_fcs_count=row["failedFCSCount"]
            )

    @classmethod
    def new(cls, *args, **kwargs):
        return cls(*args, **kwargs)

    @classmethod
    def zero(cls):
        return cls(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, failed_fcs_count=0)

    def add(self, other):
        if not isinstance(other, DataCounters):
            raise TypeError("Can only add two frame count objects!")
        result = DataCounters(
            self.management_frame_count + other.management_frame_count,
            self.association_frame_count + other.association_frame_count,
            self.reassociation_frame_count + other.reassociation_frame_count,
            self.disassociation_frame_count + other.disassociation_frame_count,
            self.control_frame_count + other.control_frame_count,
            self.rts_frame_count + other.rts_frame_count,
            self.cts_frame_count + other.cts_frame_count,
            self.ack_frame_count + other.ack_frame_count,
            self.data_frame_count + other.data_frame_count,
            self.data_throughput_in + other.data_throughput_in,
            self.data_throughput_out + other.data_throughput_out,
            self.retry_frame_count + other.retry_frame_count,
        )
        result.failed_fcs_count = (self.failed_fcs_count or 0) + (other.failed_fcs_count or 0)

        if self.power_measurements and other.power_measurements:
            result.power_measurements = self.power_measurements + other.power_measurements
        else:
            power_frame_count = 0
            power_weighted_avg_sum = 0
            power_weighted_variance_sum = 0
            if self.average_power is not None:
                power_frame_count += self.total_frame_count
                power_weighted_avg_sum += self.total_frame_count * self.average_power
                power_weighted_variance_sum += self.total_frame_count * (self.std_dev_power**2)
            if other.average_power is not None:
                power_frame_count += other.total_frame_count
                power_weighted_avg_sum += other.total_frame_count * other.average_power
                power_weighted_variance_sum += other.total_frame_count * (other.std_dev_power**2)

            if power_frame_count:
                result._average_power = power_weighted_avg_sum/power_frame_count
                result._std_dev_power = math.sqrt(power_weighted_variance_sum/power_frame_count)

        if self.rate_measurements and other.rate_measurements:
            result.rate_measurements = self.rate_measurements + other.rate_measurements
        else:
            if self.lowest_rate is not None and other.lowest_rate is not None:
                result._lowest_rate = min(self.lowest_rate, other.lowest_rate)
            elif self.lowest_rate is not None:
                result._lowest_rate = self.lowest_rate
            elif other.lowest_rate is not None:
                result._lowest_rate = other.lowest_rate
            if self.highest_rate is not None and other.highest_rate is not None:
                result._highest_rate = max(self.highest_rate, other.highest_rate)
            elif self.highest_rate is not None:
                result._highest_rate = self.highest_rate
            elif other.highest_rate is not None:
                result._highest_rate = other.highest_rate
        return result

    def __add__(self, other):
        return self.add(other)

    def to_row(self, prefix=""):
        return {
            prefix + 'managementFrameCount': self.management_frame_count,
            prefix + 'associationFrameCount': self.association_frame_count,
            prefix + 'reassociationFrameCount': self.reassociation_frame_count,
            prefix + 'disassociationFrameCount': self.disassociation_frame_count,
            prefix + 'controlFrameCount': self.control_frame_count,
            prefix + 'rtsFrameCount': self.rts_frame_count,
            prefix + 'ctsFrameCount': self.cts_frame_count,
            prefix + 'ackFrameCount': self.ack_frame_count,
            prefix + 'dataFrameCount': self.data_frame_count,
            prefix + 'dataThroughputIn': self.data_throughput_in,
            prefix + 'dataThroughputOut': self.data_throughput_out,
            prefix + 'retryFrameCount': self.retry_frame_count,
            prefix + 'averagePower': self.average_power,
            prefix + 'stdDevPower': self.std_dev_power,
            prefix + 'lowestRate': self.lowest_rate,
            prefix + 'highestRate': self.highest_rate,
            prefix + 'failedFCSCount': self.failed_fcs_count
        }

    def to_api_response(self):
        return {
            'managementFrameCount': self.management_frame_count,
            'associationFrameCount': self.association_frame_count,
            'reassociationFrameCount': self.reassociation_frame_count,
            'disassociationFrameCount': self.disassociation_frame_count,
            'controlFrameCount': self.control_frame_count,
            'rtsFrameCount': self.rts_frame_count,
            'ctsFrameCount': self.cts_frame_count,
            'ackFrameCount': self.ack_frame_count,
            'dataFrameCount': self.data_frame_count,
            'dataThroughputIn': self.data_throughput_in,
            'dataThroughputOut': self.data_throughput_out,
            'retryFrameCount': self.retry_frame_count,
            'averagePower': self.average_power,
            'stdDevPower': self.std_dev_power,
            'lowestRate': self.lowest_rate,
            'highestRate': self.highest_rate,
            'failedFCSCount': self.failed_fcs_count
        }

    def to_api_upload_payload(self):
        base_payload = {
            'managementFrameCount': self.management_frame_count,
            'associationFrameCount': self.association_frame_count,
            'reassociationFrameCount': self.reassociation_frame_count,
            'disassociationFrameCount': self.disassociation_frame_count,
            'controlFrameCount': self.control_frame_count,
            'rtsFrameCount': self.rts_frame_count,
            'ctsFrameCount': self.cts_frame_count,
            'ackFrameCount': self.ack_frame_count,
            'dataFrameCount': self.data_frame_count,
            'dataThroughputIn': self.data_throughput_in,
            'dataThroughputOut': self.data_throughput_out,
            'retryFrameCount': self.retry_frame_count
        }
        if self.average_power is not None:
            base_payload['averagePower'] = self.average_power
        if self.std_dev_power is not None:
            base_payload['stdDevPower'] = self.std_dev_power
        if self.lowest_rate is not None:
            base_payload['lowestRate'] = self.lowest_rate
        if self.highest_rate is not None:
            base_payload['highestRate'] = self.highest_rate
        if self.failed_fcs_count is not None:
            base_payload['failedFCSCount'] = self.failed_fcs_count
        return base_payload
