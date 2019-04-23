from unittest import TestCase
from assertpy import assert_that

from wifiology_node_poc.core_sqlite import create_connection, transaction_wrapper
from wifiology_node_poc.queries.core import write_schema, insert_measurement, \
    select_measurement_by_id, select_all_measurements, insert_service_set, \
    insert_service_set_infrastructure_station, insert_station, select_all_service_sets, select_all_stations, \
    select_service_set_by_id, select_station_by_id, select_service_set_by_bssid, \
    select_station_by_mac_address, select_infrastructure_stations_for_service_set, insert_measurement_station, \
    select_stations_for_measurement, select_service_sets_for_measurement, \
    insert_service_set_associated_station, select_associated_stations_for_service_set, \
    select_associated_mac_addresses_for_measurement_service_set, \
    select_infrastructure_mac_addresses_for_measurement_service_set, \
    select_measurements_that_need_upload, update_measurements_upload_status, update_service_set_network_name

from wifiology_node_poc.queries.kv import kv_store_del, kv_store_get, kv_store_get_all, kv_store_set, kv_store_get_prefix
from wifiology_node_poc.models import Measurement, Station, ServiceSet, DataCounters


class QueriesUnitTest(TestCase):
    def setUp(self):
        self.connection = create_connection(":memory:")
        write_schema(self.connection)

    def tearDown(self):
        self.connection.close()
        self.connection = None

    @staticmethod
    def assert_data_counters_equal(left, right):
        assert_that(left).is_instance_of(DataCounters)
        assert_that(right).is_instance_of(DataCounters)
        assert_that(right.management_frame_count).is_equal_to(left.management_frame_count)
        assert_that(right.control_frame_count).is_equal_to(left.control_frame_count)
        assert_that(right.data_frame_count).is_equal_to(left.data_frame_count)

        assert_that(right.association_frame_count).is_equal_to(left.association_frame_count)
        assert_that(right.reassociation_frame_count).is_equal_to(left.reassociation_frame_count)
        assert_that(right.disassociation_frame_count).is_equal_to(left.disassociation_frame_count)

        assert_that(right.rts_frame_count).is_equal_to(left.rts_frame_count)
        assert_that(right.cts_frame_count).is_equal_to(left.cts_frame_count)
        assert_that(right.ack_frame_count).is_equal_to(left.ack_frame_count)

        assert_that(right.data_throughput_in).is_equal_to(left.data_throughput_in)
        assert_that(right.data_throughput_out).is_equal_to(left.data_throughput_out)
        assert_that(right.retry_frame_count).is_equal_to(left.retry_frame_count)

        assert_that(right.average_power).is_equal_to(left.average_power)
        assert_that(right.std_dev_power).is_equal_to(left.std_dev_power)

        assert_that(right.lowest_rate).is_equal_to(left.lowest_rate)
        assert_that(right.highest_rate).is_equal_to(left.highest_rate)

    @classmethod
    def assert_measurements_equal(cls, left, right, check_data_counters=False):
        assert_that(left).is_instance_of(Measurement)
        assert_that(right).is_instance_of(Measurement)
        assert_that(left.measurement_id).is_equal_to(right.measurement_id)
        assert_that(left.measurement_start_time).is_equal_to(right.measurement_start_time)
        assert_that(left.measurement_end_time).is_equal_to(right.measurement_end_time)
        assert_that(left.measurement_duration).is_equal_to(right.measurement_duration)
        assert_that(left.channel).is_equal_to(right.channel)
        assert_that(left.extra_data).is_equal_to(right.extra_data)

        if check_data_counters:
            cls.assert_data_counters_equal(left.data_counters, right.data_counters)

    @staticmethod
    def assert_stations_equal(left, right):
        assert_that(left).is_instance_of(Station)
        assert_that(right).is_instance_of(Station)
        assert_that(left.station_id).is_equal_to(right.station_id)
        assert_that(left.mac_address).is_equal_to(right.mac_address)
        assert_that(left.extra_data).is_equal_to(right.extra_data)

    @staticmethod
    def assert_service_sets_equal(left, right):
        assert_that(left).is_instance_of(ServiceSet)
        assert_that(right).is_instance_of(ServiceSet)
        assert_that(left.service_set_id).is_equal_to(right.service_set_id)
        assert_that(left.network_name).is_equal_to(right.network_name)
        assert_that(left.extra_data).is_equal_to(right.extra_data)

    def test_measurement_crud(self):
        new_measurement = Measurement.new(
            1.0, 2.0, 0.9, 1, [],
            extra_data={"foo": "bar"}
        )

        with transaction_wrapper(self.connection) as t:
            new_measurement.measurement_id = insert_measurement(
                t, new_measurement
            )

        assert_that(new_measurement.measurement_id).is_not_none().is_instance_of(int)
        self.assert_measurements_equal(
            new_measurement, select_measurement_by_id(self.connection, new_measurement.measurement_id)
        )

        measurements = select_all_measurements(self.connection, limit=500, offset=0)
        assert_that(measurements).is_length(1)
        self.assert_measurements_equal(
            new_measurement, measurements[0]
        )

        new_measurement_2 = Measurement.new(
            3.0, 4.0, 0.8, 2,
            [],
            extra_data={"baz": "bar"}
        )

        with transaction_wrapper(self.connection) as t:
            new_measurement_2.measurement_id = insert_measurement(
                t, new_measurement_2
            )

        measurements = select_all_measurements(self.connection, limit=500, offset=0)
        assert_that(measurements).is_length(2)

        self.assert_measurements_equal(
            new_measurement_2,
            select_measurement_by_id(self.connection, new_measurement_2.measurement_id)
        )
        assert_that(new_measurement_2.to_api_response()).is_instance_of(dict)

    def test_station_crud(self):
        new_station = Station.new(
            "01:02:03:04:05:06", {"foo": [1, 2, 3], "bar": [4, 5, 6]}
        )
        with transaction_wrapper(self.connection) as t:
            new_station.station_id = insert_station(t, new_station)

        self.assert_stations_equal(
            new_station,
            select_station_by_id(self.connection, new_station.station_id)
        )

        self.assert_stations_equal(
            new_station,
            select_station_by_mac_address(self.connection, new_station.mac_address)
        )

        stations = select_all_stations(
            self.connection, limit=500
        )
        assert_that(stations).is_length(1)
        self.assert_stations_equal(
            new_station, stations[0]
        )

        assert_that(new_station.to_api_response()).is_instance_of(dict)

    def test_service_set_crud(self):
        new_service_set = ServiceSet.new(
            "00:A0:C9:00:00:00", "CU Boulder Wireless", {"baz": ["foo", "bar"]}
        )

        with transaction_wrapper(self.connection) as t:
            new_service_set.service_set_id = insert_service_set(
                t, new_service_set
            )

        self.assert_service_sets_equal(
            new_service_set,
            select_service_set_by_id(self.connection, new_service_set.service_set_id)
        )
        self.assert_service_sets_equal(
            new_service_set,
            select_service_set_by_bssid(self.connection, new_service_set.bssid)
        )

        service_sets = select_all_service_sets(
            self.connection
        )
        assert_that(service_sets).is_length(1)
        self.assert_service_sets_equal(
            new_service_set, service_sets[0]
        )
        assert_that(new_service_set.to_api_response()).is_instance_of(dict)

        with transaction_wrapper(self.connection) as t:
            update_service_set_network_name(t, "00:A0:C9:00:00:00", "other wireless")
        assert_that(select_service_set_by_id(self.connection, new_service_set.service_set_id).network_name)\
            .is_equal_to("other wireless")

    def test_station_service_set_linking(self):
        new_service_set = ServiceSet.new(
             "00:A0:C9:00:00:00", "CU Boulder Wireless", {"baz": ["foo", "bar"]}
        )
        new_service_set2 = ServiceSet.new(
            "00:A1:C9:01:02:03", "CU Guest Wireless", {}
        )
        new_service_set3 = ServiceSet.new(
            "00:A0:C9:00:00:01", "CU Boulder Wireless", {"foo": "bar"}
        )

        new_station = Station.new(
            "01:02:03:04:05:06", {"foo": [1, 2, 3], "bar": [4, 5, 6]}
        )

        new_station_2 = Station.new(
            "01:02:03:04:05:07", {"foo": [1, 2, 3], "bar": [4, 5, 6]}
        )

        new_station_3 = Station.new(
            "01:02:03:04:05:08", {"foo": [1, 2, 3], "bar": [4, 5, 6]}
        )

        new_station_4 = Station.new(
            "01:02:03:04:05:09", {"foo": [1, 2, 3], "bar": [4, 5, 6]}
        )

        with transaction_wrapper(self.connection) as t:
            measurement_id = insert_measurement(
                t, Measurement.new(0, 0, 0, 1, [], False,)
            )

            new_service_set.service_set_id = insert_service_set(
                t, new_service_set
            )
            new_service_set2.service_set_id = insert_service_set(
                t, new_service_set2
            )
            new_service_set3.service_set_id = insert_service_set(
                t, new_service_set3
            )
            new_station.station_id = insert_station(
                t, new_station
            )
            new_station_2.station_id = insert_station(
                t, new_station_2
            )

            new_station_3.station_id = insert_station(
                t, new_station_3
            )
            new_station_4.station_id = insert_station(
                t, new_station_4
            )

            insert_service_set_infrastructure_station(
                t,
                measurement_id,
                new_service_set.bssid,
                new_station.mac_address
            )
            insert_service_set_infrastructure_station(
                t,
                measurement_id,
                new_service_set.bssid,
                new_station_3.mac_address
            )
            insert_service_set_associated_station(
                t,
                measurement_id,
                new_service_set.bssid,
                new_station_2.mac_address
            )

        infra_stations = select_infrastructure_stations_for_service_set(
            self.connection, new_service_set.service_set_id
        )
        assert_that(infra_stations).is_length(2)

        self.assert_stations_equal(
            new_station, [i for i in infra_stations if i.station_id == new_station.station_id][0]
        )

        associated_stations = select_associated_stations_for_service_set(
            self.connection, new_service_set.service_set_id
        )
        assert_that(associated_stations).is_length(1)
        self.assert_stations_equal(
            new_station_2, associated_stations[0]
        )

    def test_measurement_station_map(self):
        new_measurement = Measurement.new(
            1.0, 2.0, 0.9, 1, 
            [],
            {"foo": "bar"}
        )

        with transaction_wrapper(self.connection) as t:
            new_measurement.measurement_id = insert_measurement(
                t, new_measurement
            )
        new_station = Station.new(
            "01:02:03:04:05:06", {"foo": [1, 2, 3], "bar": [4, 5, 6]}
        )
        new_station_2 = Station.new(
            "01:02:03:04:05:07", {"foo": [1, 2, 3], "bar": [4, 5, 6]}
        )
        with transaction_wrapper(self.connection) as t:
            new_station.station_id = insert_station(t, new_station)
            new_station_2.station_id = insert_station(t, new_station_2)
        my_counter = DataCounters(
            9, 2, 3,
            4, 9, 3, 3,
            3, 10, 2000, 1500,
            1, power_measurements=[1.0, 2.0, 3.0],
            rate_measurements=[1, 2, 1, 4]
        )
        with transaction_wrapper(self.connection) as t:
            insert_measurement_station(
                t, new_measurement.measurement_id, new_station.station_id, DataCounters.zero()
            )
            insert_measurement_station(
                t, new_measurement.measurement_id, new_station_2.station_id, my_counter
            )
        stations = select_stations_for_measurement(self.connection, new_measurement.measurement_id)
        assert_that(stations).is_length(2)
        self.assert_stations_equal(new_station, stations[0])
        self.assert_data_counters_equal(DataCounters.zero(), stations[0].data_counters)
        self.assert_stations_equal(new_station_2, stations[1])
        self.assert_data_counters_equal(my_counter, stations[1].data_counters)

    def test_measurement_service_set(self):
        new_measurement = Measurement.new(
            1.0, 2.0, 0.9, 1, 
            [],
            extra_data={"foo": "bar"}
        )
        with transaction_wrapper(self.connection) as t:
            new_measurement.measurement_id = insert_measurement(
                t, new_measurement
            )
        new_service_set = ServiceSet.new(
            "00:01:00:00:01:00", "CU Boulder Wireless", {"baz": ["foo", "bar"]}
        )
        new_service_set2 = ServiceSet.new(
            "00:01:00:00:01:01", "CU Boulder Wireless", {"baz": ["foo", "bar"]}
        )
        new_station = Station.new(
            "00:02:00:00:02:00", {}
        )
        new_station2 = Station.new(
            "00:02:00:00:02:01", {}
        )
        with transaction_wrapper(self.connection) as t:
            new_service_set.service_set_id = insert_service_set(
                t, new_service_set
            )
            new_service_set2.service_set_id = insert_service_set(
                t, new_service_set2
            )
            new_station.station_id = insert_station(
                t, new_station
            )
            new_station2.station_id = insert_station(
                t, new_station2
            )

        with transaction_wrapper(self.connection) as t:
            insert_service_set_associated_station(
                t, new_measurement.measurement_id, new_service_set.bssid, new_station.mac_address
            )
            insert_service_set_associated_station(
                t, new_measurement.measurement_id, new_service_set2.bssid, new_station2.mac_address
            )

        service_sets = select_service_sets_for_measurement(self.connection, new_measurement.measurement_id)
        assert_that(service_sets).is_length(2)
        for ss in service_sets:
            if ss.service_set_id == new_service_set.service_set_id:
                self.assert_service_sets_equal(ss, new_service_set)
            elif ss.service_set_id == new_service_set2.service_set_id:
                self.assert_service_sets_equal(ss, new_service_set2)
            else:
                assert False

    def test_upload_related_queries(self):
        new_measurement = Measurement.new(
            1.0, 2.0, 0.9, 1, [],
            extra_data={"foo": "bar"}
        )

        with transaction_wrapper(self.connection) as t:
            new_measurement.measurement_id = insert_measurement(
                t, new_measurement
            )
        m = select_measurements_that_need_upload(self.connection, 100)
        assert_that(m).is_length(1)
        self.assert_measurements_equal(m[0], new_measurement)

        with transaction_wrapper(self.connection) as t:
            update_measurements_upload_status(t, [new_measurement.measurement_id], True)
        assert_that(select_measurements_that_need_upload(self.connection, 100)).is_empty()

        with transaction_wrapper(self.connection) as t:
            ssid = insert_service_set(
                t, ServiceSet.new("00:00:00:01:01:01", "test")
            )
            sid1 = insert_station(
                t, Station.new("01:02:03:04:05:06")
            )
            sid2 = insert_station(
                t, Station.new("01:02:03:04:05:07")
            )
            insert_service_set_infrastructure_station(
                t, new_measurement.measurement_id, "00:00:00:01:01:01", "01:02:03:04:05:06"
            )
            insert_service_set_associated_station(
                t, new_measurement.measurement_id, "00:00:00:01:01:01", "01:02:03:04:05:07"
            )

        assert_that(select_infrastructure_mac_addresses_for_measurement_service_set(
            self.connection, new_measurement.measurement_id, ssid
        )).is_length(1).contains("01:02:03:04:05:06")
        assert_that(select_associated_mac_addresses_for_measurement_service_set(
            self.connection, new_measurement.measurement_id, ssid
        )).is_length(1).contains("01:02:03:04:05:07")

    def test_kv_functionality(self):
        assert_that(kv_store_get_all(self.connection)).is_empty()
        assert_that(kv_store_get_prefix(self.connection, "")).is_empty()

        with transaction_wrapper(self.connection) as t:
            kv_store_set(t, "foo/foo", 1)
            kv_store_set(t, "foo/bar", 2)
            kv_store_set(t, "bar/bar", 3)

        assert_that(kv_store_get_all(self.connection)).is_length(3).contains(
            ("foo/foo", 1), ("foo/bar", 2), ("bar/bar", 3)
        )
        assert_that(kv_store_get_prefix(self.connection, "")).is_length(3).contains(
            ("foo/foo", 1), ("foo/bar", 2), ("bar/bar", 3)
        )
        assert_that(kv_store_get_prefix(self.connection, "foo")).is_length(2).does_not_contain(
            ("bar/bar", 3)
        )

        assert_that(kv_store_get(self.connection, "foo/foo")).is_equal_to(1)
        assert_that(kv_store_get(self.connection, "wat/wat", "default")).is_equal_to("default")

        with transaction_wrapper(self.connection) as t:
            kv_store_del(t, "foo/foo")

        assert_that(kv_store_get_all(self.connection)).is_length(2)
        assert_that(kv_store_get(self.connection, "foo/foo")).is_none()
