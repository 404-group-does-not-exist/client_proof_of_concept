from unittest import TestCase
from assertpy import assert_that

from wifiology_node_poc.core_sqlite import create_connection, transaction_wrapper
from wifiology_node_poc.queries import write_schema, insert_measurement, \
    select_measurement_by_id, select_all_measurements, insert_service_set, \
    insert_service_set_station, insert_station, select_all_service_sets, select_all_stations, \
    select_service_set_by_id, select_service_set_by_network_name, select_station_by_id, \
    select_station_by_mac_address, select_stations_for_service_set, insert_measurement_station, \
    select_stations_for_measurement, select_service_sets_for_measurement, insert_measurement_service_set, \
    kv_store_del, kv_store_get, kv_store_get_all, kv_store_set, kv_store_get_prefix
from wifiology_node_poc.models import Measurement, Station, ServiceSet, FrameCounts


class QueriesUnitTest(TestCase):
    def setUp(self):
        self.connection = create_connection(":memory:")
        write_schema(self.connection)

    def tearDown(self):
        self.connection.close()
        self.connection = None

    @staticmethod
    def assert_frame_counts_equal(left, right):
        assert_that(left).is_instance_of(FrameCounts)
        assert_that(right).is_instance_of(FrameCounts)
        assert_that(right.management_frame_count).is_equal_to(left.management_frame_count)
        assert_that(right.control_frame_count).is_equal_to(left.control_frame_count)
        assert_that(right.rts_frame_count).is_equal_to(left.rts_frame_count)
        assert_that(right.cts_frame_count).is_equal_to(left.cts_frame_count)
        assert_that(right.ack_frame_count).is_equal_to(left.ack_frame_count)
        assert_that(right.data_frame_count).is_equal_to(left.data_frame_count)
        assert_that(right.data_throughput).is_equal_to(left.data_throughput)

    @classmethod
    def assert_measurements_equal(cls, left, right):
        assert_that(left).is_instance_of(Measurement)
        assert_that(right).is_instance_of(Measurement)
        assert_that(left.measurement_id).is_equal_to(right.measurement_id)
        assert_that(left.measurement_start_time).is_equal_to(right.measurement_start_time)
        assert_that(left.measurement_end_time).is_equal_to(right.measurement_end_time)
        assert_that(left.measurement_duration).is_equal_to(right.measurement_duration)
        assert_that(left.channel).is_equal_to(right.channel)
        cls.assert_frame_counts_equal(left.frame_counts, right.frame_counts)
        assert_that(left.extra_data).is_equal_to(right.extra_data)

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
            1.0, 2.0, 0.9, 1, 
            FrameCounts(
                1, 2, 3, 4, 5, 6, 7
            ),
            {"foo": "bar"}
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
            FrameCounts(
                7, 6, 5, 4, 3, 2, 1
            ),
            {"baz": "bar"}
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
            "CU Boulder Wireless", {"baz": ["foo", "bar"]}
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
            select_service_set_by_network_name(self.connection, new_service_set.network_name)
        )

        service_sets = select_all_service_sets(
            self.connection
        )
        assert_that(service_sets).is_length(1)
        self.assert_service_sets_equal(
            new_service_set, service_sets[0]
        )
        assert_that(new_service_set.to_api_response()).is_instance_of(dict)

    def test_station_service_set_linking(self):
        new_service_set = ServiceSet.new(
            "CU Boulder Wireless", {"baz": ["foo", "bar"]}
        )
        new_station = Station.new(
            "01:02:03:04:05:06", {"foo": [1, 2, 3], "bar": [4, 5, 6]}
        )

        with transaction_wrapper(self.connection) as t:
            new_service_set.service_set_id = insert_service_set(
                t, new_service_set
            )
            new_station.station_id = insert_station(
                t, new_station
            )
            insert_service_set_station(
                t,
                new_service_set.network_name,
                new_station.mac_address
            )

        associated_stations = select_stations_for_service_set(
            self.connection, new_service_set.service_set_id
        )
        assert_that(associated_stations).is_length(1)

        self.assert_stations_equal(
            new_station, associated_stations[0]
        )

    def test_measurement_station_map(self):
        new_measurement = Measurement.new(
            1.0, 2.0, 0.9, 1, 
            FrameCounts(
                1, 2, 3, 4, 5, 6, 7
            ),
            {"foo": "bar"}
        )

        with transaction_wrapper(self.connection) as t:
            new_measurement.measurement_id = insert_measurement(
                t, new_measurement
            )
        new_station = Station.new(
            "01:02:03:04:05:06", {"foo": [1, 2, 3], "bar": [4, 5, 6]}
        )
        with transaction_wrapper(self.connection) as t:
            new_station.station_id = insert_station(t, new_station)
        with transaction_wrapper(self.connection) as t:
            insert_measurement_station(t, new_measurement.measurement_id, new_station.mac_address)
        stations = select_stations_for_measurement(self.connection, new_measurement.measurement_id)
        assert_that(stations).is_length(1)
        self.assert_stations_equal(new_station, stations[0])

    def test_measurement_service_set(self):
        new_measurement = Measurement.new(
            1.0, 2.0, 0.9, 1, 
            FrameCounts(
                1, 2, 3, 4, 5, 6, 7
            ),
            {"foo": "bar"}
        )
        with transaction_wrapper(self.connection) as t:
            new_measurement.measurement_id = insert_measurement(
                t, new_measurement
            )
        new_service_set = ServiceSet.new(
            "CU Boulder Wireless", {"baz": ["foo", "bar"]}
        )
        with transaction_wrapper(self.connection) as t:
            new_service_set.service_set_id = insert_service_set(
                t, new_service_set
            )
        with transaction_wrapper(self.connection) as t:
            insert_measurement_service_set(t, new_measurement.measurement_id, "CU Boulder Wireless")

        service_sets = select_service_sets_for_measurement(self.connection, new_measurement.measurement_id)
        assert_that(service_sets).is_length(1)
        self.assert_service_sets_equal(new_service_set, service_sets[0])

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
