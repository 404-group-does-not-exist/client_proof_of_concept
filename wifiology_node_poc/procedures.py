import pcapy
import dpkt
import pyric.pyw as pyw
import timerfd
import select

import argparse
import logging
import time
import struct
import threading
import signal
import pprint
import os
from collections import defaultdict


from wifiology_node_poc.core_sqlite import create_connection, transaction_wrapper, optimize_db
from wifiology_node_poc.queries import write_schema, insert_measurement, insert_service_set_station, \
    insert_station, insert_service_set, select_station_by_mac_address, \
    select_service_set_by_network_name, insert_measurement_service_set, insert_measurement_station, \
    kv_store_set, kv_store_get
from wifiology_node_poc.models import Measurement, \
    Station, ServiceSet
from wifiology_node_poc import LOG_FORMAT

capture_argument_parser = argparse.ArgumentParser('wifiology_capture')
capture_argument_parser.add_argument("interface", type=str, help="The WiFi interface to capture on.")
capture_argument_parser.add_argument("tmp_dir", type=str, help="The temporary storage directory (preferably tmpfs)")
capture_argument_parser.add_argument("-l", "--log-file", type=str, default="-", help="Log file.")
capture_argument_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode.")
capture_argument_parser.add_argument(
    "-s", "--sample-seconds", default=10, type=int, help="The number of seconds to sample each channel."
)
capture_argument_parser.add_argument(
    "-db", "--database-loc", default=":memory:"
)
capture_argument_parser.add_argument(
    "-r", "--capture-rounds", default=0, type=int,
    help="The number of rounds of captures to run before exiting. 0 will run forever."
)
capture_argument_parser.add_argument(
    "--ignore-non-root", action="store_true",
    help="Ignore that the current user is not the root user."
)

procedure_logger = logging.getLogger(__name__)


class StopException(Exception):
    pass


def raise_stop(*args, **kwargs):
    raise StopException


def binary_to_mac(bin):
    if isinstance(bin, bytes):
        return ':'.join(("{:02x}".format(c)) for c in bin)
    else:
        return ':'.join(("{:02x}".format(ord(c))) for c in bin)


def capture_argparse_args_to_kwargs(args):
    return {
        'wireless_interface': args.interface,
        'log_file': args.log_file,
        'tmp_dir': args.tmp_dir,
        'verbose': args.verbose,
        'sample_seconds': args.sample_seconds,
        'database_loc': args.database_loc,
        'rounds': args.capture_rounds,
        'ignore_non_root': args.ignore_non_root
    }


def setup_logging(log_file, verbose):
    root_logger = logging.getLogger('')
    if log_file == "-":
        handler = logging.StreamHandler()
    else:
        handler = logging.FileHandler(log_file)
    handler.setFormatter(
        logging.Formatter(LOG_FORMAT)
    )
    root_logger.addHandler(handler)

    if verbose:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    else:
        handler.setLevel(logging.WARNING)
        root_logger.setLevel(logging.WARNING)


def rate_decoder(raw_rate):
    rates = []
    for byte in raw_rate:
        rate_mandatory = bool(byte & 0b10000000)
        rate_speed_mbps = (byte & 0b01111111) * 0.5
        rates.append((rate_speed_mbps, rate_mandatory))
    return rates


def setup_capture_card(wireless_interface):
    procedure_logger.info("Loading card handle from interface name..")
    card = pyw.getcard(wireless_interface)
    if pyw.modeget(card) == 'monitor':
        procedure_logger.info("Wireless card already in monitor mode.")
    else:
        procedure_logger.info("Setting wireless card to monitor mode.")
        pyw.down(card)
        pyw.modeset(card, 'monitor')
        pyw.up(card)
    return card


def run_live_capture(wireless_interface, capture_file, sample_seconds):
    pcap_dev = pcapy.create(wireless_interface)
    pcap_dev.set_snaplen(65535)
    pcap_dev.set_timeout(0)
    pcap_dev.set_promisc(True)
    pcap_dev.set_buffer_size(15 * 1024 * 1024)

    procedure_logger.info("Opening capture file: {0}".format(capture_file))
    procedure_logger.info("Arming and activating live capture...")
    pcap_dev.activate()
    fd = timerfd.create(timerfd.CLOCK_MONOTONIC, 0)
    timerfd.settime(fd, 0, sample_seconds, 0)
    start_time = time.time()

    dumper = pcap_dev.dump_open(capture_file)

    hdr, data = pcap_dev.next()
    while hdr and not select.select([fd], [], [], 0)[0]:
        dumper.dump(hdr, data)
        hdr, data = pcap_dev.next()
    dumper.close()
    pcap_dev.close()
    end_time = time.time()
    return start_time, end_time, sample_seconds


def run_offline_analysis(capture_file, start_time, end_time, sample_seconds, channel):
    counter = 0
    weird_frame_count = 0
    frame_counter = defaultdict(int)
    ssid_data = {}
    mac_addresses = set()
    ctl_counter = defaultdict(int)

    pcap_offline_dev = pcapy.open_offline(capture_file)
    header, payload = pcap_offline_dev.next()
    while header:
        counter += 1
        try:
            packet = dpkt.radiotap.Radiotap(payload)
            frame = packet.data
            frame_type = frame.type
            frame_subtype = frame.subtype

            if frame_type == dpkt.ieee80211.MGMT_TYPE:
                frame_counter['mgmt'] += 1
                if frame_subtype == dpkt.ieee80211.M_BEACON:
                    if hasattr(frame, 'ssid'):
                        ssid_name = frame.ssid.data
                        if ssid_name not in ssid_data:
                            ssid_data[ssid_name] = {
                                'stations': set(),
                                'beacons': 0
                            }
                        ssid_data[ssid_name]['stations'].add(
                            (binary_to_mac(frame.mgmt.src), channel)
                        )
                        ssid_data[ssid_name]['beacons'] += 1
                    else:
                        procedure_logger.warning("Weird beacon seen! Raw: {0}".format(frame.data))

            elif frame_type == dpkt.ieee80211.CTL_TYPE:
                frame_counter['ctl'] += 1
                if frame_subtype == dpkt.ieee80211.C_RTS:
                    ctl_counter['RTS'] += 1
                elif frame_subtype == dpkt.ieee80211.C_CTS:
                    ctl_counter['CTS'] += 1
                elif frame_subtype == dpkt.ieee80211.C_ACK:
                    ctl_counter['ACK'] += 1
            elif frame_type == dpkt.ieee80211.DATA_TYPE:
                frame_counter['data'] += 1
                mac_addresses.add(binary_to_mac(frame.data_frame.src))
                mac_addresses.add(binary_to_mac(frame.data_frame.dst))
            else:
                frame_counter['other'] += 1
        except dpkt.dpkt.UnpackError:
            logging.warning(
                "dpkt lacks support for some IE80211 features. This could be causing spurious decode problems.",
                exc_info=True
            )
            weird_frame_count += 1
        header, payload = pcap_offline_dev.next()
    pcap_offline_dev.close()

    measurement = Measurement.new(
        start_time, end_time, sample_seconds, channel,
        frame_counter.get('mgmt', 0),
        frame_counter.get('ctl', 0),
        frame_counter.get('data', 0),
        extra_data={
            'ctl_counters': dict(ctl_counter),
            'weird_frame_count': weird_frame_count
        }
    )

    stations = [
        Station.new(mac_addr) for mac_addr in mac_addresses
    ]

    service_sets = [
        ServiceSet.new(name) for name in ssid_data.keys()
    ]
    procedure_logger.info("-----------------")
    procedure_logger.info("Analysis performed on channel: {0}".format(channel))
    procedure_logger.info("Service Sets seen:")
    for service_set in service_sets:
        procedure_logger.info("-- {0}".format(service_set.network_name))
    procedure_logger.info("{0} unique stations seen.".format(len(stations)))
    procedure_logger.info("-----------------")
    return measurement, stations, service_sets, ssid_data


def write_offline_analysis_to_database(db_conn, analysis_data):
    measurement, stations, service_sets, servcie_sets_data = analysis_data

    with transaction_wrapper(db_conn) as t:
        measurement.measurement_id = insert_measurement(
            t, measurement
        )
        for station in stations:
            opt_station = select_station_by_mac_address(t, station.mac_address)
            if opt_station:
                station.station_id = opt_station.station_id
            else:
                station.station_id = insert_station(t, station)
            insert_measurement_station(t, measurement.measurement_id, station.mac_address)
        for service_set in service_sets:
            opt_service_set = select_service_set_by_network_name(t, service_set.network_name)
            if opt_service_set:
                service_set.service_set_id = opt_service_set.service_set_id
            else:
                service_set.service_set_id = insert_service_set(t, service_set)
            insert_measurement_service_set(t, measurement.measurement_id, service_set.network_name)
        for network_name, service_set_data in servcie_sets_data.items():
            for mac_address_data in service_set_data.get("stations", set()):
                insert_service_set_station(t, network_name, mac_address_data[0])
    optimize_db(db_conn)


def run_capture(wireless_interface, log_file, tmp_dir, database_loc,
                verbose=False, sample_seconds=10, rounds=0, ignore_non_root=False):
    try:
        effective_user_id = os.geteuid()
        if effective_user_id != 0 and ignore_non_root:
            procedure_logger.warning("Not running as root, attempting to proceed...")
        elif effective_user_id !=0:
            raise OSError(
                "This script requires root-level permissions to run. "
                "Please either run as superuser or use the --ignore-non-root flag."
            )

        setup_logging(log_file, verbose)

        run_forever = rounds == 0

        db_conn = create_connection(database_loc)
        write_schema(db_conn)

        with transaction_wrapper(db_conn) as t:
            kv_store_set(t, "capture/script_start_time", time.time())
            kv_store_set(t, 'capture/script_pid', os.getpid())
            kv_store_set(t, "capture/interface", wireless_interface)
            kv_store_set(t, "capture/sample_seconds", sample_seconds)

        card = setup_capture_card(wireless_interface)

        if not os.path.exists(tmp_dir):
            procedure_logger.warning("Tmp dir {0} does not exist. Creating...".format(tmp_dir))
            os.makedirs(tmp_dir)

        procedure_logger.info("Beginning channel scan.")

        current_round = 0
        while run_forever or rounds > 0:
            procedure_logger.info("Executing capture round {0}".format(current_round))
            with transaction_wrapper(db_conn) as t:
                kv_store_set(t, "capture/current_script_round", current_round)
            for channel in range(1, 12):
                procedure_logger.info("Changing to channel {0}".format(channel))

                pyw.down(card)
                pyw.up(card)
                pyw.chset(card, channel, None)
                procedure_logger.info("Opening the pcap driver...")
                capture_file = os.path.join(tmp_dir, "channel{0}-{1}.pcap".format(channel, time.time()))

                try:
                    procedure_logger.info("Beginning live capture...")
                    start_time, end_time, duration = run_live_capture(wireless_interface, capture_file, sample_seconds)
                    procedure_logger.info("Starting offline analysis...")
                    data = run_offline_analysis(
                        capture_file, start_time, end_time, duration, channel
                    )
                    procedure_logger.info("Writing analysis data to database...")
                    write_offline_analysis_to_database(
                        db_conn, data
                    )
                    procedure_logger.info("Data written...")
                finally:
                    procedure_logger.info("Cleaning up capture file..")
                    if os.path.exists(capture_file):
                        os.unlink(capture_file)
            if not run_forever:
                rounds -= 1
            current_round += 1
    except BaseException:
        procedure_logger.exception("Unhandled exception during capture! Aborting,...")
        raise
    else:
        procedure_logger.info("No more data. Ending...")
