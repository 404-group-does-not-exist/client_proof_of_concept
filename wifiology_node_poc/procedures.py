import pcapy
import dpkt
import pyric.pyw as pyw
import timerfd
import select
import requests
from urllib.parse import urljoin
from scapy.layers import dot11

import argparse
import logging
import time
import functools
import os
from wifiology_node_poc.utils import altered_stddev, altered_mean, bytes_to_str
from collections import defaultdict
from bottle import json_dumps


from wifiology_node_poc.core_sqlite import create_connection, transaction_wrapper, optimize_db, vacuum_db
from wifiology_node_poc.queries.core import write_schema, insert_measurement, insert_service_set_infrastructure_station, \
    insert_station, insert_service_set, select_station_by_mac_address, \
    select_service_set_by_bssid, insert_measurement_station, \
    insert_service_set_associated_station, update_service_set_network_name, select_measurements_that_need_upload, \
    update_measurements_upload_status, select_stations_for_measurement, select_service_sets_for_measurement, \
    select_associated_mac_addresses_for_measurement_service_set, \
    select_infrastructure_mac_addresses_for_measurement_service_set, delete_old_measurements, \
    insert_jitter_measurement, select_jitter_measurements_by_measurement_id
from wifiology_node_poc.queries.kv import kv_store_set, kv_store_get
from wifiology_node_poc.models import Measurement, \
    Station, ServiceSet, DataCounters, ServiceSetJitterMeasurement
from wifiology_node_poc import LOG_FORMAT
from wifiology_node_poc.watchdog import run_monitored


# -----------------------------------
#  CAPTURE
# -----------------------------------

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
capture_argument_parser.add_argument(
    "--db-timeout-seconds", type=int, default=60,
    help="The timeout to set on the database connection"
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


def calculate_beacon_jitter(timing_measurements, bssid):
    if not timing_measurements or len(timing_measurements) < 2:
        return None, None, None
    measurements, intervals = zip(*timing_measurements)
    sorted_measurements = sorted(measurements)
    intervals = list(set(intervals))
    if len(intervals) > 1:
        procedure_logger.warning(
            "BSSID {0} has multiple reported intervals! Something funny is going on...".format(bssid)
        )
        procedure_logger.warning("Intervals seen: {0}".format(intervals))
        bad_intervals = True
    else:
        bad_intervals = False
    chosen_interval = intervals[0]
    jitter = [
        (sorted_measurements[i] - sorted_measurements[i-1]) - (chosen_interval*1024)
        for i in range(1, len(sorted_measurements))
    ]
    return jitter, bad_intervals, intervals


def patched_network_stats(pkt):
    summary = {}
    crypto = set()
    p = pkt.payload
    while isinstance(p, dot11.Dot11Elt):
        if p.ID == 0:
            summary["ssid"] = bytes_to_str(p.info)
        elif p.ID == 3:
            summary["channel"] = p.info[0]
        elif p.ID == 7:
            summary["country"] = bytes_to_str(p.info[0:2])
        elif p.ID == 33:
            summary["power_capability"] = {
                "min": p.info[0],
                "max": p.info[1]
            }
        elif isinstance(p, dot11.Dot11EltRates):
            summary["rates"] = p.rates
        elif isinstance(p, dot11.Dot11EltRSN):
            crypto.add("WPA2")
        elif p.ID == 221:
            if isinstance(p, dot11.Dot11EltMicrosoftWPA):
                crypto.add("WPA")
        p = p.payload
    if not crypto:
        if pkt.cap.privacy:
            crypto.add("WEP")
        else:
            crypto.add("OPN")
    summary["crypto"] = list(crypto)
    return summary


def has_bad_fcs(flags):
    if len(flags.data) > 0:
        return flags.data[0] & 0x40
    else:
        return False


def sum_data_counters(data_counters):
    return functools.reduce(lambda x, y: x + y, data_counters, DataCounters.zero())


def capture_argparse_args_to_kwargs(args):
    return {
        'wireless_interface': args.interface,
        'log_file': args.log_file,
        'tmp_dir': args.tmp_dir,
        'verbose': args.verbose,
        'sample_seconds': args.sample_seconds,
        'database_loc': args.database_loc,
        'rounds': args.capture_rounds,
        'ignore_non_root': args.ignore_non_root,
        'db_timeout_seconds': args.db_timeout_seconds
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
    timer_fd = timerfd.create(timerfd.CLOCK_MONOTONIC, 0)
    try:
        timerfd.settime(timer_fd, 0, sample_seconds, 0)
        start_time = time.time()

        dumper = pcap_dev.dump_open(capture_file)

        hdr, data = pcap_dev.next()
        while hdr and not select.select([timer_fd], [], [], 0)[0]:
            dumper.dump(hdr, data)
            hdr, data = pcap_dev.next()
        dumper.close()
        pcap_dev.close()
        end_time = time.time()
        return start_time, end_time, sample_seconds
    finally:
        os.close(timer_fd)


def run_offline_analysis(capture_file, start_time, end_time, sample_seconds, channel):
    weird_frame_count = 0

    bssid_to_ssid_map = {}
    bssid_infra_macs = defaultdict(set)
    bssid_associated_macs = defaultdict(set)
    bssid_beacon_timing_payloads = defaultdict(list)
    bssid_beacon_data = {}
    bssid_to_jitter_map = {}
    bssid_to_power_map = defaultdict(list)

    noise_measurements = []
    action_counter = 0
    probe_req_counter = 0

    station_counters = defaultdict(DataCounters.zero)

    pcap_offline_dev = pcapy.open_offline(capture_file)
    header, payload = pcap_offline_dev.next()

    while header:
        try:
            # Use Scapy for the RadioTap decoding as dpkt's Radiotap decoder is totally broken.
            radiotap_frame = dot11.RadioTap(payload)

            if radiotap_frame.dBm_AntNoise is not None:
                noise_measurements.append(radiotap_frame.dBm_AntNoise)

            frame = dpkt.radiotap.Radiotap(payload).data
            frame_type = frame.type
            frame_subtype = frame.subtype

            if frame_type == dpkt.ieee80211.MGMT_TYPE:
                mac = binary_to_mac(frame.mgmt.src)
                current_counter = station_counters[mac]
                current_counter.management_frame_count += 1

                if frame_subtype == dpkt.ieee80211.M_BEACON:
                    beacon = dot11.Dot11Beacon(frame.beacon.pack())
                    bssid = binary_to_mac(frame.mgmt.bssid)

                    try:
                        beacon_data = bssid_beacon_data[bssid] = patched_network_stats(beacon)
                        target_channel = beacon_data.get("channel")
                    except:
                        procedure_logger.exception("Failed to decode network stats...")
                        target_channel = None

                    if hasattr(frame, 'ssid'):
                        bssid_to_ssid_map[bssid] = frame.ssid.data
                        bssid_infra_macs[bssid].add(mac)
                    if target_channel is None or target_channel == channel:
                        bssid_beacon_timing_payloads[bssid].append((beacon.timestamp, beacon.beacon_interval))
                        if radiotap_frame.dBm_AntSignal is not None:
                            bssid_to_power_map[bssid].append(radiotap_frame.dBm_AntSignal)
                    else:
                        procedure_logger.warning(
                            "Off channel beacon ({0} vs {1}) seen for BSSID {2}"
                            "".format(target_channel, channel, bssid)
                        )

                if frame_subtype == dpkt.ieee80211.M_PROBE_RESP:
                    if hasattr(frame, 'ssid'):
                        bssid = binary_to_mac(frame.mgmt.bssid)
                        bssid_to_ssid_map[bssid] = frame.ssid.data
                        bssid_infra_macs[bssid].add(mac)

                if frame_subtype in (dpkt.ieee80211.M_ASSOC_REQ, dpkt.ieee80211.M_ASSOC_RESP):
                    current_counter.association_frame_count += 1
                if frame_subtype in (dpkt.ieee80211.M_REASSOC_REQ, dpkt.ieee80211.M_REASSOC_RESP):
                    current_counter.reassociation_frame_count += 1
                if frame_subtype == dpkt.ieee80211.M_DISASSOC:
                    current_counter.disassociation_frame_count += 1
                if frame_subtype == dpkt.ieee80211.M_ACTION:
                    action_counter += 1
                if frame_subtype == dpkt.ieee80211.M_PROBE_REQ:
                    probe_req_counter += 1
                if frame.retry:
                    current_counter.retry_frame_count += 1
                if radiotap_frame.dBm_AntSignal is not None:
                    current_counter.power_measurements.append(radiotap_frame.dBm_AntSignal)
                if radiotap_frame.Rate is not None:
                    current_counter.rate_measurements.append(radiotap_frame.Rate)
                if radiotap_frame.Flags.badFCS is not None:
                    current_counter.failed_fcs_count += (1 if radiotap_frame.Flags.badFCS else 0)

            elif frame_type == dpkt.ieee80211.CTL_TYPE:
                include_in_extra_measurements = True

                if frame_subtype == dpkt.ieee80211.C_RTS:
                    mac = binary_to_mac(frame.rts.src)
                    current_counter = station_counters[mac]
                    current_counter.cts_frame_count += 1

                elif frame_subtype == dpkt.ieee80211.C_CTS:
                    mac = binary_to_mac(frame.cts.dst)
                    include_in_extra_measurements = False
                    current_counter = station_counters[mac]
                    current_counter.rts_frame_count += 1

                elif frame_subtype == dpkt.ieee80211.C_ACK:
                    mac = binary_to_mac(frame.ack.dst)
                    include_in_extra_measurements = False
                    current_counter = station_counters[mac]
                    current_counter.ack_frame_count += 1

                elif frame_subtype == dpkt.ieee80211.C_BLOCK_ACK:
                    mac = binary_to_mac(frame.back.src)
                    current_counter = station_counters[mac]

                elif frame_subtype == dpkt.ieee80211.C_BLOCK_ACK_REQ:
                    mac = binary_to_mac(frame.bar.src)
                    current_counter = station_counters[mac]

                elif frame_subtype == dpkt.ieee80211.C_CF_END:
                    mac = binary_to_mac(frame.cf_end.src)
                    current_counter = station_counters[mac]
                else:
                    continue
                if frame.retry:
                    current_counter.retry_frame_count += 1
                if include_in_extra_measurements:
                    current_counter.control_frame_count += 1
                    if radiotap_frame.dBm_AntSignal is not None:
                        current_counter.power_measurements.append(radiotap_frame.dBm_AntSignal)
                    if radiotap_frame.Rate is not None:
                        current_counter.rate_measurements.append(radiotap_frame.Rate)
                    if radiotap_frame.Flags.badFCS is not None:
                        current_counter.failed_fcs_count += (1 if radiotap_frame.Flags.badFCS else 0)

            elif frame_type == dpkt.ieee80211.DATA_TYPE:
                src_mac = binary_to_mac(frame.data_frame.src)
                dst_mac = binary_to_mac(frame.data_frame.dst)
                if hasattr(frame.data_frame, 'bssid'):
                    bssid = binary_to_mac(frame.data_frame.bssid)
                else:
                    bssid = None

                current_counter = station_counters[src_mac]
                dst_current_counter = station_counters[dst_mac]

                if frame.to_ds and bssid:
                    bssid_infra_macs[bssid].add(dst_mac)
                    bssid_associated_macs[bssid].add(src_mac)
                elif frame.from_ds and bssid:
                    bssid_infra_macs[bssid].add(src_mac)
                    bssid_associated_macs[bssid].add(dst_mac)
                current_counter.data_throughput_out += len(frame.data_frame.data)
                dst_current_counter.data_throughput_in += len(frame.data_frame.data)

                current_counter.data_frame_count += 1
                if frame.retry:
                    current_counter.retry_frame_count += 1
                if radiotap_frame.dBm_AntSignal is not None:
                    current_counter.power_measurements.append(radiotap_frame.dBm_AntSignal)
                if radiotap_frame.Rate is not None:
                    current_counter.rate_measurements.append(radiotap_frame.Rate)
                if radiotap_frame.Flags.badFCS is not None:
                    current_counter.failed_fcs_count += (1 if radiotap_frame.Flags.badFCS else 0)
            else:
                pass

        except dpkt.dpkt.UnpackError:
            logging.warning(
                "dpkt lacks support for some IE80211 features. This could be causing spurious decode problems.",
                exc_info=True
            )
            weird_frame_count += 1
        header, payload = pcap_offline_dev.next()
    pcap_offline_dev.close()

    measurement = Measurement.new(
        start_time,
        end_time,
        sample_seconds,
        channel,
        noise_measurements,
        data_counters=sum_data_counters(station_counters.values()),
        extra_data={
            'weird_frame_count': weird_frame_count
        }
    )

    stations = [
        Station.new(mac_addr) for mac_addr in station_counters.keys()
    ]

    service_sets = [
        ServiceSet.new(bssid, network_name=bssid_to_ssid_map.get(bssid), extra_data=bssid_beacon_data.get(bssid, {}))
        for bssid in set(bssid_infra_macs.keys()).union(set(bssid_associated_macs.keys()))
    ]
    for service_set in service_sets:
        jitter, bad_intervals, intervals = calculate_beacon_jitter(
            bssid_beacon_timing_payloads.get(service_set.bssid), service_set.bssid
        )
        if jitter is not None:
            bssid_to_jitter_map[service_set.bssid] = (
                jitter, bad_intervals, intervals
            )
    procedure_logger.info("-----------------")
    procedure_logger.info("Analysis performed on channel: {0}".format(channel))
    procedure_logger.info("Noise Level: {0} +/- {1} dBm".format(measurement.average_noise, measurement.std_dev_noise))
    procedure_logger.info("Top level result:\n{0}".format(repr(measurement.data_counters)))
    procedure_logger.info("Action Frames: {0}".format(action_counter))
    procedure_logger.info("Probe Request Frames: {0}".format(probe_req_counter))
    if service_sets:
        procedure_logger.info("Service Sets seen:")
        for service_set in service_sets:
            jitter, bad_intervals, intervals = bssid_to_jitter_map.get(service_set.bssid, (None, None, None))
            procedure_logger.info("-- {0} ({1})".format(service_set.bssid, service_set.network_name))
            if bad_intervals:
                procedure_logger.info("---- Changing intervals detected!!!")
                procedure_logger.info("---- Intervals Seen: {0}".format(intervals))
            if jitter:
                procedure_logger.info(
                    "---- Avg +/- StdDev Beacon Jitter: {0} +/- {1} (ms)".format(
                        altered_mean(jitter)/1000.0, altered_stddev(jitter)/1000.0
                    )
                )
                procedure_logger.info(
                    "---- Min/Max Beacon Jitter: {0}/{1} (ms)".format(
                         min(jitter)/1000.0, max(jitter)/1000.0
                    )
                )
                procedure_logger.info("---- Jitter Count: {0}".format(len(jitter)))
    procedure_logger.info("{0} unique stations seen.".format(len(stations)))
    procedure_logger.info("-----------------")
    return {
        'measurement': measurement,
        'stations': stations,
        'service_sets': service_sets,
        'station_counters': station_counters,
        'bssid_associated_macs': bssid_associated_macs,
        'bssid_infra_macs': bssid_infra_macs,
        'bssid_to_ssid_map': bssid_to_ssid_map,
        'bssid_to_jitter_map': bssid_to_jitter_map,
        'bssid_to_power_map': bssid_to_power_map
    }


def write_offline_analysis_to_database(db_conn, analysis_data):
    measurement = analysis_data['measurement']
    stations = analysis_data['stations']
    service_sets = analysis_data['service_sets']
    station_counters = analysis_data['station_counters']
    bssid_associated_macs = analysis_data['bssid_associated_macs']
    bssid_infra_macs = analysis_data['bssid_infra_macs']
    bssid_to_ssid_map = analysis_data['bssid_to_ssid_map']
    bssid_to_jitter_map = analysis_data['bssid_to_jitter_map']
    bssid_to_power_map = analysis_data['bssid_to_power_map']

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
            insert_measurement_station(
                t, measurement.measurement_id, station.station_id, station_counters[station.mac_address]
            )
        for service_set in service_sets:
            opt_service_set = select_service_set_by_bssid(t, service_set.bssid)
            if opt_service_set:
                service_set.service_set_id = opt_service_set.service_set_id
            else:
                service_set.service_set_id = insert_service_set(t, service_set)
            if service_set.bssid in bssid_to_jitter_map:
                jitter, bad_intervals, intervals = bssid_to_jitter_map[service_set.bssid]
                insert_jitter_measurement(
                    t, ServiceSetJitterMeasurement.new(
                        measurement.measurement_id, service_set.service_set_id, jitter,
                        intervals[0], {
                            'bad_intervals': bad_intervals,
                            'average_power': altered_mean(bssid_to_power_map.get(service_set.bssid, []))
                        }
                    )
                )
        for bssid, infra_macs in bssid_infra_macs.items():
            for mac in infra_macs:
                insert_service_set_infrastructure_station(t, measurement.measurement_id, bssid, mac)
        for bssid, associated_macs in bssid_associated_macs.items():
            for mac in associated_macs:
                insert_service_set_associated_station(t, measurement.measurement_id, bssid, mac)
        for bssid, ssid in bssid_to_ssid_map.items():
            update_service_set_network_name(t, bssid, ssid)
    optimize_db(db_conn)


def run_capture(wireless_interface, log_file, tmp_dir, database_loc,
                verbose=False, sample_seconds=10, rounds=0, ignore_non_root=False,
                db_timeout_seconds=60, heartbeat_func=lambda: None, run_with_monitor=True):
    setup_logging(log_file, verbose)
    if run_with_monitor:
        return run_monitored(run_capture, always_restart=False)(
            wireless_interface, log_file, tmp_dir, database_loc,
            verbose, sample_seconds, rounds, ignore_non_root,
            db_timeout_seconds, run_with_monitor=False
        )
    try:
        heartbeat_func()
        effective_user_id = os.geteuid()
        if effective_user_id != 0 and ignore_non_root:
            procedure_logger.warning("Not running as root, attempting to proceed...")
        elif effective_user_id !=0:
            raise OSError(
                "This script requires root-level permissions to run. "
                "Please either run as superuser or use the --ignore-non-root flag."
            )
        run_forever = rounds == 0

        db_conn = create_connection(database_loc, db_timeout_seconds)
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

        heartbeat_func()
        current_round = 0
        while run_forever or rounds > 0:
            heartbeat_func()
            procedure_logger.info("Executing capture round {0}".format(current_round))
            with transaction_wrapper(db_conn) as t:
                kv_store_set(t, "capture/current_script_round", current_round)
            for channel in range(1, 12):
                heartbeat_func()
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


# -----------------------------------------------
#  UPLOAD
# -----------------------------------------------


upload_argument_parser = argparse.ArgumentParser('wifiology_upload')
upload_argument_parser.add_argument("database_location", type=str, help="The database location on disk")
upload_argument_parser.add_argument(
    "remote_api_base_url", type=str, help="The base URL for the remote Wifiology server"
)
upload_argument_parser.add_argument(
    'node_id', type=int, help="The central server ID for this node."
)
upload_argument_parser.add_argument(
    "api_key", type=str, help="The API key to use to auth for upload."
)
upload_argument_parser.add_argument("-l", "--log-file", type=str, default="-", help="Log file.")
upload_argument_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode.")
upload_argument_parser.add_argument(
    "--db-timeout-seconds", type=int, default=60,
    help="The timeout to set on the database connection"
)
upload_argument_parser.add_argument(
    "--batch-size", type=int, default=1,
    help="The number of measurements to simultaneously pull from the DB."
)


def pull_and_upload_measurements(db_connection, remote_api_base_url, node_id, api_key, batch_size):
    with transaction_wrapper(db_connection) as t:
        target_measurements = select_measurements_that_need_upload(t, batch_size)
        for measurement in target_measurements:
            procedure_logger.info(
                "Pulling stations and service sets info for measurement {0}".format(measurement.measurement_id)
            )
            stations = select_stations_for_measurement(t, measurement.measurement_id)
            service_sets = select_service_sets_for_measurement(t, measurement.measurement_id)
            infra_macs_map = {}
            associated_macs_map = {}
            jitter_measurements = select_jitter_measurements_by_measurement_id(t, measurement.measurement_id)
            jitter_measurement_map = {}

            for ss in service_sets:
                infra_macs_map[ss.service_set_id] = select_infrastructure_mac_addresses_for_measurement_service_set(
                    t, measurement.measurement_id, ss.service_set_id
                )
                associated_macs_map[ss.service_set_id] = select_associated_mac_addresses_for_measurement_service_set(
                    t, measurement.measurement_id, ss.service_set_id
                )

            for j in jitter_measurements:
                jitter_measurement_map[j.service_set_id] = j
      
            bssid_to_network_name_map = {
                ss.bssid: ss.nice_network_name for ss in service_sets if ss.nice_network_name
            }

            procedure_logger.info("Attempting to do data upload for measurement {0}".format(measurement.measurement_id))
            upload_data = measurement.to_api_upload_payload(
                [s.to_api_upload_payload() for s in stations],
                [
                    ss.to_api_upload_payload(
                        infra_macs_map[ss.service_set_id],
                        associated_macs_map[ss.service_set_id],
                        jitter_measurement_map.get(ss.service_set_id)
                    )
                    for ss in service_sets
                ],
                bssid_to_network_name_map
            )
            response = requests.post(
                urljoin(remote_api_base_url, '/api/1.0/nodes/{nid}/measurements'.format(nid=node_id)),
                data=json_dumps(upload_data),
                headers={
                    'Content-Type': 'application/json',
                    'X-API-Key': api_key
                }
            )
            try:
                import pprint
                pprint.pprint(response.json())
            except:
                pass
            response.raise_for_status()
            procedure_logger.info(
                "Info on uploaded measurement {0}: {0}".format(measurement.measurement_id, response.json())
            )
        update_measurements_upload_status(t, [m.measurement_id for m in target_measurements], True)
    return bool(target_measurements)


def upload_argparse_args_to_kwargs(args):
    return {
        'database_location': args.database_location,
        'remote_api_base_url': args.remote_api_base_url,
        'node_id': args.node_id,
        'api_key': args.api_key,
        'log_file': args.log_file,
        'verbose': args.verbose,
        'db_timeout_seconds': args.db_timeout_seconds,
        'batch_size': args.batch_size
    }


def run_upload(database_location, node_id, remote_api_base_url, api_key, log_file, verbose,
               db_timeout_seconds=60, batch_size=2, round_delay=3):
    try:
        setup_logging(log_file, verbose)

        db_conn = create_connection(database_location, db_timeout_seconds)
        write_schema(db_conn)

        with transaction_wrapper(db_conn) as t:
            kv_store_set(t, "upload/script_start_time", time.time())
            kv_store_set(t, 'upload/script_pid', os.getpid())
            kv_store_set(t, "upload/remote_url", remote_api_base_url)
        more_work_to_do = True
        while more_work_to_do:
            procedure_logger.info("Pulling and uploading...")
            more_work_to_do = pull_and_upload_measurements(db_conn, remote_api_base_url, node_id, api_key, batch_size)
            procedure_logger.info("Snooze {0}".format(round_delay))
            time.sleep(round_delay)
    except BaseException:
        procedure_logger.exception("Unhandled exception during upload! Aborting,...")
        raise
    else:
        procedure_logger.info("Upload completed successfully. Ending...")

# -----------------------------------------------
#  JANITOR
# -----------------------------------------------


janitor_argument_parser = argparse.ArgumentParser('Wifiology Janitor')
janitor_argument_parser.add_argument("database_location", type=str, help="The database location on disk")
janitor_argument_parser.add_argument("-l", "--log-file", type=str, default="-", help="Log file.")
janitor_argument_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode.")
janitor_argument_parser.add_argument(
    "--db-timeout-seconds", type=int, default=60,
    help="The timeout to set on the database connection"
)
janitor_argument_parser.add_argument(
    "--measurement-max-age-days", type=int, default=14, help="The maximum number of days to keep measurements around."
)
janitor_argument_parser.add_argument(
    "--do-vacuum", action="store_true", help="Run a VACUUM on the database."
)
janitor_argument_parser.add_argument(
    "--do-optimize", action="store_true", help="Run  an OPTIMIZE on the database"
)


def clean_db(db_connection, measuement_max_age_days, do_vacuum=False, do_optimize=False):
    with transaction_wrapper(db_connection) as t:
        deleted_count = delete_old_measurements(t, measuement_max_age_days)
        procedure_logger.info("{0} old measurements deleted from the database".format(deleted_count))
    if do_optimize:
        procedure_logger.info("Beginning DB optimize...")
        optimize_db(db_connection)
        procedure_logger.info("DB optimize completed.")
    if do_vacuum:
        procedure_logger.info("Beginning DB vacuum...")
        vacuum_db(db_connection)
        procedure_logger.info("DB Vacuum completed.")
    return


def janitor_argparse_args_to_kwargs(args):
    return {
        'database_location': args.database_location,
        'log_file': args.log_file,
        'verbose': args.verbose,
        'db_timeout_seconds': args.db_timeout_seconds,
        'measurement_max_age_days': args.measurement_max_age_days,
        'do_vacuum': args.do_vacuum,
        'do_optimize': args.do_optimize
    }


def run_janitor(database_location, log_file, verbose, db_timeout_seconds=60, measurement_max_age_days=14,
                do_vacuum=False, do_optimize=False):
    try:
        setup_logging(log_file, verbose)

        db_conn = create_connection(database_location, db_timeout_seconds)
        write_schema(db_conn)

        with transaction_wrapper(db_conn) as t:
            kv_store_set(t, "janitor/script_start_time", time.time())
            kv_store_set(t, 'janitor/script_pid', os.getpid())
        procedure_logger.info("Sarting Janitorial tasks...")
        clean_db(db_conn, measurement_max_age_days, do_vacuum, do_optimize)
        procedure_logger.info("Database janitorial tasks finished")
    except BaseException:
        procedure_logger.exception("Unhandled exception during upload! Aborting,...")
        raise
    else:
        procedure_logger.info("Janitor completed successfully. Ending...")
