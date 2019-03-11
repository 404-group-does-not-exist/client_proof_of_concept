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

capture_argument_parser = argparse.ArgumentParser('wifiology_capture')
capture_argument_parser.add_argument("interface", type=str, help="The WiFi interface to capture on.")
capture_argument_parser.add_argument("tmp_dir", type=str, help="The temporary storage directory (preferably tmpfs)")
capture_argument_parser.add_argument("-l", "--log-file", type=str, default="-", help="Log file.")
capture_argument_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode.")
capture_argument_parser.add_argument(
    "-s", "--sample-seconds", default=10, type=int, help="The number of seconds to sample each channel."
)

procedure_logger = logging.getLogger(__name__)


class StopException(Exception):
    pass


def raise_stop(*args, **kwargs):
    raise StopException


def capture_argparse_args_to_kwargs(args):
    return {
        'wireless_interface': args.interface,
        'log_file': args.log_file,
        'tmp_dir': args.tmp_dir,
        'verbose': args.verbose,
        'sample_seconds': args.sample_seconds
    }


def setup_logging(log_file, verbose):
    root_logger = logging.getLogger('')
    if log_file == "-":
        handler = logging.StreamHandler()
    else:
        handler = logging.FileHandler(log_file)
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


def setup_capture(card, start=True, ch=None):
    """
     sets the Card card monitor mode or returns it to managed mode
     :param card: a Card object
     :param start: True = set|False = reset
     :param ch: initial ch to start on
     :returns: the new card
    """
    newcard = None
    if start:
        if pyw.modeget(card) == 'monitor':
            raise RuntimeError("Card is already in monitor mode")
        newcard = pyw.devset(card, card.dev + 'mon')
        pyw.modeset(newcard, 'monitor')
        pyw.up(newcard)
        if ch:
            pyw.chset(newcard, ch, None)
    else:
        if pyw.modeget(card) == 'managed':
            raise RuntimeError("Card is not in monitor mode")
        newcard = pyw.devset(card, card.dev[:-3])
        pyw.modeset(newcard, 'managed')
        pyw.up(newcard)
    return newcard


def run_live_capture(wireless_interface, capture_file, sample_seconds):
    pcap_dev = pcapy.create(wireless_interface)
    pcap_dev.set_snaplen(65535)
    pcap_dev.set_timeout(0)
    pcap_dev.set_promisc(True)
    pcap_dev.set_buffer_size(15 * 1024 * 1024)

    procedure_logger.info("Opening capture file: {0}".format(capture_file))
    pcap_dev.activate()
    fd = timerfd.create(timerfd.CLOCK_MONOTONIC, 0)
    timerfd.settime(fd, 0, sample_seconds, 0)

    dumper = pcap_dev.dump_open(capture_file)

    procedure_logger.info("Arming and activating live capture...")

    def live_capture(hdr, data):
        dumper.dump(hdr, data)
        if select.select([fd], [], [], 0)[0]:
            raise StopException
    try:
        pcap_dev.loop(0, live_capture)
    except StopException:
        pass
    dumper.close()
    pcap_dev.close()


def run_offline_analysis(capture_file, sample_seconds, channel):
    counter = 0
    frame_counter = defaultdict(int)
    ssid_data = {}
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
                    ssid_name = frame.ssid.data
                    if ssid_name not in ssid_data:
                        ssid_data[ssid_name] = {
                            'stations': set(),
                            'beacons': 0
                        }
                    ssid_data[ssid_name]['stations'].add(
                        (frame.mgmt.src, channel)
                    )
                    ssid_data[ssid_name]['beacons'] += 1

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
            else:
                frame_counter['other'] += 1

        except dpkt.dpkt.UnpackError:
            logging.warning(
                "dpkt lacks support for some IE80211 features. This could be causing spurious decode problems.",
                exc_info=True
            )
        header, payload = pcap_offline_dev.next()
    pcap_offline_dev.close()
    return {
        'total_counter': counter,
        'frame_counter': frame_counter,
        'ssid_data': ssid_data,
        'ctl_counters': ctl_counter
    }


def run_capture(wireless_interface, log_file, tmp_dir, verbose=False, sample_seconds=10):
    try:
        setup_logging(log_file, verbose)

        frame_stats = {}
        ssid_map = defaultdict(set)
        ssid_data = {}

        channel_data_rate = defaultdict(int)
        channel_packet_rate = defaultdict(int)
        data_s2d_packet_rate = defaultdict(lambda: defaultdict(int))

        procedure_logger.info("Loading card handle from interface name..")
        card = pyw.getcard(wireless_interface)

        if not os.path.exists(tmp_dir):
            procedure_logger.warning("Tmp dir {0} does not exist. Creating...".format(tmp_dir))
            os.makedirs(tmp_dir)

        procedure_logger.info("Beginning channel scan.")

        for channel in range(1, 12):
            procedure_logger.info("Channging to channel {0}".format(channel))

            pyw.down(card)
            pyw.up(card)
            pyw.chset(card, channel, None)

            frame_stats[channel] = defaultdict(int)
            procedure_logger.info("Opening the pcap driver...")
            capture_file = os.path.join(tmp_dir, "channel{0}-{1}.pcap".format(channel, time.time()))

            try:
                procedure_logger.info("Beginning live capture...")
                run_live_capture(wireless_interface, capture_file, sample_seconds)
                procedure_logger.info("Starting offline analysis...")
                data = run_offline_analysis(capture_file, sample_seconds, channel)
                procedure_logger.info("Analysis data: {0}".format(pprint.pformat(data)))
            finally:
                procedure_logger.info("Cleaning up capture file..")
                if os.path.exists(capture_file):
                    os.unlink(capture_file)







#
        #            channel_data_rate[channel] += len(frame)
        #            channel_packet_rate[channel] += 1
#
        #            # Beacon frames used to find APs broadcasting on this channel.
        #            if frame_type == dpkt.ieee80211.MGMT_TYPE and frame_subtype == dpkt.ieee80211.M_BEACON:
        #                ssid_map[frame.ssid.data].add((frame.mgmt.src, channel))
        #                ssid_data[frame.ssid.data] = {
        #                    'rate': rate_decoder(frame.rate.data),
        #                    'qos': frame.capability.qos,
        #                    'short_preamble': frame.capability.short_preamble
        #                }
        #            # Any data frames should be analyzed.
        #            if frame_type == dpkt.ieee80211.DATA_TYPE:
        #                data_s2d_packet_rate[frame.data_frame.src][frame.data_frame.dst] += 1
#
        #            header, payload = pcap_dev.next()
        #        except dpkt.dpkt.UnpackError:
        #            logging.warning("dpkt lacks support for some IE80211 features. This could be causing spurious decode problems.")
#
        #    channel_data_rate[channel] /= (1.0*sample_seconds)
        #    channel_packet_rate[channel] /= (1.0*sample_seconds)
#
        #    print("CH", channel, "STATS", pcap_dev.stats())
#
        #    procedure_logger.info("Channel total data rate: {0} b/s".format(channel_data_rate[channel]))
        #    procedure_logger.info("Channel total packet rate: {0} p/s".format(channel_packet_rate[channel]))
        #procedure_logger.info("SSID channel/mac map: {0}".format(dict(ssid_map)))
        #procedure_logger.info("SSID Data: {0}".format(ssid_data))
        #for src in data_s2d_packet_rate:
        #    for dst in data_s2d_packet_rate[src]:
        #        procedure_logger.info("SRC {0} DST {1} COUNT {2}".format(src, dst, data_s2d_packet_rate[src][dst]))
        #    procedure_logger.info("----")
    except BaseException:
        procedure_logger.exception("Unhandled exception during capture! Aborting,...")
        raise
    else:
        procedure_logger.info("No more data. Ending...")
