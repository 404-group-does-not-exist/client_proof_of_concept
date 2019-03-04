import pcapy
import dpkt
import argparse
import logging
import time
from collections import defaultdict

capture_argument_parser = argparse.ArgumentParser('wifiology_capture')
capture_argument_parser.add_argument("interface", type=str, help="The WiFi interface to capture on.")
capture_argument_parser.add_argument("-l", "--log-file", type=str, default="-", help="Log file.")
capture_argument_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode.")

procedure_logger = logging.getLogger(__name__)


def capture_argparse_args_to_kwargs(args):
    return {
        'wireless_interface': args.interface,
        'log_file': args.log_file,
        'verbose': args.verbose
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


def run_capture(wireless_interface, log_file, verbose=False):
    try:
        setup_logging(log_file, verbose)
        procedure_logger.info("Opening pcap listener..")
        pcap_dev = pcapy.open_live(wireless_interface, 65536, True, 0)
        procedure_logger.info("Beginning packet listening..")
        header, payload = pcap_dev.next()
        stats = defaultdict(int)
        last_print_time = 0

        while header:
            try:
                packet = dpkt.radiotap.Radiotap(payload)
                frame_type = packet.data.type
                stats[frame_type] += 1
                if time.time() - last_print_time > 5.0:
                    print("MGMT Type", stats[dpkt.ieee80211.MGMT_TYPE])
                    print("CTL Type", stats[dpkt.ieee80211.CTL_TYPE])
                    print("DATA Type", stats[dpkt.ieee80211.DATA_TYPE])
                header, payload = pcap_dev.next()
            except KeyError:
                logging.warning("dpkt lacks delba support. This could be causing spurious decode problems.")
    except BaseException:
        procedure_logger.exception("Unhandled exception during capture! Aborting,...")
        raise
    else:
        procedure_logger.info("No more data. Ending...")
