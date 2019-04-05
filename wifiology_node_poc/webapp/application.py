import argparse
import bottle
import datetime
from manuf import manuf

from wifiology_node_poc.procedures import setup_logging
from wifiology_node_poc.core_sqlite import create_connection
from wifiology_node_poc.queries import write_schema
from wifiology_node_poc.webapp.views import NodeViews
from wifiology_node_poc.webapp.api import NodeAPI

webapp_argument_parser = argparse.ArgumentParser('wifiology_capture')
webapp_argument_parser.add_argument(
    "-db", "--database-loc", default=":memory:"
)
webapp_argument_parser.add_argument("-l", "--log-file", type=str, default="-", help="Log file.")
webapp_argument_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose mode.")


def webapp_argparse_args_to_kwargs(args):
    return {
        'database_loc': args.database_loc,
        'log_file': args.log_file,
        'verbose': args.verbose
    }


def webserver_info_hof():
    start_time = datetime.datetime.now()

    def webserver_info_generator(_):
        now = datetime.datetime.now()
        return {
            'webserver_current_time': now,
            'webserver_start_time': start_time,
            'webserver_up_time': now - start_time
        }.items()
    return webserver_info_generator


def create_webapp(database_loc, log_file="-", verbose=False):
    setup_logging(log_file, verbose)

    parser = manuf.MacParser(update=True)

    def pretty_print_mac_info(mac):
        info = parser.get_all(mac)
        if info.manuf is None:
            return "Vendor: ???"
        elif info.comment is None:
            return "Vendor: {0}".format(info.manuf)
        else:
            return "Vendor: {0} ({1})".format(info.manuf, info.comment)

    app = bottle.Bottle()
    db_conn = create_connection(database_loc)
    write_schema(db_conn)
    views = NodeViews(
        app,
        db_conn, mac_decoder=pretty_print_mac_info,
        webserver_data_generator=webserver_info_hof()
    )
    views.attach()
    api = NodeAPI(
        app, db_conn
    )
    api.attach()
    return app
