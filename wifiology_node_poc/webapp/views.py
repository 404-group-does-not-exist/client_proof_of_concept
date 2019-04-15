from wifiology_node_poc.queries.kv import kv_store_get_prefix
from wifiology_node_poc.webapp import STATIC_FILE_DIR
from wifiology_node_poc.queries.core import select_service_sets_by_channel, select_stations_by_channel

from bottle import SimpleTemplate, static_file, json_dumps, template


class NodeViews(object):
    def __init__(self, app, db_conn, mac_decoder, webserver_data_generator=lambda db_conn: {},
                 static_file_root=STATIC_FILE_DIR):
        self.app = app
        self.db_conn = db_conn
        self.mac_decoder = mac_decoder
        self.webserver_data_generator = webserver_data_generator
        self.static_file_root = static_file_root


    def attach(self):
        self.app.route(
            path="/", method="GET", callback=self.index_view, name="index_view"
        )
        self.app.route(
            path="/static/<path:path>", method="GET", callback=self.static_file_handler,
            name="static_files_view"
        )
        self.app.route(
            path="/channel/<channel_num:int>", method="GET", callback=self.channel_view,
            name="channel_data_view"
        )

    def template_vars(self, extra_vars=None, **kwargs):
        vars = {
            'get_url': self.app.get_url
        }
        if extra_vars:
            vars.update(extra_vars)
        if kwargs:
            vars.update(kwargs)
        return vars

    def index_view(self):
        return template(
            'index.html',
            **self.template_vars(
                title="Wifiology Node",
                capture_data=kv_store_get_prefix(self.db_conn, "capture"),
                webserver_data=self.webserver_data_generator(self.db_conn)
            )
        )

    def channel_view(self, channel_num):
        return template('channel.html')

    def static_file_handler(self, path):
        return static_file(path, self.static_file_root)

    # def channel_view(self, channel_num):
    #     return render(
    #         CHANNEL_TEMPLATE,
    #         title="Channel {0}".format(channel_num),
    #         channel_num=channel_num,
    #         service_sets=[], # select_service_sets_by_channel(self.db_conn, channel_num),
    #         stations=select_stations_by_channel(self.db_conn, channel_num),
    #         mac_decoder=self.mac_decoder,
    #         get_url=self.app.get_url,
    #         json_dumps=json_dumps
    #     )

