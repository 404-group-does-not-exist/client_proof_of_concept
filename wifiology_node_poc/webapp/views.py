from wifiology_node_poc.queries import kv_store_get_prefix
from wifiology_node_poc.webapp import STATIC_FILE_DIR
from wifiology_node_poc.queries import select_service_sets_by_channel, select_stations_by_channel

from bottle import SimpleTemplate, static_file, json_dumps

BASE_TEMPLATE = SimpleTemplate("""
<html>
    <head>
        <title>{{ title }}</title>
        <link rel="stylesheet" type="text/css" href="{{ get_url('static_files_view', path='Skeleton-2.0.4/css/skeleton.css') }}">
        <link rel="stylesheet" type="text/css" href="{{ get_url('static_files_view', path='Skeleton-2.0.4/css/normalize.css') }}">
        <script src="{{ get_url('static_files_view', path='js/jquery-3.3.1.min.js') }}"></script>
        <script src="{{ get_url('static_files_view', path='js/Chart.bundle.min.js') }}"></script>
        <script src="{{ get_url('static_files_view', path='js/wifiology.js') }}"></script>
    </head>
    <body>
        <div class="container">
            {{! data }}
        </div>
    </body>
</html>

""")


INDEX_TEMPLATE = SimpleTemplate("""
<div class="row">
    <h1>Wifiology Node</h1>
</div>
<div class=row">
    <h2>Channel Data</h2>
    % for i in range(1, 12):
    <li><a href="{{ get_url('channel_data_view', channel_num=i) }}">Channel {{i}}</a></li>
    % end
</div>
<div class="row">
    <h2>Webserver Info</h2>
    <ul>
    % for key, value in webserver_data:
        <li><b>{{ key }}</b>: {{ value }}
    % end
    </ul>
</div>
<div class="row">   
    <h2>Capture Info</h2>
    <ul>
    % for key, value in capture_data:
        <li><b>{{ key }}</b>: {{ value }}</li>
    % end
    </ul>
</div>
""")

CHANNEL_TEMPLATE = SimpleTemplate("""
<div class="row">
    <h1>Channel {{ channel_num }} Info </h1>
</div>
<div class="row">
    <h2>Top Level Stats</h2>
    <p>TBD</p>
</div>
<div class="row">
    <h2>Latest Data</h2>
    <canvas id="latestDataChart">
    
    </canvas>
    <canvas id="latestStationCountDataChart">
    
    </canvas>
    <script>
         $(document).ready(function(){ 
         
             var channelNum = {{ channel_num }};
             var apiUrl = {{! json_dumps(get_url("latest_channel_data_api", channel_num=channel_num)) }};
             renderMeasurementData(
                 "latestDataChart", 
                 channelNum,
                 apiUrl
             );
             renderStationCount(
                 "latestStationCountDataChart",
                 channelNum,
                 apiUrl
             );
         });
    </script>
</div>
<div class="row">
    <h2>Access Points Seen</h2>
    <ul>
    % for ss in service_sets:
        <li>{{ ss.network_name }}</ap>
    % end
    </ul>
</div>
<div class="row">
    <h2>Stations Seen</h2>
    <ul>
    % for s in stations:
        <li>{{ s.mac_address }} ({{ mac_decoder(s.mac_address) }})</li>
    % end
    </ul>
</div>
""")


def render(data_template, **kwargs):
    base_template_kwargs = dict(kwargs)
    base_template_kwargs.update(data=data_template.render(kwargs))
    return BASE_TEMPLATE.render(
        **base_template_kwargs
    )


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

    def index_view(self):
        return render(
            INDEX_TEMPLATE,
            title="Wifiology Node",
            get_url=self.app.get_url,
            capture_data=kv_store_get_prefix(self.db_conn, "capture"),
            webserver_data=self.webserver_data_generator(self.db_conn)
        )

    def channel_view(self, channel_num):
        return render(
            CHANNEL_TEMPLATE,
            title="Channel {0}".format(channel_num),
            channel_num=channel_num,
            service_sets=[], # select_service_sets_by_channel(self.db_conn, channel_num),
            stations=select_stations_by_channel(self.db_conn, channel_num),
            mac_decoder=self.mac_decoder,
            get_url=self.app.get_url,
            json_dumps=json_dumps
        )

    def static_file_handler(self, path):
        return static_file(path, self.static_file_root)