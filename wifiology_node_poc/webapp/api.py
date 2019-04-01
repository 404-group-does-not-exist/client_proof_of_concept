from wifiology_node_poc.queries import select_latest_channel_measurements
from bottle import Response, json_dumps


class NodeAPI(object):
    def __init__(self, app, db_conn):
        self.app = app
        self.db_conn = db_conn

    def attach(self):
        self.app.route(
            path='/api/1.0/channel/<channel_num:int>/latest',
            method='GET',
            name='latest_channel_data_api',
            callback=self.channel_data
        )

    def channel_data(self, channel_num):
        """
        Pull the latest measurement data for the specified channel.
        """
        return Response(
            body=json_dumps({
                'data': [
                    m.to_api_response()
                    for m in reversed(select_latest_channel_measurements(self.db_conn, channel_num))
                ]
            }),
            type='application/json'
        )
