from wifiology_node_poc.queries.core import select_latest_channel_measurements, select_latest_channel_device_counts
from bottle import Response, json_dumps, request


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
        try:
            limit = int(request.query.get('limit', 250))
            if limit < 1:
                raise ValueError()
        except ValueError:
            return Response(
                body=json_dumps({
                    'error': 'Invalid Limit Value! Must be a positive integer.'
                }),
                type='application/json',
                status=400
            )
        else:
            return Response(
                body=json_dumps({
                    'data': [
                        m.to_api_response()
                        for m in reversed(select_latest_channel_measurements(self.db_conn, channel_num, limit=limit))
                    ],
                    'stationCountData': list(reversed(
                        select_latest_channel_device_counts(self.db_conn, channel_num, limit=limit)
                    ))
                }),
                type='application/json',
                status=200
            )
