% rebase('base.html.tpl')
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