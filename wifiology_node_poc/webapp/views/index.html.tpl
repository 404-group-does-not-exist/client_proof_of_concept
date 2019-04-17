% rebase('base.html.tpl')
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