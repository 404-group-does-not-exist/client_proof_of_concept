function epochSecondsToStr(epochSeconds){
    d = new Date();
    d.setUTCSeconds(epochSeconds);
    return "" + d.getHours() + ":" + d.getMinutes() + ":" + d.getSeconds();
}

function renderMeasurementData(elementID, channelNum, apiUrl){
    $.getJSON(
        apiUrl,
        function(apiData){
            var targetCanvasContext = document.getElementById(elementID).getContext('2d');
            new Chart(
                targetCanvasContext,
                {
                    type: "line",
                    data: {
                        labels: apiData.data.map(function(datum){ return epochSecondsToStr(datum.measurementStartTime)}),
                        datasets: [
                            {
                                label: 'Management Frame Count',
                                data: apiData.data.map(function(datum){ return datum.managementFrameCount }),
                                borderColor: '#ff6d6d',
                                fill: false
                            },
                            {
                                label: 'Data Frame Count',
                                data: apiData.data.map(function(datum){ return datum.dataFrameCount} ),
                                borderColor: '#6470ef',
                                fill: false
                            },
                            {
                                label: 'Control Frame Count',
                                data: apiData.data.map(function(datum){ return datum.controlFrameCount}),
                                borderColor: '#64ef87',
                                fill: false
                            }

                        ]

                    },
                    options: {
                        title: {
                            display: true,
                            text: 'Channel ' + channelNum + ' Latest Measurement Data'
                        }
                    }
                }
            );
        }
    )
}