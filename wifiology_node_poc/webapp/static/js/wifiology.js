function epochSecondsToStr(epochSeconds){
    d = new Date();
    d.setUTCSeconds(epochSeconds);
    return "" + d.getHours() + ":" + d.getMinutes() + ":" + d.getSeconds();
}

function renderMeasurementData(elementID, channelNum, apiUrl){
    $.getJSON(
        apiUrl,
        {
            limit: 250
        },
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
                                label: 'Management Frame Count Per Second',
                                data: apiData.data.map(function(datum){ return datum.managementFrameCount/datum.measurementDuration }),
                                borderColor: '#ff6d6d',
                                fill: false
                            },
                            {
                                label: 'Data Frame Count Per Second',
                                data: apiData.data.map(function(datum){ return datum.dataFrameCount/datum.measurementDuration } ),
                                borderColor: '#6470ef',
                                fill: false
                            },
                            {
                                label: 'Control Frame Count Per Second',
                                data: apiData.data.map(function(datum){ return datum.controlFrameCount/datum.measurementDuration }),
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


function renderStationCount(elementID, channelNum, apiUrl){
    $.getJSON(
        apiUrl,
        {
            limit: 250
        },
        function(apiData){
            var targetCanvasContext = document.getElementById(elementID).getContext('2d');
            console.log(apiData.stationCountData);
            new Chart(
                targetCanvasContext,
                {
                    type: "line",
                    data: {
                        labels: apiData.stationCountData.map(function(datum){ return epochSecondsToStr(datum.measurementStartTime)}),
                        datasets: [
                            {
                                label: 'Station Count',
                                data: apiData.stationCountData.map(function(datum){ return datum.stationCount; }),
                                borderColor: '#ff6d6d',
                                fill: true
                            }
                        ]

                    },
                    options: {
                        title: {
                            display: true,
                            text: 'Channel ' + channelNum + ' Latest Station Count'
                        }
                    }
                }
            );
        }
    )
}