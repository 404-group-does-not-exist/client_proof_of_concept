-- DIALECT: SQLite3
PRAGMA foreign_keys = on;

CREATE TABLE IF NOT EXISTS measurement(
    measurementID INTEGER PRIMARY KEY,
    measurementStartTime REAL NOT NULL,
    measurementEndTime REAL NOT NULL,
    measurementDuration REAL NOT NULL,
    channel INTEGER NOT NULL,
    managementFrameCount INTEGER NOT NULL,
    controlFrameCount INTEGER NOT NULL,
    dataFrameCount INTEGER NOT NULL,
    extraJSONData TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS measurement_channel_startTime_IDX ON measurement(channel, measurementStartTime);


CREATE TABLE IF NOT EXISTS station(
    stationID INTEGER PRIMARY KEY,
    macAddress TEXT UNIQUE NOT NULL,
    extraJSONData TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS serviceSet(
    serviceSetID INTEGER PRIMARY KEY,
    networkName TEXT UNIQUE NOT NULL,
    extraJSONData TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS infrastructureStationServiceSetMap(
     mapStationID INTEGER NOT NULL REFERENCES station(stationID),
     mapServiceSetID INTEGER NOT NULL REFERENCES serviceSet(serviceSetID),
     PRIMARY KEY(mapStationID, mapServiceSetID)
);

CREATE TABLE IF NOT EXISTS measurementStationMap(
    mapMeasurementID INTEGER NOT NULL REFERENCES measurement(measurementID),
    mapStationID INTEGER NOT NULL REFERENCES station(stationID), -- can we use the same name as line 30?
    PRIMARY KEY(mapMeasurementID, mapStationID)
);

-- write select for this one and test it
CREATE TABLE IF NOT EXISTS measurementServiceSetMap(
    mapMeasurementID INTEGER NOT NULL REFERENCES measurement(measurementID), -- can we use the same name as line 36?
    mapServiceSetID INTEGER NOT NULL REFERENCES serviceSet(serviceSetID),
    PRIMARY KEY(mapMeasurementID, mapServiceSetID)
);


