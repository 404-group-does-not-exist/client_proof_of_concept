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


CREATE TABLE IF NOT EXISTS radioDevice(
    radioDeviceID INTEGER PRIMARY KEY,
    macAddress TEXT UNIQUE NOT NULL,
    extraJSONData TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS ssid(
    ssidID INTEGER PRIMARY KEY,
    networkName TEXT UNIQUE NOT NULL,
    extraJSONData TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS radioServiceSetMap(
     mapRadioDeviceID INTEGER NOT NULL REFERENCES radioDevice(radioDeviceID),
     mapServiceSetIdentifierID INTEGER NOT NULL REFERENCES ssid(ssidID),
     PRIMARY KEY(mapRadioDeviceID, mapServiceSetIdentifierID)
);