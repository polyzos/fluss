---
slug: fluss-java-client
title: "Apache Fluss Java Client Guide"
authors: [giannis]
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

Fluss Java Client is a Java client for the [Fluss](https://ververica.com/fluss) streaming engine.

<!-- truncate -->

## Introduction
```java
public static final List<SensorReading> readings = List.of(
        new SensorReading(1, LocalDateTime.of(2025, 6, 23, 9, 15), 22.5, 45.0, 1013.2, 87.5),
        new SensorReading(2, LocalDateTime.of(2025, 6, 23, 9, 30), 23.1, 44.5, 1013.1, 88.0),
        new SensorReading(3, LocalDateTime.of(2025, 6, 23, 9, 45), 21.8, 46.2, 1012.9, 86.9),
        new SensorReading(4, LocalDateTime.of(2025, 6, 23, 10, 0), 24.0, 43.8, 1013.5, 89.2),
        new SensorReading(5, LocalDateTime.of(2025, 6, 23, 10, 15), 22.9, 45.3, 1013.0, 87.8),
        new SensorReading(6, LocalDateTime.of(2025, 6, 23, 10, 30), 23.4, 44.9, 1013.3, 88.3),
        new SensorReading(7, LocalDateTime.of(2025, 6, 23, 10, 45), 21.7, 46.5, 1012.8, 86.5),
        new SensorReading(8, LocalDateTime.of(2025, 6, 23, 11, 0), 24.2, 43.5, 1013.6, 89.5),
        new SensorReading(9, LocalDateTime.of(2025, 6, 23, 11, 15), 23.0, 45.1, 1013.2, 87.9),
        new SensorReading(10, LocalDateTime.of(2025, 6, 23, 11, 30), 22.6, 45.7, 1013.0, 87.4)
);
```

```java
public static final List<SensorInfo> sensorInfos = List.of(
        new SensorInfo(1, "Outdoor Temp Sensor", "Temperature", "Roof", LocalDate.of(2024, 1, 15), "OK", LocalDateTime.of(2025, 6, 23, 9, 15)),
        new SensorInfo(2, "Main Lobby Sensor", "Humidity", "Lobby", LocalDate.of(2024, 2, 20), "ERROR", LocalDateTime.of(2025, 6, 23, 9, 30)),
        new SensorInfo(3, "Server Room Sensor", "Temperature", "Server Room", LocalDate.of(2024, 3, 10), "MAINTENANCE", LocalDateTime.of(2025, 6, 23, 9, 45)),
        new SensorInfo(4, "Warehouse Sensor", "Pressure", "Warehouse", LocalDate.of(2024, 4, 5), "OK", LocalDateTime.of(2025, 6, 23, 10, 0)),
        new SensorInfo(5, "Conference Room Sensor", "Humidity", "Conference Room", LocalDate.of(2024, 5, 25), "OK", LocalDateTime.of(2025, 6, 23, 10, 15)),
        new SensorInfo(6, "Office 1 Sensor", "Temperature", "Office 1", LocalDate.of(2024, 6, 18), "LOW_BATTERY", LocalDateTime.of(2025, 6, 23, 10, 30)),
        new SensorInfo(7, "Office 2 Sensor", "Humidity", "Office 2", LocalDate.of(2024, 7, 12), "OK", LocalDateTime.of(2025, 6, 23, 10, 45)),
        new SensorInfo(8, "Lab Sensor", "Temperature", "Lab", LocalDate.of(2024, 8, 30), "ERROR", LocalDateTime.of(2025, 6, 23, 11, 0)),
        new SensorInfo(9, "Parking Lot Sensor", "Pressure", "Parking Lot", LocalDate.of(2024, 9, 14), "OK", LocalDateTime.of(2025, 6, 23, 11, 15)),
        new SensorInfo(10, "Backyard Sensor", "Temperature", "Backyard", LocalDate.of(2024, 10, 3), "OK", LocalDateTime.of(2025, 6, 23, 11, 30)),

        // SEND SOME UPDATES
        new SensorInfo(2, "Main Lobby Sensor", "Humidity", "Lobby", LocalDate.of(2024, 2, 20), "ERROR", LocalDateTime.of(2025, 6, 23, 9, 48)),
        new SensorInfo(8, "Lab Sensor", "Temperature", "Lab", LocalDate.of(2024, 8, 30), "ERROR", LocalDateTime.of(2025, 6, 23, 11, 16))
);
```

```java
public static Schema getSensorReadingsSchema() {
    return Schema.newBuilder()
            .column("sensorId", DataTypes.INT())
            .column("timestamp", DataTypes.TIMESTAMP())
            .column("temperature", DataTypes.DOUBLE())
            .column("humidity", DataTypes.DOUBLE())
            .column("pressure", DataTypes.DOUBLE())
            .column("batteryLevel", DataTypes.DOUBLE())
            .build();
}
```

## Fluss Writers
```java
public static Schema getSensorInfoSchema() {
    return Schema.newBuilder()
            .column("sensorId", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .column("type", DataTypes.STRING())
            .column("location", DataTypes.STRING())
            .column("installationDate", DataTypes.DATE())
            .column("state", DataTypes.STRING())
            .column("lastUpdated", DataTypes.TIMESTAMP())
            .primaryKey("sensorId")
            .build();
}
```

```java
public static void setupTables(Admin admin) throws ExecutionException, InterruptedException {
    TableDescriptor readingsDescriptor = TableDescriptor.builder()
            .schema(getSensorReadingsSchema())
            .distributedBy(3, "sensorId")
            .comment("This is the sensor readings table")
            .build();

    // drop the tables or ignore if they exist
    admin.dropTable(readingsTablePath, true).get();
    admin.dropTable(sensorInfoTablePath, true).get();
     
    admin.createTable(readingsTablePath, readingsDescriptor, true).get();
    
    TableDescriptor sensorInfoDescriptor = TableDescriptor.builder()
            .schema(getSensorInfoSchema())
            .distributedBy(3, "sensorId")
            .comment("This is the sensor information table")
            .build();
     
    admin.createTable(sensorInfoTablePath, sensorInfoDescriptor, true).get();
}
```

```java
public static GenericRow energyReadingToRow(SensorReading reading) {
    GenericRow row = new GenericRow(SensorReading.class.getDeclaredFields().length);
    row.setField(0, reading.sensorId());
    row.setField(1, TimestampNtz.fromLocalDateTime(reading.timestamp()));
    row.setField(2, reading.temperature());
    row.setField(3, reading.humidity());
    row.setField(4, reading.pressure());
    row.setField(5, reading.batteryLevel());
    return row;
}

public static GenericRow sensorInfoToRow(SensorInfo sensorInfo) {
    GenericRow row = new GenericRow(SensorInfo.class.getDeclaredFields().length);
    row.setField(0, sensorInfo.sensorId());
    row.setField(1, BinaryString.fromString(sensorInfo.name()));
    row.setField(2, BinaryString.fromString(sensorInfo.type()));
    row.setField(3, BinaryString.fromString(sensorInfo.location()));
    row.setField(4, (int) sensorInfo.installationDate().toEpochDay());
    row.setField(5, BinaryString.fromString(sensorInfo.state()));
    row.setField(6, TimestampNtz.fromLocalDateTime(sensorInfo.lastUpdated()));     
    return row;
}
```

```java
logger.info("Creating table writer for table {} ...", AppUtils.SENSOR_READINGS_TBL);
Table table = connection.getTable(AppUtils.getSensorReadingsTablePath());
AppendWriter writer = table.newAppend().createWriter();

AppUtils.readings.forEach(reading -> {
    GenericRow row = energyReadingToRow(reading);
    writer.append(row);
});
writer.flush();

logger.info("Sensor Readings Written Successfully.");
```


```java
logger.info("Creating table writer for table {} ...", AppUtils.SENSOR_INFORMATION_TBL);
Table sensorInfoTable = connection.getTable(AppUtils.getSensorInfoTablePath());
UpsertWriter upsertWriter = sensorInfoTable.newUpsert().createWriter();

AppUtils.sensorInfos.forEach(sensorInfo -> {
    GenericRow row = sensorInfoToRow(sensorInfo);
    upsertWriter.upsert(row);
});

upsertWriter.flush();
```

## Fluss Scanner & Lookupper
```java
LogScanner logScanner = readingsTable.newScan()         
        .createLogScanner();
```

```java
Lookuper sensorInforLookuper = sensorInfoTable
        .newLookup()
        .createLookuper();
```

```java
int numBuckets = readingsTable.getTableInfo().getNumBuckets();
for (int i = 0; i < numBuckets; i++) {     
    logger.info("Subscribing to Bucket {}.", i);
    logScanner.subscribeFromBeginning(i);
}
```


```java
 while (true) {
    logger.info("Polling for records...");
    ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
    for (TableBucket bucket : scanRecords.buckets()) {
        for (ScanRecord record : scanRecords.records(bucket)) {
            InternalRow row = record.getRow();
            
            logger.info("Received reading from sensor '{}' at '{}'.", row.getInt(0), row.getTimestampNtz(1, 6).toString());
            logger.info("Performing lookup to get the information for sensor '{}'. ", row.getInt(0));
            LookupResult lookupResult = sensorInforLookuper.lookup(row).get();
            SensorInfo sensorInfo = lookupResult.getRowList().stream().map(r -> new SensorInfo(
                    r.getInt(0),
                    r.getString(1).toString(),
                    r.getString(2).toString(),
                    r.getString(3).toString(),
                    LocalDate.ofEpochDay(r.getInt(4)),
                    r.getString(5).toString(),
                    LocalDateTime.parse(r.getTimestampNtz(6, 6).toString(), formatter)
            )).findFirst().get();
            logger.info("Retrieved information for '{}' with id: {}", sensorInfo.name(), sensorInfo.sensorId());

            SensorReading reading = new SensorReading(
                    row.getInt(0),
                    LocalDateTime.parse(row.getTimestampNtz(1, 6).toString(), formatter),
                    row.getDouble(2),
                    row.getDouble(3),
                    row.getDouble(4),
                    row.getDouble(5)
            );

            SensorReadingEnriched readingEnriched = new SensorReadingEnriched(
                    reading.sensorId(),
                    reading.timestamp(),
                    reading.temperature(),
                    reading.humidity(),
                    reading.pressure(),
                    reading.batteryLevel(),
                    sensorInfo.name(),
                    sensorInfo.type(),
                    sensorInfo.location(),
                    sensorInfo.state()
            );
            logger.info("Bucket: {} - {}", bucket, readingEnriched);
            logger.info("---------------------------------------");
        }
    }
}
```

```shell
16:07:13.594 INFO  [DownloadRemoteLog-[sensors_db.sensor_readings_tbl]] c.a.f.c.t.s.l.RemoteLogDownloader$DownloadRemoteLogThread - Starting
16:07:13.599 INFO  [main] com.ververica.scanner.FlussScanner - Subscribing to Bucket 0.
16:07:13.599 INFO  [main] com.ververica.scanner.FlussScanner - Subscribing to Bucket 1.
16:07:13.600 INFO  [main] com.ververica.scanner.FlussScanner - Subscribing to Bucket 2.
16:07:13.600 INFO  [main] com.ververica.scanner.FlussScanner - Polling for records...
16:07:13.965 INFO  [main] com.ververica.scanner.FlussScanner - Received reading from sensor '3' at '2025-06-23T09:45'.
16:07:13.966 INFO  [main] com.ververica.scanner.FlussScanner - Performing lookup to get the information for sensor '3'. 
16:07:14.032 INFO  [main] com.ververica.scanner.FlussScanner - Retrieved information for 'Server Room Sensor' with id: 3
16:07:14.033 INFO  [main] com.ververica.scanner.FlussScanner - Bucket: TableBucket{tableId=2, bucket=1} - SensorReadingEnriched[sensorId=3, timestamp=2025-06-23T09:45, temperature=21.8, humidity=46.2, pressure=1012.9, batteryLevel=86.9, name=Server Room Sensor, type=Temperature, location=Server Room, state=MAINTENANCE]
16:07:14.045 INFO  [main] com.ververica.scanner.FlussScanner - ---------------------------------------
16:07:14.046 INFO  [main] com.ververica.scanner.FlussScanner - Received reading from sensor '4' at '2025-06-23T10:00'.
16:07:14.046 INFO  [main] com.ververica.scanner.FlussScanner - Performing lookup to get the information for sensor '4'. 
16:07:14.128 INFO  [main] com.ververica.scanner.FlussScanner - Retrieved information for 'Warehouse Sensor' with id: 4
16:07:14.128 INFO  [main] com.ververica.scanner.FlussScanner - Bucket: TableBucket{tableId=2, bucket=1} - SensorReadingEnriched[sensorId=4, timestamp=2025-06-23T10:00, temperature=24.0, humidity=43.8, pressure=1013.5, batteryLevel=89.2, name=Warehouse Sensor, type=Pressure, location=Warehouse, state=OK]
16:07:14.129 INFO  [main] com.ververica.scanner.FlussScanner - ---------------------------------------
16:07:14.129 INFO  [main] com.ververica.scanner.FlussScanner - Received reading from sensor '8' at '2025-06-23T11:00'.
16:07:14.129 INFO  [main] com.ververica.scanner.FlussScanner - Performing lookup to get the information for sensor '8'. 
16:07:14.229 INFO  [main] com.ververica.scanner.FlussScanner - Retrieved information for 'Lab Sensor' with id: 8
16:07:14.229 INFO  [main] com.ververica.scanner.FlussScanner - Bucket: TableBucket{tableId=2, bucket=1} - SensorReadingEnriched[sensorId=8, timestamp=2025-06-23T11:00, temperature=24.2, humidity=43.5, pressure=1013.6, batteryLevel=89.5, name=Lab Sensor, type=Temperature, location=Lab, state=ERROR]
16:07:14.229 INFO  [main] com.ververica.scanner.FlussScanner - ---------------------------------------
```

## Reads with Column Pruning
```java
LogScanner logScanner = readingsTable.newScan()
    .project(List.of("sensorId", "timestamp", "temperature"))
    .createLogScanner();
```

```shell
16:12:35.114 INFO  [main] com.ververica.scanner.FlussScanner - Subscribing to Bucket 0.
16:12:35.114 INFO  [main] com.ververica.scanner.FlussScanner - Subscribing to Bucket 1.
16:12:35.114 INFO  [main] com.ververica.scanner.FlussScanner - Subscribing to Bucket 2.
16:12:35.114 INFO  [main] com.ververica.scanner.FlussScanner - Polling for records...
16:12:35.171 INFO  [main] com.ververica.scanner.FlussScanner - Bucket: TableBucket{tableId=2, bucket=1} - (3,2025-06-23T09:45,21.8)
16:12:35.172 INFO  [main] com.ververica.scanner.FlussScanner - ---------------------------------------
16:12:35.172 INFO  [main] com.ververica.scanner.FlussScanner - Bucket: TableBucket{tableId=2, bucket=1} - (4,2025-06-23T10:00,24.0)
16:12:35.172 INFO  [main] com.ververica.scanner.FlussScanner - ---------------------------------------
16:12:35.172 INFO  [main] com.ververica.scanner.FlussScanner - Bucket: TableBucket{tableId=2, bucket=1} - (8,2025-06-23T11:00,24.2)
16:12:35.172 INFO  [main] com.ververica.scanner.FlussScanner - ---------------------------------------
16:12:35.172 INFO  [main] com.ververica.scanner.FlussScanner - Bucket: TableBucket{tableId=2, bucket=1} - (10,2025-06-23T11:30,22.6)
```

## Conclusion
And before you go 😊 don’t forget to give Fluss 🌊 some ❤️ via ⭐ on [GitHub](https://github.com/alibaba/fluss)
