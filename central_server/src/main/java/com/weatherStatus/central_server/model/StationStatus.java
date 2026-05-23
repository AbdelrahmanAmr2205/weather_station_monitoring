package com.weatherStatus.central_server.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class StationStatus {

    /** Originating station identifier. */
    private long stationId;

    /** Message sequence number — used for gap detection / deduplication. */
    private long sequenceNumber;

    /** Battery level reported by the station: low / medium / high. */
    private String batteryStatus;

    /** Unix epoch seconds — as reported by the producer. */
    private long statusTimestamp;

    /** Unix epoch millis — wall clock time when the server received the message. */
    private long ingestedAt;

    /** ISO date string (UTC) derived from statusTimestamp, e.g. "2025-05-23". Used as partition key. */
    private String date;

    /** Embedded weather reading. */
    private WeatherReading weather;
}
