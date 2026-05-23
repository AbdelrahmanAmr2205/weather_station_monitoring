package com.weatherStatus.central_server.dto;
import com.fasterxml.jackson.annotation.JsonProperty;

public class WeatherStatusDTO {

    @JsonProperty("station_id")
    public long stationId;

    @JsonProperty("s_no")
    public long sNo;

    @JsonProperty("battery_status")
    public String batteryStatus;

    @JsonProperty("status_timestamp")
    public long statusTimestamp;

    public WeatherDTO weather;
}