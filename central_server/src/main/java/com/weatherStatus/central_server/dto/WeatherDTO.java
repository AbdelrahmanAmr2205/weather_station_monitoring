package com.weatherStatus.central_server.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WeatherDTO {

    public int humidity;
    public int temperature;

    @JsonProperty("wind_speed")
    public int windSpeed;
}