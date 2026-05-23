package com.weatherStatus.central_server.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class WeatherReading {

    private int humidity;
    private int temperature;
    private int windSpeed;
}
