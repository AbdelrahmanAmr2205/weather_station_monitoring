package com.weatherStatus.central_server.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weatherStatus.central_server.dto.WeatherDTO;
import com.weatherStatus.central_server.dto.WeatherStatusDTO;
import com.weatherStatus.central_server.model.StationStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

@Slf4j
@Service
public class BitcaskClientService {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String bitcaskUrl;

    public BitcaskClientService(
            ObjectMapper objectMapper,
            @Value("${bitcask.url:http://localhost:9095}") String bitcaskUrl) {
        this.objectMapper = objectMapper;
        this.bitcaskUrl = bitcaskUrl;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    /**
     * Updates the status of a weather station in the Bitcask key-value store.
     */
    public void updateStationStatus(StationStatus status) {
        try {
            String key = String.valueOf(status.getStationId());
            
            // Map StationStatus to WeatherStatusDTO to match the exact JSON schema required
            WeatherStatusDTO dto = new WeatherStatusDTO();
            dto.stationId = status.getStationId();
            dto.sNo = status.getSequenceNumber();
            dto.batteryStatus = status.getBatteryStatus();
            dto.statusTimestamp = status.getStatusTimestamp();
            
            WeatherDTO weatherDto = new WeatherDTO();
            weatherDto.humidity = status.getWeather().getHumidity();
            weatherDto.temperature = status.getWeather().getTemperature();
            weatherDto.windSpeed = status.getWeather().getWindSpeed();
            dto.weather = weatherDto;

            String jsonPayload = objectMapper.writeValueAsString(dto);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(bitcaskUrl + "/" + key))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                    .timeout(Duration.ofSeconds(5))
                    .build();

            // Perform request asynchronously to avoid blocking the main Kafka consumer thread
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(response -> {
                        if (response.statusCode() == 201) {
                            log.debug("Successfully updated Bitcask for stationId={}", key);
                        } else {
                            log.warn("Failed to update Bitcask for stationId={}: HTTP {} - {}", 
                                    key, response.statusCode(), response.body());
                        }
                    })
                    .exceptionally(ex -> {
                        log.error("Error communicating with Bitcask server for stationId={}: {}", 
                                key, ex.getMessage());
                        return null;
                    });

        } catch (Exception e) {
            log.error("Failed to serialize or initiate Bitcask update for stationId={}: {}", 
                    status.getStationId(), e.getMessage());
        }
    }
}
