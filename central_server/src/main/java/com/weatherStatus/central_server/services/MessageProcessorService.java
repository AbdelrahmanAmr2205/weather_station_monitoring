package com.weatherStatus.central_server.services;

import com.weatherStatus.central_server.dto.WeatherStatusDTO;
import com.weatherStatus.central_server.model.StationStatus;
import com.weatherStatus.central_server.model.WeatherReading;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;



@Slf4j
@Service
public class MessageProcessorService {

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public Optional<StationStatus> process(WeatherStatusDTO dto) {

        // --- Validation ---
        if (dto.stationId <= 0) {
            log.warn("Rejected: invalid stationId={}", dto.stationId);
            return Optional.empty();
        }
        if (dto.sNo <= 0) {
            log.warn("Rejected: invalid sequenceNumber={} for stationId={}", dto.sNo, dto.stationId);
            return Optional.empty();
        }
        if (dto.weather == null) {
            log.warn("Rejected: null weather payload for stationId={} sNo={}", dto.stationId, dto.sNo);
            return Optional.empty();
        }
        if (dto.batteryStatus == null || dto.batteryStatus.isBlank()) {
            log.warn("Rejected: missing batteryStatus for stationId={} sNo={}", dto.stationId, dto.sNo);
            return Optional.empty();
        }

        // --- Enrichment ---
        long ingestedAt = System.currentTimeMillis();

        // Derive UTC date from the producer's Unix-epoch-second timestamp.
        ZonedDateTime dt = Instant.ofEpochSecond(dto.statusTimestamp).atZone(ZoneOffset.UTC);
        String date = dt.format(DATE_FMT);

        WeatherReading weather = WeatherReading.builder()
                .humidity(dto.weather.humidity)
                .temperature(dto.weather.temperature)
                .windSpeed(dto.weather.windSpeed)
                .build();

        StationStatus status = StationStatus.builder()
                .stationId(dto.stationId)
                .sequenceNumber(dto.sNo)
                .batteryStatus(dto.batteryStatus)
                .statusTimestamp(dto.statusTimestamp)
                .ingestedAt(ingestedAt)
                .date(date)
                .weather(weather)
                .build();

        return Optional.of(status);
    }
}
