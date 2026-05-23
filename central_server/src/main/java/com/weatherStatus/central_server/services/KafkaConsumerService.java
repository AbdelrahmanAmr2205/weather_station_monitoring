package com.weatherStatus.central_server.services;

import com.weatherStatus.central_server.dto.WeatherStatusDTO;
import com.weatherStatus.central_server.model.StationStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final MessageProcessorService messageProcessorService;
    private final BatchBufferService batchBufferService;

    @KafkaListener(topics = "weather_statuses")
    public void consume(WeatherStatusDTO message) {
        Optional<StationStatus> processed = messageProcessorService.process(message);
        processed.ifPresentOrElse(
                status -> {
                    log.debug("Buffering → station={} seq={} date={}",
                            status.getStationId(), status.getSequenceNumber(),
                            status.getDate());
                    batchBufferService.add(status);
                },
                () -> log.warn("Message dropped (failed validation): stationId={} sNo={}",
                        message.stationId, message.sNo)
        );
    }
}
