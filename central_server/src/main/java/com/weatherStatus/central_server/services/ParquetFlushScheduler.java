package com.weatherStatus.central_server.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Time-based flush trigger for the Parquet archival pipeline.
 *
 * <p>Fires every {@code parquet.flush.interval.ms} milliseconds (default 120 000 = 2 minutes).
 * Uses {@code fixedDelay} semantics — the next run starts 120 s <em>after</em> the previous
 * flush completes, preventing overlapping flushes on slow I/O.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ParquetFlushScheduler {

    private final BatchBufferService batchBufferService;

    @Scheduled(fixedDelayString = "${parquet.flush.interval.ms:120000}")
    public void scheduledFlush() {
        log.info("=== Parquet time-based flush triggered ===");
        batchBufferService.flushAll();
    }
}
