package com.weatherStatus.central_server.services;

import com.weatherStatus.central_server.model.StationStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Thread-safe global in-memory buffer that accumulates {@link StationStatus} records from
 * <em>all</em> stations and flushes them when either trigger fires:
 *
 * <ul>
 *   <li><b>Batch trigger</b> — total buffered records across all stations reaches
 *       {@code parquet.batch.size} (default 10 000).</li>
 *   <li><b>Time trigger</b>  — {@link ParquetFlushScheduler} calls {@link #flushAll()}
 *       every {@code parquet.flush.interval.ms} milliseconds (default 120 000 = 2 min).</li>
 * </ul>
 *
 * <p>On flush, records are grouped by {@code (stationId, date)} and one Parquet file is
 * written per group into the corresponding partition directory. On write failure the
 * affected batch is re-queued at the head of the buffer so it is retried on the next flush.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BatchBufferService {

    private final ParquetArchiveService parquetArchiveService;

    @Value("${parquet.batch.size:10000}")
    private int batchSize;

    /** Single global buffer shared across all stations. */
    private final List<StationStatus> globalBuffer = new ArrayList<>();

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Adds a record to the global buffer.
     * Triggers an immediate flush if the total buffer size hits the batch threshold.
     */
    public synchronized void add(StationStatus record) {
        globalBuffer.add(record);
        if (globalBuffer.size() >= batchSize) {
            log.info("Batch-size trigger fired: {} total records buffered — flushing.", globalBuffer.size());
            doFlush();
        }
    }

    /**
     * Time-triggered flush — drains the entire global buffer regardless of its current size.
     * Called by {@link ParquetFlushScheduler}.
     */
    public synchronized void flushAll() {
        if (globalBuffer.isEmpty()) {
            log.debug("Scheduled flush: buffer is empty, nothing to write.");
            return;
        }
        log.info("Time-based flush triggered: {} records buffered across all stations.", globalBuffer.size());
        doFlush();
    }

    // -------------------------------------------------------------------------
    // Internal — MUST be called while holding the monitor (synchronized methods above).
    // -------------------------------------------------------------------------

    /**
     * Atomically drains the global buffer, groups records by {@code (stationId, date)},
     * and writes one Parquet file per group. Failed groups are re-queued at the head of
     * the buffer for the next flush attempt.
     */
    private void doFlush() {
        // Snapshot and clear atomically (we already hold the monitor).
        List<StationStatus> snapshot = new ArrayList<>(globalBuffer);
        globalBuffer.clear();

        // Group by (stationId, date) — each group maps to one partition directory.
        Map<String, List<StationStatus>> grouped = snapshot.stream()
                .collect(Collectors.groupingBy(
                        r -> r.getStationId() + "|" + r.getDate()));

        for (Map.Entry<String, List<StationStatus>> entry : grouped.entrySet()) {
            String[] parts   = entry.getKey().split("\\|");
            long stationId   = Long.parseLong(parts[0]);
            String date      = parts[1];
            List<StationStatus> batch = entry.getValue();

            try {
                parquetArchiveService.write(batch, stationId, date);
            } catch (Exception e) {
                log.error("Write failed for station={} date={} ({} records) — re-queuing for next flush: {}",
                        stationId, date, batch.size(), e.getMessage(), e);
                // Re-queue at the head so these records are not lost.
                globalBuffer.addAll(0, batch);
            }
        }
    }
}
