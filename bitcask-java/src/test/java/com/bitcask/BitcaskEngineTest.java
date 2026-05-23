package com.bitcask;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Full test suite for the Java Bitcask storage engine.
 *
 * Mirrors every test case in the Go {@code bitcask_test.go}:
 * <ol>
 *   <li>CRUD operations and overwrites</li>
 *   <li>Active file size rotation</li>
 *   <li>Crash-consistent state recovery</li>
 *   <li>High-volume concurrent access</li>
 *   <li>Background compaction reduces file count</li>
 *   <li>Hint-file-based fast startup</li>
 * </ol>
 */
class BitcaskEngineTest {

    // ─────────────────────────────────────────────────────────────────────────
    // 1. CRUD and overwrites
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    void crudOperationsAndOverwrites(@TempDir Path dir) throws IOException, InterruptedException {
        BitcaskDB db = BitcaskInit.initDB(dir.toString(), 1024 * 1024, 0, 0, 0);
        try {
            // Write two keys
            db.put("foo",   "bar".getBytes());
            db.put("alpha", "omega".getBytes());

            // Read back
            assertArrayEquals("bar".getBytes(),   db.get("foo"),   "foo should equal bar");
            assertArrayEquals("omega".getBytes(),  db.get("alpha"), "alpha should equal omega");

            // Overwrite
            db.put("foo", "new_bar".getBytes());
            assertArrayEquals("new_bar".getBytes(), db.get("foo"), "foo should equal new_bar after overwrite");

            // Non-existent key should throw
            assertThrows(IOException.class, () -> db.get("missing_key"));
        } finally {
            db.close();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 2. Active file size rotation
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    void activeFileSizeRotation(@TempDir Path dir) throws IOException, InterruptedException {
        // Low threshold (30 bytes) forces rotation after each write
        BitcaskDB db = BitcaskInit.initDB(dir.toString(), 30, 0, 0, 0);

        byte[] value = "telemetry_payload_bytes_long".getBytes();
        for (String key : new String[]{"key_one", "key_two", "key_three"}) {
            db.put(key, value);
            Thread.sleep(10); // let the async write loop flush
        }
        db.close();

        long dataFileCount = Files.list(dir)
                .filter(p -> p.toString().endsWith(".data"))
                .count();

        assertTrue(dataFileCount >= 2,
                "Expected multiple .data segments after rotation, found: " + dataFileCount);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3. Crash-consistent state recovery
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    void crashConsistentStateRecovery(@TempDir Path dir) throws IOException, InterruptedException {
        // Phase 1: write data and "crash"
        BitcaskDB db = BitcaskInit.initDB(dir.toString(), 1024 * 1024, 0, 0, 0);
        db.put("station_1", "status_normal".getBytes());
        db.put("station_2", "status_alert".getBytes());
        db.put("station_1", "status_updated_critical".getBytes()); // overwrite
        Thread.sleep(10);
        db.close();

        // Phase 2: recover from disk
        BitcaskDB recovered = BitcaskInit.initDB(dir.toString(), 1024 * 1024, 0, 0, 0);
        try {
            assertArrayEquals("status_updated_critical".getBytes(),
                    recovered.get("station_1"),
                    "station_1 should recover with latest value");

            assertArrayEquals("status_alert".getBytes(),
                    recovered.get("station_2"),
                    "station_2 should recover");
        } finally {
            recovered.close();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 4. High-volume concurrent access
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    void highVolumeConcurrentAccess(@TempDir Path dir) throws Exception {
        BitcaskDB db = BitcaskInit.initDB(dir.toString(), 1024 * 1024, 0, 0, 0);

        int workerCount          = 20;
        int operationsPerWorker  = 50;

        AtomicInteger errors = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(workerCount);

        try (ExecutorService pool = Executors.newFixedThreadPool(workerCount)) {
            for (int i = 0; i < workerCount; i++) {
                final int workerId = i;
                pool.submit(() -> {
                    try {
                        String key = "concurrent_key_" + workerId;
                        for (int j = 0; j < operationsPerWorker; j++) {
                            db.put(key, "payload_update".getBytes());
                            db.get(key);
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            boolean finished = latch.await(30, TimeUnit.SECONDS);
            assertTrue(finished, "Concurrent workers did not finish in time");
        }

        db.close();

        assertEquals(0, errors.get(), "Expected zero errors during concurrent access");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 5. Background compaction
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    void backgroundCompactionReducesFileCount(@TempDir Path dir) throws IOException, InterruptedException {
        // 25-byte max file, 100 ms compaction — forces rapid rotation + merging
        BitcaskDB db = BitcaskInit.initDB(dir.toString(), 25, 100, 0, 50);

        for (int i = 0; i < 10; i++) {
            db.put("persistent_key", ("payload_update_index_" + i).getBytes());
            Thread.sleep(20); // let files rotate naturally
        }

        Thread.sleep(300); // allow compaction to fire

        // Latest value must be intact
        byte[] got = db.get("persistent_key");
        assertNotNull(got);
        assertTrue(new String(got).contains("payload_update_index_9"),
                "Expected latest payload after compaction, got: " + new String(got));

        db.close();

        long dataFileCount = Files.list(dir)
                .filter(p -> p.toString().endsWith(".data"))
                .count();

        assertTrue(dataFileCount <= 4,
                "Compaction should have collapsed files; found " + dataFileCount + " remaining");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 6. Hint-file-based fast startup
    // ─────────────────────────────────────────────────────────────────────────

    @Test
    void hintFileBoot(@TempDir Path dir) throws IOException, InterruptedException {
        // Trigger compaction to produce .hint files
        BitcaskDB db = BitcaskInit.initDB(dir.toString(), 25, 100, 0, 50);

        for (int i = 0; i < 10; i++) {
            db.put("hint_key", ("payload_" + i).getBytes());
            Thread.sleep(20);
        }

        Thread.sleep(300); // let compaction run
        db.close();

        // Verify hint files were created
        long hintFileCount = Files.list(dir)
                .filter(p -> p.toString().endsWith(".hint"))
                .count();

        assertTrue(hintFileCount > 0, "Compaction should have produced .hint files");

        // Boot a fresh instance — it should use hint files
        BitcaskDB recovered = BitcaskInit.initDB(dir.toString(), 25, 100, 0, 50);
        try {
            byte[] got = recovered.get("hint_key");
            assertArrayEquals("payload_9".getBytes(), got,
                    "Hint-file recovery should return latest value");
        } finally {
            recovered.close();
        }
    }
}
