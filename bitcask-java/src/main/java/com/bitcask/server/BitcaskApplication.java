package com.bitcask.server;

import com.bitcask.BitcaskDB;
import com.bitcask.BitcaskInit;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Spring Boot entry point for the Bitcask HTTP server.
 * Mirrors Go's {@code cmd/bitcask-server/main.go}.
 *
 * <p>Configuration is done via environment variables:
 * <ul>
 *   <li>{@code PORT}                      — HTTP port (default 9092)</li>
 *   <li>{@code BITCASK_DIR}               — data directory</li>
 *   <li>{@code BITCASK_MAX_FILE_SIZE}     — max active file size in bytes (default 4 MB)</li>
 *   <li>{@code BITCASK_COMPACT_INTERVAL}  — compaction interval in ms (default 3 600 000)</li>
 *   <li>{@code BITCASK_SYNC_PERIOD}       — sync period in ms (default 0 = disabled)</li>
 *   <li>{@code BITCASK_EXPECTED_WRITE_RATE} — bytes/sec (default 16 384)</li>
 * </ul>
 */
@SpringBootApplication
public class BitcaskApplication {

    private static final Logger LOG = Logger.getLogger(BitcaskApplication.class.getName());

    public static void main(String[] args) {
        // Port is set via Spring's server.port property, driven by the PORT env var
        String port = getEnv("PORT", "9092");
        System.setProperty("server.port", port);

        SpringApplication.run(BitcaskApplication.class, args);
    }

    @Bean(destroyMethod = "close")
    public BitcaskDB bitcaskDB() throws IOException {
        String homeDir = System.getProperty("user.home");

        String directory         = getEnv("BITCASK_DIR",                homeDir + "/bitcask/data");
        long   maxFileSize       = getEnvLong("BITCASK_MAX_FILE_SIZE",  4_194_304L);
        long   compactIntervalMs = getEnvLong("BITCASK_COMPACT_INTERVAL", TimeUnit.HOURS.toMillis(1));
        long   syncPeriodMs      = getEnvLong("BITCASK_SYNC_PERIOD",    0L);
        long   expectedWriteRate = getEnvLong("BITCASK_EXPECTED_WRITE_RATE", 16_384L);

        LOG.info("Starting Bitcask engine at: " + directory);

        return BitcaskInit.initDB(directory, maxFileSize, compactIntervalMs, syncPeriodMs, expectedWriteRate);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static String getEnv(String key, String fallback) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : fallback;
    }

    private static long getEnvLong(String key, long fallback) {
        String val = System.getenv(key);
        if (val != null && !val.isBlank()) {
            try { return Long.parseLong(val.trim()); } catch (NumberFormatException ignored) {}
        }
        return fallback;
    }
}
