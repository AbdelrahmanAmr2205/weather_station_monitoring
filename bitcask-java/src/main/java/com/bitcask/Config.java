package com.bitcask;

/**
 * Database configuration.
 * Mirrors Go's {@code bitcask.Config} struct.
 */
public final class Config {

    /** Path to the data directory on disk. */
    public final String directory;

    /** Max size in bytes before rotating the active file. */
    public final long maxActiveFileSize;

    /** Time between background compaction merges (milliseconds). */
    public volatile long compactIntervalMs;

    /** Optional background fsync interval (0 = disabled). */
    public final long syncPeriodMs;

    /** Expected write throughput in bytes/sec — used for adaptive compaction tuning. */
    public volatile long expectedWriteRate;

    /** Estimated number of data files generated per compaction interval. */
    public volatile int expectedFilesPerInterval;

    public Config(
            String directory,
            long maxActiveFileSize,
            long compactIntervalMs,
            long syncPeriodMs,
            long expectedWriteRate,
            int expectedFilesPerInterval) {
        this.directory              = directory;
        this.maxActiveFileSize      = maxActiveFileSize;
        this.compactIntervalMs      = compactIntervalMs;
        this.syncPeriodMs           = syncPeriodMs;
        this.expectedWriteRate      = expectedWriteRate;
        this.expectedFilesPerInterval = expectedFilesPerInterval;
    }
}
