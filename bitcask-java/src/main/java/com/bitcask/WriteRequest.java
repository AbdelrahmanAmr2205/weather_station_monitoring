package com.bitcask;

import java.util.concurrent.CompletableFuture;

/**
 * A single write request placed on the internal write queue.
 * Mirrors Go's {@code bitcask.writeRequest} struct.
 *
 * The caller blocks on {@code future} until the write loop completes the operation.
 */
public final class WriteRequest {

    public final String key;
    public final byte[] value;
    public final int    timestamp;

    /** Completed with {@code null} on success, or an exception on failure. */
    public final CompletableFuture<Void> future;

    public WriteRequest(String key, byte[] value, int timestamp) {
        this.key       = key;
        this.value     = value;
        this.timestamp = timestamp;
        this.future    = new CompletableFuture<>();
    }
}
