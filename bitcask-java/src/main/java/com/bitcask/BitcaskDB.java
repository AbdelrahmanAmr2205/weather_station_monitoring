package com.bitcask;

import com.bitcask.datafile.DataFile;
import com.bitcask.datafile.Record;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * Core Bitcask storage engine.
 *
 * <p>Design mirrors the Go implementation:
 * <ul>
 *   <li>A single background write-loop thread serialises all writes (replacing Go's goroutine + channel)</li>
 *   <li>Two {@link ReentrantReadWriteLock}s protect {@code files} and {@code keyDir} (replacing Go's sync.RWMutex)</li>
 *   <li>A {@link LinkedBlockingQueue} is the write channel (replacing Go's buffered channel)</li>
 *   <li>A {@code volatile boolean} stop flag coordinates shutdown (replacing Go's context.CancelFunc)</li>
 * </ul>
 */
public final class BitcaskDB {

    private static final Logger LOG = Logger.getLogger(BitcaskDB.class.getName());

    // ── Locks ──────────────────────────────────────────────────────────────
    final ReadWriteLock muFiles  = new ReentrantReadWriteLock();
    final ReadWriteLock muKeyDir = new ReentrantReadWriteLock();

    // ── State ──────────────────────────────────────────────────────────────
    final Map<Integer, DataFile>    files  = new HashMap<>();
    final Map<String, KeyDirEntry>  keyDir = new HashMap<>();

    volatile DataFile activeFile;

    final Config config;

    // ── Write loop ─────────────────────────────────────────────────────────
    final LinkedBlockingQueue<WriteRequest> writeChan = new LinkedBlockingQueue<>(100);

    private volatile boolean stopped = false;
    private Thread writeThread;
    private Thread compactionThread;

    // Package-private: constructed by BitcaskInit
    BitcaskDB(Config config) {
        this.config = config;
    }

    // ── Write loop ─────────────────────────────────────────────────────────

    /**
     * Starts the write loop thread and the compaction loop thread.
     * Called once after DB state has been initialised.
     */
    void start() {
        writeThread = Thread.ofVirtual()
                            .name("bitcask-write-loop")
                            .start(this::writeLoop);

        compactionThread = Thread.ofVirtual()
                                 .name("bitcask-compaction-loop")
                                 .start(new Compaction(this)::run);
    }

    /**
     * Blocking write loop — mirrors Go's {@code startWriteLoop} goroutine.
     */
    private void writeLoop() {
        while (!stopped) {
            WriteRequest req;
            try {
                // Poll so we can check the stopped flag periodically
                req = writeChan.poll(50, java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (req == null) {
                continue;
            }

            try {
                processWrite(req);
            } catch (Exception e) {
                req.future.completeExceptionally(e);
            }
        }
    }

    private void processWrite(WriteRequest req) throws IOException {
        Record record  = Record.of(req.key, req.value, req.timestamp);
        byte[] encoded = record.encode();

        long offset = activeFile.writeRecord(encoded);

        long valOffset = offset + Record.HEADER_SIZE + req.key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        writeKeyDirEntry(req.key, new KeyDirEntry(
                req.value.length,
                valOffset,
                activeFile.getId(),
                req.timestamp
        ));

        req.future.complete(null);

        // Rotate if active file exceeded max size
        if (activeFile.size() >= config.maxActiveFileSize) {
            try {
                createNewActiveFile(activeFile.getId() + 1);
            } catch (IOException e) {
                LOG.warning("Failed to rotate active file: " + e.getMessage());
            }
        }
    }

    // ── Public API ─────────────────────────────────────────────────────────

    /**
     * Persists a key-value pair.
     * Blocks until the write loop has flushed it to disk.
     * Mirrors Go's {@code DB.Put}.
     *
     * @throws IOException if the write fails
     */
    public void put(String key, byte[] value) throws IOException {
        if (stopped) {
            throw new IOException("Database is closed");
        }
        int timestamp = (int) (System.currentTimeMillis() / 1000L);
        WriteRequest req = new WriteRequest(key, value, timestamp);
        try {
            writeChan.put(req);
            req.future.get(); // block until the write loop completes
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Put interrupted", e);
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException ioe) throw ioe;
            throw new IOException("Write failed", cause);
        }
    }

    /**
     * Retrieves the value for the given key.
     * Mirrors Go's {@code DB.Get}.
     *
     * @return the value bytes
     * @throws IOException if the key does not exist or file read fails
     */
    public byte[] get(String key) throws IOException {
        KeyDirEntry entry = readKeyDirEntry(key);
        if (entry == null) {
            throw new IOException("Key does not exist");
        }

        DataFile targetFile;
        muFiles.readLock().lock();
        try {
            targetFile = files.get(entry.fileId);
        } finally {
            muFiles.readLock().unlock();
        }

        if (targetFile == null) {
            throw new IOException("Target database file segment missing");
        }

        return targetFile.readValue(entry.valOffset, entry.valSize);
    }

    /**
     * Gracefully shuts down all background threads and closes all open files.
     * Mirrors Go's {@code DB.Close}.
     */
    public void close() throws IOException {
        stopped = true;

        if (writeThread != null) {
            writeThread.interrupt();
        }
        if (compactionThread != null) {
            compactionThread.interrupt();
        }

        muFiles.writeLock().lock();
        try {
            for (DataFile f : files.values()) {
                f.close();
            }
        } finally {
            muFiles.writeLock().unlock();
        }
    }

    // ── File rotation ──────────────────────────────────────────────────────

    /**
     * Creates a new active data file and registers it.
     * Mirrors Go's {@code createNewActiveFile}.
     */
    void createNewActiveFile(int fileId) throws IOException {
        Path dir = Path.of(config.directory);
        IOException lastErr = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                DataFile newFile = DataFile.open(dir, fileId);
                muFiles.writeLock().lock();
                try {
                    activeFile = newFile;
                    files.put(fileId, newFile);
                } finally {
                    muFiles.writeLock().unlock();
                }
                return;
            } catch (IOException e) {
                lastErr = e;
            }
        }
        throw new IOException("Couldn't create new active file after 5 attempts", lastErr);
    }

    // ── Internal helpers ───────────────────────────────────────────────────

    /**
     * Returns a snapshot of all currently stored keys.
     * Used by the {@code GET /} list-all endpoint.
     */
    public Set<String> keys() {
        muKeyDir.readLock().lock();
        try {
            return new java.util.HashSet<>(keyDir.keySet());
        } finally {
            muKeyDir.readLock().unlock();
        }
    }

    void writeKeyDirEntry(String key, KeyDirEntry entry) {
        muKeyDir.writeLock().lock();
        try {
            keyDir.put(key, entry);
        } finally {
            muKeyDir.writeLock().unlock();
        }
    }

    KeyDirEntry readKeyDirEntry(String key) {
        muKeyDir.readLock().lock();
        try {
            return keyDir.get(key);
        } finally {
            muKeyDir.readLock().unlock();
        }
    }

    boolean isStopped() {
        return stopped;
    }
}
