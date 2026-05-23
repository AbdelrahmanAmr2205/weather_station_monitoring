package com.bitcask;

import com.bitcask.datafile.DataFile;
import com.bitcask.datafile.HintRecord;
import com.bitcask.datafile.Record;
import com.bitcask.datafile.ScannedEntry;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
 * Background compaction loop and merge logic.
 *
 * Mirrors Go's {@code compaction.go}: {@code RunCompactionLoop},
 * {@code getCompactionBatch}, and {@code mergeBatch}.
 */
public final class Compaction implements Runnable {

    private static final Logger LOG = Logger.getLogger(Compaction.class.getName());

    private final BitcaskDB db;

    public Compaction(BitcaskDB db) {
        this.db = db;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Main loop
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Entry point for the compaction background thread.
     * Mirrors Go's {@code RunCompactionLoop}.
     */
    @Override
    public void run() {
        long intervalMs = db.config.compactIntervalMs;
        if (intervalMs <= 0) {
            return;
        }

        long lastWriteOffset = 0;
        db.muFiles.readLock().lock();
        try {
            if (db.activeFile != null) {
                lastWriteOffset = db.activeFile.size();
            }
        } finally {
            db.muFiles.readLock().unlock();
        }

        while (!db.isStopped()) {
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            if (db.isStopped()) {
                return;
            }

            // 1. Recalibrate adaptive write-rate metrics before the pass
            List<Integer> batch = getCompactionBatch();

            db.muFiles.readLock().lock();
            DataFile activeFile;
            try {
                activeFile = db.activeFile;
            } finally {
                db.muFiles.readLock().unlock();
            }

            if (activeFile != null) {
                long   bytesWritten  = (long) batch.size() * db.config.maxActiveFileSize;
                double durationSec   = intervalMs / 1000.0;
                long   currentOffset = activeFile.size();

                long estimatedRate;
                if (bytesWritten > 0) {
                    estimatedRate = (long) (bytesWritten / durationSec);
                } else {
                    bytesWritten  = currentOffset - lastWriteOffset;
                    estimatedRate = (long) (bytesWritten / durationSec);
                }

                if (estimatedRate > 0) {
                    db.config.expectedWriteRate = estimatedRate;
                    double totalExpected  = db.config.expectedWriteRate * durationSec;
                    int    computedFiles  = (int) Math.ceil(totalExpected / db.config.maxActiveFileSize);
                    if (computedFiles < 4) computedFiles = 4;
                    db.config.expectedFilesPerInterval = computedFiles;
                }
                lastWriteOffset = currentOffset;
            }

            // 2. Skip trivial batches
            if (batch.size() < 2) {
                continue;
            }

            // 3. Execute merge
            try {
                mergeBatch(batch);
            } catch (IOException e) {
                LOG.warning("Compaction merge failed: " + e.getMessage());
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Batch selection
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Returns a sorted list of immutable (non-active) file IDs selected for
     * the next compaction pass.
     * Mirrors Go's {@code getCompactionBatch}.
     */
    List<Integer> getCompactionBatch() {
        List<Integer> immutableIds = new ArrayList<>();

        db.muFiles.readLock().lock();
        try {
            int activeId = db.activeFile.getId();
            for (int id : db.files.keySet()) {
                if (id < activeId) {
                    immutableIds.add(id);
                }
            }
        } finally {
            db.muFiles.readLock().unlock();
        }

        if (immutableIds.size() < 2) {
            return Collections.emptyList();
        }

        Collections.sort(immutableIds);

        int maxBatchSize = db.config.expectedFilesPerInterval;
        if (maxBatchSize < 2)  maxBatchSize = 2;
        if (maxBatchSize > 16) maxBatchSize = 16; // hard upper ceiling — prevent I/O saturation

        if (immutableIds.size() > maxBatchSize) {
            return new ArrayList<>(immutableIds.subList(0, maxBatchSize));
        }
        return immutableIds;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Merge pass
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Holds a pending keyDir pointer update to be applied atomically after
     * the merge file has been written and renamed.
     */
    private record PendingUpdate(String key, KeyDirEntry entry) {}

    /**
     * Executes a full compaction merge pass on the given batch of file IDs.
     *
     * <ol>
     *   <li>Linearly scans every live record (those the keyDir currently
     *       points to) from the batch files — lock-free.</li>
     *   <li>Writes a temp {@code .merge} data file and a {@code .merge-hint}
     *       file in the same directory.</li>
     *   <li>Under a short exclusive lock, atomically renames both temp files
     *       into their final names, updates the keyDir, and removes stale
     *       file segments from the registry and disk.</li>
     * </ol>
     *
     * Mirrors Go's {@code mergeBatch}.
     */
    void mergeBatch(List<Integer> batch) throws IOException {
        int  highestId         = batch.get(batch.size() - 1);
        Path dir               = Path.of(db.config.directory);
        Path mergeFilePath     = dir.resolve(String.format("%010d.merge",      highestId));
        Path mergeHintFilePath = dir.resolve(String.format("%010d.merge-hint", highestId));

        List<PendingUpdate> updates          = new ArrayList<>();
        long                currentWriteOff  = 0;

        // ── 1. Lock-free linear scan ──────────────────────────────────────────
        try (FileOutputStream mergeOut     = new FileOutputStream(mergeFilePath.toFile());
             FileOutputStream mergeHintOut = new FileOutputStream(mergeHintFilePath.toFile())) {

            for (int id : batch) {
                DataFile file;
                db.muFiles.readLock().lock();
                try {
                    file = db.files.get(id);
                } finally {
                    db.muFiles.readLock().unlock();
                }

                if (file == null) {
                    cleanup(mergeFilePath, mergeHintFilePath);
                    throw new IOException("File segment missing mid-compaction: " + id);
                }

                List<ScannedEntry> scanned;
                try {
                    scanned = file.scanRecords();
                } catch (IOException e) {
                    cleanup(mergeFilePath, mergeHintFilePath);
                    throw e;
                }

                for (ScannedEntry entry : scanned) {
                    // Liveness check: keyDir must point to this exact file + offset
                    KeyDirEntry memEntry;
                    db.muKeyDir.readLock().lock();
                    try {
                        memEntry = db.keyDir.get(entry.key());
                    } finally {
                        db.muKeyDir.readLock().unlock();
                    }

                    if (memEntry == null
                            || memEntry.fileId    != id
                            || memEntry.valOffset != entry.valOffset()) {
                        continue; // stale — garbage collect
                    }

                    // Read live value bytes
                    byte[] valBytes;
                    try {
                        valBytes = file.readValue(entry.valOffset(), entry.valSize());
                    } catch (IOException e) {
                        cleanup(mergeFilePath, mergeHintFilePath);
                        throw e;
                    }

                    // Write data record
                    Record record  = Record.of(entry.key(), valBytes, entry.timestamp());
                    byte[] encoded = record.encode();
                    mergeOut.write(encoded);

                    long valOffset = currentWriteOff
                            + Record.HEADER_SIZE
                            + entry.key().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;

                    // Write hint record
                    HintRecord hint        = HintRecord.of(entry.timestamp(), entry.valSize(), valOffset, entry.key());
                    byte[]     hintEncoded = hint.encode();
                    mergeHintOut.write(hintEncoded);

                    updates.add(new PendingUpdate(entry.key(), new KeyDirEntry(
                            entry.valSize(),
                            valOffset,
                            highestId,
                            entry.timestamp()
                    )));

                    currentWriteOff += encoded.length;
                }
            }

            mergeOut.flush();
            mergeHintOut.flush();
        }

        // ── 2. Atomic swap under exclusive locks ─────────────────────────────
        Path finalDataPath = dir.resolve(String.format("%010d.data", highestId));
        Path finalHintPath = dir.resolve(String.format("%010d.hint", highestId));

        List<Path> toDelete = new ArrayList<>();

        db.muFiles.writeLock().lock();
        db.muKeyDir.writeLock().lock();
        try {
            // Close old file handle for highestId (if any)
            DataFile oldFile = db.files.get(highestId);
            if (oldFile != null) {
                try { oldFile.close(); } catch (IOException ignored) {}
            }

            Files.move(mergeFilePath,     finalDataPath, StandardCopyOption.REPLACE_EXISTING);
            Files.move(mergeHintFilePath, finalHintPath, StandardCopyOption.REPLACE_EXISTING);

            DataFile finalDataFile = DataFile.open(dir, highestId);

            // Apply updated keyDir pointers
            for (PendingUpdate up : updates) {
                KeyDirEntry current = db.keyDir.get(up.key());
                // Guard: a concurrent Put() may have produced a newer entry
                if (current != null
                        && Integer.compareUnsigned(current.timestamp, up.entry().timestamp) > 0) {
                    continue;
                }
                db.keyDir.put(up.key(), up.entry());
            }

            // Untrack stale segments
            for (int id : batch) {
                if (id == highestId) {
                    db.files.put(id, finalDataFile);
                    continue;
                }
                DataFile f = db.files.remove(id);
                if (f != null) {
                    try { f.close(); } catch (IOException ignored) {}
                    toDelete.add(dir.resolve(String.format("%010d.data", id)));
                    toDelete.add(dir.resolve(String.format("%010d.hint", id)));
                }
            }
        } catch (IOException e) {
            db.muKeyDir.writeLock().unlock();
            db.muFiles.writeLock().unlock();
            cleanup(mergeFilePath, mergeHintFilePath);
            throw e;
        } finally {
            // Guard against double-unlock if exception path already unlocked
            try { db.muKeyDir.writeLock().unlock(); } catch (IllegalMonitorStateException ignored) {}
            try { db.muFiles.writeLock().unlock();  } catch (IllegalMonitorStateException ignored) {}
        }

        // ── 3. Delete stale files outside the lock ───────────────────────────
        for (Path p : toDelete) {
            try { Files.deleteIfExists(p); } catch (IOException ignored) {}
        }
    }

    // ─────────────────────────────────────────────────────────────────────────

    private static void cleanup(Path... paths) {
        for (Path p : paths) {
            try { Files.deleteIfExists(p); } catch (IOException ignored) {}
        }
    }
}
