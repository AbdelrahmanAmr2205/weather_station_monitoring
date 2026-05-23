package com.bitcask;

import com.bitcask.datafile.DataFile;
import com.bitcask.datafile.HintFile;
import com.bitcask.datafile.ScannedEntry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Database initialisation logic.
 *
 * Mirrors Go's {@code InitDB}, {@code loadFiles}, and {@code buildKeyDir}.
 */
public final class BitcaskInit {

    private BitcaskInit() {}

    /**
     * Initialises a Bitcask database at the given directory.
     *
     * <ul>
     *   <li>Creates the directory if it does not exist.</li>
     *   <li>Loads all existing {@code .data} file segments.</li>
     *   <li>Rebuilds the in-memory key directory, preferring {@code .hint} files.</li>
     *   <li>Starts the write loop and compaction background threads.</li>
     * </ul>
     *
     * @param directoryPath       path to the data directory
     * @param maxActiveFileSize   max bytes before rotating the active file (0 = 4 MB default)
     * @param compactIntervalMs   milliseconds between compaction passes (0 = 1 hour default)
     * @param syncPeriodMs        background fsync period in ms (0 = disabled)
     * @param expectedWriteRate   expected bytes/sec write volume (0 = 16 kB/s default)
     * @return an initialised, running {@link BitcaskDB}
     * @throws IOException if the directory cannot be created or files cannot be opened
     */
    public static BitcaskDB initDB(
            String directoryPath,
            long   maxActiveFileSize,
            long   compactIntervalMs,
            long   syncPeriodMs,
            long   expectedWriteRate) throws IOException {

        Path dir = Path.of(directoryPath);
        Files.createDirectories(dir);

        if (maxActiveFileSize <= 0) {
            maxActiveFileSize = 1024L * 1024 * 4; // 4 MB default
        }
        if (compactIntervalMs <= 0) {
            compactIntervalMs = 60L * 60 * 1000; // 1 hour default
        }
        if (expectedWriteRate <= 0) {
            expectedWriteRate = 16L * 1024; // 16 kB/s default
        }

        double totalExpectedBytes = (double) expectedWriteRate * (compactIntervalMs / 1000.0);
        int expectedFiles = Math.max((int) (totalExpectedBytes / maxActiveFileSize), 2);

        Config config = new Config(
                directoryPath,
                maxActiveFileSize,
                compactIntervalMs,
                syncPeriodMs,
                expectedWriteRate,
                expectedFiles
        );

        BitcaskDB db = new BitcaskDB(config);

        loadFiles(db, dir);
        buildKeyDir(db, dir);

        db.start();

        return db;
    }

    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Discovers and opens all {@code .data} files in the directory.
     * Mirrors Go's {@code loadFiles}.
     */
    static void loadFiles(BitcaskDB db, Path dir) throws IOException {
        List<Path> entries;
        try (var stream = Files.list(dir)) {
            entries = stream.filter(p -> p.toString().endsWith(".data"))
                            .sorted()
                            .toList();
        }

        if (entries.isEmpty()) {
            // Bootstrap: create the first active file
            DataFile first = DataFile.open(dir, 1);
            db.muFiles.writeLock().lock();
            try {
                db.files.put(1, first);
                db.activeFile = first;
            } finally {
                db.muFiles.writeLock().unlock();
            }
            return;
        }

        int maxFileId = 0;

        db.muFiles.writeLock().lock();
        try {
            for (Path p : entries) {
                String name   = p.getFileName().toString();
                String stem   = name.substring(0, name.length() - ".data".length());
                int    fileId = Integer.parseInt(stem);

                if (fileId > maxFileId) {
                    maxFileId = fileId;
                }

                db.files.put(fileId, DataFile.open(dir, fileId));
            }
            db.activeFile = db.files.get(maxFileId);
        } finally {
            db.muFiles.writeLock().unlock();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Rebuilds the in-memory keyDir from disk, preferring {@code .hint} files
     * over full data scans for speed.
     *
     * Mirrors Go's {@code buildKeyDir}.
     */
    static void buildKeyDir(BitcaskDB db, Path dir) throws IOException {
        List<Integer> sortedIds;

        db.muFiles.readLock().lock();
        try {
            sortedIds = new ArrayList<>(db.files.keySet());
        } finally {
            db.muFiles.readLock().unlock();
        }
        Collections.sort(sortedIds);

        for (int id : sortedIds) {
            Path hintPath = dir.resolve(String.format("%010d.hint", id));

            List<ScannedEntry> entries;
            if (Files.exists(hintPath)) {
                try {
                    entries = HintFile.scan(hintPath);
                } catch (IOException e) {
                    // Fallback to full scan if hint file is corrupted
                    db.muFiles.readLock().lock();
                    try {
                        entries = db.files.get(id).scanRecords();
                    } finally {
                        db.muFiles.readLock().unlock();
                    }
                }
            } else {
                db.muFiles.readLock().lock();
                try {
                    entries = db.files.get(id).scanRecords();
                } finally {
                    db.muFiles.readLock().unlock();
                }
            }

            for (ScannedEntry entry : entries) {
                KeyDirEntry current = db.readKeyDirEntry(entry.key());

                if (current != null && Integer.compareUnsigned(current.timestamp, entry.timestamp()) > 0) {
                    continue; // existing entry is newer
                }

                db.writeKeyDirEntry(entry.key(), new KeyDirEntry(
                        entry.valSize(),
                        entry.valOffset(),
                        id,
                        entry.timestamp()
                ));
            }
        }
    }
}
