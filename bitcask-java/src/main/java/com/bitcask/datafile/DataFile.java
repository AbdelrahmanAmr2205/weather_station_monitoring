package com.bitcask.datafile;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Append-only data file segment for the Bitcask storage engine.
 *
 * Thread-safe via a {@link ReentrantReadWriteLock} — mirrors Go's
 * {@code sync.RWMutex} usage inside {@code DataFile}.
 *
 * File naming convention: {@code %010d.data} (zero-padded 10-digit ID).
 */
public final class DataFile implements AutoCloseable {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final int id;
    private final FileChannel channel;
    private long writeOffset; // next-write position (== file size at open time)

    private DataFile(int id, FileChannel channel, long initialSize) {
        this.id          = id;
        this.channel     = channel;
        this.writeOffset = initialSize;
    }

    /**
     * Opens (or creates) a {@code .data} file for the given directory and ID.
     * Equivalent to Go's {@code datafile.OpenDataFile}.
     */
    public static DataFile open(Path directory, int fileId) throws IOException {
        String name = String.format("%010d.data", fileId);
        Path   path = directory.resolve(name);

        FileChannel ch = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        long size = ch.size();
        // Position to end for appends
        ch.position(size);

        return new DataFile(fileId, ch, size);
    }

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    /**
     * Appends an encoded record to the file.
     *
     * @param data pre-encoded record bytes
     * @return the byte offset at which this record starts
     * @throws IOException on write failure
     */
    public long writeRecord(byte[] data) throws IOException {
        lock.writeLock().lock();
        try {
            long startingOffset = writeOffset;
            ByteBuffer buf = ByteBuffer.wrap(data);
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
            writeOffset += data.length;
            return startingOffset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    /**
     * Reads {@code size} bytes at the absolute {@code offset}.
     * Mirrors Go's {@code DataFile.ReadValue}.
     */
    public byte[] readValue(long offset, int size) throws IOException {
        lock.readLock().lock();
        try {
            ByteBuffer buf = ByteBuffer.allocate(size);
            int total = 0;
            while (total < size) {
                int n = channel.read(buf, offset + total);
                if (n == -1) {
                    throw new IOException("Unexpected EOF reading value at offset " + offset);
                }
                total += n;
            }
            return buf.array();
        } finally {
            lock.readLock().unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Scan
    // -------------------------------------------------------------------------

    /**
     * Sequentially scans all records from position 0, returning lightweight
     * metadata entries (without reading the value bytes into memory).
     *
     * Mirrors Go's {@code DataFile.ScanRecords}.
     */
    public List<ScannedEntry> scanRecords() throws IOException {
        // Use a separate, seeked InputStream so we don't disturb the channel position
        Path filePath = channel.map(FileChannel.MapMode.READ_ONLY, 0, 0)
                               .equals(null) ? null : null; // placeholder, see below
        // Obtain the file path via the channel — use a BufferedInputStream over the channel directly
        return scanRecordsViaChannel();
    }

    private List<ScannedEntry> scanRecordsViaChannel() throws IOException {
        lock.readLock().lock();
        try {
            List<ScannedEntry> entries = new ArrayList<>();
            long currentOffset = 0;

            // Snapshot the current file size to avoid reading past it
            long fileSize = channel.size();
            if (fileSize == 0) {
                return entries;
            }

            // Read the entire file into a buffer for efficient sequential parsing
            ByteBuffer fullBuf = ByteBuffer.allocate((int) Math.min(fileSize, Integer.MAX_VALUE));
            channel.read(fullBuf, 0);
            fullBuf.flip();

            while (fullBuf.remaining() >= Record.HEADER_SIZE) {
                long headerStart = currentOffset;

                // Read header fields (little-endian)
                int timestamp = Integer.reverseBytes(fullBuf.getInt());
                int keySize   = Integer.reverseBytes(fullBuf.getInt());
                int valSize   = Integer.reverseBytes(fullBuf.getInt());

                if (fullBuf.remaining() < keySize + valSize) {
                    break; // Truncated record — stop cleanly
                }

                byte[] keyBytes = new byte[keySize];
                fullBuf.get(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                // Skip past value bytes
                fullBuf.position(fullBuf.position() + valSize);

                long valOffset = headerStart + Record.HEADER_SIZE + keySize;
                int  recordSize = Record.HEADER_SIZE + keySize + valSize;

                entries.add(new ScannedEntry(key, valSize, valOffset, timestamp));
                currentOffset += recordSize;
            }

            return entries;
        } finally {
            lock.readLock().unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public int getId() {
        return id;
    }

    public long size() {
        lock.readLock().lock();
        try {
            return writeOffset;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            channel.close();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
