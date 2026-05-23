package com.bitcask.datafile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Represents the complete structural data layout exactly as it exists on disk.
 *
 * Binary format (little-endian, compatible with the Go implementation):
 * <pre>
 *   [timestamp : 4 bytes uint32]
 *   [keySize   : 4 bytes uint32]
 *   [valSize   : 4 bytes uint32]
 *   [key       : keySize bytes ]
 *   [value     : valSize bytes ]
 * </pre>
 *
 * Mirrors Go's {@code datafile.Record}.
 */
public final class Record {

    /** Fixed header size in bytes: 3 × 4-byte fields. */
    public static final int HEADER_SIZE = 12;

    private final int timestamp;
    private final byte[] key;
    private final byte[] val;

    private Record(int timestamp, byte[] key, byte[] val) {
        this.timestamp = timestamp;
        this.key = key;
        this.val = val;
    }

    public static Record of(String key, byte[] val, int timestamp) {
        return new Record(timestamp, key.getBytes(java.nio.charset.StandardCharsets.UTF_8), val);
    }

    public byte[] getVal() {
        return val;
    }

    public int totalSize() {
        return HEADER_SIZE + key.length + val.length;
    }

    /**
     * Encodes the record into a byte array using little-endian byte order,
     * matching the Go {@code binary.LittleEndian} encoding exactly.
     */
    public byte[] encode() {
        ByteBuffer buf = ByteBuffer.allocate(totalSize()).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(timestamp);
        buf.putInt(key.length);
        buf.putInt(val.length);
        buf.put(key);
        buf.put(val);
        return buf.array();
    }

    /**
     * Decodes the fixed 12-byte header from the given stream.
     * Returns {@code null} on clean EOF (no bytes available).
     *
     * @throws IOException on partial read or I/O error
     */
    public static RecordHeader decodeHeader(InputStream in) throws IOException {
        byte[] buf = new byte[HEADER_SIZE];
        int firstByte = in.read();
        if (firstByte == -1) {
            return null; // clean EOF
        }
        buf[0] = (byte) firstByte;
        int read = in.read(buf, 1, HEADER_SIZE - 1);
        if (read != HEADER_SIZE - 1) {
            throw new IOException("Corrupted header: expected " + (HEADER_SIZE - 1) + " bytes, got " + read);
        }
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        int timestamp = bb.getInt();
        int keySize   = bb.getInt();
        int valSize   = bb.getInt();
        return new RecordHeader(timestamp, keySize, valSize);
    }

    /** Immutable value object for decoded record headers. */
    public record RecordHeader(int timestamp, int keySize, int valSize) {}
}
