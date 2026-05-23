package com.bitcask.datafile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Represents a single entry in a .hint file.
 *
 * Binary format (little-endian, compatible with the Go implementation):
 * <pre>
 *   [timestamp : 4 bytes uint32]
 *   [keySize   : 4 bytes uint32]
 *   [valSize   : 4 bytes uint32]
 *   [valOffset : 8 bytes int64 ]
 *   [key       : keySize bytes ]
 * </pre>
 *
 * Mirrors Go's {@code datafile.HintRecord}.
 */
public final class HintRecord {

    /** Fixed hint header size: 3 × 4-byte + 1 × 8-byte fields. */
    public static final int HINT_HEADER_SIZE = 20;

    private final int timestamp;
    private final int valSize;
    private final long valOffset;
    private final byte[] key;

    private HintRecord(int timestamp, int valSize, long valOffset, byte[] key) {
        this.timestamp = timestamp;
        this.valSize   = valSize;
        this.valOffset = valOffset;
        this.key       = key;
    }

    public static HintRecord of(int timestamp, int valSize, long valOffset, String key) {
        return new HintRecord(timestamp, valSize, valOffset,
                key.getBytes(StandardCharsets.UTF_8));
    }

    public int totalSize() {
        return HINT_HEADER_SIZE + key.length;
    }

    /**
     * Encodes the hint record into a byte array using little-endian byte order.
     */
    public byte[] encode() {
        ByteBuffer buf = ByteBuffer.allocate(totalSize()).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(timestamp);
        buf.putInt(key.length);
        buf.putInt(valSize);
        buf.putLong(valOffset);
        buf.put(key);
        return buf.array();
    }

    /**
     * Decodes the fixed 20-byte hint header from the given stream.
     * Returns {@code null} on clean EOF.
     *
     * @throws IOException on partial read or I/O error
     */
    public static HintHeader decodeHintHeader(InputStream in) throws IOException {
        byte[] buf = new byte[HINT_HEADER_SIZE];
        int firstByte = in.read();
        if (firstByte == -1) {
            return null; // clean EOF
        }
        buf[0] = (byte) firstByte;
        int read = in.read(buf, 1, HINT_HEADER_SIZE - 1);
        if (read != HINT_HEADER_SIZE - 1) {
            throw new IOException("Corrupted hint header: expected " + (HINT_HEADER_SIZE - 1) + " bytes, got " + read);
        }
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        int  timestamp = bb.getInt();
        int  keySize   = bb.getInt();
        int  valSize   = bb.getInt();
        long valOffset = bb.getLong();
        return new HintHeader(timestamp, keySize, valSize, valOffset);
    }

    /** Immutable value object for decoded hint headers. */
    public record HintHeader(int timestamp, int keySize, int valSize, long valOffset) {}
}
