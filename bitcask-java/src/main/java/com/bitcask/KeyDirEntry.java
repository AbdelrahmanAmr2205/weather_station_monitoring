package com.bitcask;

/**
 * In-memory index entry for a single key.
 * Mirrors Go's {@code bitcask.keyDirEntry} struct.
 */
public final class KeyDirEntry {

    public final int  valSize;
    public final long valOffset;
    public final int  fileId;
    public final int  timestamp;

    public KeyDirEntry(int valSize, long valOffset, int fileId, int timestamp) {
        this.valSize   = valSize;
        this.valOffset = valOffset;
        this.fileId    = fileId;
        this.timestamp = timestamp;
    }
}
