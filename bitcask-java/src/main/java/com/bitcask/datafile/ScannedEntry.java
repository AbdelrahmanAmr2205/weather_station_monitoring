package com.bitcask.datafile;

/**
 * DTO carrying the metadata scanned from a single record in a .data or .hint file.
 * Mirrors Go's {@code datafile.ScannedEntry}.
 */
public record ScannedEntry(
        String key,
        int valSize,
        long valOffset,
        int timestamp
) {}
