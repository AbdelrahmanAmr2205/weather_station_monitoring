package com.bitcask.datafile;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility for reading {@code .hint} files produced during compaction.
 *
 * Hint files contain lightweight metadata records (no value bytes), enabling
 * fast {@code keyDir} reconstruction on startup without scanning full data files.
 *
 * Mirrors Go's {@code datafile.ScanHintFile}.
 */
public final class HintFile {

    private HintFile() {}

    /**
     * Scans all hint records in the file at {@code path} and returns
     * a list of {@link ScannedEntry} objects.
     *
     * @param path absolute path to a {@code .hint} file
     * @return list of scanned entries in file order
     * @throws IOException on read failure
     */
    public static List<ScannedEntry> scan(Path path) throws IOException {
        List<ScannedEntry> entries = new ArrayList<>();

        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(path.toFile()))) {
            while (true) {
                HintRecord.HintHeader header = HintRecord.decodeHintHeader(in);
                if (header == null) {
                    break; // clean EOF
                }

                byte[] keyBuf = in.readNBytes(header.keySize());
                if (keyBuf.length != header.keySize()) {
                    throw new IOException("Corrupted hint file: expected " + header.keySize()
                            + " key bytes, got " + keyBuf.length);
                }
                String key = new String(keyBuf, StandardCharsets.UTF_8);

                entries.add(new ScannedEntry(key, header.valSize(), header.valOffset(), header.timestamp()));
            }
        }

        return entries;
    }
}
