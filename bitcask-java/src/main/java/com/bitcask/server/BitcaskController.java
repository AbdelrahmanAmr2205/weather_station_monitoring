package com.bitcask.server;

import com.bitcask.BitcaskDB;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * REST controller exposing the Bitcask storage engine over HTTP.
 *
 * <ul>
 *   <li>{@code GET  /}        — list all keys with their latest values (JSON object)</li>
 *   <li>{@code GET  /{key}}   — retrieve the JSON value for a single key</li>
 *   <li>{@code POST /{key}}   — store a JSON value for a key</li>
 * </ul>
 *
 * Mirrors Go's HTTP handler in {@code cmd/bitcask-server/main.go},
 * with the addition of the {@code GET /} endpoint required by the client spec.
 */
@RestController
public class BitcaskController {

    private final BitcaskDB db;
    private final ObjectMapper mapper;

    public BitcaskController(BitcaskDB db, ObjectMapper mapper) {
        this.db     = db;
        this.mapper = mapper;
    }

    // ── GET / — list all keys with their values ───────────────────────────────

    /**
     * Returns a JSON object mapping every stored key to its current value.
     * Used by the {@code --view-all} and {@code --perf} client modes.
     */
    @GetMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getAll() {
        Set<String> keys = db.keys();
        Map<String, Object> result = new LinkedHashMap<>();

        for (String key : keys) {
            try {
                byte[] raw = db.get(key);
                // Parse stored raw bytes back to a JSON node so the outer map
                // serialises correctly (avoids double-encoding the value string).
                result.put(key, mapper.readTree(raw));
            } catch (IOException e) {
                // Key may have been deleted / rotated between listing and reading — skip it
                result.put(key, null);
            }
        }

        try {
            return ResponseEntity.ok()
                                 .contentType(MediaType.APPLICATION_JSON)
                                 .body(mapper.writeValueAsString(result));
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // ── GET /{key} ────────────────────────────────────────────────────────────

    @GetMapping(value = "/{key}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<byte[]> get(@PathVariable String key) {
        try {
            byte[] val = db.get(key);
            return ResponseEntity.ok()
                                 .contentType(MediaType.APPLICATION_JSON)
                                 .body(val);
        } catch (IOException e) {
            if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
            }
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // ── POST /{key} ───────────────────────────────────────────────────────────

    @PostMapping(value = "/{key}", consumes = MediaType.APPLICATION_JSON_VALUE,
                                   produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> put(@PathVariable String key,
                                       @RequestBody byte[] body) {
        // Validate that the body is well-formed JSON
        try {
            mapper.readTree(body);
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                                 .body("Invalid JSON payload");
        }

        try {
            db.put(key, body);
            return ResponseEntity.status(HttpStatus.CREATED)
                                 .body("{\"status\":\"success\"}");
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                                 .body("Failed to save key: " + e.getMessage());
        }
    }
}
