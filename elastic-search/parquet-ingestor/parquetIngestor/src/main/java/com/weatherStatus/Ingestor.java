package com.weatherStatus;

import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import co.elastic.clients.elasticsearch.ElasticsearchClient;

public class Ingestor {
    private final BulkIngester<Void> ingester;
    private List<File> parquetFiles;
    private Path sharedDir;
    private Path unprocessedDir;
    private Path processedDir;
    private Path failedDir;

    public Ingestor(ElasticsearchClient client, String sharedDirString) {
        System.out.println("[Ingestor] Constructing Ingestor instance...");
        initializeDirectories(sharedDirString);
        this.parquetFiles = new ArrayList<>();
        this.ingester = new BulkIngester.Builder<Void>()
                .client(client)
                .maxOperations(100)
                .flushInterval(5, TimeUnit.SECONDS)
                .build();
        System.out.println("[Ingestor] BulkIngester successfully initialized.");
    }

    public void readParquetFilesFromDirectory() { // dir is /app/data
        System.out.println("[Ingestor] Scanning for Parquet files in: " + unprocessedDir.toString());
        File[] files = new File(unprocessedDir.toString()).listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile() && file.getName().endsWith(".parquet")) {
                    System.out.println("[Ingestor] Found target file: " + file.getName());
                    parquetFiles.add(file);
                }
            }
        }
        System.out.println("[Ingestor] Scan complete. Total files enqueued for ingestion: " + parquetFiles.size());
    }

    public void indexParquetFiles() {
        if (parquetFiles.isEmpty()) {
            System.out.println("[Ingestor] No Parquet files found to index.");
            return;
        }
        System.out.println("[Ingestor] Starting indexing of " + parquetFiles.size() + " files...");
        Configuration conf = new Configuration();
        for (File file : parquetFiles) {
            System.out.println("[Ingestor] Processing file: " + file.getAbsolutePath());
            boolean success = true;
            try (
                    ParquetReader<GenericRecord> reader = AvroParquetReader
                            .<GenericRecord>builder(HadoopInputFile.fromPath(new Path(file.getAbsolutePath()), conf))
                            .build()) {
                GenericRecord record;
                int recordCount = 0;
                while ((record = reader.read()) != null) {
                    final GenericRecord currentRecord = record;
                    String docId = ((Number) currentRecord.get("station_id")).longValue() + "_" + ((Number) currentRecord.get("s_no")).longValue();

                    Map<String, Object> docMap = mapAvroRecordToMap(currentRecord);
                    // Handle the nested weather record
                    GenericRecord weatherRecord = (GenericRecord) currentRecord.get("weather");

                    Map<String, Object> weatherMap = mapWeatherAvroRecordToMap(weatherRecord);
                    docMap.put("weather", weatherMap);

                    ingester.add(op -> op.create(c -> c
                            .index("weather_status")
                            .id(docId)
                            .document(docMap))); // Pass the standard Map, not the Avro Record
                    recordCount++;
                }
                System.out.println("[Ingestor] Enqueued " + recordCount + " records from " + file.getName() + " to BulkIngester.");
                System.out.println("[Ingestor] Flushing BulkIngester to Elasticsearch...");
                ingester.flush();
                System.out.println("[Ingestor] Flush completed.");
            } catch (Exception e) {
                System.err.println("[Ingestor] Error processing file " + file.getName() + ": " + e.getMessage());
                e.printStackTrace();
                success = false;
                moveFailedFile(file);
            } finally {
                if (success) {
                    moveProcessedFile(file);
                }
            }
        }
    }

    private void initializeDirectories(String sharedDirString) {
        System.out.println("[Ingestor] Initializing directory layout under base path: " + sharedDirString);
        this.sharedDir = new Path(sharedDirString);
        this.unprocessedDir = new Path(this.sharedDir, "unprocessed");
        this.processedDir = new Path(this.sharedDir, "processed");
        this.failedDir = new Path(this.sharedDir, "failed");
        if (!new File(unprocessedDir.toString()).exists()) {
            System.out.println("[Ingestor] Creating directory: " + unprocessedDir.toString());
            new File(unprocessedDir.toString()).mkdirs();
        }
        if (!new File(processedDir.toString()).exists()) {
            System.out.println("[Ingestor] Creating directory: " + processedDir.toString());
            new File(processedDir.toString()).mkdirs();
        }
        if (!new File(failedDir.toString()).exists()) {
            System.out.println("[Ingestor] Creating directory: " + failedDir.toString());
            new File(failedDir.toString()).mkdirs();
        }
        System.out.println("[Ingestor] Directory layout setup completed.");
    }

    private Map<String, Object> mapWeatherAvroRecordToMap(GenericRecord weatherRecord) {
        Map<String, Object> weatherMap = new HashMap<>();
        weatherMap.put("humidity", ((Number) weatherRecord.get("humidity")).longValue());
        weatherMap.put("temperature", ((Number) weatherRecord.get("temperature")).longValue());
        weatherMap.put("wind_speed", ((Number) weatherRecord.get("wind_speed")).longValue());
        return weatherMap;
    }

    private Map<String, Object> mapAvroRecordToMap(GenericRecord currentRecord) {
        Map<String, Object> map = new HashMap<>();
        map.put("station_id", ((Number) currentRecord.get("station_id")).longValue());
        map.put("s_no", ((Number) currentRecord.get("s_no")).longValue());
        map.put("battery_status", currentRecord.get("battery_status").toString());
        // Convert epoch seconds to epoch milliseconds for Elasticsearch date type
        long epochSeconds = ((Number) currentRecord.get("status_timestamp")).longValue();
        map.put("status_timestamp", epochSeconds * 1000L);
        return map;
    }

    private void moveProcessedFile(File file) {
        try {
            System.out.println("[Ingestor] Moving successfully processed file: " + file.getName() + " to 'processed' folder.");
            // Move the file
            java.nio.file.Files.move(
                    file.toPath(),
                    new File(this.processedDir.toString(), file.getName()).toPath(),
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            System.err.println("[Ingestor] Failed to move file " + file.getName() + " to processed directory: " + e.getMessage());
        }
    }

    private void moveFailedFile(File file) {
        try {
            System.out.println("[Ingestor] Moving failed file: " + file.getName() + " to 'failed' folder.");
            // Move the file
            java.nio.file.Files.move(
                    file.toPath(),
                    new File(this.failedDir.toString(), file.getName()).toPath(),
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            System.err.println("[Ingestor] Failed to move file " + file.getName() + " to failed directory: " + e.getMessage());
        }
    }

    public void clearParquetFiles() {
        System.out.println("[Ingestor] Clearing list of processed Parquet files from memory.");
        parquetFiles.clear();
    }
}
