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
        initializeDirectories(sharedDirString);
        this.parquetFiles = new ArrayList<>();
        this.ingester = new BulkIngester.Builder<Void>()
                .client(client)
                .maxOperations(100)
                .flushInterval(5, TimeUnit.SECONDS)
                .build();
    }

    public void readParquetFilesFromDirectory() { // dir is /app/data
        File[] files = new File(unprocessedDir.toString()).listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile() && file.getName().endsWith(".parquet")) {
                    parquetFiles.add(file);
                }
            }
        }
    }

    public void indexParquetFiles() {
        Configuration conf = new Configuration();
        for (File file : parquetFiles) {
            boolean success = true;
            try (
                    ParquetReader<GenericRecord> reader = AvroParquetReader
                            .<GenericRecord>builder(HadoopInputFile.fromPath(new Path(file.getAbsolutePath()), conf))
                            .build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    final GenericRecord currentRecord = record;
                    String docId = currentRecord.get("station_id") + "_" + currentRecord.get("s_no");

                    Map<String, Object> docMap = mapAvroRecordToMap(currentRecord);
                    // Handle the nested weather record
                    GenericRecord weatherRecord = (GenericRecord) currentRecord.get("weather");

                    Map<String, Object> weatherMap = mapWeatherAvroRecordToMap(weatherRecord);
                    docMap.put("weather", weatherMap);

                    ingester.add(op -> op.create(c -> c
                            .index("weather_status")
                            .id(docId)
                            .document(docMap))); // Pass the standard Map, not the Avro Record
                }
            } catch (Exception e) {
                System.err.println("Error processing file " + file.getName() + ": " + e.getMessage());
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
        this.sharedDir = new Path(sharedDirString);
        this.unprocessedDir = new Path(this.sharedDir, "unprocessed");
        this.processedDir = new Path(this.sharedDir, "processed");
        this.failedDir = new Path(this.sharedDir, "failed");
        if (!new File(unprocessedDir.toString()).exists()) {
            new File(unprocessedDir.toString()).mkdirs();
        }
        if (!new File(processedDir.toString()).exists()) {
            new File(processedDir.toString()).mkdirs();
        }
        if (!new File(failedDir.toString()).exists()) {
            new File(failedDir.toString()).mkdirs();
        }
    }

    private Map<String, Object> mapWeatherAvroRecordToMap(GenericRecord weatherRecord) {
        Map<String, Object> weatherMap = new HashMap<>();
        weatherMap.put("humidity", weatherRecord.get("humidity"));
        weatherMap.put("temperature", weatherRecord.get("temperature"));
        weatherMap.put("wind_speed", weatherRecord.get("wind_speed"));
        return weatherMap;
    }

    private Map<String, Object> mapAvroRecordToMap(GenericRecord currentRecord) {
        Map<String, Object> map = new HashMap<>();
        map.put("station_id", currentRecord.get("station_id"));
        map.put("s_no", currentRecord.get("s_no"));
        map.put("battery_status", currentRecord.get("battery_status").toString());
        map.put("status_timestamp", currentRecord.get("status_timestamp"));
        return map;
    }

    private void moveProcessedFile(File file) {
        try {

            // Move the file
            java.nio.file.Files.move(
                    file.toPath(),
                    new File(this.processedDir.toString(), file.getName()).toPath(),
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            System.err.println("Failed to move file " + file.getName() + " to processed directory: " + e.getMessage());
        }
    }

    private void moveFailedFile(File file) {
        try {
            // Move the file
            java.nio.file.Files.move(
                    file.toPath(),
                    new File(this.failedDir.toString(), file.getName()).toPath(),
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            System.err.println("Failed to move file " + file.getName() + " to failed directory: " + e.getMessage());
        }
    }

    public void clearParquetFiles() {
        parquetFiles.clear();
    }
}
