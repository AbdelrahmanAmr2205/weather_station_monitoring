package com.weatherStatus.central_server.services;

import com.weatherStatus.central_server.model.StationStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.net.URI;
import java.util.List;

/**
 * Writes a batch of {@link StationStatus} records to a Parquet file on the local filesystem.
 *
 * <p>Output layout (Hive-style, analytics-ready):
 * <pre>
 *   {parquet.output.dir}/
 *     station_id={N}/
 *       date={YYYY-MM-DD}/
 *         {epoch_ms}_{stationId}.parquet
 * </pre>
 */
@Slf4j
@Service
public class ParquetArchiveService {

    // ---------------------------------------------------------------------------
    // Parquet schema — must stay in sync with the StationStatus domain model.
    // ---------------------------------------------------------------------------
    static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
        "message StationStatus {\n"
        + "  required int64  station_id;\n"
        + "  required int64  s_no;\n"
        + "  required binary battery_status (UTF8);\n"
        + "  required int64  status_timestamp;\n"
        + "  required group weather {\n"
        + "    required int32 humidity;\n"
        + "    required int32 temperature;\n"
        + "    required int32 wind_speed;\n"
        + "  }\n"
        + "}"
    );

    @Value("${parquet.output.dir:data/parquet}")
    private String outputDir;

    /**
     * Persists {@code records} to a single Parquet file under the partition path
     * {@code station_id=stationId/date=date/}.
     *
     * <p>The caller (BatchBufferService) guarantees that all records in the list share the
     * same stationId and date — they were grouped before this call.
     *
     * @throws Exception propagated to the caller for retry / buffer-requeue logic.
     */
    public void write(List<StationStatus> records, long stationId, String date)
            throws Exception {

        if (records.isEmpty()) {
            return;
        }

        // Build partition directory path and ensure it exists.
        String partitionDir = String.format(
                "%s/station_id=%d/date=%s",
                outputDir, stationId, date);

        File dir = new File(partitionDir);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalStateException("Could not create partition directory: " + partitionDir);
        }

        // File name: <epoch_ms>_<stationId>.parquet — ts prefix ensures natural sort order.
        String fileName = String.format("%s/%d_%d.parquet",
                partitionDir, System.currentTimeMillis(), stationId);

        log.info("Writing {} records → {}", records.size(), fileName);

        // Use file:// URI so Hadoop's LocalFileSystem is selected unambiguously.
        URI fileUri = new File(fileName).toURI();
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(fileUri);

        Configuration hadoopConf = new Configuration();
        // Suppress Parquet summary-metadata files (_metadata, _common_metadata) for now.
        hadoopConf.setBoolean("parquet.enable.summary-metadata", false);

        SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);

        try (ParquetWriter<org.apache.parquet.example.data.Group> writer =
                ExampleParquetWriter.builder(hadoopPath)
                        .withType(SCHEMA)
                        .withConf(hadoopConf)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withValidation(false)
                        .build()) {

            for (StationStatus record : records) {
                SimpleGroup group = (SimpleGroup) factory.newGroup();
                group.add("station_id",       record.getStationId());
                group.add("s_no",             record.getSequenceNumber());
                group.add("battery_status",   safeStr(record.getBatteryStatus()));
                group.add("status_timestamp", record.getStatusTimestamp());

                group.addGroup("weather")
                     .append("humidity",    record.getWeather().getHumidity())
                     .append("temperature", record.getWeather().getTemperature())
                     .append("wind_speed",  record.getWeather().getWindSpeed());

                writer.write(group);
            }
        }

        log.info("Parquet write complete: {}", fileName);
    }

    private static String safeStr(String s) {
        return s != null ? s : "";
    }
}
