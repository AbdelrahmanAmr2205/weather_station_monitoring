package com.weatherStatus;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ScheduledExecuter {
    private Ingestor ingestor;
    private ScheduledExecutorService scheduler;
    private String dataDir;

    public ScheduledExecuter(Ingestor ingestor, String dataDir) {
        this.ingestor = ingestor;
        this.dataDir = dataDir;
        this.scheduler = Executors.newScheduledThreadPool(1);
    }

    public void start() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                ingestor.readParquetFilesFromDirectory();
                ingestor.indexParquetFiles();
                ingestor.clearParquetFiles();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 10, TimeUnit.SECONDS);
    }

    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }
}
