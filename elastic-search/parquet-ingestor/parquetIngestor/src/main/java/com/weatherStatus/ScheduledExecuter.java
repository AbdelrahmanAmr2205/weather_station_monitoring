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
        System.out.println("[ScheduledExecuter] Scheduler started. Checking for files every 10 seconds.");
        scheduler.scheduleAtFixedRate(() -> {
            try {
                System.out.println("[ScheduledExecuter] Starting periodic ingestion run...");
                ingestor.readParquetFilesFromDirectory();
                ingestor.indexParquetFiles();
                ingestor.clearParquetFiles();
                System.out.println("[ScheduledExecuter] Ingestion run finished.");
            } catch (Exception e) {
                System.err.println("[ScheduledExecuter] Error occurred during periodic run:");
                e.printStackTrace();
            }
        }, 0, 10, TimeUnit.SECONDS);
    }

    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }
}
