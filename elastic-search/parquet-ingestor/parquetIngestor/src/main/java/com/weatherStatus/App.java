package com.weatherStatus;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        String elasticsearchHost = System.getenv().getOrDefault("ELASTICSEARCH_HOST", "http://localhost:9200");
        System.out.println("[App] Initializing Elasticsearch client connected to: " + elasticsearchHost);

        RestClient restClient = RestClient.builder(
                HttpHost.create(elasticsearchHost))
                .build();

        // Jackson mapper (for JSON serialization)
        RestClientTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // Create the API Client
        ElasticsearchClient client = new ElasticsearchClient(transport);

        // Initialize and start Ingestor
        String dataDir = System.getenv().getOrDefault("SHARED_STORAGE_PATH", "/app/data");
        System.out.println("[App] Initializing Ingestor with data directory: " + dataDir);
        Ingestor ingestor = new Ingestor(client, dataDir);

        System.out.println("[App] Initializing ScheduledExecuter...");
        ScheduledExecuter scheduler = new ScheduledExecuter(ingestor, dataDir);
        
        System.out.println("[App] Starting ScheduledExecuter...");
        scheduler.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("[App] Shutting down cleanly...");
                scheduler.getScheduler().shutdown();
                restClient.close();
                System.out.println("[App] Shutdown complete.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

    }
}
