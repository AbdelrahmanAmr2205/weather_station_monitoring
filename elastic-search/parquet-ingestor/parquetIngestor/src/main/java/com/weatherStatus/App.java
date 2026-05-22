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

        RestClient restClient = RestClient.builder(
                HttpHost.create(System.getenv().getOrDefault("ELASTICSEARCH_HOST", "http://localhost:9200")))
                .build();

        // Jackson mapper (for JSON serialization)
        RestClientTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // Create the API Client
        ElasticsearchClient client = new ElasticsearchClient(transport);

        // Initialize and start Ingestor
        Ingestor ingestor = new Ingestor(client, "/app/data");

        ScheduledExecuter scheduler = new ScheduledExecuter(ingestor, "/app/data");
        scheduler.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("Shutting down cleanly...");
                scheduler.getScheduler().shutdown();
                restClient.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

    }
}
