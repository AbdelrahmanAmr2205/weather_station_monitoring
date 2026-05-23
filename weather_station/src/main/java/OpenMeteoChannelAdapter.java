import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Properties;

public class OpenMeteoChannelAdapter {
    private static final String TOPIC = "weather_statuses";
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_BROKER", "127.0.0.1:9092");
    
    // Open-Meteo API URL (Pulling current weather for Alexandria, Egypt - returned in Fahrenheit)
    private static final String API_URL = "https://api.open-meteo.com/v1/forecast?latitude=31.2001&longitude=29.9187&current=temperature_2m,relative_humidity_2m,wind_speed_10m&temperature_unit=fahrenheit";
    
    // We will assign a dedicated "Station ID" for the external API so the Central Station can index it separately
    private static final long EXTERNAL_STATION_ID = 99;

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        
        long sNo = 0;

        System.out.println("Starting Open-Meteo Channel Adapter...");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            while (true) {
                // 1. Fetch Data from External System (Open-Meteo)
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(API_URL))
                        .GET()
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                
                if (response.statusCode() == 200) {
                    sNo++;
                    
                    // 2. Parse the external JSON
                    JsonNode rootNode = mapper.readTree(response.body());
                    JsonNode current = rootNode.path("current");
                    
                    int tempF = current.path("temperature_2m").asInt();
                    int humidity = current.path("relative_humidity_2m").asInt();
                    int windSpeed = current.path("wind_speed_10m").asInt();

                    // 3. Transform / Adapt into our System's Schema
                    ObjectNode weatherNode = mapper.createObjectNode();
                    weatherNode.put("humidity", humidity);
                    weatherNode.put("temperature", tempF);
                    weatherNode.put("wind_speed", windSpeed);

                    ObjectNode messageNode = mapper.createObjectNode();
                    messageNode.put("station_id", EXTERNAL_STATION_ID);
                    messageNode.put("s_no", sNo);
                    messageNode.put("battery_status", "high"); // Virtual API never runs out of battery
                    messageNode.put("status_timestamp", System.currentTimeMillis() / 1000L);
                    messageNode.set("weather", weatherNode);

                    String jsonPayload = mapper.writeValueAsString(messageNode);
                    
                    // 4. Publish to Messaging System (Kafka)
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, String.valueOf(EXTERNAL_STATION_ID), jsonPayload);
                    producer.send(record);
                    System.out.println("Adapter Published: " + jsonPayload);
                } else {
                    System.err.println("Failed to fetch API data. HTTP Code: " + response.statusCode());
                }

                // Poll every 5 seconds (to avoid hitting rate limits on the free tier)
                Thread.sleep(5000); 
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}