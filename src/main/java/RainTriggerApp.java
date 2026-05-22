import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch; // Added this import

public class RainTriggerApp {
    private static final String INPUT_TOPIC = "weather_statuses";
    private static final String OUTPUT_TOPIC = "rain_alerts";
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_BROKER", "127.0.0.1:9092");

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-trigger-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceStream = builder.stream(INPUT_TOPIC);
        ObjectMapper mapper = new ObjectMapper();

        sourceStream.filter((key, value) -> {
            try {
                JsonNode rootNode = mapper.readTree(value);
                int humidity = rootNode.path("weather").path("humidity").asInt();
                return humidity > 70;
            } catch (Exception e) {
                return false;
            }
        }).to(OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        
        // Use a latch to keep the main thread alive
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach a shutdown hook to cleanly close the streams and release the latch
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await(); // This locks the app here, keeping it running!
        } catch (InterruptedException e) {
            System.exit(1);
        }
    }
}