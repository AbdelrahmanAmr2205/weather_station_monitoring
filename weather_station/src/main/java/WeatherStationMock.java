import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class WeatherStationMock {
    private static final String TOPIC = "weather_statuses";
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_BROKER", "127.0.0.1:9092");

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();
        
        // Use an environment variable for the station ID so you can easily spin up 10 instances later
        long stationId = Long.parseLong(System.getenv().getOrDefault("STATION_ID", "1"));
        long sNo = 0;

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            while (true) {
                sNo++; // Increment with each intended message 
                
                // 1. Simulate 10% Drop Rate 
                if (random.nextInt(100) < 10) {
                    System.out.println("Station " + stationId + ": Message s_no " + sNo + " dropped.");
                    Thread.sleep(1000); // 
                    continue;
                }

                // 2. Battery Status Distribution (30% low, 40% medium, 30% high) 
                int batteryRoll = random.nextInt(100);
                String batteryStatus = (batteryRoll < 30) ? "low" : (batteryRoll < 70) ? "medium" : "high";

                // 3. Construct JSON Payload
                ObjectNode weatherNode = mapper.createObjectNode();
                weatherNode.put("humidity", random.nextInt(101)); 
                weatherNode.put("temperature", random.nextInt(120)); 
                weatherNode.put("wind_speed", random.nextInt(150)); 

                ObjectNode messageNode = mapper.createObjectNode();
                messageNode.put("station_id", stationId);
                messageNode.put("s_no", sNo);
                messageNode.put("battery_status", batteryStatus);
                messageNode.put("status_timestamp", System.currentTimeMillis() / 1000L);
                messageNode.set("weather", weatherNode);

                String jsonPayload = mapper.writeValueAsString(messageNode);
                
                // 4. Send to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, String.valueOf(stationId), jsonPayload);
                producer.send(record);
                System.out.println("Sent: " + jsonPayload);

                Thread.sleep(1000); // 1 message per second 
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}