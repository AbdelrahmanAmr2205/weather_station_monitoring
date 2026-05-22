# Weather Station Data Acquisition Service

This microservice handles the Data Acquisition stage of the system. It simulates physical weather sensors, processes real-time rain alerts, and integrates with the external Open-Meteo API.

## Components
1. **WeatherStationMock:** Generates weather payloads every 1 second. Enforces 30/40/30 battery distribution and a 10% message drop rate.
2. **RainTriggerApp:** A Kafka Streams processor that listens to `weather_statuses` and forwards payloads with >70% humidity to the `rain_alerts` topic.
3. **OpenMeteoChannelAdapter:** Fetches real-time weather data for Alexandria, Egypt, formats it to our system schema, and pushes it to Kafka.

## How to Build
Navigate to this folder and compile the fat JAR:
\`\`\`bash
mvn clean package
\`\`\`

## How to Run Locally for Testing
Make sure the Kafka cluster (from the root `docker-compose.yml`) is running first. Open separate terminal windows for each app:

**1. Start the Rain Trigger (Run this first):**
\`\`\`bash
java -cp target/weather-monitoring-1.0-SNAPSHOT-jar-with-dependencies.jar RainTriggerApp
\`\`\`

**2. Start a Weather Station (Sensor 1):**
\`\`\`bash
export STATION_ID=1  # Use $env:STATION_ID="1" if on Windows PowerShell
java -cp target/weather-monitoring-1.0-SNAPSHOT-jar-with-dependencies.jar WeatherStationMock
\`\`\`

**3. Start the API Adapter (Optional Bonus):**
\`\`\`bash
java -cp target/weather-monitoring-1.0-SNAPSHOT-jar-with-dependencies.jar OpenMeteoChannelAdapter
\`\`\`