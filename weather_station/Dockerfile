# Use a standard OpenJDK image
FROM openjdk:11-jre-slim

WORKDIR /app

# Copy the compiled fat JAR from the target directory
COPY target/weather-monitoring-1.0-SNAPSHOT-jar-with-dependencies.jar /app/weather-station.jar

# Run the Weather Station Mock
CMD ["java", "-jar", "weather-station.jar"]