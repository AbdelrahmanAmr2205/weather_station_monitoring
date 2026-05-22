# Stage 1: Build the JAR
FROM maven:4.0.0-rc-5-amazoncorretto-25 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: Runtime environment
FROM amazoncorretto:25
WORKDIR /app
# Copy the built JAR from the previous stage
COPY --from=build /app/target/*.jar parquet-ingestor.jar

# Run the application
ENTRYPOINT ["java", "-jar", "parquet-ingestor.jar"]