# Stage 1: Build the JAR
FROM maven:3.9-amazoncorretto-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: Runtime environment
FROM amazoncorretto:17
WORKDIR /app
# Copy the built fat JAR from the previous stage
COPY --from=build /app/target/*-jar-with-dependencies.jar parquet-ingestor.jar

# Run the application
ENTRYPOINT ["java", "-jar", "parquet-ingestor.jar"]