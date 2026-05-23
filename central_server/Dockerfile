# Stage 1: Build the JAR
FROM maven:3.9-amazoncorretto-21 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: Runtime environment
FROM amazoncorretto:21
WORKDIR /app
# Copy the built JAR from the previous stage
COPY --from=build /app/target/*.jar central_server.jar

# Run the application
ENTRYPOINT ["java", "-jar", "central_server.jar"]
