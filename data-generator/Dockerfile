###############################################################################
# Build Click Count Job
###############################################################################

FROM maven:3.8-eclipse-temurin-17 AS builder

# Get data-generator and compile it
COPY ./* /opt/index-calc/data-generator/
WORKDIR /opt/index-calc/data-generator
RUN echo $(ls -alt)
RUN mvn clean verify


###############################################################################
# Build Data Genrator Image
###############################################################################

FROM openjdk:11-jre-slim

WORKDIR /app
# Copy Click Count Job
COPY --from=builder /opt/index-calc/data-generator/target/index-calc-*.jar /app/data-generator.jar