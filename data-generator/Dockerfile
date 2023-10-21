###############################################################################
# Build Click Count Job
###############################################################################

FROM maven:3.8-eclipse-temurin-17 AS builder

# Get data-generator and compile it
COPY ./src /opt/data-generator/src/
COPY ./pom.xml /opt/data-generator/
WORKDIR /opt/data-generator
RUN echo $(ls)
RUN mvn clean verify


###############################################################################
# Build Data Genrator Image
###############################################################################

FROM openjdk:11-jre-slim

WORKDIR /app
# Copy Data Generator Job
COPY --from=builder /opt/data-generator/target/data-generator-*.jar /app/data-generator.jar
#Run Data Generator Job
COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]