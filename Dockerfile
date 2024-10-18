FROM adoptopenjdk:11
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} load-service.jar
EXPOSE ${SLA_LOAD_SERVICE_PORT}
ENTRYPOINT [ "java", "-jar", "load-service.jar" ]