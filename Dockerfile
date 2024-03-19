FROM eclipse-temurin:17-jre-alpine
WORKDIR /
ADD ./target/MessagingService.jar MessagingService.jar
ENTRYPOINT ["java", "-Dprocess.name=MessagingService", "-jar", "MessagingService.jar"]
