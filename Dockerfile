FROM amazoncorretto:8
WORKDIR /
ADD ./target/MessagingService.jar MessagingService.jar
ENTRYPOINT ["java", "-Dprocess.name=MessagingService", "-jar", "MessagingService.jar"]
