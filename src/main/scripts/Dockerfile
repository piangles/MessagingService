FROM java:8
WORKDIR /
ADD MessagingService.jar MessagingService.jar
ENTRYPOINT ["java", "-Dprocess.name=MessagingService", "-jar", "MessagingService.jar"]
