FROM frolvlad/alpine-oraclejdk8:slim
VOLUME /tmp
ADD qqlei-dataaggr-service-0.0.1-SNAPSHOT.jar app.jar
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app.jar"]
EXPOSE 8762