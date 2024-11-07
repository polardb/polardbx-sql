FROM pxc-registry.cn-shanghai.cr.aliyuncs.com/base/maven:3.5-jdk-8 AS build

WORKDIR /root/workspace

ADD . /root/workspace/

RUN mkdir -p polardbx-transfer/src/main/java/com/alibaba/polardbx/transfer &&  \
    # dependencies
    find . -mindepth 1 -maxdepth 1 -type d ! -name "polardbx-transfer" -exec cp -r {} ./polardbx-transfer/src/main/java/com/alibaba/polardbx/transfer \; && \
    # main class
    cp Runner.java polardbx-transfer/src/main/java/com/alibaba/polardbx/transfer/ && \
    mkdir -p /root/.m2 && \
    mv docker/settings.xml /root/.m2/settings.xml && \
    mv docker/pom.xml polardbx-transfer/pom.xml && \
    cd polardbx-transfer && \
    mvn clean package -DskipTests

FROM pxc-registry.cn-shanghai.cr.aliyuncs.com/base/openjdk:8-jre-alpine
COPY --from=build /root/workspace/polardbx-transfer/target/transfer-test-1.0.0-jar-with-dependencies.jar transfer-test.jar
COPY --from=build /root/workspace/docker/logback.xml logback.xml
COPY --from=build /root/workspace/docker/config.toml config.toml
ENV TZ Asia/Shanghai
ENTRYPOINT ["java", "-Dlogback.configurationFile=logback.xml", "-jar", "transfer-test.jar"]
