FROM openjdk:8-alpine

RUN set -ex && \
    apk upgrade --no-cache && \
    apk add --no-cache bash tini libc6-compat && \
    mkdir -p /opt/spark-demo && \
    touch /opt/spark-demo/RELEASE && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd

COPY target/lib /opt/spark-demo/lib
COPY target/spark-demo-*.jar /opt/spark-demo/lib/
COPY entrypoint.sh /opt/

ENV APP_HOME=/opt/spark-demo

STOPSIGNAL SIGTERM

EXPOSE 4040

WORKDIR /opt/spark-demo/

ENTRYPOINT [ "/opt/entrypoint.sh" ]