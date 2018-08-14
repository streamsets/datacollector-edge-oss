ARG base=alpine
FROM openjdk:8 AS builder-base
WORKDIR /usr/src/datacollector-edge
RUN git init
COPY ./gradle ./gradle
COPY ./gradlew ./build.gradle ./gradle.properties ./
RUN ./gradlew --no-daemon init
ENV CGO_ENABLED=0
ONBUILD ARG platform=LinuxAmd64
ONBUILD COPY . .
ONBUILD RUN ./gradlew --no-daemon -Prelease "install${platform}"

FROM builder-base as builder

FROM ${base}
RUN apk --no-cache add libc6-compat
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/src/datacollector-edge/dist /opt/datacollector-edge/

EXPOSE 18633
ENTRYPOINT ["/opt/datacollector-edge/bin/edge"]

# Metadata
LABEL org.label-schema.vendor="StreamSets" \
  org.label-schema.url="https://streamsets.com" \
  org.label-schema.name="Data Collector Edge" \
  org.label-schema.version="${version}" \
  org.label-schema.docker.schema-version="1.0"
