# StreamSets Data Collector Edge (SDC Edge)

## Minimum Requirements

* Go 1.9
* Gradle 4.2

## Requirements for Kafka Connector

* librdkafka

### Installing librdkafka

Kafka destination stage depends on librdkafka v0.11.0 or later, so you either need to install librdkafka through your OS/distributions package manager,
or download and build it from source.

- For Debian and Ubuntu based distros, install `librdkafka-dev` from the standard
repositories or using [Confluent's Deb repository](http://docs.confluent.io/current/installation.html#installation-apt).
- For Redhat based distros, install `librdkafka-devel` using [Confluent's YUM repository](http://docs.confluent.io/current/installation.html#rpm-packages-via-yum).
- For MacOS X, install `librdkafka` from Homebrew.
- For Windows, see the `librdkafka.redist` NuGet package.


## Clone Repository

* Create directory $GOPATH/src/github.com/streamsets
* Clone this repository in directory $GOPATH/src/github.com/streamsets
* Reference - https://golang.org/doc/code.html#Organization

## Building

    ./gradlew clean build

## Building for all platforms

    ./gradlew clean buildAll

## Publishing Binaries to Maven Repo for all platforms

    ./gradlew publish

## Building DockerImage

    ./gradlew buildDockerImage

## By default Kafka Connector is not included in build, to include Kafka Connector pass '-PincludeStage=kafka'

    ./gradlew clean build -PincludeStage=kafka

## Run tests

    ./gradlew test

## Run coverage

    ./gradlew coverage

## Run checks (test, fmt and vet)

    ./gradlew check

## Running

    cd dist
    bin/edge

### To start pipeline on SDC Edge start

    bin/edge -start=<pipelineId>

### To pass runtime parameters

    bin/edge -start=tailFileToHttp -runtimeParameters='{"filePath":"/tmp/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

### To enable DEBUG Log Level

    bin/edge -debug -start=tailFileToHttp

## REST API

    curl -X GET http://localhost:18633/rest/v1/pipeline/:pipelineId/status
    curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/start
    curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/stop
    curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/resetOffset
    curl -X GET http://localhost:18633/rest/v1/pipeline/:pipelineId/metrics

### To pass runtime parameters during start

    curl -X POST http://localhost:18633/rest/v1/pipeline/tailFileToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"filePath":"/tmp/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## Docker run the image

To run a container from the resulting image:

    docker run --publish 18633:18633 --name edge --rm streamsets/datacollector-edge:3.1.0.0-SNAPSHOT

Getting inside the container

    docker exec -it datacollector-edge /bin/sh

## Release

    ./gradlew release


## CPU & Heap Profile

    curl http://localhost:18633/debug/pprof/profile > cpu.pb.gz
    curl http://localhost:18633/debug/pprof/heap > heap.pb.gz
    curl http://localhost:18633/debug/pprof/goroutine > goroutine.pb.gz
    curl http://localhost:18633/debug/pprof/block > block.pb.gz
