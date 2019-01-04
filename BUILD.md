# StreamSets Data Collector Edge (SDC Edge)

## Minimum Requirements

* Java >= 8u101 (for supporting Let's Encrypt SSL Certificates as used by gopkg.in)

## Clone Repository

* Create directory $GOPATH/src/github.com/streamsets
* Clone this repository in directory $GOPATH/src/github.com/streamsets
* Reference - https://golang.org/doc/code.html#Organization

## Building a distribution archive for a specific platform

    ./gradlew <platform>DistTar | <platform>DistZip

Where platform is one of:

* darwinAmd64
* linuxAmd64
* linuxArm
* windowsAmd64

The zip variant is used for Windows targets, and tar for all other targets.

e.g. `./gradlew darwinAmd64DistTar`

## Building distribution archives for all platforms

    ./gradlew clean dist

## Install an unarchived distribution into the dist folder

    ./gradlew install<platform>

Where platform is one of:

* DarwinAmd64
* LinuxAmd64
* LinuxArm
* WindowsAmd64

e.g. `./gradlew installDarwinAmd64`

## Publishing Binaries to Maven Repo for all platforms

    ./gradlew clean dist publishToMavenLocal

## Building Docker image

    docker build -t streamsets/datacollector-edge .

## Building Docker image for alternate platform

    docker build --build-arg base=<target image> --build-arg platform=<platform>

Where platform is one of:

* DarwinAmd64
* LinuxAmd64
* LinuxArm
* WindowsAmd64

e.g.
`docker build --build-arg base=arm32v6/alpine --build-arg platform=LinuxArm`

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


### To include TensorFlow Processor in the Edge binary

* Install TensorFlow for Go - https://www.tensorflow.org/install/install_go
* Build Edge using:
    ./gradlew install<platform> -PincludeStage="kafka javascript tensorflow"

    Where platform is one of:

    * DarwinAmd64
    * LinuxAmd64
    * LinuxArm
    * WindowsAmd64

    e.g. `./gradlew installDarwinAmd64  -PincludeStage="kafka javascript tensorflow" `

or use TensorFlow Docker build

    > resources/docker/tensorflow/build.sh
    > docker run --publish 18633:18633 --name edge --rm streamsets/datacollector-edge:tf


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

    docker run -d --publish 18633:18633 --name edge --rm streamsets/datacollector-edge:latest

Getting inside the container

    docker exec -it datacollector-edge /bin/sh

## Release

    ./gradlew publish -Prelease


## CPU & Heap Profile

    curl http://localhost:18633/debug/pprof/profile > cpu.pb.gz
    curl http://localhost:18633/debug/pprof/heap > heap.pb.gz
    curl http://localhost:18633/debug/pprof/goroutine > goroutine.pb.gz
    curl http://localhost:18633/debug/pprof/block > block.pb.gz

or

    go tool pprof http://localhost:18633/debug/pprof/profile
    go tool pprof http://localhost:18633/debug/pprof/heap
    go tool pprof http://localhost:18633/debug/pprof/goroutine
    go tool pprof http://localhost:18633/debug/pprof/block
    

### Running StreamSets Data Collector Edge as service
####( Currently supports Windows XP+, Linux/(systemd | Upstart | SysV), and OSX/Launchd.)

    Service Name - datacollector-edge

| Command                                | Description                                        |
|----------------------------------------|----------------------------------------------------|
| `bin/edge -service install`            | Install Data Collector Edge as a service           |
| `bin/edge -service uninstall`          | Uninstall the Data Collector Edge service          |
| `bin/edge -service start`              | Start the Data Collector Edge service              |
| `bin/edge -service stop`               | Stop the Data Collector Edge service               |
| `bin/edge -service restart`            | Restart the Data Collector Edge service            |
| `bin/edge -service status`             | Displays status of the Data Collector Edge service |
