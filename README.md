# StreamSets Data Collector To Go (SDC2Go)

## Minimum Requirements

* Go 1.8
* make


## Clone Repository

* Create directory $GOPATH/src/github.com/streamsets
* Clone this repository in directory $GOPATH/src/github.com/streamsets
* Reference - https://golang.org/doc/code.html#Organization

## Building
    $ make clean dist

## Building for all platforms

    $ make clean dist-all

## To run tests

    $ make test

## Running

    $ cd dist
    $ bin/sdc2go

### To start pipeline on SDE start

    $ bin/sdc2go -start=<pipelineId>

### To pass runtime parameters

    $ bin/sdc2go -start=tailFileToHttp -runtimeParameters='{"filePath":"/tmp/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

### To enable DEBUG Log Level

    $ bin/sdc2go -debug -start=tailFileToHttp

## REST API

    $ curl -X GET http://localhost:18633/rest/v1/pipeline/:pipelineId/status
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/start
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/stop
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/resetOffset
    $ curl -X GET http://localhost:18633/rest/v1/pipeline/:pipelineId/metrics

### To pass runtime parameters during start

    $ curl -X POST http://localhost:18633/rest/v1/pipeline/tailFileToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"filePath":"/tmp/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## Docker Build and run the image


Invoke Docker from the sdc2go directory to build an image using the Dockerfile:


    $ docker build -t streamsets/sdc2go .


This will fetch the alpine base image from Docker Hub, copy the already built package (linux/amd64), and tag the resulting image as streamsets/sdc2go.


To run a container from the resulting image:

    $ docker run --publish 18633:18633 --name sde --rm streamsets/sdc2go

Getting inside the container

    $ docker exec -it sde /bin/sh
