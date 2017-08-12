# StreamSets Data Collector Edge (SDCe)

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
    $ bin/edge

### To start pipeline on SDCe start

    $ bin/edge -start=<pipelineId>

### To pass runtime parameters

    $ bin/edge -start=tailFileToHttp -runtimeParameters='{"filePath":"/tmp/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

### To enable DEBUG Log Level

    $ bin/edge -debug -start=tailFileToHttp

## REST API

    $ curl -X GET http://localhost:18633/rest/v1/pipeline/:pipelineId/status
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/start
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/stop
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/resetOffset
    $ curl -X GET http://localhost:18633/rest/v1/pipeline/:pipelineId/metrics

### To pass runtime parameters during start

    $ curl -X POST http://localhost:18633/rest/v1/pipeline/tailFileToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"filePath":"/tmp/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## Docker Build and run the image


Invoke Docker from the edge directory to build an image using the Dockerfile:


    $ docker build -t streamsets/datacollector-edge .


This will fetch the alpine base image from Docker Hub, copy the already built package (linux/amd64), and tag the resulting image as streamsets/datacollector-edge.


To run a container from the resulting image:

    $ docker run --publish 18633:18633 --name edge --rm streamsets/datacollector-edge

Getting inside the container

    $ docker exec -it datacollector-edge /bin/sh


## Pipeline Templates

* [Tail File To Http](data/pipelines/tailFileToHttp)
* [Directory Spooler to Http](data/pipelines/directoryToHttp)
* [MQTT To HTTP](data/pipelines/mqttToHttp)
* [Random Data to MQTT](data/pipelines/randomToMqtt)
* [Random Data to CoAP](data/pipelines/randomToCoap)
* [Random Data To Http](data/pipelines/randomToHttp)
* [HTTP Server To Trash](data/pipelines/httpServerToTrash)
* [Random Data To Identity Processor To Trash](data/pipelines/randomToIdentityToTrash)



