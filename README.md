# StreamSets Data Extractor 

## Minimum Requirements

* Go 1.8
* make


## Clone Repository

* Create directory $GOPATH/src/github.com/streamsets
* Clone this repository in directory $GOPATH/src/github.com/streamsets
* Reference - https://golang.org/doc/code.html#Organization
    
## Building
    $ make clean dist
    
## Running

    $ cd dist
    $ bin/dataextractor
    
### To start pipeline on SDE start

    $ bin/dataextractor -start=<pipelineId>
    
### To pass runtime parameters   
        
    $ bin/dataextractor -start=tailFileToHttp -runtimeParameters='{"filePath":"/tmp/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

### To enable DEBUG Log Level    
    
    $ bin/dataextractor -debug -start=tailFileToHttp    
    
## REST API

    $ curl -X GET http://localhost:18633/rest/v1/pipeline/:pipelineId/status
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/start
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/stop
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/resetOffset
    
### To pass runtime parameters during start

    $ curl -X POST http://localhost:18633/rest/v1/pipeline/tailFileToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"filePath":"/tmp/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'
       

## Building for all platforms

    $ make clean dist-all


## Docker Build and run the image


Invoke Docker from the dataextractor directory to build an image using the Dockerfile:

    
    $ docker build -t streamsets/dataextractor .


This will fetch the alpine base image from Docker Hub, copy the already built package (linux/amd64), and tag the resulting image as streamsets/dataextractor.


To run a container from the resulting image:

    $ docker run --publish 18633:18633 --name sde --rm streamsets/dataextractor

Getting inside the container

    $ docker exec -it sde /bin/sh 


## To run tests

    $ make test
