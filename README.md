# StreamSets Data Extractor 

## Minimum Requirements

* Go 1.8
* make
    
## Building

    $ make clean dist
    
## Running

    $ cd dist
    $ bin/dataextractor
    
## REST API

    $ curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/status
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/start
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/stop
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/resetOffset
   

## Building for all platforms

    $ make clean dist-all