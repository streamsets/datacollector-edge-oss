# StreamSets Data Extractor 

## Building

    make clean dist
    
## Running

    cd dist
    bin/dataextractor
    
## REST API

    curl -X POST http://localhost:18633/rest/v1/pipeline/status
    curl -X POST http://localhost:18633/rest/v1/pipeline/start
    curl -X POST http://localhost:18633/rest/v1/pipeline/stop
   

