# StreamSets Data Collector Edge (SDCe) QuickStart

## Running

    cd streamsets-datacollector-edge
    bin/edge

### To start pipeline on SDCe start

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
