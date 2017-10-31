# StreamSets Data Collector Edge (SDCe) QuickStart

## Running

    <SDCE_DIST>/bin/edge

### To start pipeline on SDCe start

    <SDCE_DIST>/bin/edge -start=<pipelineId>

### To pass runtime parameters

    <SDCE_DIST>/bin/edge -start=tailFileToHttp -runtimeParameters='{"filePath":"/tmp/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

### To enable DEBUG Log Level

    <SDCE_DIST>/bin/edge -debug -start=tailFileToHttp

## SDCe Logs

    <SDCE_DIST>/log/edge.log


## SDCe commands via REST API

### List all pipelines
    curl -X POST http://localhost:18633/rest/v1/pipelines

### Start Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/start

### To pass runtime parameters during start
    curl -X POST http://localhost:18633/rest/v1/pipeline/tailFileToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"filePath":"/tmp/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

### Check Pipeline Status
    curl -X GET http://localhost:18633/rest/v1/pipeline/:pipelineId/status

### Check Pipeline Metrics
    curl -X GET http://localhost:18633/rest/v1/pipeline/:pipelineId/metrics

### Stop Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/stop

### Reset Origin Offset
    curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/resetOffset




