# StreamSets Data Collector Edge (SDC Edge) QuickStart

## Running

    <SDC Edge_home>/bin/edge

### To start pipeline on SDC Edge start

    <SDC Edge_home>/bin/edge -start=<pipelineId>

### To pass runtime parameters

    <SDC Edge_home>/bin/edge -start=tailFileToHttp -runtimeParameters='{"filePath":"/tmp/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

### To enable DEBUG Log Level

    <SDC Edge_home>/bin/edge -debug -start=tailFileToHttp

### To enable logging to console

    <SDC Edge_home>/bin/edge -logToConsole

## SDC Edge Logs

    <SDC Edge_home>/log/edge.log

## To change SDC Edge Logs directory from default directory <SDC Edge_home>/log/ to /var/sdce/log

    <SDC Edge_home>/bin/edge -logDir=/var/sdce/log

## SDC Edge commands via REST API

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




