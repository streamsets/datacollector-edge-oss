# Tail File to HTTP

[![Try Now](../trynow.png)](http://localhost:18630?pipelineTitle=tailFileToHttp&importPipelineFromUrl=https://raw.githubusercontent.com/streamsets/datacollector-edge/master/resources/samplePipelines/tailFileToHttp/pipeline.json)

### To start pipeline on SDE start

    <SDC Edge_home>/bin/edge -start=tailFileToHttp

### To pass runtime parameters

    <SDC Edge_home>/bin/edge -start=tailFileToHttp -runtimeParameters='{"filePath":"/Users/tempUser/log/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## SDC Edge commands via REST API

### Start Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/tailFileToHttp/start

### To pass runtime parameters during start
    curl -X POST http://localhost:18633/rest/v1/pipeline/tailFileToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"filePath":"/Users/tempUser/log/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

### Check Pipeline Status
    curl -X GET http://localhost:18633/rest/v1/pipeline/tailFileToHttp/status

### Check Pipeline Metrics
    curl -X GET http://localhost:18633/rest/v1/pipeline/tailFileToHttp/metrics

### Stop Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/tailFileToHttp/stop

### Reset Origin Offset
    curl -X POST http://localhost:18633/rest/v1/pipeline/tailFileToHttp/resetOffset


## SDC Edge Sending Pipeline

![Image of SDC Edge Sending Pipeline](edge.png)


## SDC Receiving Pipeline

![Image of SDC Receiving Pipeline](sdchttp.png)
