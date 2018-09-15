# Directory Spooler to HTTP

[![Try Now](../trynow.png)](http://localhost:18630?pipelineTitle=directoryToHttp&importPipelineFromUrl=https://raw.githubusercontent.com/streamsets/datacollector-edge/master/resources/samplePipelines/directoryToHttp/pipeline.json)

### To start pipeline on SDE start

    <SDC Edge_home>/bin/edge -start=directoryToHttp

### To pass runtime parameters

    <SDC Edge_home>/bin/edge -start=directoryToHttp -runtimeParameters='{"directoryPath":"/tmp/out/dir","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## SDC Edge commands via REST API

### Start Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/directoryToHttp/start

### To pass runtime parameters during start
    curl -X POST http://localhost:18633/rest/v1/pipeline/directoryToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"directoryPath":"/tmp/out/dir","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

### Check Pipeline Status
    curl -X GET http://localhost:18633/rest/v1/pipeline/directoryToHttp/status

### Check Pipeline Metrics
    curl -X GET http://localhost:18633/rest/v1/pipeline/directoryToHttp/metrics

### Stop Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/directoryToHttp/stop

### Reset Origin Offset
    curl -X POST http://localhost:18633/rest/v1/pipeline/directoryToHttp/resetOffset


## SDC Edge Sending Pipeline

![Image of SDC Edge Sending Pipeline](edge.png)


## SDC Receiving Pipeline

![Image of SDC Receiving Pipeline](sdchttp.png)
