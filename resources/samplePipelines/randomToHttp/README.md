# Random to HTTP

[![Try Now](../trynow.png)](http://localhost:18630?pipelineTitle=randomToHttp&importPipelineFromUrl=https://raw.githubusercontent.com/streamsets/datacollector-edge/master/resources/samplePipelines/randomToHttp/pipeline.json)

### To start pipeline on SDE start

    <SDC Edge_home>/bin/edge -start=randomToHttp

### To pass runtime parameters

    <SDC Edge_home>/bin/edge -start=randomToHttp -runtimeParameters='{"httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## SDC Edge commands via REST API

### Start Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/randomToHttp/start

### To pass runtime parameters during start
    curl -X POST http://localhost:18633/rest/v1/pipeline/randomToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

### Check Pipeline Status
    curl -X GET http://localhost:18633/rest/v1/pipeline/randomToHttp/status

### Check Pipeline Metrics
    curl -X GET http://localhost:18633/rest/v1/pipeline/randomToHttp/metrics

### Stop Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/randomToHttp/stop


## SDC Edge Sending Pipeline

![Image of SDC Edge Sending Pipeline](edge.png)


## SDC Receiving Pipeline

![Image of SDC Receiving Pipeline](sdchttp.png)
