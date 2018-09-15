# HTTP Server to Trash

[![Try Now](../trynow.png)](http://localhost:18630?pipelineTitle=httpServerToTrash&importPipelineFromUrl=https://raw.githubusercontent.com/streamsets/datacollector-edge/master/resources/samplePipelines/httpServerToTrash/pipeline.json)

### To start pipeline on SDE start

    <SDC Edge_home>/bin/edge -start=httpServerToTrash

### To pass runtime parameters

    <SDC Edge_home>/bin/edge -start=httpServerToTrash -runtimeParameters='{"httpPort":"8888","sdeAppId":"sde"}'


## SDC Edge commands via REST API

### Start Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/httpServerToTrash/start

### To pass runtime parameters during start
    curl -X POST http://localhost:18633/rest/v1/pipeline/httpServerToTrash/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"httpPort":"8888","sdeAppId":"sde"}'

### Check Pipeline Status
    curl -X GET http://localhost:18633/rest/v1/pipeline/httpServerToTrash/status

### Check Pipeline Metrics
    curl -X GET http://localhost:18633/rest/v1/pipeline/httpServerToTrash/metrics

### Stop Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/httpServerToTrash/stop

## SDC Edge Sending Pipeline

![Image of SDC Edge Sending Pipeline](edge.png)

