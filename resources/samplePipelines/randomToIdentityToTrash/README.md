# Random Origin -> Identity Processor -> Trash

[![Try Now](../trynow.png)](http://localhost:18630?pipelineTitle=randomToIdentityToTrash&importPipelineFromUrl=https://raw.githubusercontent.com/streamsets/datacollector-edge/master/resources/samplePipelines/randomToIdentityToTrash/pipeline.json)

### To start pipeline on SDE start

    <SDC Edge_home>/bin/edge -start=randomToIdentityToTrash

## SDC Edge commands via REST API

### Start Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/randomToIdentityToTrash/start

### Check Pipeline Status
    curl -X GET http://localhost:18633/rest/v1/pipeline/randomToIdentityToTrash/status

### Check Pipeline Metrics
    curl -X GET http://localhost:18633/rest/v1/pipeline/randomToIdentityToTrash/metrics

### Stop Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/randomToIdentityToTrash/stop


## SDC Edge Sending Pipeline

![Image of SDC Edge Sending Pipeline](edge.png)

