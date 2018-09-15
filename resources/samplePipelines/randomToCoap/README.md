# Random to CoAP

[![Try Now](../trynow.png)](http://localhost:18630?pipelineTitle=randomToCoap&importPipelineFromUrl=https://raw.githubusercontent.com/streamsets/datacollector-edge/master/resources/samplePipelines/randomToCoap/pipeline.json)

### To start pipeline on SDE start

    <SDC Edge_home>/bin/edge -start=randomToCoap

## SDC Edge commands via REST API

### Start Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/randomToCoap/start

### Check Pipeline Status
    curl -X GET http://localhost:18633/rest/v1/pipeline/randomToCoap/status

### Check Pipeline Metrics
    curl -X GET http://localhost:18633/rest/v1/pipeline/randomToCoap/metrics

### Stop Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/randomToCoap/stop


## SDC Edge Sending Pipeline

![Image of SDC Edge Sending Pipeline](edge.png)


## SDC Receiving Pipeline

![Image of SDC Receiving Pipeline](sdccoap.png)
