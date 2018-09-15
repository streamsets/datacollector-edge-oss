# Windows Event Log to HTTP

[![Try Now](../trynow.png)](http://localhost:18630?pipelineTitle=windowsEventToHttp&importPipelineFromUrl=https://raw.githubusercontent.com/streamsets/datacollector-edge/master/resources/samplePipelines/windowsEventToHttp/pipeline.json)

### To start pipeline on SDE start

    <SDC Edge_home>/bin/edge -start=windowsEventToHttp

### To pass runtime parameters

    <SDC Edge_home>/bin/edge -start=windowsEventToHttp -runtimeParameters="{\"logName\":\"System\", \"httpUrl\":\"http://localhost:9999\",\"sdcAppId\":\"sde\"}"


## SDC Edge commands via REST API

### Start Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/start

### To pass runtime parameters during start
    curl -X POST http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"logName":"System","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

### Check Pipeline Status
    curl -X GET http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/status

### Check Pipeline Metrics
    curl -X GET http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/metrics

### Stop Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/stop


###Note
  In Windows command line, make sure to escape double quotes properly in runtime parameters/curl commands

## SDC Edge Sending Pipeline

![Image of SDC Edge Sending Pipeline](edge.png)


## SDC Receiving Pipeline

![Image of SDC Receiving Pipeline](sdchttp.png)
