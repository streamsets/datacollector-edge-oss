# Tail File to HTTP

### To start pipeline on SDE start

    <SDCE_DIST>/bin/edge -start=tailFileToHttp

### To pass runtime parameters

    <SDCE_DIST>/bin/edge -start=tailFileToHttp -runtimeParameters='{"filePath":"/Users/tempUser/log/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## SDCe commands via REST API

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


## SDCe Sending Pipeline

![Image of SDCe Sending Pipeline](edge.png)


## SDC Receiving Pipeline

![Image of SDC Receiving Pipeline](sdchttp.png)
