# Directory Spooler to HTTP

### To start pipeline on SDE start

    <SDCE_DIST>/bin/edge -start=directoryToHttp

### To pass runtime parameters

    <SDCE_DIST>/bin/edge -start=directoryToHttp -runtimeParameters='{"directoryPath":"/tmp/out/dir","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## SDCe commands via REST API

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


## SDCe Sending Pipeline

![Image of SDCe Sending Pipeline](edge.png)


## SDC Receiving Pipeline

![Image of SDC Receiving Pipeline](sdchttp.png)
