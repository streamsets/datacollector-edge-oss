# Windows Event Log to HTTP

### To start pipeline on SDE start

    <SDCE_DIST>/bin/edge -start=windowsEventToHttp

### To pass runtime parameters

    <SDCE_DIST>/bin/edge -start=windowsEventToHttp -runtimeParameters='{"logName":"System", "httpUrl":"http://localhost:9999","sdcAppId":"sde"}'


## SDCe commands via REST API

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

## SDCe Sending Pipeline

![Image of SDCe Sending Pipeline](edge.png)


## SDC Receiving Pipeline

![Image of SDC Receiving Pipeline](sdchttp.png)
