# HTTP Server to Trash

### To start pipeline on SDE start

    <SDCe_home>/bin/edge -start=httpServerToTrash

### To pass runtime parameters

    <SDCe_home>/bin/edge -start=httpServerToTrash -runtimeParameters='{"httpPort":"8888","sdeAppId":"sde"}'


## SDCe commands via REST API

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

## SDCe Sending Pipeline

![Image of SDCe Sending Pipeline](edge.png)

