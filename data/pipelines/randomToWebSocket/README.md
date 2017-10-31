# Random to WebSocket

### To start pipeline on SDE start

    <SDCE_DIST>/bin/edge -start=randomToMqtt

### To pass runtime parameters

    <SDCE_DIST>/bin/edge -start=randomToWebSocket -runtimeParameters='{"webSocketUrl":"ws://localhost:8080","sdcAppId":"edge"}'

## SDCe commands via REST API

### Start Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/randomToWebSocket/start

### To pass runtime parameters during start
    curl -X POST http://localhost:18633/rest/v1/pipeline/randomToWebSocket/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"webSocketUrl":"ws://localhost:8080","sdcAppId":"edge"}'

### Check Pipeline Status
    curl -X GET http://localhost:18633/rest/v1/pipeline/randomToWebSocket/status

### Check Pipeline Metrics
    curl -X GET http://localhost:18633/rest/v1/pipeline/randomToWebSocket/metrics

### Stop Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/randomToWebSocket/stop


## SDCe Sending Pipeline

![Image of SDCe Sending Pipeline](edge.png)


## SDC Receiving Pipeline

![Image of SDC Receiving Pipeline](sdcwebsocket.png)
