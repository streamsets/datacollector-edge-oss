# Random Origin -> Identity Processor -> Trash

### To start pipeline on SDE start

    <SDCE_DIST>/bin/edge -start=randomToIdentityToTrash

## SDCe commands via REST API

### Start Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/randomToIdentityToTrash/start

### Check Pipeline Status
    curl -X GET http://localhost:18633/rest/v1/pipeline/randomToIdentityToTrash/status

### Check Pipeline Metrics
    curl -X GET http://localhost:18633/rest/v1/pipeline/randomToIdentityToTrash/metrics

### Stop Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/randomToIdentityToTrash/stop


## SDCe Sending Pipeline

![Image of SDCe Sending Pipeline](edge.png)

