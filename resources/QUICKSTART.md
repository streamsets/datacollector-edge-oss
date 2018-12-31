# StreamSets Data Collector Edge (SDC Edge) QuickStart

## Running

    <SDC Edge_home>/bin/edge

### To start pipeline on SDC Edge start

    <SDC Edge_home>/bin/edge -start=<pipelineId>

### To pass runtime parameters

    <SDC Edge_home>/bin/edge -start=tailFileToHttp -runtimeParameters='{"filePath":"/tmp/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

### To enable DEBUG Log Level

    <SDC Edge_home>/bin/edge -debug -start=tailFileToHttp

### To enable logging to console

    <SDC Edge_home>/bin/edge -logToConsole

## SDC Edge Logs

    <SDC Edge_home>/log/edge.log

## StreamSets DataCollector UI 
   Use Data Collector UI or Control Hub Pipeline designer for designing, publishing, validating, previewing, starting, monitoring with metrics and stopping edge pipelines.

## SDC Edge commands via REST API

### List all pipelines
    curl -X GET http://localhost:18633/rest/v1/pipelines

### Start Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/start

### To pass runtime parameters during start
    curl -X POST http://localhost:18633/rest/v1/pipeline/tailFileToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"filePath":"/tmp/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

### Check Pipeline Status
    curl -X GET http://localhost:18633/rest/v1/pipeline/:pipelineId/status

### Check Pipeline Metrics
    curl -X GET http://localhost:18633/rest/v1/pipeline/:pipelineId/metrics

### Stop Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/stop

### Reset Origin Offset
    curl -X POST http://localhost:18633/rest/v1/pipeline/:pipelineId/resetOffset



### Running StreamSets Data Collector Edge as service
####( Currently supports Windows XP+, Linux/(systemd | Upstart | SysV), and OSX/Launchd.)

    Service Name - datacollector-edge

| Command                                | Description                                        |
|----------------------------------------|----------------------------------------------------|
| `bin/edge -service install`            | Install Data Collector Edge as a service           |
| `bin/edge -service uninstall`          | Uninstall the Data Collector Edge service          |
| `bin/edge -service start`              | Start the Data Collector Edge service              |
| `bin/edge -service stop`               | Stop the Data Collector Edge service               |
| `bin/edge -service restart`            | Restart the Data Collector Edge service            |
| `bin/edge -service status`             | Displays status of the Data Collector Edge service |

