# MQTT to HTTP

[![Try Now](../trynow.png)](http://localhost:18630?pipelineTitle=mqttToHttp&importPipelineFromUrl=https://raw.githubusercontent.com/streamsets/datacollector-edge/master/resources/samplePipelines/mqttToHttp/pipeline.json)

### To start pipeline on SDE start

    <SDC Edge_home>/bin/edge -start=mqttToHttp

### To pass runtime parameters

    <SDC Edge_home>/bin/edge -start=mqttToHttp -runtimeParameters='{"mqttClientId":"mqttSubscriber","mqttBrokerUrl":"tcp://localhost:1883","mqttTopic":"sample","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## SDC Edge commands via REST API

### Start Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/mqttToHttp/start

### To pass runtime parameters during start
    curl -X POST http://localhost:18633/rest/v1/pipeline/mqttToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"mqttClientId":"mqttSubscriber","mqttBrokerUrl":"tcp://localhost:1883","mqttTopic":"sample","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

### Check Pipeline Status
    curl -X GET http://localhost:18633/rest/v1/pipeline/mqttToHttp/status

### Check Pipeline Metrics
    curl -X GET http://localhost:18633/rest/v1/pipeline/mqttToHttp/metrics

### Stop Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/mqttToHttp/stop


## SDC Edge Sending Pipeline

![Image of SDC Edge Sending Pipeline](edge.png)


## SDC Receiving Pipeline

![Image of SDC Receiving Pipeline](sdchttp.png)
