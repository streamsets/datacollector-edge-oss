# Random to MQTT

[![Try Now](../trynow.png)](http://localhost:18630?pipelineTitle=randomToMqtt&importPipelineFromUrl=https://raw.githubusercontent.com/streamsets/datacollector-edge/master/resources/samplePipelines/randomToMqtt/pipeline.json)

### To start pipeline on SDE start

    <SDC Edge_home>/bin/edge -start=randomToMqtt

### To pass runtime parameters

    <SDC Edge_home>/bin/edge -start=randomToMqtt -runtimeParameters='{"mqttClientId":"sdeMqttPublisher","mqttBrokerUrl":"tcp://localhost:1883","mqttTopic":"sample"}'

## SDC Edge commands via REST API

### Start Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/randomToMqtt/start

### To pass runtime parameters during start
    curl -X POST http://localhost:18633/rest/v1/pipeline/randomToMqtt/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"mqttClientId":"sdeMqttPublisher","mqttBrokerUrl":"tcp://localhost:1883","mqttTopic":"sample"}'

### Check Pipeline Status
    curl -X GET http://localhost:18633/rest/v1/pipeline/randomToMqtt/status

### Check Pipeline Metrics
    curl -X GET http://localhost:18633/rest/v1/pipeline/randomToMqtt/metrics

### Stop Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/randomToMqtt/stop


## SDC Edge Sending Pipeline

![Image of SDC Edge Sending Pipeline](edge.png)


## SDC Receiving Pipeline

![Image of SDC Receiving Pipeline](sdcmqtt.png)
