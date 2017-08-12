# Random to MQTT

### To start pipeline on SDE start

    $ bin/edge -start=randomToMqtt

### To pass runtime parameters

    $ bin/edge -start=randomToMqtt -runtimeParameters='{"mqttClientId":"sdeMqttPublisher","mqttBrokerUrl":"http://localhost:9999","mqttTopic":"sample"}'

## REST API

    $ curl -X GET http://localhost:18633/rest/v1/pipeline/randomToMqtt/status
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/randomToMqtt/start
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/randomToMqtt/stop
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/randomToMqtt/resetOffset
    $ curl -X GET http://localhost:18633/rest/v1/pipeline/randomToMqtt/metrics

### To pass runtime parameters during start

    $ curl -X POST http://localhost:18633/rest/v1/pipeline/randomToMqtt/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"mqttClientId":"sdeMqttPublisher","mqttBrokerUrl":"http://localhost:9999","mqttTopic":"sample"}'

