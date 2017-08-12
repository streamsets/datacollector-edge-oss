# MQTT to HTTP

### To start pipeline on SDE start

    $ bin/edge -start=mqttToHttp

### To pass runtime parameters

    $ bin/edge -start=mqttToHttp -runtimeParameters='{"mqttClientId":"mqttSubscriber","mqttBrokerUrl":"tcp://localhost:1883","mqttTopic":"sample","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## REST API

    $ curl -X GET http://localhost:18633/rest/v1/pipeline/mqttToHttp/status
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/mqttToHttp/start
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/mqttToHttp/stop
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/mqttToHttp/resetOffset
    $ curl -X GET http://localhost:18633/rest/v1/pipeline/mqttToHttp/metrics

### To pass runtime parameters during start

    $ curl -X POST http://localhost:18633/rest/v1/pipeline/mqttToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"mqttClientId":"mqttSubscriber","mqttBrokerUrl":"tcp://localhost:1883","mqttTopic":"sample","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

