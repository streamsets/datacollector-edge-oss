# Random to HTTP

### To start pipeline on SDE start

    $ bin/edge -start=randomToHttp

### To pass runtime parameters

    $ bin/edge -start=randomToHttp -runtimeParameters='{"httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## REST API

    $ curl -X GET http://localhost:18633/rest/v1/pipeline/randomToHttp/status
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/randomToHttp/start
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/randomToHttp/stop
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/randomToHttp/resetOffset
    $ curl -X GET http://localhost:18633/rest/v1/pipeline/randomToHttp/metrics

### To pass runtime parameters during start

    $ curl -X POST http://localhost:18633/rest/v1/pipeline/randomToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"httpUrl":"http://localhost:9999","sdcAppId":"sde"}'


## SDCe Sending Pipeline

![Image of SDCe Sending Pipeline](edge.png)


## SDC Receiving Pipeline

![Image of SDC Receiving Pipeline](sdchttp.png)
