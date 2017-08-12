# Directory Spooler to HTTP

### To start pipeline on SDE start

    $ bin/edge -start=directoryToHttp

### To pass runtime parameters

    $ bin/edge -start=directoryToHttp -runtimeParameters='{"directoryPath":"/tmp/out/dir","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## REST API

    $ curl -X GET http://localhost:18633/rest/v1/pipeline/directoryToHttp/status
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/directoryToHttp/start
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/directoryToHttp/stop
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/directoryToHttp/resetOffset
    $ curl -X GET http://localhost:18633/rest/v1/pipeline/directoryToHttp/metrics

### To pass runtime parameters during start

    $ curl -X POST http://localhost:18633/rest/v1/pipeline/directoryToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"directoryPath":"/tmp/out/dir","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## SDC Edge Pipeline

![Image of SDC Edge Pipeline](edge.png)


## SDC Pipeline

![Image of SDC Pipeline](sdchttp.png)
