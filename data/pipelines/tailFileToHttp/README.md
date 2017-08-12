# Tail File to HTTP

### To start pipeline on SDE start

    $ bin/edge -start=tailFileToHttp

### To pass runtime parameters

    $ bin/edge -start=tailFileToHttp -runtimeParameters='{"filePath":"/Users/tempUser/log/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## REST API

    $ curl -X GET http://localhost:18633/rest/v1/pipeline/tailFileToHttp/status
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/tailFileToHttp/start
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/tailFileToHttp/stop
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/tailFileToHttp/resetOffset
    $ curl -X GET http://localhost:18633/rest/v1/pipeline/tailFileToHttp/metrics

### To pass runtime parameters during start

    $ curl -X POST http://localhost:18633/rest/v1/pipeline/tailFileToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"filePath":"/Users/tempUser/log/sds.log","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

