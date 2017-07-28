# Random to HTTP

### To start pipeline on SDE start

    $ bin/edge -start=windowsEventToHttp

### To pass runtime parameters

    $ bin/edge -start=windowsEventToHttp -runtimeParameters='{"logName":"Application","readMode":"all","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## REST API

    $ curl -X GET http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/status
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/start
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/stop
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/resetOffset
    $ curl -X GET http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/metrics

### To pass runtime parameters during start

    $ curl -X POST http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"logName":"Application","readMode":"all","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

