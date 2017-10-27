# WindowsEventLog to HTTP

### To start pipeline on SDE start

    $ bin/edge -start=windowsEventToHttp

### To pass runtime parameters

    $ bin/edge -start=windowsEventToHttp -runtimeParameters='{"logName":"System", "httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## REST API

    $ curl -X GET http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/status
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/start
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/stop
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/resetOffset
    $ curl -X GET http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/metrics

### To pass runtime parameters during start

    $ curl -X POST http://localhost:18633/rest/v1/pipeline/windowsEventToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"logName":"System","httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

###Note
  In Windows command line, make sure to escape double quotes properly in runtime parameters/curl commands

## SDCe Sending Pipeline

![Image of SDCe Sending Pipeline](edge.png)


## SDC Receiving Pipeline

![Image of SDC Receiving Pipeline](sdchttp.png)
