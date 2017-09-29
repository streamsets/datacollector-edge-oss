# Random to Field Filter To HTTP

### To start pipeline on SDE start

    $ bin/edge -start=randomToFieldFilterToHttp

### To pass runtime parameters

    $ bin/edge -start=randomToFieldFilterToHttp -runtimeParameters='{"randomFields":"a,b,c","filterOperation":"REMOVE","filterFields":["/a","/c"],"httpUrl":"http://localhost:9999","sdcAppId":"sde"}'

## REST API

    $ curl -X GET http://localhost:18633/rest/v1/pipeline/randomToFieldFilterToHttp/status
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/randomToFieldFilterToHttp/start
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/randomToFieldFilterToHttp/stop
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/randomToFieldFilterToHttp/resetOffset
    $ curl -X GET http://localhost:18633/rest/v1/pipeline/randomToFieldFilterToHttp/metrics

### To pass runtime parameters during start

    $ curl -X POST http://localhost:18633/rest/v1/pipeline/randomToFieldFilterToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"randomFields":"a,b,c","filterOperation":"REMOVE","filterFields":["a","b"],"httpUrl":"http://localhost:9999","sdcAppId":"sde"}'


## SDC Edge Pipeline

![Image of SDC Edge Pipeline](edge.png)


## SDC Pipeline

![Image of SDC Pipeline](sdchttp.png)
