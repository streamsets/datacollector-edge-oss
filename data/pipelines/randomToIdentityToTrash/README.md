# Random Origin -> Identity Processor -> Trash

### To start pipeline on SDE start

    $ bin/sdc2go -start=randomToIdentityToTrash

## REST API

    $ curl -X GET http://localhost:18633/rest/v1/pipeline/randomToIdentityToTrash/status
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/randomToIdentityToTrash/start
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/randomToIdentityToTrash/stop
    $ curl -X POST http://localhost:18633/rest/v1/pipeline/randomToIdentityToTrash/resetOffset
    $ curl -X GET http://localhost:18633/rest/v1/pipeline/randomToIdentityToTrash/metrics
