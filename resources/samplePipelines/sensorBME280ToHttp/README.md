#  Bosch BME280 Humidity, Barometric Pressure, Temperature Sensor data to HTTP

[![Try Now](../trynow.png)](http://localhost:18630?pipelineTitle=sensorBME280ToHttp&importPipelineFromUrl=https://raw.githubusercontent.com/streamsets/datacollector-edge/master/resources/samplePipelines/sensorBME280ToHttp/pipeline.json)

### To start pipeline on SDE start

    <SDC Edge_home>/bin/edge -start=sensorBME280ToHttp

### To pass runtime parameters

    <SDC Edge_home>/bin/edge -start=sensorBME280ToHttp -runtimeParameters='{"i2cAddress":"0x77", "httpUrl":"http://localhost:9999","sdcAppId":"edge"}'

## SDC Edge commands via REST API

### Start Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/sensorBME280ToHttp/start

### To pass runtime parameters during start
    curl -X POST http://localhost:18633/rest/v1/pipeline/sensorBME280ToHttp/start -H 'Content-Type: application/json;charset=UTF-8' --data-binary '{"i2cAddress":"0x77", "httpUrl":"http://localhost:9999","sdcAppId":"edge"}'

### Check Pipeline Status
    curl -X GET http://localhost:18633/rest/v1/pipeline/sensorBME280ToHttp/status

### Check Pipeline Metrics
    curl -X GET http://localhost:18633/rest/v1/pipeline/sensorBME280ToHttp/metrics

### Stop Pipeline
    curl -X POST http://localhost:18633/rest/v1/pipeline/sensorBME280ToHttp/stop


## SDC Edge Sending Pipeline

![Image of SDC Edge Sending Pipeline](edge.png)


## SDC Receiving Pipeline

![Image of SDC Receiving Pipeline](sdchttp.png)
