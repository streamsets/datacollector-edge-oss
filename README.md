<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
--->

![Data Collector Edge Splash Image](resources/sdcEdgeSplash.png)

[![Go Report Card](https://goreportcard.com/badge/github.com/streamsets/datacollector-edge)](https://goreportcard.com/report/github.com/streamsets/datacollector-edge)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fstreamsets%2Fdatacollector-edge.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fstreamsets%2Fdatacollector-edge?ref=badge_shield)

# What is StreamSets Data Collector Edge?

The StreamSets Data Collector Edge (SDC Edge) enables at-scale data ingestion and analytics for edge systems.
An ultralight, small-footprint agent, it is an ideal solution for use cases like Internet of Things (IoT) or
cybersecurity applications that collect data from resource-constrained sensors and personal devices.

To learn more, check out [https://streamsets.com/products/streamsets-data-collector-edge](https://streamsets.com/products/streamsets-data-collector-edge)

## License

StreamSets Data Collector Edge is built on open source technologies, our code is licensed with the
[Apache License 2.0](LICENSE.txt).

## Getting Help

A good place to start is to check out [http://streamsets.com/community](http://streamsets.com/community). On that page
you will find all the ways you can reach us and channels our team monitors. You can post questions on
[Google Groups sdc-user](https://groups.google.com/a/streamsets.com/forum/#!forum/sdc-user) or on [StackExchange](http://stackexchange.com) using the
tag #StreamSets. Post bugs at [http://issues.streamsets.com](http://issues.streamsets.com) or tweet at us with #StreamSets.

If you need help with production systems, you can check out the variety of support options offered on our
[support page](http://streamsets.com/support).

# Useful resources

* [Nightly Builds](http://nightly.streamsets.com/latest/tarball/SDCe)
* [Quickstart](resources/QUICKSTART.md)
* [Building StreamSets Data Collector Edge](BUILD.md)
* [StreamSets Data Collector Edge Documentation](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Edge_Mode/EdgePipelines_Overview.html#concept_d4h_kkq_4bb)

## Sample Pipelines
* [System Metrics to HTTP](resources/samplePipelines/systemMetricsToHttp)
* [Windows Event To Http](resources/samplePipelines/windowsEventToHttp)
* [Tail File To Http](resources/samplePipelines/tailFileToHttp)
* [Directory Spooler to Http](resources/samplePipelines/directoryToHttp)
* [Bosch BME280 Humidity, Barometric Pressure, Temperature Sensor data to HTTP](resources/samplePipelines/sensorBME280ToHttp)
* [Stream Ripple Ledger Information](resources/samplePipelines/websocketClientToTrash)
* [MQTT To HTTP](resources/samplePipelines/mqttToHttp)
* [Dev Raw Data To Expression Evaluator to Kafka](resources/samplePipelines/devRawDataToExpressionToKafka)
* [Random Data to MQTT](resources/samplePipelines/randomToMqtt)
* [Random Data to CoAP](resources/samplePipelines/randomToCoap)
* [Random Data To Http](resources/samplePipelines/randomToHttp)
* [HTTP Server To Trash](resources/samplePipelines/httpServerToTrash)
* [Random Data To Identity Processor To Trash](resources/samplePipelines/randomToIdentityToTrash)


## Contributing code

We welcome contributors, please check out our [guidelines](CONTRIBUTING.md) to get started.
