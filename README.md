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

<img style="width:100%;" src="sdcEdgeSplash.png">

What is StreamSets Data Collector Edge?
-----------

The StreamSets Data Collector Edge (SDC Edge) enables at-scale data ingestion and analytics for edge systems.
An ultralight, small-footprint agent, it is an ideal solution for use cases like Internet of Things (IoT) or
cybersecurity applications that collect data from resource-constrained sensors and personal devices.


To learn more, check out [https://streamsets.com/products/streamsets-data-collector-edge](https://streamsets.com/products/streamsets-data-collector-edge)

License
------
StreamSets Data Collector Edge is built on open source technologies, our code is licensed with the
[Apache License 2.0](LICENSE.txt).

Getting Help
----------

A good place to start is to check out [http://streamsets.com/community](http://streamsets.com/community). On that page
you will find all the ways you can reach us and channels our team monitors. You can post questions on
[Google Groups sdc-user](https://groups.google.com/a/streamsets.com/forum/#!forum/sdc-user) or on [StackExchange](http://stackexchange.com) using the
tag #StreamSets. Post bugs at [http://issues.streamsets.com](http://issues.streamsets.com) or tweet at us with #StreamSets.

If you need help with production systems, you can check out the variety of support options offered on our
[support page](http://streamsets.com/support).



# Useful resources
* [Nightly Builds](http://nightly.streamsets.com/latest/tarball/SDCe)
* [Quickstart](QUICKSTART.md)
* [Building StreamSets Data Collector Edge](BUILD.md)
* [StreamSets Data Collector Edge Documentation](https://streamsets.com/documentation/datacollector/latest/help/index.html#Edge_Mode/EdgePipelines_title.html%23concept_fyf_gkq_4bb)


## Sample Pipelines
* [Windows Event To Http](data/pipelines/windowsEventToHttp)
* [Tail File To Http](data/pipelines/tailFileToHttp)
* [Directory Spooler to Http](data/pipelines/directoryToHttp)
* [Bosch BME280 Humidity, Barometric Pressure, Temperature Sensor data to HTTP](data/pipelines/sensorBME280ToHttp)
* [MQTT To HTTP](data/pipelines/mqttToHttp)
* [Dev Raw Data To Expression Evaluator to Kafka](data/pipelines/devRawDataToExpressionToKafka)
* [Random Data to MQTT](data/pipelines/randomToMqtt)
* [Random Data to CoAP](data/pipelines/randomToCoap)
* [Random Data To Http](data/pipelines/randomToHttp)
* [HTTP Server To Trash](data/pipelines/httpServerToTrash)
* [Random Data To Identity Processor To Trash](data/pipelines/randomToIdentityToTrash)


Contributing code
-----------
We welcome contributors, please check out our [guidelines](CONTRIBUTING.md) to get started.
