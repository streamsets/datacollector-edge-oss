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

## Origins
* [Directory Spooler](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Origins/Directory.html#concept_qcq_54n_jq)
* [File Tail](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Origins/FileTail.html#concept_n1y_qyp_5q)
* [gRPC Client](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Origins/gRPCClient.html)
* [HTTP Client](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Origins/HTTPClient.html#concept_wk4_bjz_5r)
* [HTTP Server](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Origins/HTTPServer.html)
* [MQTT](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Origins/MQTTSubscriber.html#concept_ukz_3vt_lz)
* [System Metrics](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Origins/SystemMetrics.html#concept_gzy_gmv_32b)
* [WebSocket Client](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Origins/WebSocketClient.html#concept_unk_nzk_fbb)
* [Windows Event Log](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Origins/WindowsLog.html#concept_agf_5jv_sbb)

## Destinations
* [Amazon Kinesis](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Destinations/KinProducer.html#concept_swk_h1j_yr)
* [Amazon Kinesis Firehose](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Destinations/KinFirehose.html#concept_bjv_dpk_kv)
* [Amazon S3](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Destinations/AmazonS3.html#concept_avx_bnq_rt)
* [Apache Kafka](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Destinations/KProducer.html#concept_oq2_5jl_zq)
* [Azure Event Hub](https://streamsets.com/documentation/datacollector/latest/help//datacollector/UserGuide/Destinations/AzureEventHubProducer.html#concept_xq5_d5q_1bb)
* [Azure IoT Hub](https://streamsets.com/documentation/datacollector/latest/help//datacollector/UserGuide/Destinations/AzureIoTHub.html#concept_pnd_jkq_1bb)
* [CoAP](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Destinations/CoAPClient.html#concept_hw5_s3n_sz)
* [HTTP Client](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Destinations/HTTPClient.html#concept_khl_sg5_lz)
* [InfluxDB](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Destinations/InfluxDB.html#concept_inf_db_sr)
* [MQTT](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Destinations/MQTTPublisher.html#concept_odz_txt_lz)
* [WebSocket Client](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Destinations/WebSocketClient.html#concept_l4d_mjn_lz)

## Processors
* [Delay](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Processors/Delay.html#concept_ez5_pvf_wbb)
* [Expression Evaluator](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Processors/Expression.html#concept_zm2_pp3_wq)
* [Field Remover](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Processors/FieldRemover.html#concept_jdd_blr_wq)
* [HTTP Client](https://streamsets.com/documentation/datacollector/latest/help//datacollector/UserGuide/Processors/HTTPClient.html#concept_ghx_ypr_fw)
* [JavaScript Evaluator](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Processors/JavaScript.html#concept_n2p_jgf_lr)
* [Stream Selector](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Processors/StreamSelector.html#concept_tqv_t5r_wq)
* [TensorFlow Evaluator](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Processors/TensorFlow.html#concept_otg_csh_z2b)

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
