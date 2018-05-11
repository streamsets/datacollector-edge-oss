// Copyright 2018 StreamSets Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package common

import (
	"encoding/json"
	"testing"
)

func TestProcessFragmentStages(t *testing.T) {
	pipelineConfiguration := &PipelineConfiguration{}

	err := json.Unmarshal([]byte(samplePipelineConfigWithFragments), pipelineConfiguration)

	if err != nil {
		t.Error(err)
		return
	}

	pipelineConfiguration.ProcessFragmentStages()

	if len(pipelineConfiguration.Stages) != 7 {
		t.Error("Fragment stages not resolved properly")
	}

	if pipelineConfiguration.Stages[0].InstanceName != "OriginEdgeFragment_01_DevRandomRecordSource_01" &&
		pipelineConfiguration.Stages[1].InstanceName != "OriginEdgeFragment_01_DevIdentity_01" &&
		pipelineConfiguration.Stages[2].InstanceName != "originedgeprocessor_01_DevIdentity_01" &&
		pipelineConfiguration.Stages[3].InstanceName != "originedgeprocessor_01_ExpressionEvaluator_01" &&
		pipelineConfiguration.Stages[4].InstanceName != "originedgeprocessor_01_JavaScriptEvaluator_01" &&
		pipelineConfiguration.Stages[5].InstanceName != "Trash_01" &&
		pipelineConfiguration.Stages[6].InstanceName != "destinationedgefragment_01_Trash_01" {
		t.Error("Sorting order is incorrect")
	}
}

var samplePipelineConfigWithFragments = `
{
  "schemaVersion" : 5,
  "version" : 9,
  "pipelineId" : "Edge Pipeline",
  "title" : "Edge Pipeline",
  "description" : "",
  "uuid" : "d7ff5edd-79fb-4d8c-8961-a3f48f2aecac",
  "configuration" : [ {
    "name" : "executionMode",
    "value" : "EDGE"
  }, {
    "name" : "edgeHttpUrl",
    "value" : "http://localhost:18633"
  }, {
    "name" : "deliveryGuarantee",
    "value" : "AT_LEAST_ONCE"
  }, {
    "name" : "startEventStage",
    "value" : "streamsets-datacollector-basic-lib::com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget::1"
  }, {
    "name" : "stopEventStage",
    "value" : "streamsets-datacollector-basic-lib::com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget::1"
  }, {
    "name" : "shouldRetry",
    "value" : true
  }, {
    "name" : "retryAttempts",
    "value" : -1
  }, {
    "name" : "memoryLimit",
    "value" : "${jvm:maxMemoryMB() * 0.85}"
  }, {
    "name" : "memoryLimitExceeded",
    "value" : "LOG"
  }, {
    "name" : "notifyOnStates",
    "value" : [ "RUN_ERROR", "STOPPED", "FINISHED" ]
  }, {
    "name" : "emailIDs",
    "value" : [ ]
  }, {
    "name" : "constants",
    "value" : [ ]
  }, {
    "name" : "badRecordsHandling",
    "value" : "streamsets-datacollector-basic-lib::com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget::1"
  }, {
    "name" : "errorRecordPolicy",
    "value" : "ORIGINAL_RECORD"
  }, {
    "name" : "workerCount",
    "value" : 0
  }, {
    "name" : "clusterSlaveMemory",
    "value" : 2048
  }, {
    "name" : "clusterSlaveJavaOpts",
    "value" : "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -Dlog4j.debug"
  }, {
    "name" : "clusterLauncherEnv",
    "value" : [ ]
  }, {
    "name" : "mesosDispatcherURL",
    "value" : null
  }, {
    "name" : "hdfsS3ConfDir",
    "value" : null
  }, {
    "name" : "rateLimit",
    "value" : 0
  }, {
    "name" : "maxRunners",
    "value" : 0
  }, {
    "name" : "shouldCreateFailureSnapshot",
    "value" : true
  }, {
    "name" : "runnerIdleTIme",
    "value" : 60
  }, {
    "name" : "webhookConfigs",
    "value" : [ ]
  }, {
    "name" : "sparkConfigs",
    "value" : [ ]
  }, {
    "name" : "statsAggregatorStage",
    "value" : ""
  } ],
  "uiInfo" : { },
  "fragments" : [ {
    "schemaVersion" : 5,
    "version" : 1,
    "fragmentId" : "destinationedgefragmentcf0b8882-a457-469d-805c-f5875027b23a",
    "fragmentInstanceId" : "destinationedgefragment_01",
    "title" : "destination edge fragment",
    "description" : "",
    "uuid" : "018cd411-5b26-4905-a663-c537126e418d",
    "configuration" : [ {
      "name" : "executionMode",
      "value" : "EDGE"
    }, {
      "name" : "constants",
      "value" : [ ]
    } ],
    "uiInfo" : {
      "fragmentStageConfiguration" : {
        "instanceName" : "destinationedgefragmentcf0b8882a457469d805cf5875027b23a_destinationedgefragment_01",
        "library" : "streamsets-datacollector-basic-lib",
        "stageName" : "com_streamsets_pipeline_stage_destination_fragment_FragmentTarget",
        "stageVersion" : "1",
        "configuration" : [ {
          "name" : "conf.fragmentId",
          "value" : "destinationedgefragmentcf0b8882-a457-469d-805c-f5875027b23a"
        }, {
          "name" : "conf.fragmentInstanceId",
          "value" : "destinationedgefragment_01"
        } ],
        "uiInfo" : {
          "label" : "destination edge fragment",
          "description" : "",
          "xPos" : 60,
          "yPos" : 50,
          "stageType" : "TARGET",
          "outputStreamLabels" : null,
          "rawSource" : null,
          "firstStageInstanceName" : "Trash_01",
          "fragmentId" : "destinationedgefragmentcf0b8882-a457-469d-805c-f5875027b23a",
          "fragmentInstanceId" : "destinationedgefragment_01",
          "fragmentGroupStage" : true,
          "fragmentName" : "destination edge fragment",
          "pipelineId" : "5f5b7df8-dd07-4449-b3e8-190eff888477:demo",
          "pipelineCommitId" : "91baebfc-f7f1-4587-9c47-33e7c30132f9:demo",
          "pipelineCommitLabel" : "v1"
        },
        "inputLanes" : [ ],
        "outputLanes" : [ ],
        "eventLanes" : [ ],
        "services" : [ ]
      }
    },
    "stages" : [ {
      "instanceName" : "destinationedgefragment_01_Trash_01",
      "library" : "streamsets-datacollector-basic-lib",
      "stageName" : "com_streamsets_pipeline_stage_destination_devnull_NullDTarget",
      "stageVersion" : "1",
      "configuration" : [ ],
      "uiInfo" : {
        "outputStreamLabels" : null,
        "yPos" : 50,
        "stageType" : "TARGET",
        "rawSource" : null,
        "fragmentId" : "destinationedgefragmentcf0b8882-a457-469d-805c-f5875027b23a",
        "description" : "",
        "label" : "Trash 1",
        "xPos" : 60,
        "fragmentInstanceId" : "destinationedgefragment_01"
      },
      "inputLanes" : [ "originedgeprocessor_01_ExpressionEvaluator_01OutputLane15242481570070" ],
      "outputLanes" : [ ],
      "eventLanes" : [ ],
      "services" : [ ]
    } ],
    "info" : {
      "pipelineId" : "destinationedgefragmentcf0b8882-a457-469d-805c-f5875027b23a",
      "title" : "destination edge fragment",
      "description" : "",
      "created" : 1524248192611,
      "lastModified" : 1524248192611,
      "creator" : "demo@demo",
      "lastModifier" : "demo@demo",
      "lastRev" : "0",
      "uuid" : "018cd411-5b26-4905-a663-c537126e418d",
      "valid" : false,
      "metadata" : null,
      "name" : "destinationedgefragmentcf0b8882-a457-469d-805c-f5875027b23a",
      "sdcVersion" : "3.3.0.0-SNAPSHOT",
      "sdcId" : "821a93c6-44b1-11e8-aeda-b1308f4e42ee"
    },
    "metadata" : {
      "dpm.pipeline.rules.id" : "cce27c52-cc0e-4a58-a84d-f11894c426b3:demo",
      "dpm.pipeline.id" : "5f5b7df8-dd07-4449-b3e8-190eff888477:demo",
      "dpm.base.url" : "http://localhost:18631",
      "dpm.pipeline.version" : "1",
      "dpm.pipeline.commit.id" : "91baebfc-f7f1-4587-9c47-33e7c30132f9:demo",
      "labels" : [ "Destinations" ]
    },
    "valid" : true,
    "issues" : {
      "pipelineIssues" : [ ],
      "stageIssues" : { },
      "issueCount" : 0
    },
    "previewable" : false,
    "fragments" : [ ]
  }, {
    "schemaVersion" : 5,
    "version" : 1,
    "fragmentId" : "originedgeprocessoree1f32a2-7e0b-4706-bd63-e85e72f58c35",
    "fragmentInstanceId" : "originedgeprocessor_01",
    "title" : "origin edge processor",
    "description" : "",
    "uuid" : "e8d5eaee-975e-42c5-b8c1-4d11ab5a2ac3",
    "configuration" : [ {
      "name" : "executionMode",
      "value" : "EDGE"
    }, {
      "name" : "constants",
      "value" : [ ]
    } ],
    "uiInfo" : {
      "fragmentStageConfiguration" : {
        "instanceName" : "originedgeprocessoree1f32a27e0b4706bd63e85e72f58c35_originedgeprocessor_01",
        "library" : "streamsets-datacollector-basic-lib",
        "stageName" : "com_streamsets_pipeline_stage_processor_fragment_FragmentProcessor",
        "stageVersion" : "1",
        "configuration" : [ {
          "name" : "conf.fragmentId",
          "value" : "originedgeprocessoree1f32a2-7e0b-4706-bd63-e85e72f58c35"
        }, {
          "name" : "conf.fragmentInstanceId",
          "value" : "originedgeprocessor_01"
        } ],
        "uiInfo" : {
          "label" : "origin edge processor",
          "description" : "",
          "xPos" : 280,
          "yPos" : 110,
          "stageType" : "PROCESSOR",
          "outputStreamLabels" : null,
          "rawSource" : null,
          "firstStageInstanceName" : "DevIdentity_01",
          "fragmentId" : "originedgeprocessoree1f32a2-7e0b-4706-bd63-e85e72f58c35",
          "fragmentInstanceId" : "originedgeprocessor_01",
          "fragmentGroupStage" : true,
          "fragmentName" : "origin edge processor",
          "pipelineId" : "23153a48-80b3-4737-8fea-414cb632711e:demo",
          "pipelineCommitId" : "63d4f727-1c0f-4057-aabb-8d353e481872:demo",
          "pipelineCommitLabel" : "v1"
        },
        "inputLanes" : [ ],
        "outputLanes" : [ "originedgeprocessor_01_ExpressionEvaluator_01OutputLane15242481570070", "originedgeprocessor_01_JavaScriptEvaluator_01OutputLane15242481579750" ],
        "eventLanes" : [ ],
        "services" : [ ]
      }
    },
    "stages" : [ {
      "instanceName" : "originedgeprocessor_01_DevIdentity_01",
      "library" : "streamsets-datacollector-dev-lib",
      "stageName" : "com_streamsets_pipeline_stage_processor_identity_IdentityProcessor",
      "stageVersion" : "1",
      "configuration" : [ {
        "name" : "stageOnRecordError",
        "value" : "TO_ERROR"
      }, {
        "name" : "stageRequiredFields",
        "value" : [ ]
      }, {
        "name" : "stageRecordPreconditions",
        "value" : [ ]
      } ],
      "uiInfo" : {
        "outputStreamLabels" : null,
        "yPos" : 130,
        "stageType" : "PROCESSOR",
        "rawSource" : null,
        "fragmentId" : "originedgeprocessoree1f32a2-7e0b-4706-bd63-e85e72f58c35",
        "description" : "",
        "label" : "Dev Identity 1",
        "xPos" : 51,
        "fragmentInstanceId" : "originedgeprocessor_01"
      },
      "inputLanes" : [ "OriginEdgeFragment_01_DevIdentity_01OutputLane15242481235270" ],
      "outputLanes" : [ "originedgeprocessor_01_DevIdentity_01OutputLane15242481623610" ],
      "eventLanes" : [ ],
      "services" : [ ]
    }, {
      "instanceName" : "originedgeprocessor_01_ExpressionEvaluator_01",
      "library" : "streamsets-datacollector-basic-lib",
      "stageName" : "com_streamsets_pipeline_stage_processor_expression_ExpressionDProcessor",
      "stageVersion" : "2",
      "configuration" : [ {
        "name" : "expressionProcessorConfigs",
        "value" : [ {
          "fieldToSet" : "/",
          "expression" : "${record:value('/')}"
        } ]
      }, {
        "name" : "headerAttributeConfigs",
        "value" : [ { } ]
      }, {
        "name" : "fieldAttributeConfigs",
        "value" : [ {
          "fieldToSet" : "/"
        } ]
      }, {
        "name" : "stageOnRecordError",
        "value" : "TO_ERROR"
      }, {
        "name" : "stageRequiredFields",
        "value" : [ ]
      }, {
        "name" : "stageRecordPreconditions",
        "value" : [ ]
      } ],
      "uiInfo" : {
        "outputStreamLabels" : null,
        "yPos" : 50,
        "stageType" : "PROCESSOR",
        "rawSource" : null,
        "fragmentId" : "originedgeprocessoree1f32a2-7e0b-4706-bd63-e85e72f58c35",
        "description" : "",
        "label" : "Expression Evaluator 1",
        "xPos" : 280,
        "fragmentInstanceId" : "originedgeprocessor_01"
      },
      "inputLanes" : [ "originedgeprocessor_01_DevIdentity_01OutputLane15242481623610" ],
      "outputLanes" : [ "originedgeprocessor_01_ExpressionEvaluator_01OutputLane15242481570070" ],
      "eventLanes" : [ ],
      "services" : [ ]
    }, {
      "instanceName" : "originedgeprocessor_01_JavaScriptEvaluator_01",
      "library" : "streamsets-datacollector-basic-lib",
      "stageName" : "com_streamsets_pipeline_stage_processor_javascript_JavaScriptDProcessor",
      "stageVersion" : "2",
      "configuration" : [ {
        "name" : "processingMode",
        "value" : "BATCH"
      }, {
        "name" : "initScript",
        "value" : "/**\n * Available Objects:\n * \n *  state: a dict that is preserved between invocations of this script. \n *        Useful for caching bits of data e.g. counters and long-lived resources.\n *\n *  log.<loglevel>(msg, obj...): use instead of print to send log messages to the log4j log instead of stdout.\n *                               loglevel is any log4j level: e.g. info, error, warn, trace.\n *   sdcFunctions.getFieldNull(Record, 'field path'): Receive a constant defined above \n *                          to check if the field is typed field with value null\n *   sdcFunctions.createMap(boolean listMap): Create a map for use as a field in a record. \n *                          Pass true to this function to create a list map (ordered map)\n */\n\n// state['connection'] = new Connection().open();\n\n"
      }, {
        "name" : "script",
        "value" : "/**\n * Available constants: \n *   They are to assign a type to a field with a value null.\n *   NULL_BOOLEAN, NULL_CHAR, NULL_BYTE, NULL_SHORT, NULL_INTEGER, NULL_LONG\n *   NULL_FLOATNULL_DOUBLE, NULL_DATE, NULL_DATETIME, NULL_TIME, NULL_DECIMAL\n *   NULL_BYTE_ARRAY, NULL_STRING, NULL_LIST, NULL_MAP\n *\n * Available Objects:\n * \n *  records: an array of records to process, depending on the JavaScript processor\n *           processing mode it may have 1 record or all the records in the batch.\n *\n *  state: a dict that is preserved between invocations of this script. \n *        Useful for caching bits of data e.g. counters.\n *\n *  log.<loglevel>(msg, obj...): use instead of print to send log messages to the log4j log instead of stdout.\n *                               loglevel is any log4j level: e.g. info, error, warn, trace.\n *\n *  output.write(record): writes a record to processor output\n *\n *  error.write(record, message): sends a record to error\n *\n *  sdcFunctions.getFieldNull(Record, 'field path'): Receive a constant defined above\n *                            to check if the field is typed field with value null\n *  sdcFunctions.createRecord(String recordId): Creates a new record.\n *                            Pass a recordId to uniquely identify the record and include enough information to track down the record source. \n *  sdcFunctions.createMap(boolean listMap): Create a map for use as a field in a record.\n *                            Pass true to this function to create a list map (ordered map)\n *\n *  sdcFunctions.createEvent(String type, int version): Creates a new event.\n *                            Create new empty event with standard headers.\n *  sdcFunctions.toEvent(Record): Send event to event stream\n *                            Only events created with sdcFunctions.createEvent are supported.\n *  sdcFunctions.isPreview(): Determine if pipeline is in preview mode.\n *\n * Available Record Header Variables:n *\n *  record.attributes: a map of record header attributes.\n *\n *  record.<header name>: get the value of 'header name'.\n */\n\n// Sample JavaScript code\nfor(var i = 0; i < records.length; i++) {\n  try {\n    // Change record root field value to a STRING value\n    //records[i].value = 'Hello ' + i;\n\n\n    // Change record root field value to a MAP value and create an entry\n    //records[i].value = { V : 'Hello' };\n\n    // Access a MAP entry\n    //records[i].value.X = records[i].value['V'] + ' World';\n\n    // Modify a MAP entry\n    //records[i].value.V = 5;\n\n    // Create an ARRAY entry\n    //records[i].value.A = ['Element 1', 'Element 2'];\n\n    // Access a Array entry\n    //records[i].value.B = records[i].value['A'][0];\n\n    // Modify an existing ARRAY entry\n    //records[i].value.A[0] = 100;\n\n    // Assign a integer type to a field and value null\n    // records[i].value.null_int = NULL_INTEGER \n\n    // Check if the field is NULL_INTEGER. If so, assign a value \n    // if(sdcFunctions.getFieldNull(records[i], '/null_int') == NULL_INTEGER)\n    //    records[i].value.null_int = 123\n\n    // Create a new record with map field \n    // var newRecord = sdcFunctions.createRecord(records[i].sourceId + ':newRecordId');\n    // newRecord.value = {'field1' : 'val1', 'field2' : 'val2'};\n    // output.write(newRecord);\n    // Create a new map and add it to the original record\n    // var newMap = sdcFunctions.createMap(true);\n    // newMap['key'] = 'value';\n    // records[i].value['b'] = newMap;\n\n    //Applies if the source uses WHOLE_FILE as data format\n    //var input_stream = record.value['fileRef'].getInputStream();\n    //try {\n      //input_stream.read(); //Process the input stream\n    //} finally{\n      //input_stream.close()\n    //}\n\n    // Modify a header attribute entry\n    // records[i].attributes['name'] = records[i].attributes['first_name'] + ' ' + records[i].attributes['last_name']    //\n\n    // Get a record header with field names ex. get sourceId and errorCode\n    // var sourceId = records[i].sourceId\n    // var errorCode = ''\n    // if(records[i].errorCode) {\n    //     errorCode = records[i].errorCode\n    // }\n\n    // Write record to processor output\n    output.write(records[i]);\n  } catch (e) {\n    // Send record to error\n    error.write(records[i], e);\n  }\n}\n"
      }, {
        "name" : "destroyScript",
        "value" : "/**\n * Available Objects:\n * \n *  state: a dict that is preserved between invocations of this script. \n *        Useful for caching bits of data e.g. counters and long-lived resources.\n *\n *  log.<loglevel>(msg, obj...): use instead of print to send log messages to the log4j log instead of stdout.\n *                               loglevel is any log4j level: e.g. info, error, warn, trace.\n *   sdcFunctions.getFieldNull(Record, 'field path'): Receive a constant defined above \n *                          to check if the field is typed field with value null\n *   sdcFunctions.createMap(boolean listMap): Create a map for use as a field in a record. \n *                          Pass true to this function to create a list map (ordered map)\n *   sdcFunctions.createEvent(String type, int version): Creates a new event.\n *                          Create new empty event with standard headers.\n *   sdcFunctions.toEvent(Record): Send event to event stream\n *                          Only events created with sdcFunctions.createEvent are supported.\n */\n\n// state['connection'].close();\n\n"
      }, {
        "name" : "stageOnRecordError",
        "value" : "TO_ERROR"
      }, {
        "name" : "stageRequiredFields",
        "value" : [ ]
      }, {
        "name" : "stageRecordPreconditions",
        "value" : [ ]
      } ],
      "uiInfo" : {
        "outputStreamLabels" : null,
        "yPos" : 259,
        "stageType" : "PROCESSOR",
        "rawSource" : null,
        "fragmentId" : "originedgeprocessoree1f32a2-7e0b-4706-bd63-e85e72f58c35",
        "description" : "",
        "label" : "JavaScript Evaluator 1",
        "xPos" : 286,
        "fragmentInstanceId" : "originedgeprocessor_01"
      },
      "inputLanes" : [ "originedgeprocessor_01_DevIdentity_01OutputLane15242481623610" ],
      "outputLanes" : [ "originedgeprocessor_01_JavaScriptEvaluator_01OutputLane15242481579750" ],
      "eventLanes" : [ ],
      "services" : [ ]
    } ],
    "info" : {
      "pipelineId" : "originedgeprocessoree1f32a2-7e0b-4706-bd63-e85e72f58c35",
      "title" : "origin edge processor",
      "description" : "",
      "created" : 1524248148960,
      "lastModified" : 1524248148960,
      "creator" : "demo@demo",
      "lastModifier" : "demo@demo",
      "lastRev" : "0",
      "uuid" : "e8d5eaee-975e-42c5-b8c1-4d11ab5a2ac3",
      "valid" : false,
      "metadata" : null,
      "name" : "originedgeprocessoree1f32a2-7e0b-4706-bd63-e85e72f58c35",
      "sdcVersion" : "3.3.0.0-SNAPSHOT",
      "sdcId" : "821a93c6-44b1-11e8-aeda-b1308f4e42ee"
    },
    "metadata" : {
      "dpm.pipeline.rules.id" : "dee6a79a-744f-46ca-af31-78ad4db8e0ff:demo",
      "dpm.pipeline.id" : "23153a48-80b3-4737-8fea-414cb632711e:demo",
      "dpm.base.url" : "http://localhost:18631",
      "dpm.pipeline.version" : "1",
      "dpm.pipeline.commit.id" : "63d4f727-1c0f-4057-aabb-8d353e481872:demo",
      "labels" : [ "Processors" ]
    },
    "valid" : true,
    "issues" : {
      "pipelineIssues" : [ ],
      "stageIssues" : { },
      "issueCount" : 0
    },
    "previewable" : false,
    "fragments" : [ ]
  }, {
    "schemaVersion" : 5,
    "version" : 1,
    "fragmentId" : "OriginEdgeFragment2034cb02-fce5-4b9f-90f4-ace09e2b3dde",
    "fragmentInstanceId" : "OriginEdgeFragment_01",
    "title" : "Origin Edge Fragment",
    "description" : "",
    "uuid" : "8f57899c-4304-4c7d-8825-695160717f4c",
    "configuration" : [ {
      "name" : "executionMode",
      "value" : "EDGE"
    }, {
      "name" : "constants",
      "value" : [ ]
    } ],
    "uiInfo" : {
      "fragmentStageConfiguration" : {
        "instanceName" : "OriginEdgeFragment2034cb02fce54b9f90f4ace09e2b3dde_OriginEdgeFragment_01",
        "library" : "streamsets-datacollector-basic-lib",
        "stageName" : "com_streamsets_pipeline_stage_origin_fragment_FragmentSource",
        "stageVersion" : "1",
        "configuration" : [ {
          "name" : "conf.fragmentId",
          "value" : "OriginEdgeFragment2034cb02-fce5-4b9f-90f4-ace09e2b3dde"
        }, {
          "name" : "conf.fragmentInstanceId",
          "value" : "OriginEdgeFragment_01"
        } ],
        "uiInfo" : {
          "label" : "Origin Edge Fragment",
          "description" : "",
          "xPos" : 500,
          "yPos" : 50,
          "stageType" : "SOURCE",
          "outputStreamLabels" : null,
          "rawSource" : null,
          "fragmentId" : "OriginEdgeFragment2034cb02-fce5-4b9f-90f4-ace09e2b3dde",
          "fragmentInstanceId" : "OriginEdgeFragment_01",
          "fragmentGroupStage" : true,
          "fragmentName" : "Origin Edge Fragment",
          "pipelineId" : "f5b5da79-f971-4b95-882a-4947fc3e0525:demo",
          "pipelineCommitId" : "c5612a75-59bc-4851-a815-41bc99fa1c8e:demo",
          "pipelineCommitLabel" : "v1"
        },
        "inputLanes" : [ ],
        "outputLanes" : [ "OriginEdgeFragment_01_DevIdentity_01OutputLane15242481235270" ],
        "eventLanes" : [ ],
        "services" : [ ]
      }
    },
    "stages" : [ {
      "instanceName" : "OriginEdgeFragment_01_DevRandomRecordSource_01",
      "library" : "streamsets-datacollector-dev-lib",
      "stageName" : "com_streamsets_pipeline_stage_devtest_RandomSource",
      "stageVersion" : "1",
      "configuration" : [ {
        "name" : "fields",
        "value" : "a,b,c"
      }, {
        "name" : "delay",
        "value" : 1000
      }, {
        "name" : "maxRecordsToGenerate",
        "value" : 922337203685
      }, {
        "name" : "stageOnRecordError",
        "value" : "TO_ERROR"
      } ],
      "uiInfo" : {
        "outputStreamLabels" : null,
        "yPos" : 50,
        "stageType" : "SOURCE",
        "rawSource" : null,
        "fragmentId" : "OriginEdgeFragment2034cb02-fce5-4b9f-90f4-ace09e2b3dde",
        "description" : "",
        "label" : "Dev Random Record Source 1",
        "xPos" : 60,
        "fragmentInstanceId" : "OriginEdgeFragment_01"
      },
      "inputLanes" : [ ],
      "outputLanes" : [ "OriginEdgeFragment_01_DevRandomRecordSource_01OutputLane15242481179480" ],
      "eventLanes" : [ ],
      "services" : [ ]
    }, {
      "instanceName" : "OriginEdgeFragment_01_DevIdentity_01",
      "library" : "streamsets-datacollector-dev-lib",
      "stageName" : "com_streamsets_pipeline_stage_processor_identity_IdentityProcessor",
      "stageVersion" : "1",
      "configuration" : [ {
        "name" : "stageOnRecordError",
        "value" : "TO_ERROR"
      }, {
        "name" : "stageRequiredFields",
        "value" : [ ]
      }, {
        "name" : "stageRecordPreconditions",
        "value" : [ ]
      } ],
      "uiInfo" : {
        "outputStreamLabels" : null,
        "yPos" : 50,
        "stageType" : "PROCESSOR",
        "rawSource" : null,
        "fragmentId" : "OriginEdgeFragment2034cb02-fce5-4b9f-90f4-ace09e2b3dde",
        "description" : "",
        "label" : "Dev Identity 1",
        "xPos" : 280,
        "fragmentInstanceId" : "OriginEdgeFragment_01"
      },
      "inputLanes" : [ "OriginEdgeFragment_01_DevRandomRecordSource_01OutputLane15242481179480" ],
      "outputLanes" : [ "OriginEdgeFragment_01_DevIdentity_01OutputLane15242481235270" ],
      "eventLanes" : [ ],
      "services" : [ ]
    } ],
    "info" : {
      "pipelineId" : "OriginEdgeFragment2034cb02-fce5-4b9f-90f4-ace09e2b3dde",
      "title" : "Origin Edge Fragment",
      "description" : "",
      "created" : 1524248095650,
      "lastModified" : 1524248095650,
      "creator" : "demo@demo",
      "lastModifier" : "demo@demo",
      "lastRev" : "0",
      "uuid" : "8f57899c-4304-4c7d-8825-695160717f4c",
      "valid" : false,
      "metadata" : null,
      "name" : "OriginEdgeFragment2034cb02-fce5-4b9f-90f4-ace09e2b3dde",
      "sdcVersion" : "3.3.0.0-SNAPSHOT",
      "sdcId" : "821a93c6-44b1-11e8-aeda-b1308f4e42ee"
    },
    "metadata" : {
      "dpm.pipeline.rules.id" : "bed5bd8d-b1fd-40a7-b0e9-dcc364e3801f:demo",
      "dpm.pipeline.id" : "f5b5da79-f971-4b95-882a-4947fc3e0525:demo",
      "dpm.base.url" : "http://localhost:18631",
      "dpm.pipeline.version" : "1",
      "dpm.pipeline.commit.id" : "c5612a75-59bc-4851-a815-41bc99fa1c8e:demo",
      "labels" : [ "Origins" ]
    },
    "valid" : true,
    "issues" : {
      "pipelineIssues" : [ ],
      "stageIssues" : { },
      "issueCount" : 0
    },
    "previewable" : false,
    "fragments" : [ ]
  } ],
  "stages" : [ {
    "instanceName" : "destinationedgefragmentcf0b8882a457469d805cf5875027b23a_destinationedgefragment_01",
    "library" : "streamsets-datacollector-basic-lib",
    "stageName" : "com_streamsets_pipeline_stage_destination_fragment_FragmentTarget",
    "stageVersion" : "1",
    "configuration" : [ {
      "name" : "conf.fragmentId",
      "value" : "destinationedgefragmentcf0b8882-a457-469d-805c-f5875027b23a"
    }, {
      "name" : "conf.fragmentInstanceId",
      "value" : "destinationedgefragment_01"
    } ],
    "uiInfo" : {
      "yPos" : 40,
      "stageType" : "TARGET",
      "rawSource" : null,
      "pipelineCommitLabel" : "v1",
      "description" : "",
      "firstStageInstanceName" : "Trash_01",
      "label" : "destination edge fragment",
      "xPos" : 500,
      "fragmentGroupStage" : true,
      "pipelineId" : "5f5b7df8-dd07-4449-b3e8-190eff888477:demo",
      "outputStreamLabels" : null,
      "fragmentName" : "destination edge fragment",
      "fragmentId" : "destinationedgefragmentcf0b8882-a457-469d-805c-f5875027b23a",
      "fragmentInstanceId" : "destinationedgefragment_01",
      "pipelineCommitId" : "91baebfc-f7f1-4587-9c47-33e7c30132f9:demo"
    },
    "inputLanes" : [ "originedgeprocessor_01_ExpressionEvaluator_01OutputLane15242481570070" ],
    "outputLanes" : [ ],
    "eventLanes" : [ ],
    "services" : [ ]
  }, {
    "instanceName" : "originedgeprocessoree1f32a27e0b4706bd63e85e72f58c35_originedgeprocessor_01",
    "library" : "streamsets-datacollector-basic-lib",
    "stageName" : "com_streamsets_pipeline_stage_processor_fragment_FragmentProcessor",
    "stageVersion" : "1",
    "configuration" : [ {
      "name" : "conf.fragmentId",
      "value" : "originedgeprocessoree1f32a2-7e0b-4706-bd63-e85e72f58c35"
    }, {
      "name" : "conf.fragmentInstanceId",
      "value" : "originedgeprocessor_01"
    } ],
    "uiInfo" : {
      "yPos" : 110,
      "stageType" : "PROCESSOR",
      "rawSource" : null,
      "pipelineCommitLabel" : "v1",
      "description" : "",
      "firstStageInstanceName" : "DevIdentity_01",
      "label" : "origin edge processor",
      "xPos" : 280,
      "fragmentGroupStage" : true,
      "pipelineId" : "23153a48-80b3-4737-8fea-414cb632711e:demo",
      "outputStreamLabels" : null,
      "fragmentName" : "origin edge processor",
      "fragmentId" : "originedgeprocessoree1f32a2-7e0b-4706-bd63-e85e72f58c35",
      "fragmentInstanceId" : "originedgeprocessor_01",
      "pipelineCommitId" : "63d4f727-1c0f-4057-aabb-8d353e481872:demo"
    },
    "inputLanes" : [ "OriginEdgeFragment_01_DevIdentity_01OutputLane15242481235270" ],
    "outputLanes" : [ "originedgeprocessor_01_ExpressionEvaluator_01OutputLane15242481570070", "originedgeprocessor_01_JavaScriptEvaluator_01OutputLane15242481579750" ],
    "eventLanes" : [ ],
    "services" : [ ]
  }, {
    "instanceName" : "OriginEdgeFragment2034cb02fce54b9f90f4ace09e2b3dde_OriginEdgeFragment_01",
    "library" : "streamsets-datacollector-basic-lib",
    "stageName" : "com_streamsets_pipeline_stage_origin_fragment_FragmentSource",
    "stageVersion" : "1",
    "configuration" : [ {
      "name" : "conf.fragmentId",
      "value" : "OriginEdgeFragment2034cb02-fce5-4b9f-90f4-ace09e2b3dde"
    }, {
      "name" : "conf.fragmentInstanceId",
      "value" : "OriginEdgeFragment_01"
    } ],
    "uiInfo" : {
      "yPos" : 50,
      "stageType" : "SOURCE",
      "rawSource" : null,
      "pipelineCommitLabel" : "v1",
      "description" : "",
      "label" : "Origin Edge Fragment",
      "xPos" : 60,
      "fragmentGroupStage" : true,
      "pipelineId" : "f5b5da79-f971-4b95-882a-4947fc3e0525:demo",
      "outputStreamLabels" : null,
      "fragmentName" : "Origin Edge Fragment",
      "fragmentId" : "OriginEdgeFragment2034cb02-fce5-4b9f-90f4-ace09e2b3dde",
      "fragmentInstanceId" : "OriginEdgeFragment_01",
      "pipelineCommitId" : "c5612a75-59bc-4851-a815-41bc99fa1c8e:demo"
    },
    "inputLanes" : [ ],
    "outputLanes" : [ "OriginEdgeFragment_01_DevIdentity_01OutputLane15242481235270" ],
    "eventLanes" : [ ],
    "services" : [ ]
  }, {
    "instanceName" : "Trash_01",
    "library" : "streamsets-datacollector-basic-lib",
    "stageName" : "com_streamsets_pipeline_stage_destination_devnull_NullDTarget",
    "stageVersion" : "1",
    "configuration" : [ ],
    "uiInfo" : {
      "outputStreamLabels" : null,
      "yPos" : 170,
      "stageType" : "TARGET",
      "rawSource" : null,
      "description" : "",
      "label" : "Trash 1",
      "xPos" : 500
    },
    "inputLanes" : [ "originedgeprocessor_01_JavaScriptEvaluator_01OutputLane15242481579750" ],
    "outputLanes" : [ ],
    "eventLanes" : [ ],
    "services" : [ ]
  } ],
  "errorStage" : {
    "instanceName" : "Discard_ErrorStage",
    "library" : "streamsets-datacollector-basic-lib",
    "stageName" : "com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget",
    "stageVersion" : "1",
    "configuration" : [ ],
    "uiInfo" : {
      "outputStreamLabels" : null,
      "yPos" : 50,
      "stageType" : "TARGET",
      "rawSource" : null,
      "description" : "",
      "label" : "Error Records - Discard",
      "xPos" : 772
    },
    "inputLanes" : [ ],
    "outputLanes" : [ ],
    "eventLanes" : [ ],
    "services" : [ ]
  },
  "info" : {
    "pipelineId" : "Edge Pipeline",
    "title" : "Edge Pipeline",
    "description" : "",
    "created" : 1524257698460,
    "lastModified" : 1524257698509,
    "creator" : "demo@demo",
    "lastModifier" : "demo@demo",
    "lastRev" : "0",
    "uuid" : "d7ff5edd-79fb-4d8c-8961-a3f48f2aecac",
    "valid" : true,
    "metadata" : {
      "dpm.pipeline.rules.id" : "941cde27-3c48-46ca-a83f-e4b9ddb6a2b6:demo",
      "dpm.pipeline.id" : "b133e543-6213-477e-a574-eb77fe655651:demo",
      "dpm.base.url" : "http://localhost:18631",
      "dpm.pipeline.version" : "1",
      "dpm.pipeline.commit.id" : "fcaf979d-ecef-41b7-abf5-fae7c412b1b9:demo",
      "lastConfigId" : "d7ff5edd-79fb-4d8c-8961-a3f48f2aecac",
      "lastRulesId" : "1a0b9736-7c2d-4484-8ad2-83eede216d51"
    },
    "name" : "Edge Pipeline",
    "sdcVersion" : "3.3.0.0-SNAPSHOT",
    "sdcId" : "821a93c6-44b1-11e8-aeda-b1308f4e42ee"
  },
  "metadata" : {
    "dpm.pipeline.rules.id" : "941cde27-3c48-46ca-a83f-e4b9ddb6a2b6:demo",
    "dpm.pipeline.id" : "b133e543-6213-477e-a574-eb77fe655651:demo",
    "dpm.base.url" : "http://localhost:18631",
    "dpm.pipeline.version" : "1",
    "dpm.pipeline.commit.id" : "fcaf979d-ecef-41b7-abf5-fae7c412b1b9:demo",
    "lastConfigId" : "d7ff5edd-79fb-4d8c-8961-a3f48f2aecac",
    "lastRulesId" : "1a0b9736-7c2d-4484-8ad2-83eede216d51"
  },
  "statsAggregatorStage" : null,
  "startEventStages" : [ {
    "instanceName" : "Discard_StartEventStage",
    "library" : "streamsets-datacollector-basic-lib",
    "stageName" : "com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget",
    "stageVersion" : "1",
    "configuration" : [ ],
    "uiInfo" : {
      "outputStreamLabels" : null,
      "yPos" : 50,
      "stageType" : "TARGET",
      "rawSource" : null,
      "description" : "",
      "label" : "Start Event - Discard",
      "xPos" : 280
    },
    "inputLanes" : [ ],
    "outputLanes" : [ ],
    "eventLanes" : [ ],
    "services" : [ ]
  } ],
  "stopEventStages" : [ {
    "instanceName" : "Discard_StopEventStage",
    "library" : "streamsets-datacollector-basic-lib",
    "stageName" : "com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget",
    "stageVersion" : "1",
    "configuration" : [ ],
    "uiInfo" : {
      "outputStreamLabels" : null,
      "yPos" : 50,
      "stageType" : "TARGET",
      "rawSource" : null,
      "description" : "",
      "label" : "Stop Event - Discard",
      "xPos" : 280
    },
    "inputLanes" : [ ],
    "outputLanes" : [ ],
    "eventLanes" : [ ],
    "services" : [ ]
  } ],
  "valid" : true,
  "issues" : {
    "pipelineIssues" : [ ],
    "stageIssues" : { },
    "issueCount" : 0
  },
  "previewable" : true
}
`
