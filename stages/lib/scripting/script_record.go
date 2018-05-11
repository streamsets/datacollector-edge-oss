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
package scripting

import "github.com/streamsets/datacollector-edge/api"

func NewScriptRecord(record api.Record, scriptObject interface{}) (map[string]interface{}, error) {
	var err error
	scriptRecord := map[string]interface{}{
		"record":               record,
		"value":                scriptObject,
		"stageCreator":         record.GetHeader().GetStageCreator(),
		"sourceId":             record.GetHeader().GetSourceId(),
		"previousTrackingId":   record.GetHeader().GetPreviousTrackingId(),
		"attributes":           make(map[string]string),
		"errorDataCollectorId": record.GetHeader().GetErrorDataCollectorId(),
		"errorPipelineName":    record.GetHeader().GetErrorPipelineName(),
		"errorCode":            record.GetHeader().GetErrorMessage(),
		"errorMessage":         record.GetHeader().GetErrorMessage(),
		"errorStage":           record.GetHeader().GetErrorStage(),
		"errorTimestamp":       record.GetHeader().GetErrorTimestamp(),
		"errorStackTrace":      record.GetHeader().GetErrorMessage(),
	}

	attributes := scriptRecord["attributes"].(map[string]string)
	for _, key := range record.GetHeader().GetAttributeNames() {
		attributes[key] = record.GetHeader().GetAttribute(key).(string)
	}

	return scriptRecord, err
}
