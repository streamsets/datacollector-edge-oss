/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sdcrecord

import (
	"errors"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"math/big"
)

type SdcRecordField struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
	//For compat with SDC (Always will be set to /)
	Sqpath string `json:"sqpath"`
	Dqpath string `json:"dqpath"`
}

func newSDCRecordField(typ string, value interface{}) *SdcRecordField {
	return &SdcRecordField{Type: typ, Value: value, Sqpath: "/", Dqpath: "/"}
}

type SDCRecord struct {
	Header *common.HeaderImpl `json:"header"`
	Value  *SdcRecordField    `json:"value"`
}

func NewSdcRecordFromRecord(r api.Record) (*SDCRecord, error) {
	var typ string
	var err error = nil
	sdcRecord := new(SDCRecord)

	rootField, _ := r.Get()
	val := rootField.Value
	//Supporting primitives only (and other complex types are simple byte arrays
	// which has to be parsed out in SDC),
	// as currently we don't want to support any parsing inside Data Collector Edge
	//It is the responsibility of stages to basically create records with either primitives or
	//pass in complex types as byte arrays
	switch val.(type) {
	case string:
		typ = "STRING"
	case []byte:
		typ = "BYTE_ARRAY"
	case byte:
		typ = "BYTE"
	case int8:
		typ = "SHORT"
	case int32:
	case int:
		typ = "INTEGER"
	case int64:
		typ = "LONG"
	case float32:
		typ = "FLOAT"
	case float64:
		typ = "DOUBLE"
	case big.Int:
	case big.Float:
		typ = "DECIMAL"
	default:
		err = errors.New("Unsupported Field Type, cannot serialize")
	}

	if err == nil {
		sdcRecord = &SDCRecord{
			Header: r.GetHeader().(*common.HeaderImpl),
			Value:  newSDCRecordField(typ, val),
		}
	}
	return sdcRecord, err
}

func NewRecordFromSDCRecord(stageContext api.StageContext, sdcRecord *SDCRecord) api.Record {
	record, _ := stageContext.CreateRecord(sdcRecord.Header.GetSourceId(), sdcRecord.Value.Value)
	originalHeaderImpl := sdcRecord.Header
	newHeaderImpl := record.GetHeader().(*common.HeaderImpl)
	//Set Headers to be same as the oldOne
	*newHeaderImpl = *originalHeaderImpl
	return record
}
