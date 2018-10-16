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
package delimitedrecord

import (
	"bytes"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/api/linkedhashmap"
	"github.com/streamsets/datacollector-edge/container/common"
	"testing"
)

const sampleCsvData = `policyID,statecode,county,eq_site_limit,hu_site_limit,fl_site_limit,fr_site_limit,tiv_2011,tiv_2012,eq_site_deductible,hu_site_deductible,fl_site_deductible,fr_site_deductible,point_latitude,point_longitude,line,construction,point_granularity
119736,FL,CLAY COUNTY,498960,498960,498960,498960,498960,792148.9,0,9979.2,0,0,30.102261,-81.711777,Residential,Masonry,1
448094,FL,CLAY COUNTY,1322376.3,1322376.3,1322376.3,1322376.3,1322376.3,1438163.57,0,0,0,0,30.063936,-81.707664,Residential,Masonry,3
206893,FL,CLAY COUNTY,190724.4,190724.4,190724.4,190724.4,190724.4,192476.78,0,0,0,0,30.089579,-81.700455,Residential,Wood,1`

const sampleCustomCsvData = `policyID|statecode|county
119736|FL|CLAY COUNTY
448094|FL|CLAY COUNTY
206893|FL|CLAY COUNTY`

func CreateStageContext() api.StageContext {
	return &common.StageContextImpl{
		StageConfig: &common.StageConfiguration{InstanceName: "Dummy Stage"},
		Parameters:  nil,
	}
}

func TestReadDelimitedRecord_ListType_WithHeader(t *testing.T) {
	sampleDelimitedData := bytes.NewBuffer([]byte(sampleCsvData))

	stageContext := CreateStageContext()
	readerFactoryImpl := &DelimitedReaderFactoryImpl{
		CsvRecordType: List,
		CsvHeader:     WithHeader,
	}
	recordReader, err := readerFactoryImpl.CreateReader(stageContext, sampleDelimitedData, "m")
	if err != nil {
		t.Fatal(err.Error())
	}

	recordCount := 0
	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			t.Fatal(err.Error())
		}

		if record == nil {
			break
		}

		rootField, _ := record.Get()
		if rootField.Type != fieldtype.LIST {
			t.Errorf("Excpeted record type : Map, but received: %s", rootField.Type)
		}

		listField := rootField.Value.([]*api.Field)

		recordCount++

		if recordCount == 1 {
			cell1Value := listField[0].Value.(map[string]*api.Field)
			if cell1Value["header"].Value.(string) != "policyID" {
				t.Errorf("Excpeted field value policyID, but received: %s", cell1Value["header"].Value)
			}
			if cell1Value["value"].Value.(string) != "119736" {
				t.Errorf("Excpeted field value 119736, but received: %s", cell1Value["value"].Value)
			}

			cell2Value := listField[1].Value.(map[string]*api.Field)
			if cell2Value["header"].Value.(string) != "statecode" {
				t.Errorf("Excpeted field value statecode, but received: %s", cell2Value["header"].Value)
			}
			if cell2Value["value"].Value.(string) != "FL" {
				t.Errorf("Excpeted field value FL, but received: %s", cell2Value["value"].Value)
			}

			cell3Value := listField[2].Value.(map[string]*api.Field)
			if cell3Value["header"].Value.(string) != "county" {
				t.Errorf("Excpeted field value county, but received: %s", cell3Value["header"].Value)
			}
			if cell3Value["value"].Value.(string) != "CLAY COUNTY" {
				t.Errorf("Excpeted field value CLAY COUNTY, but received: %s", cell3Value["value"].Value)
			}

			cell4Value := listField[3].Value.(map[string]*api.Field)
			if cell4Value["header"].Value.(string) != "eq_site_limit" {
				t.Errorf("Excpeted field value eq_site_limit, but received: %s", cell4Value["header"].Value)
			}
		}

		if recordCount == 2 {
			cell1Value := listField[0].Value.(map[string]*api.Field)
			if cell1Value["header"].Value.(string) != "policyID" {
				t.Errorf("Excpeted field value policyID, but received: %s", cell1Value["header"].Value)
			}
			if cell1Value["value"].Value.(string) != "448094" {
				t.Errorf("Excpeted field value 448094, but received: %s", cell1Value["value"].Value)
			}
		}

		if recordCount == 3 {
			cell1Value := listField[0].Value.(map[string]*api.Field)
			if cell1Value["header"].Value.(string) != "policyID" {
				t.Errorf("Excpeted field value policyID, but received: %s", cell1Value["header"].Value)
			}
			if cell1Value["value"].Value.(string) != "206893" {
				t.Errorf("Excpeted field value 206893, but received: %s", cell1Value["value"].Value)
			}
		}
	}

	if recordCount != 3 {
		t.Errorf("Excpeted 3 records, but received: %d", recordCount)
	}

	recordReader.Close()
}

func TestReadDelimitedRecord_ListType_IgnoreHeader(t *testing.T) {
	sampleDelimitedData := bytes.NewBuffer([]byte(sampleCsvData))

	stageContext := CreateStageContext()
	readerFactoryImpl := &DelimitedReaderFactoryImpl{
		CsvRecordType: List,
		CsvHeader:     IgnoreHeader,
	}
	recordReader, err := readerFactoryImpl.CreateReader(stageContext, sampleDelimitedData, "m")
	if err != nil {
		t.Fatal(err.Error())
	}

	recordCount := 0
	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			t.Fatal(err.Error())
		}

		if record == nil {
			break
		}

		rootField, _ := record.Get()
		if rootField.Type != fieldtype.LIST {
			t.Errorf("Excpeted record type : Map, but received: %s", rootField.Type)
		}

		listField := rootField.Value.([]*api.Field)

		recordCount++

		if recordCount == 1 {
			cell1Value := listField[0].Value.(map[string]*api.Field)
			if cell1Value["value"].Value.(string) != "119736" {
				t.Errorf("Excpeted field value 119736, but received: %s", cell1Value["value"].Value)
			}
		}

		if recordCount == 2 {
			cell1Value := listField[0].Value.(map[string]*api.Field)
			if cell1Value["value"].Value.(string) != "448094" {
				t.Errorf("Excpeted field value 448094, but received: %s", cell1Value["value"].Value)
			}
		}

		if recordCount == 3 {
			cell1Value := listField[0].Value.(map[string]*api.Field)
			if cell1Value["value"].Value.(string) != "206893" {
				t.Errorf("Excpeted field value 206893, but received: %s", cell1Value["value"].Value)
			}
		}
	}

	if recordCount != 3 {
		t.Errorf("Excpeted 3 records, but received: %d", recordCount)
	}

	recordReader.Close()
}

func TestReadDelimitedRecord_ListType_NoHeader(t *testing.T) {
	sampleDelimitedData := bytes.NewBuffer([]byte(sampleCsvData))

	stageContext := CreateStageContext()
	readerFactoryImpl := &DelimitedReaderFactoryImpl{
		CsvRecordType: List,
		CsvHeader:     NoHeader,
	}
	recordReader, err := readerFactoryImpl.CreateReader(stageContext, sampleDelimitedData, "m")
	if err != nil {
		t.Fatal(err.Error())
	}

	recordCount := 0
	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			t.Fatal(err.Error())
		}

		if record == nil {
			break
		}

		rootField, _ := record.Get()
		if rootField.Type != fieldtype.LIST {
			t.Errorf("Excpeted record type : Map, but received: %s", rootField.Type)
		}

		listField := rootField.Value.([]*api.Field)

		recordCount++

		if recordCount == 1 {
			cell1Value := listField[0].Value.(map[string]*api.Field)
			if cell1Value["value"].Value.(string) != "policyID" {
				t.Errorf("Excpeted field value policyID, but received: %s", cell1Value["value"].Value)
			}

			cell2Value := listField[1].Value.(map[string]*api.Field)
			if cell2Value["value"].Value.(string) != "statecode" {
				t.Errorf("Excpeted field value statecode, but received: %s", cell2Value["value"].Value)
			}

			cell3Value := listField[2].Value.(map[string]*api.Field)
			if cell3Value["value"].Value.(string) != "county" {
				t.Errorf("Excpeted field value county, but received: %s", cell3Value["value"].Value)
			}

			cell4Value := listField[3].Value.(map[string]*api.Field)
			if cell4Value["value"].Value.(string) != "eq_site_limit" {
				t.Errorf("Excpeted field value eq_site_limit, but received: %s", cell4Value["value"].Value)
			}
		}

		if recordCount == 2 {
			cell1Value := listField[0].Value.(map[string]*api.Field)
			if cell1Value["value"].Value.(string) != "119736" {
				t.Errorf("Excpeted field value 119736, but received: %s", cell1Value["value"].Value)
			}
		}

		if recordCount == 3 {
			cell1Value := listField[0].Value.(map[string]*api.Field)
			if cell1Value["value"].Value.(string) != "448094" {
				t.Errorf("Excpeted field value 448094, but received: %s", cell1Value["value"].Value)
			}
		}

		if recordCount == 4 {
			cell1Value := listField[0].Value.(map[string]*api.Field)
			if cell1Value["value"].Value.(string) != "206893" {
				t.Errorf("Excpeted field value 206893, but received: %s", cell1Value["value"].Value)
			}
		}
	}

	if recordCount != 4 {
		t.Errorf("Excpeted 3 records, but received: %d", recordCount)
	}

	recordReader.Close()
}

func TestReadDelimitedRecord_SkipLines(t *testing.T) {
	sampleDelimitedData := bytes.NewBuffer([]byte(sampleCsvData))

	stageContext := CreateStageContext()
	readerFactoryImpl := &DelimitedReaderFactoryImpl{
		CsvRecordType:     List,
		CsvHeader:         IgnoreHeader,
		CsvSkipStartLines: 3,
	}
	recordReader, err := readerFactoryImpl.CreateReader(stageContext, sampleDelimitedData, "m")
	if err != nil {
		t.Fatal(err.Error())
	}

	recordCount := 0
	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			t.Fatal(err.Error())
		}

		if record == nil {
			break
		}

		rootField, _ := record.Get()
		if rootField.Type != fieldtype.LIST {
			t.Errorf("Excpeted record type : Map, but received: %s", rootField.Type)
		}

		listField := rootField.Value.([]*api.Field)

		recordCount++

		if recordCount == 1 {
			cell1Value := listField[0].Value.(map[string]*api.Field)
			if cell1Value["value"].Value.(string) != "206893" {
				t.Errorf("Excpeted field value 206893, but received: %s", cell1Value["value"].Value)
			}
		}
	}

	if recordCount != 1 {
		t.Errorf("Excpeted 1 records, but received: %d", recordCount)
	}

	recordReader.Close()
}

func TestReadDelimitedRecord_ListMapType_WithHeader(t *testing.T) {
	sampleDelimitedData := bytes.NewBuffer([]byte(sampleCsvData))

	stageContext := CreateStageContext()
	readerFactoryImpl := &DelimitedReaderFactoryImpl{
		CsvRecordType: ListMap,
		CsvHeader:     WithHeader,
	}
	recordReader, err := readerFactoryImpl.CreateReader(stageContext, sampleDelimitedData, "m")
	if err != nil {
		t.Fatal(err.Error())
	}

	recordCount := 0
	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			t.Fatal(err.Error())
		}

		if record == nil {
			break
		}

		rootField, _ := record.Get()
		if rootField.Type != fieldtype.LIST_MAP {
			t.Errorf("Excpeted record type : LIST_MAP, but received: %s", rootField.Type)
		}

		listMapField := rootField.Value.(*linkedhashmap.Map)

		recordCount++
		if recordCount == 1 {
			if val, found := listMapField.Get("policyID"); !found {
				t.Errorf("Missing policyID key")
			} else {
				valField := val.(*api.Field)
				if valField.Value != "119736" {
					t.Errorf("Excpeted field value 119736, but received: %s", val)
				}
			}

			if val, found := listMapField.Get("statecode"); !found {
				t.Errorf("Missing statecode key")
			} else {
				valField := val.(*api.Field)
				if valField.Value != "FL" {
					t.Errorf("Excpeted field value FL, but received: %s", val)
				}
			}

			if val, found := listMapField.Get("county"); !found {
				t.Errorf("Missing county key")
			} else {
				valField := val.(*api.Field)
				if valField.Value != "CLAY COUNTY" {
					t.Errorf("Excpeted field value CLAY COUNTY, but received: %s", val)
				}
			}

			if val, found := listMapField.Get("eq_site_limit"); !found {
				t.Errorf("Missing eq_site_limit key")
			} else {
				valField := val.(*api.Field)
				if valField.Value != "498960" {
					t.Errorf("Excpeted field value 498960, but received: %s", val)
				}
			}

			// check order
			keys := listMapField.Keys()

			if keys[0] != "policyID" {
				t.Errorf("Expected column policyId in first position")
			}

			if keys[1] != "statecode" {
				t.Errorf("Expected column statecode in first position")
			}

			if keys[2] != "county" {
				t.Errorf("Expected column county in first position")
			}

			if keys[3] != "eq_site_limit" {
				t.Errorf("Expected column eq_site_limit in first position")
			}
		}
	}

	if recordCount != 3 {
		t.Errorf("Excpeted 3 records, but received: %d", recordCount)
	}

	recordReader.Close()
}

func TestReadDelimitedRecord_ListMapType_IgnoreHeader(t *testing.T) {
	sampleDelimitedData := bytes.NewBuffer([]byte(sampleCsvData))

	stageContext := CreateStageContext()
	readerFactoryImpl := &DelimitedReaderFactoryImpl{
		CsvRecordType: ListMap,
		CsvHeader:     IgnoreHeader,
	}
	recordReader, err := readerFactoryImpl.CreateReader(stageContext, sampleDelimitedData, "m")
	if err != nil {
		t.Fatal(err.Error())
	}

	recordCount := 0
	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			t.Fatal(err.Error())
		}

		if record == nil {
			break
		}

		rootField, _ := record.Get()
		if rootField.Type != fieldtype.LIST_MAP {
			t.Errorf("Excpeted record type : LIST_MAP, but received: %s", rootField.Type)
		}

		listMapField := rootField.Value.(*linkedhashmap.Map)

		recordCount++
		if recordCount == 1 {
			if val, found := listMapField.Get("0"); !found {
				t.Errorf("Missing 0 key")
			} else {
				valField := val.(*api.Field)
				if valField.Value != "119736" {
					t.Errorf("Excpeted field value 119736, but received: %s", val)
				}
			}

			if val, found := listMapField.Get("1"); !found {
				t.Errorf("Missing 1 key")
			} else {
				valField := val.(*api.Field)
				if valField.Value != "FL" {
					t.Errorf("Excpeted field value FL, but received: %s", val)
				}
			}

			if val, found := listMapField.Get("2"); !found {
				t.Errorf("Missing 2 key")
			} else {
				valField := val.(*api.Field)
				if valField.Value != "CLAY COUNTY" {
					t.Errorf("Excpeted field value CLAY COUNTY, but received: %s", val)
				}
			}

			if val, found := listMapField.Get("3"); !found {
				t.Errorf("Missing 3 key")
			} else {
				valField := val.(*api.Field)
				if valField.Value != "498960" {
					t.Errorf("Excpeted field value 498960, but received: %s", val)
				}
			}

			// check order
			keys := listMapField.Keys()

			if keys[0] != "0" {
				t.Errorf("Expected column 0 in first position")
			}

			if keys[1] != "1" {
				t.Errorf("Expected column 1 in first position")
			}

			if keys[2] != "2" {
				t.Errorf("Expected column 2 in first position")
			}

			if keys[3] != "3" {
				t.Errorf("Expected column 3 in first position")
			}
		}
	}

	if recordCount != 3 {
		t.Errorf("Excpeted 3 records, but received: %d", recordCount)
	}

	recordReader.Close()
}

func TestReadDelimitedRecord_ListMapType_NoHeader(t *testing.T) {
	sampleDelimitedData := bytes.NewBuffer([]byte(sampleCsvData))

	stageContext := CreateStageContext()
	readerFactoryImpl := &DelimitedReaderFactoryImpl{
		CsvRecordType: ListMap,
		CsvHeader:     NoHeader,
	}
	recordReader, err := readerFactoryImpl.CreateReader(stageContext, sampleDelimitedData, "m")
	if err != nil {
		t.Fatal(err.Error())
	}

	recordCount := 0
	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			t.Fatal(err.Error())
		}

		if record == nil {
			break
		}

		rootField, _ := record.Get()
		if rootField.Type != fieldtype.LIST_MAP {
			t.Errorf("Excpeted record type : LIST_MAP, but received: %s", rootField.Type)
		}

		listMapField := rootField.Value.(*linkedhashmap.Map)

		recordCount++
		if recordCount == 1 {
			if val, found := listMapField.Get("0"); !found {
				t.Errorf("Missing 0 key")
			} else {
				valField := val.(*api.Field)
				if valField.Value != "policyID" {
					t.Errorf("Excpeted field value policyID, but received: %s", val)
				}
			}

			if val, found := listMapField.Get("1"); !found {
				t.Errorf("Missing 1 key")
			} else {
				valField := val.(*api.Field)
				if valField.Value != "statecode" {
					t.Errorf("Excpeted field value statecode, but received: %s", val)
				}
			}

			if val, found := listMapField.Get("2"); !found {
				t.Errorf("Missing 2 key")
			} else {
				valField := val.(*api.Field)
				if valField.Value != "county" {
					t.Errorf("Excpeted field value county, but received: %s", val)
				}
			}

			if val, found := listMapField.Get("3"); !found {
				t.Errorf("Missing 3 key")
			} else {
				valField := val.(*api.Field)
				if valField.Value != "eq_site_limit" {
					t.Errorf("Excpeted field value eq_site_limit, but received: %s", val)
				}
			}

			// check order
			keys := listMapField.Keys()

			if keys[0] != "0" {
				t.Errorf("Expected column 0 in first position")
			}

			if keys[1] != "1" {
				t.Errorf("Expected column 1 in first position")
			}

			if keys[2] != "2" {
				t.Errorf("Expected column 2 in first position")
			}

			if keys[3] != "3" {
				t.Errorf("Expected column 3 in first position")
			}
		}
	}

	if recordCount != 4 {
		t.Errorf("Excpeted 4 records, but received: %d", recordCount)
	}

	recordReader.Close()
}

func TestReadDelimitedRecord_ListMapType_Custom_Format(t *testing.T) {
	sampleDelimitedData := bytes.NewBuffer([]byte(sampleCustomCsvData))

	stageContext := CreateStageContext()
	readerFactoryImpl := &DelimitedReaderFactoryImpl{
		CsvFileFormat:      Custom,
		CsvCustomDelimiter: "|",
		CsvRecordType:      ListMap,
		CsvHeader:          NoHeader,
	}
	recordReader, err := readerFactoryImpl.CreateReader(stageContext, sampleDelimitedData, "m")
	if err != nil {
		t.Fatal(err.Error())
	}

	recordCount := 0
	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			t.Fatal(err.Error())
		}

		if record == nil {
			break
		}

		rootField, _ := record.Get()
		if rootField.Type != fieldtype.LIST_MAP {
			t.Errorf("Excpeted record type : LIST_MAP, but received: %s", rootField.Type)
		}

		listMapField := rootField.Value.(*linkedhashmap.Map)

		recordCount++
		if recordCount == 1 {
			if val, found := listMapField.Get("0"); !found {
				t.Errorf("Missing 0 key")
			} else {
				valField := val.(*api.Field)
				if valField.Value != "policyID" {
					t.Errorf("Excpeted field value policyID, but received: %s", val)
				}
			}

			if val, found := listMapField.Get("1"); !found {
				t.Errorf("Missing 1 key")
			} else {
				valField := val.(*api.Field)
				if valField.Value != "statecode" {
					t.Errorf("Excpeted field value statecode, but received: %s", val)
				}
			}

			if val, found := listMapField.Get("2"); !found {
				t.Errorf("Missing 2 key")
			} else {
				valField := val.(*api.Field)
				if valField.Value != "county" {
					t.Errorf("Excpeted field value county, but received: %s", val)
				}
			}

			// check order
			keys := listMapField.Keys()

			if keys[0] != "0" {
				t.Errorf("Expected column 0 in first position")
			}

			if keys[1] != "1" {
				t.Errorf("Expected column 1 in first position")
			}

			if keys[2] != "2" {
				t.Errorf("Expected column 2 in first position")
			}
		}
	}

	if recordCount != 4 {
		t.Errorf("Excpeted 4 records, but received: %d", recordCount)
	}

	recordReader.Close()
}
