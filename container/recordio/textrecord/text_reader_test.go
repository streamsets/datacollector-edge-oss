package textrecord

import (
	"bytes"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/container/common"
	"strconv"
	"testing"
)

func CreateStageContext() api.StageContext {
	return &common.StageContextImpl{
		StageConfig: common.StageConfiguration{InstanceName: "Dummy Stage"},
		Parameters:  nil,
	}
}

func TestReadTextRecord(t *testing.T) {
	sampleTextData := bytes.NewBuffer([]byte("test data 1\ntest data 2\ntest data 3"))

	stageContext := CreateStageContext()
	readerFactoryImpl := &TextReaderFactoryImpl{}
	recordReader, err := readerFactoryImpl.CreateReader(stageContext, sampleTextData)
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
		if rootField.Type != fieldtype.MAP {
			t.Errorf("Excpeted record type : Map, but received: %s", rootField.Type)
		}

		mapField := rootField.Value.(map[string]*api.Field)
		testData := "test data " + strconv.Itoa(recordCount+1)
		if mapField["text"].Value.(string) != testData {
			t.Errorf("Excpeted field value %s, but received: %s", testData, mapField["text"].Value)
		}
		recordCount++
	}

	if recordCount != 3 {
		t.Errorf("Excpeted 3 records, but received: %d", recordCount)
	}

	recordReader.Close()
}
