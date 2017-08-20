package textrecord

import (
	"bytes"
	"testing"
)

func TestWriteTextRecord(t *testing.T) {
	stageContext := CreateStageContext()
	record1, err := stageContext.CreateRecord("Id1", map[string]interface{}{"text": "log line 1"})
	record2, err := stageContext.CreateRecord("Id2", map[string]interface{}{"text": "log line 2"})
	record3, err := stageContext.CreateRecord("Id3", map[string]interface{}{"text": "log line 3"})

	bufferWriter := bytes.NewBuffer([]byte{})
	recordWriterFactory := &TextWriterFactoryImpl{}
	recordWriter, err := recordWriterFactory.CreateWriter(stageContext, bufferWriter)
	if err != nil {
		t.Fatal(err)
	}

	err = recordWriter.WriteRecord(record1)
	if err != nil {
		t.Fatal(err)
	}

	err = recordWriter.WriteRecord(record2)
	if err != nil {
		t.Fatal(err)
	}

	err = recordWriter.WriteRecord(record3)
	if err != nil {
		t.Fatal(err)
	}

	recordWriter.Flush()
	recordWriter.Close()

	testData := "log line 1\nlog line 2\nlog line 3\n"
	if bufferWriter.String() != "log line 1\nlog line 2\nlog line 3\n" {
		t.Errorf("Excpeted field value %s, but received: %s", testData, bufferWriter.String())
	}
}
