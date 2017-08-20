package jsonrecord

import (
	"encoding/json"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"io"
)

type JsonWriterFactoryImpl struct {
	// TODO: Add needed configs
}

func (j *JsonWriterFactoryImpl) CreateWriter(
	context api.StageContext,
	writer io.Writer,
) (recordio.RecordWriter, error) {
	var recordWriter recordio.RecordWriter
	recordWriter = newRecordWriter(context, writer)
	return recordWriter, nil
}

type JsonWriterImpl struct {
	context api.StageContext
	writer  io.Writer
	encoder *json.Encoder
}

func (jsonWriter *JsonWriterImpl) WriteRecord(r api.Record) error {
	jsonObject, err := writeFieldToJsonObject(r.Get())
	if err != nil {
		return err
	}
	jsonWriter.encoder.Encode(jsonObject)
	return nil
}

func (jsonWriter *JsonWriterImpl) Flush() error {
	return recordio.Flush(jsonWriter.writer)
}

func (jsonWriter *JsonWriterImpl) Close() error {
	return recordio.Close(jsonWriter.writer)
}

func newRecordWriter(context api.StageContext, writer io.Writer) *JsonWriterImpl {
	return &JsonWriterImpl{
		context: context,
		writer:  writer,
		encoder: json.NewEncoder(writer),
	}
}

func writeFieldToJsonObject(field api.Field) (interface{}, error) {
	if field.Value == nil {
		return nil, nil
	}
	var err error = nil
	switch field.Type {
	case fieldtype.LIST:
		jsonObject := []interface{}{}
		fieldValue := field.Value.([]api.Field)
		for _, v := range fieldValue {
			fieldJsonObject, err := writeFieldToJsonObject(v)
			if err != nil {
				return nil, err
			}
			jsonObject = append(jsonObject, fieldJsonObject)
		}
		return jsonObject, err
	case fieldtype.MAP:
		jsonObject := make(map[string]interface{})
		fieldValue := field.Value.(map[string]api.Field)
		for k, v := range fieldValue {
			jsonObject[k], err = writeFieldToJsonObject(v)
			if err != nil {
				return nil, err
			}
		}
		return jsonObject, err
	default:
		return field.Value, nil
	}
	return nil, err
}
