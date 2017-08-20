package jsonrecord

import (
	"encoding/json"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"io"
)

type JsonReaderFactoryImpl struct {
	// TODO: Add needed configs
}

func (j *JsonReaderFactoryImpl) CreateReader(
	context api.StageContext,
	reader io.Reader,
) (recordio.RecordReader, error) {
	var recordReader recordio.RecordReader
	recordReader = newRecordReader(context, reader)
	return recordReader, nil
}

type JsonReaderImpl struct {
	context api.StageContext
	reader  io.Reader
	decoder *json.Decoder
}

func (jsonReader *JsonReaderImpl) ReadRecord() (api.Record, error) {
	var f interface{}
	err := jsonReader.decoder.Decode(&f)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	return jsonReader.context.CreateRecord("sourceId", f)
}

func (jsonReader *JsonReaderImpl) Close() error {
	return recordio.Close(jsonReader.reader)
}

func newRecordReader(context api.StageContext, reader io.Reader) *JsonReaderImpl {
	return &JsonReaderImpl{
		context: context,
		reader:  reader,
		decoder: json.NewDecoder(reader),
	}
}
