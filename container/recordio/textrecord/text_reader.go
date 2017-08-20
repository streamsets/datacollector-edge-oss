package textrecord

import (
	"bufio"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"io"
	"strings"
)

type TextReaderFactoryImpl struct {
	// TODO: Add needed configs
}

func (j *TextReaderFactoryImpl) CreateReader(
	context api.StageContext,
	reader io.Reader,
) (recordio.RecordReader, error) {
	var recordReader recordio.RecordReader
	recordReader = newRecordReader(context, reader)
	return recordReader, nil
}

type TextReaderImpl struct {
	context api.StageContext
	reader  *bufio.Reader
}

func (textReader *TextReaderImpl) ReadRecord() (api.Record, error) {
	var err error
	line, err := textReader.reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return nil, err
	}
	if len(line) > 0 {
		recordValue := map[string]interface{}{"text": strings.Replace(line, "\n", "", 1)}
		return textReader.context.CreateRecord("sourceId", recordValue)
	}
	return nil, nil
}

func (textReader *TextReaderImpl) Close() error {
	return recordio.Close(textReader.reader)
}

func newRecordReader(context api.StageContext, reader io.Reader) *TextReaderImpl {
	return &TextReaderImpl{
		context: context,
		reader:  bufio.NewReader(reader),
	}
}
