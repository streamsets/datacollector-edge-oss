package sdcrecord

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"io"
)

const (
	//Just support Json For now
	JSON1_MAGIC_NUMBER = byte(0xa0) | byte(0x01)
)

type SDCRecordReaderFactoryImpl struct {
	//TODO: Add needed configs
}

func (srrf *SDCRecordReaderFactoryImpl) CreateReader(
	context api.StageContext,
	reader io.Reader,
) (recordio.RecordReader, error) {
	var recordReader recordio.RecordReader
	b := make([]byte, 1)
	//Magic number read.
	nb, err := reader.Read(b)
	if err == nil {
		if nb != 1 {
			err = errors.New(
				fmt.Sprintf(
					"Error Creating Reader, when reading magic byte."+
						" Read : %x, number of bytes: %d",
					b,
					nb,
				),
			)
		} else if b[0] != JSON1_MAGIC_NUMBER {
			err = errors.New("Error Creating Reader: Magic number does not point to JSON")
		}
		recordReader = newRecordReader(context, reader)
	}
	return recordReader, err
}

type SDCRecordWriterFactoryImpl struct {
	//TODO: Add needed configs
}

func (srwf *SDCRecordWriterFactoryImpl) CreateWriter(
	context api.StageContext,
	writer io.Writer,
) (recordio.RecordWriter, error) {
	var recordWriter recordio.RecordWriter

	//Magic Number for SDC record
	_, err := writer.Write([]byte{JSON1_MAGIC_NUMBER})

	if err == nil {
		recordWriter = newRecordWriter(context, writer)
	}

	return recordWriter, err
}

type SDCRecordReaderImpl struct {
	context api.StageContext
	reader  io.Reader
	decoder *json.Decoder
}

func (srr *SDCRecordReaderImpl) ReadRecord() (api.Record, error) {
	sdcRecord := new(SDCRecord)
	err := srr.decoder.Decode(sdcRecord)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	return NewRecordFromSDCRecord(srr.context, sdcRecord), nil
}

func (srr *SDCRecordReaderImpl) Close() error {
	return recordio.Close(srr.reader)
}

type SDCRecordWriterImpl struct {
	context api.StageContext
	writer  io.Writer
	encoder *json.Encoder
}

func (srw *SDCRecordWriterImpl) WriteRecord(r api.Record) error {
	sdcRecord, err := NewSdcRecordFromRecord(r)
	if err == nil {
		err = srw.encoder.Encode(*sdcRecord)
	}
	return err
}

func (srw *SDCRecordWriterImpl) Flush() error {
	return recordio.Flush(srw.writer)
}

func (srw *SDCRecordWriterImpl) Close() error {
	return recordio.Close(srw.writer)
}

func newRecordWriter(context api.StageContext, writer io.Writer) *SDCRecordWriterImpl {
	return &SDCRecordWriterImpl{
		context: context,
		writer:  writer,
		encoder: json.NewEncoder(writer),
	}
}

func newRecordReader(context api.StageContext, reader io.Reader) *SDCRecordReaderImpl {
	return &SDCRecordReaderImpl{
		context: context,
		reader:  reader,
		decoder: json.NewDecoder(reader),
	}
}
