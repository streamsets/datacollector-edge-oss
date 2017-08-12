package recordio

import (
	"github.com/streamsets/datacollector-edge/api"
	"io"
)

type RecordReaderFactory interface {
	CreateReader(context api.StageContext, reader io.Reader) (RecordReader, error)
}

type RecordWriterFactory interface {
	CreateWriter(context api.StageContext, writer io.Writer) (RecordWriter, error)
}

type RecordWriter interface {
	WriteRecord(r api.Record) error
	Flush() error
	Close() error
}

type RecordReader interface {
	ReadRecord() (api.Record, error)
	Close() error
}

type Flusher interface {
	Flush() error
}

func Flush(v interface{}) error {
	c, ok := v.(Flusher)
	if ok {
		return c.Flush()
	}
	return nil
}

func Close(v interface{}) error {
	c, ok := v.(io.Closer)
	if ok {
		return c.Close()
	}
	return nil
}
