package filetail

import (
	"context"
	"fmt"
	"github.com/streamsets/dataextractor/api"
	"strconv"
	"time"
)

type FileTailOrigin struct {
}

func (f *FileTailOrigin) Init(ctx context.Context) {
	fmt.Println("FileTailOrigin Init method: ")
}

func (f *FileTailOrigin) Destroy() {
	fmt.Println("FileTailOrigin Destroy method")
}

func (f *FileTailOrigin) Produce(lastSourceOffset string, maxBatchSize int, batchMaker api.BatchMaker) (string, error) {
	fmt.Println("FileTailOrigin produce method: lastSourceOffset: " + lastSourceOffset)
	offset := 0
	if lastSourceOffset != "" {
		offset, _ = strconv.Atoi(lastSourceOffset)
	}

	time.Sleep(time.Duration(3000) * time.Millisecond)

	batchMaker.AddRecord(api.Record{Value: "value1"})
	batchMaker.AddRecord(api.Record{Value: "value2"})
	batchMaker.AddRecord(api.Record{Value: "value3"})
	batchMaker.AddRecord(api.Record{Value: "value4"})
	batchMaker.AddRecord(api.Record{Value: "value5"})

	return strconv.Itoa(offset + 1), nil
}
