package filetail

import (
	"github.com/streamsets/dataextractor/api"
	"fmt"
)

type FileTailOrigin struct {
}

func (f *FileTailOrigin) Init() {
	fmt.Println("FileTailOrigin Init method")
}

func (f *FileTailOrigin) Destroy() {
	fmt.Println("FileTailOrigin Destroy method")
}

func (f *FileTailOrigin) Produce(lastSourceOffset string, maxBatchSize int, batchMaker api.BatchMaker) (string, error) {
	fmt.Println("FileTailOrigin produce method")

	batchMaker.AddRecord(api.Record{Value:1})
	batchMaker.AddRecord(api.Record{Value:2})
	batchMaker.AddRecord(api.Record{Value:3})
	batchMaker.AddRecord(api.Record{Value:4})
	batchMaker.AddRecord(api.Record{Value:5})

	return "", nil
}
