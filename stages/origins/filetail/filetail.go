package filetail

import (
	"fmt"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
)

type FileTailOrigin struct {
}

func (f *FileTailOrigin) Init(stageConfig common.StageConfiguration) {
	fmt.Println("FileTailOrigin Init method")
}

func (f *FileTailOrigin) Destroy() {
	fmt.Println("FileTailOrigin Destroy method")
}

func (f *FileTailOrigin) Produce(lastSourceOffset string, maxBatchSize int, batchMaker api.BatchMaker) (string, error) {
	fmt.Println("FileTailOrigin produce method")

	batchMaker.AddRecord(api.Record{Value: "value1"})
	batchMaker.AddRecord(api.Record{Value: "value2"})
	batchMaker.AddRecord(api.Record{Value: "value3"})
	batchMaker.AddRecord(api.Record{Value: "value4"})
	batchMaker.AddRecord(api.Record{Value: "value5"})

	return "", nil
}
