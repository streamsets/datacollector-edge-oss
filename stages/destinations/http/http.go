package http

import (
	"fmt"
	"github.com/streamsets/dataextractor/api"
)

type HttpClientDestination struct {
}

func (h *HttpClientDestination) Init() {
	fmt.Println("HttpClientDestination Init method")
}

func (h *HttpClientDestination) Destroy() {

}

func (h *HttpClientDestination) Write(batch api.Batch) error {
	fmt.Println("HttpClientDestination write method")
	for _, record := range batch.GetRecords() {
		fmt.Println(record.Value)
	}
	return nil
}
