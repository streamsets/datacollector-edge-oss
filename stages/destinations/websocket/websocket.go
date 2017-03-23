package websocket

import (
	"fmt"
	"github.com/streamsets/dataextractor/api"
)

type WebSocketClientDestination struct {
}

func (w *WebSocketClientDestination) Init() {
	fmt.Println("WebSocketClientDestination Init method")
}

func (w *WebSocketClientDestination) Destroy() {

}

func (w *WebSocketClientDestination) Write(batch api.Batch) error {
	fmt.Println("WebSocketClientDestination write method")
	for _, record := range batch.GetRecords() {
		fmt.Println(record.Value)
	}
	return nil
}
