package main

import (
	"fmt"
	"github.com/streamsets/dataextractor/container/dataextractor"
	"runtime"
)

func main() {
	fmt.Println("StreamSets Data Extractor")
	fmt.Printf("OS: %s\nArchitecture: %s\n", runtime.GOOS, runtime.GOARCH)
	dataextractor.DoMain()
}
