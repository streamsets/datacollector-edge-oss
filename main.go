package main

import (
	"fmt"
	"github.com/streamsets/dataextractor/container/dataextractor"
	_ "github.com/streamsets/dataextractor/stages/destinations"
	_ "github.com/streamsets/dataextractor/stages/origins"
	"runtime"
)

func main() {
	fmt.Println("StreamSets Data Extractor: ")
	fmt.Printf("OS: %s\nArchitecture: %s\n", runtime.GOOS, runtime.GOARCH)
	dataextractor.DoMain()
}
