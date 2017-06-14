package main

import (
	"fmt"
	"github.com/streamsets/dataextractor/container/sdc2go"
	_ "github.com/streamsets/dataextractor/stages/destinations"
	_ "github.com/streamsets/dataextractor/stages/origins"
	"runtime"
)

func main() {
	fmt.Println("StreamSets sdc2go: ")
	fmt.Printf("OS: %s\nArchitecture: %s\n", runtime.GOOS, runtime.GOARCH)
	sdc2go.DoMain()
}
