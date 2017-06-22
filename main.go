package main

import (
	"fmt"
	"github.com/streamsets/sdc2go/container/sdc2go"
	_ "github.com/streamsets/sdc2go/stages/destinations"
	_ "github.com/streamsets/sdc2go/stages/origins"
	_ "github.com/streamsets/sdc2go/stages/processors"
	"runtime"
)

func main() {
	fmt.Println("StreamSets Data Collector To Go (SDC2Go): ")
	fmt.Printf("OS: %s\nArchitecture: %s\n", runtime.GOOS, runtime.GOARCH)
	sdc2go.DoMain()
}
