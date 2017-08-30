package main

import (
	"flag"
	"fmt"
	"github.com/streamsets/datacollector-edge/container/edge"
	_ "github.com/streamsets/datacollector-edge/stages/destinations"
	_ "github.com/streamsets/datacollector-edge/stages/origins"
	_ "github.com/streamsets/datacollector-edge/stages/processors"
	"os"
	"path"
	"runtime"
	"strings"
)

func main() {
	debugFlag := flag.Bool("debug", false, "Debug flag")
	startFlag := flag.String("start", "", "Start Pipeline flag")
	runtimeParametersFlag := flag.String("runtimeParameters", "", "Runtime Parameters flag")
	flag.Parse()

	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	baseDir := strings.TrimSuffix(path.Dir(ex), "/bin")

	fmt.Println("StreamSets Data Collector Edge (SDCe): ")
	fmt.Printf("OS: %s\nArchitecture: %s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Println("Base Dir: ", baseDir)

	dataCollectorEdge, _ := edge.DoMain(baseDir, *debugFlag, *startFlag, *runtimeParametersFlag)
	dataCollectorEdge.WebServerTask.Run()
}
