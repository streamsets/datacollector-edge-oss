package main

import (
	"flag"
	"fmt"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/edge"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"
	"syscall"
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

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		fmt.Printf("Program got a system signal %v\n", <-c)
		if pipelineInfos, er := dataCollectorEdge.PipelineStoreTask.GetPipelines(); er == nil {
			for _, pipelineInfo := range pipelineInfos {
				runner := dataCollectorEdge.Manager.GetRunner(pipelineInfo.PipelineId)
				if pipelineState, er := runner.GetStatus(); er == nil && pipelineState.Status == common.RUNNING || pipelineState.Status == common.STARTING {
					fmt.Printf("Stopping pipeline : %s\n", pipelineInfo.PipelineId)
					if runner.StopPipeline(); er != nil {
						fmt.Printf("Error happened when stopping pipeline : %s\n", pipelineInfo.PipelineId)
					}
				}
			}
		}
		dataCollectorEdge.WebServerTask.Shutdown()
		fmt.Println("Data Collector Edge shutting down")
	}()

	dataCollectorEdge.WebServerTask.Run()
}
