// Copyright 2018 StreamSets Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package grpc_client

import (
	"context"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	pb "github.com/streamsets/datacollector-edge/stages/origins/grpc_client/testing/simple"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"io"
	"net"
	"testing"
	"time"
)

func getStageContext(
	configuration []common.Config,
	parameters map[string]interface{},
) (*common.StageContextImpl, *common.ErrorSink) {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = Library
	stageConfig.StageName = StageName
	stageConfig.Configuration = configuration
	errorSink := common.NewErrorSink()
	return &common.StageContextImpl{
		StageConfig: &stageConfig,
		Parameters:  parameters,
		ErrorSink:   errorSink,
	}, errorSink
}

func getUnaryRPCValidConfig(resourceUrl string) []common.Config {
	return []common.Config{
		{
			Name:  "conf.resourceUrl",
			Value: resourceUrl,
		},
		{
			Name:  "conf.serviceMethod",
			Value: "simple.SimpleService/UnaryRPCExample",
		},
		{
			Name:  "conf.requestData",
			Value: `{"msg": "world"}`,
		},
		{
			Name:  "conf.gRPCMode",
			Value: UnaryRPC,
		},
		{
			Name:  "conf.plaintext",
			Value: "true",
		},
		{
			Name:  "conf.dataFormat",
			Value: "JSON",
		},
	}
}

func getServerStreamingRPCValidConfig(resourceUrl string) []common.Config {
	return []common.Config{
		{
			Name:  "conf.resourceUrl",
			Value: resourceUrl,
		},
		{
			Name:  "conf.serviceMethod",
			Value: "simple.SimpleService/ServerStreamingRPC",
		},
		{
			Name:  "conf.requestData",
			Value: `{"msg": "world", "delay": 0, "totalMessages": 2}`,
		},
		{
			Name:  "conf.gRPCMode",
			Value: ServerStreamingRPC,
		},
		{
			Name:  "conf.plaintext",
			Value: "true",
		},
		{
			Name:  "conf.dataFormat",
			Value: "JSON",
		},
	}
}

func TestOrigin_Init(t *testing.T) {
	grpcServer, resourceUrl := startHelloWorldGRPCServer(t)

	configuration := getUnaryRPCValidConfig(resourceUrl)

	stageContext, _ := getStageContext(configuration, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	if stageInstance.(*Origin).Conf.ResourceUrl != resourceUrl {
		t.Error("Failed to inject config value for ResourceUrl")
	}

	issues := stageInstance.Init(stageContext)

	if len(issues) > 0 {
		t.Fatal(issues[0].Message)
	}

	stageInstance.Destroy()
	grpcServer.Stop()
}

func TestOrigin_Init_InvalidUrl(t *testing.T) {
	configuration := getUnaryRPCValidConfig("invalidHost:5004")

	stageContext, _ := getStageContext(configuration, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	issues := stageInstance.Init(stageContext)

	if len(issues) != 1 {
		t.Error("Excepted validation error - lookup invalidHost: no such host")
	}

	stageInstance.Destroy()
}

func TestOrigin_Produce_Unary_RPC_InvalidMethod(t *testing.T) {
	grpcServer, resourceUrl := startHelloWorldGRPCServer(t)
	configuration := []common.Config{
		{
			Name:  "conf.resourceUrl",
			Value: resourceUrl,
		},
		{
			Name:  "conf.serviceMethod",
			Value: "helloworld.Greeter/invalid",
		},
		{
			Name:  "conf.requestData",
			Value: `{"name": "world"}`,
		},
		{
			Name:  "conf.gRPCMode",
			Value: UnaryRPC,
		},
		{
			Name:  "conf.plaintext",
			Value: "true",
		},
		{
			Name:  "conf.dataFormat",
			Value: "JSON",
		},
	}

	stageContext, errorSink := getStageContext(configuration, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}
	stageInstance := stageBean.Stage

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Fatal(issues[0].Message)
	}

	// first batch
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&lastOffset, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}

	records := batchMaker.GetStageOutput()
	if len(records) != 0 {
		t.Fatal("Expected 0 records but got - ", len(records))
	}

	if errorSink.GetTotalErrorMessages() != 1 {
		t.Fatalf("Excepted 1 stage error, but got: %d", errorSink.GetTotalErrorMessages())
	}

	stageInstance.Destroy()
	grpcServer.Stop()
}

func TestOrigin_Produce_Unary_RPC(t *testing.T) {
	grpcServer, resourceUrl := startHelloWorldGRPCServer(t)
	configuration := getUnaryRPCValidConfig(resourceUrl)

	stageContext, _ := getStageContext(configuration, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}
	stageInstance := stageBean.Stage

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Fatal(issues[0].Message)
	}

	// first batch
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&lastOffset, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}

	records := batchMaker.GetStageOutput()
	if len(records) != 1 {
		t.Error("Expected 1 records but got - ", len(records))
		return
	}

	rootField, _ := records[0].Get()
	mapFieldValue := rootField.Value.(map[string]*api.Field)
	if mapFieldValue["msg"].Value != "Hello world" {
		t.Error("Expected 'Hello world' but got - ", rootField.Value)
	}

	// Second Batch
	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&lastOffset, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}

	records = batchMaker.GetStageOutput()
	if len(records) != 1 {
		t.Error("Expected 1 records but got - ", len(records))
		return
	}

	rootField, _ = records[0].Get()
	mapFieldValue = rootField.Value.(map[string]*api.Field)
	if mapFieldValue["msg"].Value != "Hello world" {
		t.Error("Expected 'Hello world' but got - ", rootField.Value)
	}

	stageInstance.Destroy()
	grpcServer.Stop()
}

func TestOrigin_Produce_Server_Streaming_RPC(t *testing.T) {
	grpcServer, resourceUrl := startHelloWorldGRPCServer(t)
	configuration := getServerStreamingRPCValidConfig(resourceUrl)

	stageContext, errorSink := getStageContext(configuration, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}
	stageInstance := stageBean.Stage

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Fatal(issues[0].Message)
	}

	// first batch
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&lastOffset, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}

	if errorSink.GetTotalErrorMessages() != 0 {
		t.Fatal(errorSink.GetStageErrorMessages("")[0].Stacktrace)
	}

	records := batchMaker.GetStageOutput()
	if len(records) != 1 {
		t.Error("Expected 1 records but got - ", len(records))
		return
	}

	rootField, _ := records[0].Get()
	mapFieldValue := rootField.Value.(map[string]*api.Field)
	if mapFieldValue["msg"].Value != "Hello world" {
		t.Error("Expected 'Hello world' but got - ", rootField.Value)
	}

	// Second Batch
	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&lastOffset, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}

	records = batchMaker.GetStageOutput()
	if len(records) != 1 {
		t.Error("Expected 1 records but got - ", len(records))
		return
	}

	rootField, _ = records[0].Get()
	mapFieldValue = rootField.Value.(map[string]*api.Field)
	if mapFieldValue["msg"].Value != "Hello world" {
		t.Error("Expected 'Hello world' but got - ", rootField.Value)
	}

	// Third Batch should return empty records since totalMessage set to 2
	batchMaker = runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&lastOffset, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}

	records = batchMaker.GetStageOutput()
	if len(records) != 0 {
		t.Error("Expected 0 records but got - ", len(records))
		return
	}

	stageInstance.Destroy()
	grpcServer.Stop()
}

type TestGRPCServer struct{}

func (s *TestGRPCServer) UnaryRPCExample(ctx context.Context, in *pb.SimpleInputData) (*pb.SimpleOutputData, error) {
	return &pb.SimpleOutputData{Msg: "Hello " + in.Msg}, nil
}

func (s *TestGRPCServer) ServerStreamingRPC(
	in *pb.SimpleInputData,
	stream pb.SimpleService_ServerStreamingRPCServer,
) error {
	for messageCount := int64(0); messageCount < in.TotalMessages; messageCount++ {
		time.Sleep(time.Duration(in.Delay) * time.Second)
		stream.Send(&pb.SimpleOutputData{Msg: "Hello " + in.Msg})
	}
	return io.EOF
}

func (s *TestGRPCServer) ClientStreamingRPC(stream pb.SimpleService_ClientStreamingRPCServer) error {
	return nil
}

func (s *TestGRPCServer) BidirectionalStreamingRPC(stream pb.SimpleService_BidirectionalStreamingRPCServer) error {
	return nil
}

func startHelloWorldGRPCServer(t *testing.T) (*grpc.Server, string) {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterSimpleServiceServer(s, &TestGRPCServer{})
	// Register reflection service on gRPC server.
	reflection.Register(s)

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Fatalf("failed to serve: %v", err)
		}
	}()

	return s, lis.Addr().String()
}
