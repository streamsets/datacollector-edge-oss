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
package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
	pb "github.com/streamsets/datacollector-edge/stages/origins/grpc_client/testing/simple"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"io"
	"net"
	"time"
)

const (
	port = ":50053"
)

type server struct{}

func (s *server) UnaryRPCExample(ctx context.Context, in *pb.SimpleInputData) (*pb.SimpleOutputData, error) {
	return &pb.SimpleOutputData{Msg: "Hello " + in.Msg}, nil
}

func (s *server) ServerStreamingRPC(in *pb.SimpleInputData, stream pb.SimpleService_ServerStreamingRPCServer) error {
	for messageCount := int64(0); messageCount < in.TotalMessages; messageCount++ {
		time.Sleep(time.Duration(in.Delay) * time.Second)
		stream.Send(&pb.SimpleOutputData{Msg: "Hello " + in.Msg})
	}
	return io.EOF
}

func (s *server) ClientStreamingRPC(stream pb.SimpleService_ClientStreamingRPCServer) error {
	return nil
}

func (s *server) BidirectionalStreamingRPC(stream pb.SimpleService_BidirectionalStreamingRPCServer) error {
	return nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		logrus.Fatal(err)
	}
	s := grpc.NewServer()
	defer s.Stop()
	pb.RegisterSimpleServiceServer(s, &server{})
	// Register reflection service on gRPC server.
	reflection.Register(s)
	fmt.Printf("Running gRPC Simple Server on: %s \n", lis.Addr())
	fmt.Println("Unary RPC Mode:")
	fmt.Println("  method - simple.SimpleService/UnaryRPCExample")
	fmt.Printf("  request data - %s \n", `{"msg": "world"}`)
	fmt.Println("Server Streaming RPC Mode:")
	fmt.Println("  method - simple.SimpleService/ServerStreamingRPC")
	fmt.Printf("  request data - %s \n", `{"msg": "world", "delay": 1, "totalMessages": 200000}`)
	if err := s.Serve(lis); err != nil {
		logrus.Fatalf("failed to serve: %v", err)
	}
}
