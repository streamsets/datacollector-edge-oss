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
	"context"
	pb "github.com/streamsets/datacollector-edge/stages/origins/grpc_client/testing/simple"
	"google.golang.org/grpc"
	"io"
	"log"
)

func main() {
	conn, err := grpc.Dial("localhost:50053", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to connect: %s", err)
	}
	defer conn.Close()

	client := pb.NewSimpleServiceClient(conn)
	stream, err := client.ServerStreamingRPC(context.Background(), &pb.SimpleInputData{Msg: "Simple Server"})

	for {
		in, err := stream.Recv()
		log.Println("Received value")
		if err == io.EOF {
			return
		}
		if err != nil {
			panic(err)
		}
		log.Println("Got " + in.Msg)
	}
}
