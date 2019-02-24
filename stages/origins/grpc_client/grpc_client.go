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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/fullstorydev/grpcurl"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/grpcreflect"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/lib/dataparser"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	reflectpb "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/grpc/status"
	"strings"
	"time"
)

const (
	Library            = "streamsets-datacollector-basic-lib"
	StageName          = "com_streamsets_pipeline_stage_origin_grpcclient_GrpcClientDSource"
	UnaryRPC           = "UNARY_RPC"
	ServerStreamingRPC = "SERVER_STREAMING_RPC"
)

var lastOffset = "lastOffset"

type Origin struct {
	*common.BaseStage
	Conf                 ClientConfig `ConfigDefBean:"conf"`
	incomingRecords      []api.Record
	incomingRecordStream chan []api.Record
	gRPCCtxCancelFnc     context.CancelFunc
	destroyed            bool
	dec                  *json.Decoder
	descSource           grpcurl.DescriptorSource
	reqCount             int
	respCount            int
	stat                 *status.Status
}

type ClientConfig struct {
	ResourceUrl      string                            `ConfigDef:"type=STRING,required=true"`
	ServiceMethod    string                            `ConfigDef:"type=STRING,required=true"`
	RequestData      string                            `ConfigDef:"type=STRING,required=true"`
	GRPCMode         string                            `ConfigDef:"type=STRING,required=true"`
	PollingInterval  float64                           `ConfigDef:"type=NUMBER,required=true"`
	ConnectTimeout   float64                           `ConfigDef:"type=NUMBER,required=true"`
	KeepaliveTime    float64                           `ConfigDef:"type=NUMBER,required=true"`
	AddlHeaders      map[string]string                 `ConfigDef:"type=MAP,required=true"`
	EmitDefaults     bool                              `ConfigDef:"type=BOOLEAN,required=true"`
	TlsConfig        TlsConfigBean                     `ConfigDefBean:"tlsConfig"`
	Insecure         bool                              `ConfigDef:"type=BOOLEAN,required=true"`
	Authority        string                            `ConfigDef:"type=STRING,required=true"`
	ServerName       string                            `ConfigDef:"type=STRING,required=true"`
	DataFormat       string                            `ConfigDef:"type=STRING,required=true"`
	DataFormatConfig dataparser.DataParserFormatConfig `ConfigDefBean:"dataFormatConfig"`
}

type TlsConfigBean struct {
	TlsEnabled         bool   `ConfigDef:"type=BOOLEAN,required=true"`
	TrustStoreFilePath string `ConfigDef:"type=STRING,required=true"`
	KeyStoreFilePath   string `ConfigDef:"type=STRING,required=true"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &Origin{BaseStage: &common.BaseStage{}}
	})
}

func (o *Origin) Init(stageContext api.StageContext) []validation.Issue {
	issues := o.BaseStage.Init(stageContext)
	o.destroyed = false

	issues = o.Conf.DataFormatConfig.Init(o.Conf.DataFormat, stageContext, issues)
	if len(issues) > 0 {
		return issues
	}

	_, err := o.dial(context.Background())
	if err != nil {
		issues = append(issues, stageContext.CreateConfigIssue(err.Error()))
		return issues
	}

	if o.Conf.GRPCMode == ServerStreamingRPC {
		o.incomingRecordStream = make(chan []api.Record)
		go o.connectToServer()
	}

	return issues
}

func (o *Origin) Produce(lastSourceOffset *string, maxBatchSize int, batchMaker api.BatchMaker) (*string, error) {
	if o.Conf.GRPCMode == UnaryRPC {
		return o.produceUnaryRPC(lastSourceOffset, maxBatchSize, batchMaker)
	} else if o.Conf.GRPCMode == ServerStreamingRPC {
		return o.produceStreamingRPC(maxBatchSize, batchMaker)
	}
	return &lastOffset, nil
}

func (o *Origin) produceUnaryRPC(
	lastSourceOffset *string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (*string, error) {
	if lastSourceOffset != nil && len(*lastSourceOffset) != 0 {
		// Ignore sleeping for first batch
		time.Sleep(time.Duration(o.Conf.PollingInterval) * time.Millisecond)
	}
	err := o.connectToServer()
	if err != nil {
		log.WithError(err).Error("Failed to produce records")
		o.GetStageContext().ReportError(err)
		return &lastOffset, nil
	}

	for _, record := range o.incomingRecords {
		batchMaker.AddRecord(record)
	}

	return &lastOffset, nil
}

func (o *Origin) produceStreamingRPC(maxBatchSize int, batchMaker api.BatchMaker) (*string, error) {
	records := <-o.incomingRecordStream
	if records != nil {
		for _, record := range records {
			batchMaker.AddRecord(record)
		}
	} else {
		// nil means done reading stream
		return nil, nil
	}
	return &lastOffset, nil
}

func (o *Origin) Destroy() error {
	log.Debugf("gRPC Client origin destroy called")
	o.destroyed = true

	if o.gRPCCtxCancelFnc != nil {
		o.gRPCCtxCancelFnc()
	}
	return nil
}

func (o *Origin) connectToServer() error {
	var err error
	var ctx context.Context

	ctx, o.gRPCCtxCancelFnc = context.WithCancel(context.Background())

	var cc *grpc.ClientConn
	var refClient *grpcreflect.Client

	headers := make([]string, 0)
	for key, value := range o.Conf.AddlHeaders {
		headers = append(headers, fmt.Sprintf("%s: %s", key, value))
	}

	md := grpcurl.MetadataFromHeaders(headers)
	refCtx := metadata.NewOutgoingContext(ctx, md)
	cc, err = o.dial(ctx)
	if err != nil {
		return err
	}

	refClient = grpcreflect.NewClient(refCtx, reflectpb.NewServerReflectionClient(cc))
	o.descSource = grpcurl.DescriptorSourceFromServer(ctx, refClient)

	o.dec = json.NewDecoder(strings.NewReader(o.Conf.RequestData))
	if err != nil {
		return err
	}

	err = grpcurl.InvokeRPC(
		ctx,
		o.descSource,
		cc,
		o.Conf.ServiceMethod,
		headers,
		o,
		func(message proto.Message) error {
			var msg json.RawMessage
			if err := o.dec.Decode(&msg); err != nil {
				return err
			}
			o.reqCount++
			return jsonpb.Unmarshal(bytes.NewReader(msg), message)
		},
	)

	if err != nil {
		return err
	}
	reqSuffix := ""
	respSuffix := ""
	if o.reqCount != 1 {
		reqSuffix = "s"
	}
	if o.respCount != 1 {
		respSuffix = "s"
	}

	log.Debugf("Sent %d request%s and received %d response%s\n", o.reqCount, reqSuffix, o.respCount, respSuffix)

	if o.stat.Code() != codes.OK {
		return fmt.Errorf("ERROR:\n  Code: %s\n  Message: %s\n", o.stat.Code().String(), o.stat.Message())
	}

	return nil
}

func (o *Origin) dial(ctx context.Context) (*grpc.ClientConn, error) {
	if o.Conf.ConnectTimeout > 0 {
		dialTime := time.Duration(o.Conf.ConnectTimeout) * time.Second
		_, cancel := context.WithTimeout(ctx, dialTime)
		defer cancel()
	}

	var opts []grpc.DialOption
	if o.Conf.KeepaliveTime > 0 {
		timeout := time.Duration(o.Conf.KeepaliveTime * float64(time.Second))
		opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    timeout,
			Timeout: timeout,
		}))
	}

	if o.Conf.Authority != "" {
		opts = append(opts, grpc.WithAuthority(o.Conf.Authority))
	}

	var creds credentials.TransportCredentials
	if o.Conf.TlsConfig.TlsEnabled {
		var err error
		creds, err = grpcurl.ClientTransportCredentials(
			o.Conf.Insecure,
			o.Conf.TlsConfig.TrustStoreFilePath,
			o.Conf.TlsConfig.TrustStoreFilePath,
			o.Conf.TlsConfig.KeyStoreFilePath,
		)
		if err != nil {
			panic(err.Error() + "Failed to configure transport credentials")
		}
		if o.Conf.ServerName != "" {
			if err := creds.OverrideServerName(o.Conf.ServerName); err != nil {
				panic(err.Error() + ": Failed to override server name as %q" + o.Conf.ServerName)
			}
		}
	}
	network := "tcp" // or unix

	return grpcurl.BlockingDial(ctx, network, o.Conf.ResourceUrl, creds, opts...)
}

func (o *Origin) OnResolveMethod(md *desc.MethodDescriptor) {
	txt, err := grpcurl.GetDescriptorText(md, o.descSource)
	if err == nil {
		log.Debugf("\nResolved method descriptor:\n%s\n", txt)
	}
}

func (o *Origin) OnSendHeaders(md metadata.MD) {
	log.Debugf("\nRequest metadata to send:\n%s\n", grpcurl.MetadataToString(md))
}

func (o *Origin) OnReceiveHeaders(md metadata.MD) {
	log.Debugf("Response headers received:\n%s", grpcurl.MetadataToString(md))
}

func (o *Origin) OnReceiveResponse(resp proto.Message) {
	log.WithField("message", resp).Debug("OnReceiveResponse")
	o.respCount++
	jsm := jsonpb.Marshaler{EmitDefaults: o.Conf.EmitDefaults, Indent: "  "}
	respStr, err := jsm.MarshalToString(resp)
	if err != nil {
		log.WithError(err).Error("failed to generate JSON form of response message")
		o.GetStageContext().ReportError(err)
		return
	}

	recordReaderFactory := o.Conf.DataFormatConfig.RecordReaderFactory
	recordBuffer := bytes.NewBufferString(respStr)
	recordReader, err := recordReaderFactory.CreateReader(o.GetStageContext(), recordBuffer, "gRPC")
	if err != nil {
		log.WithError(err).Error("Failed to create record reader")
	}
	defer recordReader.Close()
	o.incomingRecords = make([]api.Record, 0)
	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			log.WithError(err).Error("Failed to parse raw data")
			o.GetStageContext().ReportError(err)
		}

		if record == nil {
			break
		}

		o.incomingRecords = append(o.incomingRecords, record)
	}

	if o.Conf.GRPCMode == ServerStreamingRPC && len(o.incomingRecords) > 0 && !o.destroyed {
		o.incomingRecordStream <- o.incomingRecords
	}
}

func (o *Origin) OnReceiveTrailers(stat *status.Status, md metadata.MD) {
	log.Debugf("\nResponse trailers received:\n%s\n", grpcurl.MetadataToString(md))
	o.stat = stat
	if o.Conf.GRPCMode == ServerStreamingRPC && !o.destroyed {
		o.incomingRecordStream <- nil
	}
}
