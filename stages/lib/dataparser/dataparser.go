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
package dataparser

import (
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"github.com/streamsets/datacollector-edge/container/recordio/binaryrecord"
	"github.com/streamsets/datacollector-edge/container/recordio/delimitedrecord"
	"github.com/streamsets/datacollector-edge/container/recordio/jsonrecord"
	"github.com/streamsets/datacollector-edge/container/recordio/sdcrecord"
	"github.com/streamsets/datacollector-edge/container/recordio/textrecord"
	"github.com/streamsets/datacollector-edge/container/recordio/wholefilerecord"
)

const (
	CompressedNone = "NONE"
	CompressedFile = "COMPRESSED_FILE"
)

type DataParserFormatConfig struct {
	Compression         string `ConfigDef:"type=STRING,required=true"`
	FlePatternInArchive string `ConfigDef:"type=STRING,required=true"`

	/* Charset Related -- Shown last */
	Charset         string `ConfigDef:"type=STRING,required=true"`
	RemoveCtrlChars bool   `ConfigDef:"type=BOOLEAN,required=true"`

	/** For TEXT Content **/
	TextMaxLineLen                  float64 `ConfigDef:"type=NUMBER,required=true"`
	UseCustomDelimiter              bool    `ConfigDef:"type=BOOLEAN,required=true"`
	CustomDelimiter                 string  `ConfigDef:"type=STRING,required=true"`
	IncludeCustomDelimiterInTheText bool    `ConfigDef:"type=BOOLEAN,required=true"`

	/** For JSON **/
	JsonContent string `ConfigDef:"type=STRING,required=true"`

	/** For DELIMITED Content **/
	CsvFileFormat        string  `ConfigDef:"type=STRING,required=true"`
	CsvHeader            string  `ConfigDef:"type=STRING,required=true"`
	CsvAllowExtraColumns bool    `ConfigDef:"type=BOOLEAN,required=true"`
	CsvExtraColumnPrefix string  `ConfigDef:"type=STRING,required=true"`
	CsvMaxObjectLen      float64 `ConfigDef:"type=NUMBER,required=true"`
	CsvCustomDelimiter   string  `ConfigDef:"type=STRING,required=true"`
	CsvCustomEscape      string  `ConfigDef:"type=STRING,required=true"`
	CsvEnableComments    bool    `ConfigDef:"type=BOOLEAN,required=true"`
	CsvCommentMarker     string  `ConfigDef:"type=STRING,required=true"`
	CsvIgnoreEmptyLines  bool    `ConfigDef:"type=BOOLEAN,required=true"`
	CsvRecordType        string  `ConfigDef:"type=STRING,required=true"`
	CsvSkipStartLines    float64 `ConfigDef:"type=NUMBER,required=true"`
	ParseNull            bool    `ConfigDef:"type=BOOLEAN,required=true"`
	NullConstant         string  `ConfigDef:"type=STRING,required=true"`

	/** For XML Content **/
	XmlRecordElement            string            `ConfigDef:"type=STRING,required=true"`
	IncludeFieldXpathAttributes bool              `ConfigDef:"type=BOOLEAN,required=true"`
	XPathNamespaceContext       map[string]string `ConfigDef:"type=MAP,required=true"`
	OutputFieldAttributes       bool              `ConfigDef:"type=BOOLEAN,required=true"`
	XmlMaxObjectLen             float64           `ConfigDef:"type=NUMBER,required=true"`

	// LOG Configuration
	LogMode               string        `ConfigDef:"type=STRING,required=true"`
	LogMaxObjectLen       float64       `ConfigDef:"type=NUMBER,required=true"`
	RetainOriginalLine    bool          `ConfigDef:"type=BOOLEAN,required=true"`
	CustomLogFormat       string        `ConfigDef:"type=STRING,required=true"`
	Regex                 string        `ConfigDef:"type=STRING,required=true"`
	FieldPathsToGroupName []RegExConfig `ConfigDef:"type=MODEL" ListBeanModel:"name=fieldPathsToGroupName"`
	// GROK
	GrokPatternDefinition      string  `ConfigDef:"type=STRING,required=true"`
	GrokPattern                string  `ConfigDef:"type=STRING,required=true"`
	OnParseError               string  `ConfigDef:"type=STRING,required=true"`
	MaxStackTraceLines         float64 `ConfigDef:"type=NUMBER,required=true"`
	EnableLog4jCustomLogFormat bool    `ConfigDef:"type=BOOLEAN,required=true"`
	Log4jCustomLogFormat       string  `ConfigDef:"type=STRING,required=true"`

	/** For AVRO Content **/
	AvroSchemaSource                  string   `ConfigDef:"type=STRING,required=true"`
	AvroSchema                        string   `ConfigDef:"type=STRING,required=true"`
	RegisterSchema                    bool     `ConfigDef:"type=BOOLEAN,required=true"`
	SchemaRegistryUrlsForRegistration []string `ConfigDef:"type=LIST,required=true"`
	SchemaRegistryUrls                []string `ConfigDef:"type=LIST,required=true"`
	SchemaLookupMode                  string   `ConfigDef:"type=STRING,required=true"`
	Subject                           string   `ConfigDef:"type=STRING,required=true"`
	SchemaId                          float64  `ConfigDef:"type=STRING,required=true"`

	/** For Protobuf Content **/
	ProtoDescriptorFile string `ConfigDef:"type=STRING,required=true"`
	MessageType         string `ConfigDef:"type=STRING,required=true"`
	IsDelimited         bool   `ConfigDef:"type=BOOLEAN,required=true"`

	/** For Binary Content **/
	BinaryMaxObjectLen float64 `ConfigDef:"type=NUMBER,required=true"`

	// DATAGRAM
	DatagramMode    string `ConfigDef:"type=STRING,required=true"`
	TypesDbPath     string `ConfigDef:"type=STRING,required=true"`
	ConvertTime     bool   `ConfigDef:"type=BOOLEAN,required=true"`
	ExcludeInterval bool   `ConfigDef:"type=BOOLEAN,required=true"`
	AuthFilePath    string `ConfigDef:"type=STRING,required=true"`

	// Netflow v9
	NetflowOutputValuesMode         string  `ConfigDef:"type=STRING,required=true"`
	MaxTemplateCacheSize            float64 `ConfigDef:"type=NUMBER,required=true"`
	TemplateCacheTimeoutMs          float64 `ConfigDef:"type=NUMBER,required=true"`
	NetflowOutputValuesModeDatagram string  `ConfigDef:"type=STRING,required=true"`
	MaxTemplateCacheSizeDatagram    float64 `ConfigDef:"type=NUMBER,required=true"`
	TemplateCacheTimeoutMsDatagram  float64 `ConfigDef:"type=NUMBER,required=true"`

	/** For Whole File Content **/
	WholeFileMaxObjectLen float64 `ConfigDef:"type=NUMBER,required=true"`
	RateLimit             string  `ConfigDef:"type=STRING,required=true"`
	VerifyChecksum        bool    `ConfigDef:"type=BOOLEAN,required=true"`

	// Used to parse records from input stream
	RecordReaderFactory recordio.RecordReaderFactory

	// Used to create record for origins generating single line of text - Fail Tail & Directory Spooler
	RecordCreator recordio.RecordCreator
}

type RegExConfig struct {
	FieldPath string  `ConfigDef:"type=STRING,required=true"`
	Group     float64 `ConfigDef:"type=NUMBER,required=true"`
}

func (d *DataParserFormatConfig) Init(
	dataFormat string,
	stageContext api.StageContext,
	issues []validation.Issue,
) []validation.Issue {
	switch dataFormat {
	case "TEXT":
		d.RecordReaderFactory = &textrecord.TextReaderFactoryImpl{
			TextMaxLineLen: cast.ToInt(d.TextMaxLineLen),
		}
		d.RecordCreator = &textrecord.RecordCreator{
			TextMaxLineLen: cast.ToInt(d.TextMaxLineLen),
		}
	case "JSON":
		d.RecordReaderFactory = &jsonrecord.JsonReaderFactoryImpl{}
		d.RecordCreator = &jsonrecord.RecordCreator{}
	case "DELIMITED":
		d.RecordReaderFactory = &delimitedrecord.DelimitedReaderFactoryImpl{
			CsvFileFormat:        d.CsvFileFormat,
			CsvHeader:            d.CsvHeader,
			CsvAllowExtraColumns: d.CsvAllowExtraColumns,
			CsvExtraColumnPrefix: d.CsvExtraColumnPrefix,
			CsvMaxObjectLen:      d.CsvMaxObjectLen,
			CsvCustomDelimiter:   d.CsvCustomDelimiter,
			CsvCustomEscape:      d.CsvCustomEscape,
			CsvEnableComments:    d.CsvEnableComments,
			CsvCommentMarker:     d.CsvCommentMarker,
			CsvIgnoreEmptyLines:  d.CsvIgnoreEmptyLines,
			CsvRecordType:        d.CsvRecordType,
			CsvSkipStartLines:    d.CsvSkipStartLines,
			ParseNull:            d.ParseNull,
			NullConstant:         d.NullConstant,
		}
		d.RecordCreator = &delimitedrecord.RecordCreator{
			CsvFileFormat:      d.CsvFileFormat,
			CsvCustomDelimiter: d.CsvCustomDelimiter,
			CsvRecordType:      d.CsvRecordType,
		}
	case "BINARY":
		d.RecordReaderFactory = &binaryrecord.BinaryReaderFactoryImpl{
			BinaryMaxObjectLen: cast.ToInt(d.TextMaxLineLen),
			Compression:        d.Compression,
		}
	case "WHOLE_FILE":
		d.RecordReaderFactory = &wholefilerecord.WholeFileReaderFactoryImpl{
			WholeFileMaxObjectLen: cast.ToInt(d.WholeFileMaxObjectLen),
			RateLimit:             d.RateLimit,
			VerifyChecksum:        d.VerifyChecksum,
		}
	case "SDC_JSON":
		d.RecordReaderFactory = &sdcrecord.SDCRecordReaderFactoryImpl{}
		d.RecordCreator = &sdcrecord.RecordCreator{}
	default:
		issues = append(issues, stageContext.CreateConfigIssue("Unsupported Data Format - "+dataFormat))
	}
	return issues
}
