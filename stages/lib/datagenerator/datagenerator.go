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
package datagenerator

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"github.com/streamsets/datacollector-edge/container/recordio/binaryrecord"
	"github.com/streamsets/datacollector-edge/container/recordio/jsonrecord"
	"github.com/streamsets/datacollector-edge/container/recordio/sdcrecord"
	"github.com/streamsets/datacollector-edge/container/recordio/textrecord"
)

type DataGeneratorFormatConfig struct {
	/* Charset Related -- Shown last */
	Charset string `ConfigDef:"type=STRING,required=true"`

	/** For DELIMITED Content **/
	CsvFileFormat            string `ConfigDef:"type=STRING,required=true"`
	CsvHeader                string `ConfigDef:"type=STRING,required=true"`
	CsvReplaceNewLines       bool   `ConfigDef:"type=BOOLEAN,required=true"`
	CsvReplaceNewLinesString string `ConfigDef:"type=STRING,required=true"`
	CsvCustomDelimiter       string `ConfigDef:"type=STRING,required=true"`
	CsvCustomEscape          string `ConfigDef:"type=STRING,required=true"`
	CsvCustomQuote           string `ConfigDef:"type=STRING,required=true"`

	/** For JSON **/
	JsonMode string `ConfigDef:"type=STRING,required=true"`

	/** For TEXT Content **/
	TextFieldPath          string `ConfigDef:"type=STRING,required=true"`
	TextRecordSeparator    string `ConfigDef:"type=STRING,required=true"`
	TextFieldMissingAction string `ConfigDef:"type=STRING,required=true"`
	TextEmptyLineIfNull    bool   `ConfigDef:"type=BOOLEAN,required=true"`

	/** For AVRO Content **/
	AvroSchemaSource                  string   `ConfigDef:"type=STRING,required=true"`
	AvroSchema                        string   `ConfigDef:"type=STRING,required=true"`
	RegisterSchema                    bool     `ConfigDef:"type=BOOLEAN,required=true"`
	SchemaRegistryUrlsForRegistration []string `ConfigDef:"type=LIST,required=true"`
	SchemaRegistryUrls                []string `ConfigDef:"type=LIST,required=true"`
	SchemaLookupMode                  string   `ConfigDef:"type=STRING,required=true"`
	SubjectToRegister                 string   `ConfigDef:"type=STRING,required=true"`
	SchemaId                          float64  `ConfigDef:"type=NUMBER,required=true"`
	IncludeSchema                     bool     `ConfigDef:"type=BOOLEAN,required=true"`
	AvroCompression                   string   `ConfigDef:"type=STRING,required=true"`

	/** For Binary Content **/
	BinaryFieldPath string `ConfigDef:"type=STRING,required=true"`

	/** For Protobuf Content **/
	ProtoDescriptorFile string `ConfigDef:"type=STRING,required=true"`
	MessageType         string `ConfigDef:"type=STRING,required=true"`

	/** For Whole File Content **/
	FileNameEL                 string `ConfigDef:"type=STRING,required=true,evaluation=EXPLICIT"`
	WholeFileExistsAction      string `ConfigDef:"type=STRING,required=true"`
	IncludeChecksumInTheEvents bool   `ConfigDef:"type=BOOLEAN,required=true"`
	ChecksumAlgorithm          string `ConfigDef:"type=STRING,required=true"`

	/** For XML Content **/
	XmlPrettyPrint    bool   `ConfigDef:"type=BOOLEAN,required=true"`
	XmlValidateSchema bool   `ConfigDef:"type=BOOLEAN,required=true"`
	XmlSchema         string `ConfigDef:"type=STRING,required=true"`
	IsDelimited       bool   `ConfigDef:"type=BOOLEAN,required=true"`

	RecordWriterFactory recordio.RecordWriterFactory
}

func (d *DataGeneratorFormatConfig) Init(
	dataFormat string,
	stageContext api.StageContext,
	issues []validation.Issue,
) []validation.Issue {
	switch dataFormat {
	case "TEXT":
		d.RecordWriterFactory = &textrecord.TextWriterFactoryImpl{TextFieldPath: d.TextFieldPath}
	case "JSON":
		d.RecordWriterFactory = &jsonrecord.JsonWriterFactoryImpl{Mode: d.JsonMode}
	case "BINARY":
		d.RecordWriterFactory = &binaryrecord.BinaryWriterFactoryImpl{BinaryFieldPath: d.BinaryFieldPath}
	case "WHOLE_FILE":
		// Supported format
	case "SDC_JSON":
		d.RecordWriterFactory = &sdcrecord.SDCRecordWriterFactoryImpl{}
	default:
		issues = append(issues, stageContext.CreateConfigIssue("Unsupported Data Format - "+dataFormat))
	}
	return issues
}
