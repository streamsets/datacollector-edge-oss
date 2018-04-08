package dataparser

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"github.com/streamsets/datacollector-edge/container/recordio/jsonrecord"
	"github.com/streamsets/datacollector-edge/container/recordio/sdcrecord"
	"github.com/streamsets/datacollector-edge/container/recordio/textrecord"
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

	RecordReaderFactory recordio.RecordReaderFactory
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
		d.RecordReaderFactory = &textrecord.TextReaderFactoryImpl{}
	case "JSON":
		d.RecordReaderFactory = &jsonrecord.JsonReaderFactoryImpl{}
	case "SDC_JSON":
		d.RecordReaderFactory = &sdcrecord.SDCRecordReaderFactoryImpl{}
	default:
		issues = append(issues, stageContext.CreateConfigIssue("Unsupported Data Format - "+dataFormat))
	}
	return issues
}
