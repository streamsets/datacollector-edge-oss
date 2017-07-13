package common

const (
	CURRENT_OFFSET_VERSION = 2
	POLL_SOURCE_OFFSET_KEY = "$com.streamsets.sdc2go.pollsource.offset$"
)

type SourceOffset struct {
	Version int
	Offset  map[string]string
}

func GetDefaultOffset() SourceOffset {
	return SourceOffset{
		Version: CURRENT_OFFSET_VERSION,
		Offset:  map[string]string{POLL_SOURCE_OFFSET_KEY: ""},
	}
}
