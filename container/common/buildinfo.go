package common

type BuildInfo struct {
	BuiltBy           string `json:"builtBy"`
	BuiltDate         string `json:"builtDate"`
	BuiltRepoSha      string `json:"builtRepoSha"`
	SourceMd5Checksum string `json:"sourceMd5Checksum"`
	Version           string `json:"version"`
}

var (
	BuiltBy           string
	BuiltDate         string
	BuiltRepoSha      string
	SourceMd5Checksum string
	Version           string
)

func NewBuildInfo() (*BuildInfo, error) {
	buildInfo := BuildInfo{}
	buildInfo.BuiltBy = BuiltBy
	buildInfo.BuiltDate = BuiltDate
	buildInfo.BuiltRepoSha = BuiltRepoSha
	buildInfo.SourceMd5Checksum = SourceMd5Checksum
	buildInfo.Version = Version
	return &buildInfo, nil
}
