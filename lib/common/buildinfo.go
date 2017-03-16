package common

type BuildInfo struct {
	BuiltBy           string
	BuiltDate         string
	BuiltRepoSha      string
	SourceMd5Checksum string
	Version           string
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
