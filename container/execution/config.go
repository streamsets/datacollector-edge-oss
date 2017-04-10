package execution

const (
	DefaultMaxBatchSize = 1000
)

type Config struct {
	MaxBatchSize int `toml:"max-batch-size"`
}

// NewConfig returns a new Config with default settings.
func NewConfig() Config {
	return Config{
		MaxBatchSize: DefaultMaxBatchSize,
	}
}
