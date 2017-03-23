package http

const (
	DefaultBindAddress = ":18633"
)

type Config struct {
	BindAddress string `toml:"bind-address"`
}

// NewConfig returns a new Config with default settings.
func NewConfig() Config {
	return Config{
		BindAddress: DefaultBindAddress,
	}
}
