package dpm

const (
	DefaultBaseUrl = "http://localhost:18631"
)

type Config struct {
	Enabled      bool   `toml:"enabled"`
	BaseUrl      string `toml:"base-url"`
	AppAuthToken string `toml:"app-auth-token"`
}

// NewConfig returns a new Config with default settings.
func NewConfig() Config {
	return Config{
		Enabled:      false,
		BaseUrl:      DefaultBaseUrl,
		AppAuthToken: "",
	}
}
