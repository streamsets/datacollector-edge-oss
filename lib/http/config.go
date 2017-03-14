package http

const (
	DefaultBindAddress = ":18633"
	DefaultRealm = "StreamSets"
)

type Config struct {
	BindAddress        string `toml:"bind-address"`
	AuthEnabled        bool   `toml:"auth-enabled"`
	Realm		   string `toml:"realm"`

}

// NewConfig returns a new Config with default settings.
func NewConfig() Config {
	return Config{
		BindAddress: DefaultBindAddress,
		AuthEnabled: true,
		Realm: DefaultRealm,
	}
}
