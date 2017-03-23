package dpm

const (
	DefaultBaseUrl              = "http://localhost:18631"
	AllLabel                    = "all"
	DefaultEventsRecipient      = "job-runner"
	DefaultPingFrequency        = 5000
	DefaultStatusEventsInterval = 60000
)

type Config struct {
	Enabled              bool     `toml:"enabled"`
	BaseUrl              string   `toml:"base-url"`
	AppAuthToken         string   `toml:"app-auth-token"`
	JobLabels            []string `toml:"job-labels"`
	EventsRecipient      string   `toml:"events-recipient"`
	PingFrequency        int      `toml:"ping-frequency"`
	StatusEventsInterval int      `toml:"status-events-interval"`
}

// NewConfig returns a new Config with default settings.
func NewConfig() Config {
	return Config{
		Enabled:              false,
		BaseUrl:              DefaultBaseUrl,
		AppAuthToken:         "",
		JobLabels:            []string{AllLabel},
		EventsRecipient:      DefaultEventsRecipient,
		PingFrequency:        DefaultPingFrequency,
		StatusEventsInterval: DefaultStatusEventsInterval,
	}
}
