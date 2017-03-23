package validation

type Issue struct {
	InstanceName   string
	ConfigGroup    string
	ConfigName     string
	Message        string
	AdditionalInfo map[string]string
}
