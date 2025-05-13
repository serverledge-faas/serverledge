package container

// RuntimeInfo contains information about a supported function runtime env.
type RuntimeInfo struct {
	Image                string
	InvocationCmd        []string
	ConcurrencySupported bool
}

const CUSTOM_RUNTIME = "custom"

var refreshedImages = map[string]bool{}

var RuntimeToInfo = map[string]RuntimeInfo{
	"python310":  {"grussorusso/serverledge-python310", []string{"python", "/entrypoint.py"}, true},
	"nodejs17ng": {"grussorusso/serverledge-nodejs17ng", []string{}, false},
}
