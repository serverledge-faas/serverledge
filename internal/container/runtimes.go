package container

// RuntimeInfo contains information about a supported function runtime env.
type RuntimeInfo struct {
	Image         string
	InvocationCmd []string
}

const CUSTOM_RUNTIME = "custom"
const WASI_RUNTIME = "wasi"

var refreshedImages = map[string]bool{}

var RuntimeToInfo = map[string]RuntimeInfo{
	"python310":  {"serverledge-faas/serverledge-python310", []string{"python", "/entrypoint.py"}},
	"nodejs17":   {"serverledge-faas/serverledge-nodejs17", []string{"node", "/entrypoint.js"}},
	"nodejs17ng": {"serverledge-faas/serverledge-nodejs17ng", []string{}},
}
