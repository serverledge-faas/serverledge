package container

// RuntimeInfo contains information about a supported function runtime env.
type RuntimeInfo struct {
	Image                string
	InvocationCmd        []string
	ConcurrencySupported bool
	Architectures        []string
}

const CUSTOM_RUNTIME = "custom"
const X86 = "amd64"
const ARM = "arm64"

var refreshedImages = map[string]bool{}

var RuntimeToInfo = map[string]RuntimeInfo{
	"python314":   {"grussorusso/serverledge-python314", []string{"python", "/entrypoint.py"}, true, []string{X86, ARM}},
	"nodejs17ng":  {"grussorusso/serverledge-nodejs17ng", []string{}, false, []string{X86, ARM}},
	"go125":       {"grussorusso/serverledge-go125", []string{"/entrypoint.sh"}, true, []string{X86, ARM}},
	"python312ml": {"grussorusso/serverledge-python312ml", []string{"python", "/entrypoint.py"}, true, []string{X86, ARM}},
}

// CustomRuntimeToInfo Map to keep track of architectures compatible with each custom runtime image associated with a function registered
// by users
var CustomRuntimeToInfo = map[string]RuntimeInfo{}
