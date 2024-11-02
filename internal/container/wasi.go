package container

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/bytecodealliance/wasmtime-go/v25"
	"github.com/grussorusso/serverledge/utils"
)

type WasiType string

const WASI_TYPE_MODULE WasiType = "module"
const WASI_TYPE_COMPONENT WasiType = "component"
const WASI_TYPE_UNDEFINED WasiType = "undefined"

type WasiFactory struct {
	ctx     context.Context
	runners sync.Map // ContainerID -> *wasiRunner
	engine  *wasmtime.Engine
}

type wasiRunner struct {
	copyInit, startInit sync.Once // Single initialization

	wasiType WasiType // WasiModule is executed using wasmtime-go; WasiComponent using Wasmtime CLI
	// WASI Module Specifics
	envKeys, envValues []string         // List of environment variables keys and values
	dir, mount         string           // Wasm Directory and its mount point
	linker             *wasmtime.Linker // Used to instantiate module
	module             *wasmtime.Module // Compiled WASM
	// WASI Component Specifics
	cliArgs []string
}

// Utility struct to keep configuration and temporary files
type wasiCustomConfig struct {
	wasiConfig     *wasmtime.WasiConfig // Actual Configuration
	stdout, stderr *os.File             // Temporary files for stdout and stderr
}

func (wr *wasiRunner) Close() {
	if wr.module != nil {
		wr.module.Close()
	}
	if wr.linker != nil {
		wr.linker.Close()
	}
	if wr.dir != "" {
		if err := os.RemoveAll(wr.dir); err != nil {
			log.Printf("[WasiFactory] Failed to delete temporary directory: %v", err)
		}
	}
}

func (wcc *wasiCustomConfig) Close() {
	if wcc.wasiConfig != nil {
		wcc.wasiConfig.Close()
	}
	if wcc.stdout != nil {
		wcc.stdout.Close()
		if err := os.Remove(wcc.stdout.Name()); err != nil {
			log.Printf("[WasiCustomConfig] Failed to remove stdout %s: %v", wcc.stdout.Name(), err)
		}
	}
	if wcc.stderr != nil {
		wcc.stderr.Close()
		if err := os.Remove(wcc.stderr.Name()); err != nil {
			log.Printf("[WasiCustomConfig] Failed to remove stderr %s: %v", wcc.stderr.Name(), err)
		}
	}
}

func InitWasiFactory() *WasiFactory {
	ctx := context.Background()
	// Create Engine configuration
	engineConfig := wasmtime.NewConfig()
	engineConfig.SetWasmRelaxedSIMD(true)
	engineConfig.SetWasmBulkMemory(true)
	engineConfig.SetWasmMultiValue(true)
	engineConfig.SetStrategy(wasmtime.StrategyCranelift)
	engineConfig.SetCraneliftOptLevel(wasmtime.OptLevelSpeed)

	// Create wasmtime engine, shared for all modules
	engine := wasmtime.NewEngineWithConfig(engineConfig)

	// Create the factory
	wasiFactory := &WasiFactory{ctx: ctx, engine: engine}
	if factories == nil {
		factories = make(map[string]Factory)
	}
	factories[WASI_FACTORY_KEY] = wasiFactory
	return wasiFactory
}

// Image is the ID
// NOTE: this approach requires Runtime to be set to wasi and CustomImage to an identifier (e.g. function name)
func (wf *WasiFactory) Create(image string, opts *ContainerOptions) (ContainerID, error) {
	_, ok := wf.runners.Load(image)
	if ok {
		return image, nil
	}
	var envKeys, envVals, cliArgs []string
	for _, v := range opts.Env {
		cliArgs = append(cliArgs, "--env", v)
		// Splitting the env array to separate keys and values
		// Assuming env is formatted correctly: KEY=VALUE
		split := strings.Split(v, "=")
		key := split[0]
		value := split[1]
		envKeys = append(envKeys, key)
		envVals = append(envVals, value)
	}

	wasiConfig := wasmtime.NewWasiConfig()
	wasiConfig.SetEnv(envKeys, envVals)

	wf.runners.Store(image, &wasiRunner{
		envKeys:   envKeys,
		envValues: envVals,
		cliArgs:   cliArgs,
		wasiType:  WASI_TYPE_UNDEFINED,
	})
	return image, nil
}

// Untar the decoded function code into a temporary directory
func (wf *WasiFactory) CopyToContainer(contID ContainerID, content io.Reader, destPath string) error {
	wrValue, _ := wf.runners.Load(contID) // assuming runners already exists
	wr := wrValue.(*wasiRunner)
	externalError := *new(error)
	wr.copyInit.Do(func() {
		// Additional buffer, used to determine if it's a URL or not
		var buffer bytes.Buffer

		// Create new reader that reads from content and writes to buffer
		teeReader := io.TeeReader(content, &buffer)

		// Read from the newly created reader
		data, err := io.ReadAll(teeReader)
		if err != nil {
			externalError = fmt.Errorf("[WasiFactory] Failed to read content: %v", err)
			return
		}
		// Restore content value (assuming code is a tar)
		content = &buffer

		// Check if data is a url
		u, err := url.ParseRequestURI(string(data))
		if err == nil && u.Scheme != "" && u.Host != "" {
			// data is url; it has to be downloaded
			resp, err := http.Get(string(data))
			if err != nil {
				externalError = fmt.Errorf("[WasiFactory] Failed to download code for %s: %v", contID, err)
				return
			}
			defer resp.Body.Close()

			// Content is now the downloaded tar file
			content = resp.Body
		}

		// Create temporary directory to store untar-ed wasm file
		dir, err := os.MkdirTemp("", contID)
		if err != nil {
			externalError = fmt.Errorf("[WasiFactory] Failed to create temporary directory for %s: %v", contID, err)
			return
		}
		// Untar code
		if err := utils.Untar(content, dir); err != nil {
			externalError = fmt.Errorf("[WasiFactory] Failed to untar code for %s: %v", contID, err)
			return
		}
		// NOTE: hard-coding `destPath` as `/`
		// this is required to correctly use the official Python interpreter
		wr.mount = "/"
		wr.dir = dir
		wr.cliArgs = append(wr.cliArgs, "--dir", wr.dir+"::"+wr.mount)
	})

	return externalError
}

// WASI Module: compiles the module
// Component: creates the CLI command
// NOTE: using contID (set as custom_image from CLI as the wasm filename inside the tar)
func (wf *WasiFactory) Start(contID ContainerID) error {
	// Get the wasi runner
	wrValue, _ := wf.runners.Load(contID)
	wr := wrValue.(*wasiRunner)

	externalError := *new(error)
	wr.startInit.Do(func() {
		// Create a linker
		wr.linker = wasmtime.NewLinker(wf.engine)
		if err := wr.linker.DefineWasi(); err != nil {
			wr.Close()
			externalError = fmt.Errorf("[WasiFactory] Failed to define WASI in the linker for %s: %v", contID, err)
			return
		}

		// Determine wasm file name
		wasmFileName := filepath.Join(wr.dir, contID+".wasm")

		// Try to compile the WASI Module
		module, err := wasmtime.NewModuleFromFile(wf.engine, wasmFileName)
		if err != nil {
			if strings.HasPrefix(err.Error(), "expected a WebAssembly module but was given a WebAssembly component") {
				// File is a WASI Component
				wr.cliArgs = append(wr.cliArgs, wasmFileName)
				wr.wasiType = WASI_TYPE_COMPONENT
				return
			}
			// There was another error; wasm file is incorrect
			wr.Close()
			externalError = fmt.Errorf("[WasiFactory] Failed to create WASI Module for %s: %v", contID, err)
			return
		}
		// File was compiled successfully
		wr.module = module
		wr.wasiType = WASI_TYPE_MODULE
	})
	return externalError
}

func (wf *WasiFactory) Destroy(id ContainerID) error {
	wrValue, ok := wf.runners.Load(id)
	if ok {
		wrValue.(*wasiRunner).Close()
		wf.runners.Delete(id)
	}
	return nil
}

func (wf *WasiFactory) HasImage(string) bool {
	log.Println("[WasiFactory] HasImage unimplemented")
	return false
}

func (wf *WasiFactory) PullImage(string) error {
	log.Println("[WasiFactory] PullImage unimplemented")
	return nil
}

func (wf *WasiFactory) GetIPAddress(ContainerID) (string, error) {
	log.Println("[WasiFactory] GetIPAddress unimplemented")
	return "", nil
}

func (wf *WasiFactory) GetMemoryMB(id ContainerID) (int64, error) {
	log.Println("[WasiFactory] GetMemoryMB unimplemented")
	return 0, nil
}

// Utility function to create a Wasi Configuration for this runner
// The WasiConfiguration cannot be shared among threads because it's not thread-safe
func (wr *wasiRunner) BuildWasiConfig(contID ContainerID, handler string, params string) (wasiCustomConfig, error) {
	var wcc wasiCustomConfig
	// Create new Wasi Configuration
	wasiConfig := wasmtime.NewWasiConfig()
	// Set environment variables
	wasiConfig.SetEnv(wr.envKeys, wr.envValues)

	// Create temporary files for stdout and stderr for this function
	stdout, err := os.CreateTemp("", fmt.Sprintf("%s-stdout", contID))
	if err != nil {
		return wcc, fmt.Errorf("[WasiRunner]: failed to create temp stdout file for %s: %v", contID, err)
	}
	stderr, err := os.CreateTemp("", fmt.Sprintf("%s-stdout", contID))
	if err != nil {
		return wcc, fmt.Errorf("[WasiRunner]: failed to create temp stderr file for %s: %v", contID, err)
	}

	// Set wasmtime to use the temporary files for stdout and stderr
	if err := wasiConfig.SetStdoutFile(stdout.Name()); err != nil {
		return wcc, fmt.Errorf("[WasiRunner] Failed to set stdout file: %v", err)
	}
	if err := wasiConfig.SetStderrFile(stderr.Name()); err != nil {
		return wcc, fmt.Errorf("[WasiRunner] Failed to set stderr file: %v", err)
	}

	// Mount the temporary directory to the specified mount point
	if err := wasiConfig.PreopenDir(wr.dir, wr.mount); err != nil {
		return wcc, fmt.Errorf("[WasiRunner] Failed to preopen %s: %v", wr.mount, err)
	}

	// Create argv (first element is usually the program name, leaving empty)
	argv := []string{""}
	if handler != "" {
		// Add handler if available (used in Python for the source file)
		argv = append(argv, wr.mount+handler)
	}
	// Add additional params as a JSON string
	argv = append(argv, params)

	// Set argv in Wasi
	wasiConfig.SetArgv(argv)

	// Save references into custom configuration
	wcc.wasiConfig = wasiConfig
	wcc.stdout = stdout
	wcc.stderr = stderr
	return wcc, nil
}
