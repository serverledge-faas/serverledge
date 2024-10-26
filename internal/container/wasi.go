package container

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/bytecodealliance/wasmtime-go/v25"
	"github.com/grussorusso/serverledge/utils"
)

type WasiType string

const WASI_TYPE_MODULE WasiType = "module"
const WASI_TYPE_COMPONENT WasiType = "component"

type WasiFactory struct {
	ctx     context.Context
	runners map[string]*wasiRunner
	engine  *wasmtime.Engine
}

type wasiRunner struct {
	wasiType WasiType // WasiModule is executed using wasmtime-go; WasiComponent using Wasmtime CLI
	env      []string // List of KEY=VALUE pairs
	dir      string   // Wasm Directory
	mount    string   // Wasm Directory is preloaded to this mount-point
	// WASI Module Specifics
	store  *wasmtime.Store  // Group of WASM instances
	linker *wasmtime.Linker // Used to instantiate module
	module *wasmtime.Module // Compiled WASM
	stdout *os.File         // Temporary file for the stdout
	stderr *os.File         // Temporary file for the stderr
	// WASI Component Specifics
	cliArgs []string
}

func (wr *wasiRunner) Close() {
	if wr.module != nil {
		wr.module.Close()
	}
	if wr.linker != nil {
		wr.linker.Close()
	}
	if wr.store != nil {
		wr.store.Close()
	}
	if wr.stdout != nil {
		wr.stdout.Close()
	}
	if wr.stderr != nil {
		wr.stderr.Close()
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
	wasiFactory := &WasiFactory{ctx, make(map[string]*wasiRunner), engine}
	if factories == nil {
		factories = make(map[string]Factory)
	}
	factories[WASI_FACTORY_KEY] = wasiFactory
	return wasiFactory
}

// Image is the ID
// NOTE: this approach requires Runtime to be set to wasi and CustomImage to an identifier (e.g. function name)
func (wf *WasiFactory) Create(image string, opts *ContainerOptions) (ContainerID, error) {
	// Create the new runner
	wf.runners[image] = &wasiRunner{env: opts.Env}
	return image, nil
}

// Untar the decoded function code into a temporary directory
func (wf *WasiFactory) CopyToContainer(contID ContainerID, content io.Reader, destPath string) error {
	// Create temporary directory to store untar-ed wasm file
	dir, err := os.MkdirTemp("", contID)
	if err != nil {
		return fmt.Errorf("[WasiFactory] Failed to create temporary directory for %s: %v", contID, err)
	}
	// Save directory name
	wf.runners[contID].dir = dir
	// Untar code
	if err := utils.Untar(content, dir); err != nil {
		return fmt.Errorf("[WasiFactory] Failed to untar code for %s: %v", contID, err)
	}
	// NOTE: hard-coding `destPath` as `/`
	// this is required to correctly use the official Python interpreter
	wf.runners[contID].mount = "/"
	return nil
}

// WASI Module: compiles the module
// Component: creates the CLI command
// NOTE: using contID (set as custom_image from CLI as the wasm filename inside the tar)
func (wf *WasiFactory) Start(contID ContainerID) error {
	// Get the wasi runner
	wasiRunner, ok := wf.runners[contID]
	if !ok {
		return fmt.Errorf("[WasiFactory]: no runner with %s found", contID)
	}

	// Create new store
	wasiRunner.store = wasmtime.NewStore(wf.engine)
	// Create WASI Configuration
	wasiConfig := wasmtime.NewWasiConfig()

	// Create temporary files for stdout and stderr for this function
	stdout, err := os.CreateTemp("", fmt.Sprintf("%s-stdout", contID))
	if err != nil {
		return fmt.Errorf("[WasiFactory]: failed to create temp stdout file for %s: %v", contID, err)
	}
	stderr, err := os.CreateTemp("", fmt.Sprintf("%s-stdout", contID))
	if err != nil {
		return fmt.Errorf("[WasiFactory]: failed to create temp stderr file for %s: %v", contID, err)
	}
	// Set wasmtime to use the temporary files for stdout and stderr
	wasiConfig.SetStdoutFile(stdout.Name())
	wasiConfig.SetStderrFile(stderr.Name())
	// Save the references to the temporary files
	wasiRunner.stdout = stdout
	wasiRunner.stderr = stderr

	// Mount the temporary directory to the specified mount point
	if err := wasiConfig.PreopenDir(wasiRunner.dir, wasiRunner.mount); err != nil {
		return fmt.Errorf("[WasiFactory] Failed to preopen %s: %v", wasiRunner.mount, err)
	}

	// Splitting the env array to separate keys and values
	// Assuming env is formatted correctly: KEY=VALUE
	var envKeys, envVals []string
	for _, v := range wasiRunner.env {
		split := strings.Split(v, "=")
		key := split[0]
		value := split[1]
		envKeys = append(envKeys, key)
		envVals = append(envVals, value)
	}
	// Set environment variables in WASI
	wasiConfig.SetEnv(envKeys, envVals)

	// Save the WASI Configuration to the store
	wasiRunner.store.SetWasi(wasiConfig)

	// Create a linker
	wasiRunner.linker = wasmtime.NewLinker(wf.engine)
	if err := wasiRunner.linker.DefineWasi(); err != nil {
		wasiRunner.Close()
		return fmt.Errorf("[WasiFactory] Failed to define WASI in the linker for %s: %v", contID, err)
	}

	// Determine wasm file name
	wasmFileName := filepath.Join(wasiRunner.dir, contID+".wasm")

	// Read module code
	moduleData, err := os.ReadFile(wasmFileName)
	if err != nil {
		wasiRunner.Close()
		return fmt.Errorf("[WasiFactory] Failed to read the WASI code for %s: %v", contID, err)
	}
	// Try to compile the WASI Module
	module, err := wasmtime.NewModule(wf.engine, moduleData)
	if err != nil {
		if strings.HasPrefix(err.Error(), "failed to parse WebAssembly module") {
			// File is a WASI Component
			wasiRunner.cliArgs = append(wasiRunner.cliArgs, "--dir", wasiRunner.dir+"::"+wasiRunner.mount)
			for _, v := range wasiRunner.env {
				wasiRunner.cliArgs = append(wasiRunner.cliArgs, "--env")
				wasiRunner.cliArgs = append(wasiRunner.cliArgs, v)
			}
			wasiRunner.cliArgs = append(wasiRunner.cliArgs, wasmFileName)
			wasiRunner.wasiType = WASI_TYPE_COMPONENT
			return nil
		}
		// There was another error; wasm file is incorrect
		wasiRunner.Close()
		return fmt.Errorf("[WasiFactory] Failed to create WASI Module for %s: %v", contID, err)
	}
	// File was compiled successfully
	wasiRunner.module = module
	wasiRunner.wasiType = WASI_TYPE_MODULE
	return nil
}

func (wf *WasiFactory) Destroy(id ContainerID) error {
	if wasiRunner, ok := wf.runners[id]; ok {
		wasiRunner.Close()
	}
	delete(wf.runners, id)
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
