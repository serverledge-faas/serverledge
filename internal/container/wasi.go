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

const WasiModule WasiType = "module"
const WasiComponent WasiType = "component"

type WasiFactory struct {
	ctx     context.Context
	runners map[string]*wasiRunner
}

type wasiRunner struct {
	wasiType WasiType  // WasiModule is executed using wasmtime-go; WasiComponent using Wasmtime CLI
	env      []string  // List of KEY=VALUE
	mount    string    // Directories are preloaded to this mount-point
	tar      io.Reader // Tar of .wasm and other required files
	// WASI Module Specifics
	store  *wasmtime.Store  // Group of WASM instances
	linker *wasmtime.Linker // Used to instantiate module
	module *wasmtime.Module // Compiled WASM
	stdout *os.File         // Temporary file for the stdout
	stderr *os.File         // Temporary file for the stderr
	// WASI Component Specifics
	cliArgs []string

	// TODO: check if it's possible to set RAM and CPU quota in CLI and wasmtime-go
	// - config.SetMaxWasmStack can be used to set the max "RAM"
	// - fuel or epoch interruption can probably be used to simulate the CPU quota
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
	wasiFactory := &WasiFactory{ctx, make(map[string]*wasiRunner)}
	if factories == nil {
		factories = make(map[string]Factory)
	}
	factories[WASI_FACTORY_KEY] = wasiFactory
	return wasiFactory
}

// Image is the ID
// NOTE: this approach requires Runtime to be set to wasi and CustomImage to an identifier (e.g. function name)
func (wf *WasiFactory) Create(image string, opts *ContainerOptions) (ContainerID, error) {
	wf.runners[image] = &wasiRunner{env: opts.Env}
	return image, nil
}

// Saves the decoded function code in the Wasi Runner
func (wf *WasiFactory) CopyToContainer(contID ContainerID, content io.Reader, destPath string) error {
	wf.runners[contID].tar = content
	wf.runners[contID].mount = destPath
	return nil
}

// WASI Module: compiles the module
// Component: creates the CLI command
// NOTE: using contID (set as custom_image from CLI as the wasm filename inside the tar)
func (wf *WasiFactory) Start(contID ContainerID) error {
	wasiRunner, ok := wf.runners[contID]
	if !ok {
		return fmt.Errorf("[WasiFactory]: no runner with %s found", contID)
	}

	// Create Store configuration
	storeConfig := wasmtime.NewConfig()
	storeConfig.SetWasmRelaxedSIMD(true)
	storeConfig.SetWasmBulkMemory(true)
	storeConfig.SetWasmMultiValue(true)
	// NOTE: this can probably be the RAM limit
	// storeConfig.SetMaxWasmStack()

	// Create wasmtime engine
	engine := wasmtime.NewEngineWithConfig(storeConfig)
	// Create new store
	wasiRunner.store = wasmtime.NewStore(engine)
	// Create WASI Configuration
	wasiConfig := wasmtime.NewWasiConfig()

	stdout, err := os.CreateTemp("", fmt.Sprintf("%s-stdout", contID))
	if err != nil {
		return fmt.Errorf("[WasiFactory]: failed to create temp stdout file for %s: %v", contID, err)
	}
	stderr, err := os.CreateTemp("", fmt.Sprintf("%s-stdout", contID))
	if err != nil {
		return fmt.Errorf("[WasiFactory]: failed to create temp stderr file for %s: %v", contID, err)
	}
	wasiConfig.SetStdoutFile(stdout.Name())
	wasiConfig.SetStderrFile(stderr.Name())
	wasiRunner.stdout = stdout
	wasiRunner.stderr = stderr

	untarDest, err := os.MkdirTemp("", contID)
	if err != nil {
		return fmt.Errorf("[WasiFactory] Failed to create temporary directory for %s: %v", contID, err)
	}

	wasiConfig.PreopenDir(untarDest, wasiRunner.mount)

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
	wasiConfig.SetEnv(envKeys, envVals)

	// Save the WASI Configuration to the store
	wasiRunner.store.SetWasi(wasiConfig)

	// Create a linker
	wasiRunner.linker = wasmtime.NewLinker(engine)
	if err := wasiRunner.linker.DefineWasi(); err != nil {
		wasiRunner.Close()
		return fmt.Errorf("[WasiFactory] Failed to define WASI in the linker for %s: %v", contID, err)
	}

	// Untar the code in a temporary folder
	if err := utils.Untar(wasiRunner.tar, untarDest); err != nil {
		return fmt.Errorf("[WasiFactory] Faield to untar code for %s: %v", contID, err)
	}

	wasmFileName := filepath.Join(untarDest, contID+".wasm")

	// Read module code
	moduleData, err := os.ReadFile(wasmFileName)
	if err != nil {
		wasiRunner.Close()
		return fmt.Errorf("[WasiFactory] Failed to read the WASI code for %s: %v", contID, err)
	}
	// Compile the WASI Module
	module, err := wasmtime.NewModule(engine, moduleData)
	if err != nil {
		if strings.HasPrefix(err.Error(), "failed to parse WebAssembly module") {
			// File is a WASI Component
			wasiRunner.cliArgs = append(wasiRunner.cliArgs, "--dir", untarDest+"::/app")
			for _, v := range wasiRunner.env {
				wasiRunner.cliArgs = append(wasiRunner.cliArgs, "--env")
				wasiRunner.cliArgs = append(wasiRunner.cliArgs, v)
			}
			wasiRunner.cliArgs = append(wasiRunner.cliArgs, wasmFileName)
			wasiRunner.wasiType = WasiComponent
			return nil
		}
		wasiRunner.Close()
		return fmt.Errorf("[WasiFactory] Failed to create WASI Module for %s: %v", contID, err)
	}
	wasiRunner.module = module
	wasiRunner.wasiType = WasiModule
	return nil
}

func (wf *WasiFactory) Destroy(id ContainerID) error {
	wasiRunner := wf.runners[id]
	if wasiRunner.wasiType == WasiModule {
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
	// NOTE: this can probably be the WasmStackSize
	log.Println("[WasiFactory] GetMemoryMB unimplemented")
	return 0, nil
}
