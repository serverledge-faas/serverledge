package cli

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/labstack/gommon/log"
	"github.com/serverledge-faas/serverledge/internal/api"

	"github.com/serverledge-faas/serverledge/internal/client"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/workflow"
	"github.com/serverledge-faas/serverledge/utils"
	"github.com/spf13/cobra"
)

var ServerConfig config.RemoteServerConf

var rootCmd = &cobra.Command{
	Use:   "serverledge-cli",
	Short: "CLI utility for Serverledge",
	Long:  `CLI utility to interact with a Serverledge FaaS platform.`,
}

var invokeCmd = &cobra.Command{
	Use:   "invoke",
	Short: "Invokes a function",
	Run:   invoke,
}

var pollCmd = &cobra.Command{
	Use:   "poll",
	Short: "Polls the result of an asynchronous invocation",
	Run:   poll,
}

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Registers a new function",
	Run:   create,
}

var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Deletes a function",
	Run:   deleteFunction,
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists registered functions",
	Run:   listFunctions,
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Prints status information about the system",
	Run:   getStatus,
}

// ========== FUNCTION COMPOSITION ===========

var compCreateCmd = &cobra.Command{
	Use:   "create-workflow",
	Short: "Registers a new workflow",
	Run:   createWorkflow,
}

var compDeleteCmd = &cobra.Command{
	Use:   "delete-workflow",
	Short: "Deletes a workflow and optionally the associated functions",
	Run:   deleteWorkflow,
}

var compListCmd = &cobra.Command{
	Use:   "list-workflows",
	Short: "Lists registered workflows",
	Run:   listWorkflows,
}

var compInvokeCmd = &cobra.Command{
	Use:   "invoke-workflow",
	Short: "Invokes a workflow",
	Run:   invokeWorkflow,
}

var compName, funcName, runtime, handler, customImage, src, qosClass, jsonSrc string
var requestId string
var memory int64
var cpuDemand, qosMaxRespT float64
var inputs []string
var outputs []string
var params []string
var paramsFile string
var asyncInvocation bool
var verbose bool
var returnOutput bool
var update bool
var maxConcurrency int16

func Init() {
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")
	rootCmd.PersistentFlags().StringVarP(&ServerConfig.Host, "host", "H", ServerConfig.Host, "remote Serverledge host")
	rootCmd.PersistentFlags().IntVarP(&ServerConfig.Port, "port", "P", ServerConfig.Port, "remote Serverledge port")

	rootCmd.AddCommand(invokeCmd)
	invokeCmd.Flags().StringVarP(&funcName, "function", "f", "", "name of the function")
	invokeCmd.Flags().Float64VarP(&qosMaxRespT, "resptime", "", -1.0, "Max. response time (optional)")
	invokeCmd.Flags().StringVarP(&qosClass, "class", "c", "", "QoS class (optional)")
	invokeCmd.Flags().StringSliceVarP(&params, "param", "p", nil, "Function parameter: <name>:<value>")
	invokeCmd.Flags().StringVarP(&paramsFile, "params_file", "j", "", "File containing parameters (JSON)")
	invokeCmd.Flags().BoolVarP(&asyncInvocation, "async", "a", false, "Asynchronous invocation")
	invokeCmd.Flags().BoolVarP(&returnOutput, "ret_output", "o", false, "Capture function output (if supported by used runtime)")

	rootCmd.AddCommand(createCmd)
	createCmd.Flags().BoolVarP(&update, "update", "u", false, "Overwrite any function with the same name")
	createCmd.Flags().StringVarP(&funcName, "function", "f", "", "name of the function")
	createCmd.Flags().StringVarP(&runtime, "runtime", "", "python38", "runtime for the function")
	createCmd.Flags().StringVarP(&handler, "handler", "", "", "function handler (runtime specific)")
	createCmd.Flags().Int64VarP(&memory, "memory", "", 128, "memory (in MB) for the function")
	createCmd.Flags().Int16VarP(&maxConcurrency, "max_concurrency", "C", 1, "max concurrency for the function (if supported by the runtime)")
	createCmd.Flags().Float64VarP(&cpuDemand, "cpu", "", 0.0, "estimated CPU demand for the function (1.0 = 1 core)")
	createCmd.Flags().StringVarP(&src, "src", "", "", "source for the function (single file, directory or TAR archive) (not necessary for runtime==custom)")
	createCmd.Flags().StringVarP(&customImage, "custom_image", "", "", "custom container image (only if runtime == 'custom')")
	createCmd.Flags().StringSliceVarP(&inputs, "input", "i", nil, "Input parameter: <name>:<type>")
	createCmd.Flags().StringSliceVarP(&outputs, "output", "o", nil, "Output specification: <name>:<type>")

	rootCmd.AddCommand(deleteCmd)
	deleteCmd.Flags().StringVarP(&funcName, "function", "f", "", "name of the function")

	rootCmd.AddCommand(listCmd)

	rootCmd.AddCommand(statusCmd)

	rootCmd.AddCommand(pollCmd)
	pollCmd.Flags().StringVarP(&requestId, "request", "", "", "ID of the async request")

	// Workflow

	rootCmd.AddCommand(compInvokeCmd)
	compInvokeCmd.Flags().StringVarP(&compName, "workflow", "f", "", "name of the workflow")
	compInvokeCmd.Flags().Float64VarP(&qosMaxRespT, "resptime", "r", -1.0, "Max. response time (optional)")
	compInvokeCmd.Flags().StringVarP(&qosClass, "class", "c", "", "QoS class (optional)")
	compInvokeCmd.Flags().StringSliceVarP(&params, "param", "p", nil, "Workflow parameter: <name>:<value>")
	compInvokeCmd.Flags().StringVarP(&paramsFile, "params_file", "j", "", "File containing parameters (JSON) for workflow")
	compInvokeCmd.Flags().BoolVarP(&asyncInvocation, "async", "a", false, "Asynchronous workflow invocation")

	rootCmd.AddCommand(compCreateCmd)
	compCreateCmd.Flags().StringVarP(&compName, "workflow", "f", "", "name of the workflow")
	compCreateCmd.Flags().StringVarP(&jsonSrc, "src", "s", "", "source Amazon States Language file  that defines the workflow")

	rootCmd.AddCommand(compDeleteCmd)
	compDeleteCmd.Flags().StringVarP(&compName, "workflow", "f", "", "name of the workflow")

	rootCmd.AddCommand(compListCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func showHelpAndExit(cmd *cobra.Command) {
	err := cmd.Help()
	if err != nil {
		fmt.Printf("Error while showing help for %s: %s\n", cmd.Use, err)
	}
	os.Exit(1)
}

func invoke(cmd *cobra.Command, args []string) {
	if len(funcName) < 1 {
		fmt.Printf("Invalid function name.\n")
		showHelpAndExit(cmd)
	}

	// Parse parameters
	paramsMap := make(map[string]interface{})

	// Parameters can be specified either via file ("--params_file") or via cli ("--param")
	if len(params) > 0 && len(paramsFile) > 0 {
		fmt.Println("Parameters must be specified using either --param OR --params_file")
		os.Exit(1)
	}
	if len(params) > 0 {
		for _, rawParam := range params {
			tokens := strings.Split(rawParam, ":")
			if len(tokens) < 2 {
				showHelpAndExit(cmd)
			}
			paramsMap[tokens[0]] = strings.Join(tokens[1:], ":")
		}
	}
	if len(paramsFile) > 0 {
		jsonFile, err := os.Open(paramsFile)

		defer func(jsonFile *os.File) {
			err := jsonFile.Close()
			if err != nil {
				fmt.Printf("Could not close JSON file '%s'\n", jsonFile.Name())
				os.Exit(1)
			}
		}(jsonFile)

		byteValue, _ := io.ReadAll(jsonFile)
		err = json.Unmarshal(byteValue, &paramsMap)
		if err != nil {
			fmt.Printf("Could not parse JSON-encoded parameters from '%s'\n", paramsFile)
			os.Exit(1)
		}
	}

	// Prepare request
	request := client.InvocationRequest{
		Params:   paramsMap,
		QoSClass: api.DecodeServiceClass(qosClass),
		// QoSClass:        qosClass,
		QoSMaxRespT:     qosMaxRespT,
		CanDoOffloading: true,
		ReturnOutput:    returnOutput,
		Async:           asyncInvocation}
	invocationBody, err := json.Marshal(request)
	if err != nil {
		showHelpAndExit(cmd)
	}

	// Send invocation request
	url := fmt.Sprintf("http://%s:%d/invoke/%s", ServerConfig.Host, ServerConfig.Port, funcName)
	resp, err := utils.PostJson(url, invocationBody)
	if err != nil {
		fmt.Printf("Invocation failed: %v\n", err)
		os.Exit(2)
	}
	utils.PrintJsonResponse(resp.Body)
}

func buildSignature() (*function.Signature, error) {
	sb := function.NewSignature()

	if inputs != nil {
		for _, str := range inputs {
			tokens := strings.Split(str, ":")
			if len(tokens) < 2 {
				return nil, fmt.Errorf("invalid input specification: %s", str)
			}
			dataType, err := function.StringToDataType(tokens[1])
			if err != nil {
				return nil, fmt.Errorf("\ninvalid signature input specification '%s': valid types are Int, Text, Float, Bool, Array[Int], Array[Text], Array[Float] or Array[Bool]", tokens[1])
			}
			sb = sb.AddInput(tokens[0], dataType)
		}
	}
	if outputs != nil {
		for _, str := range outputs {
			tokens := strings.Split(str, ":")
			if len(tokens) < 2 {
				return nil, fmt.Errorf("invalid output specification: %s", str)
			}
			dataType, err := function.StringToDataType(tokens[1])
			if err != nil {
				return nil, fmt.Errorf("\ninvalid signature input specification '%s'. Available types are Int, Text, Float, Bool, ArrayInt, ArrayText, ArrayFloat, ArrayBool, ArrayArrayInt, ArrayArrayFloat", tokens[1])
			}
			sb = sb.AddOutput(tokens[0], dataType)
		}
	}

	return sb.Build(), nil
}

func create(cmd *cobra.Command, args []string) {
	if funcName == "" || runtime == "" {
		showHelpAndExit(cmd)
	}
	if runtime == "custom" && customImage == "" {
		showHelpAndExit(cmd)
	} else if runtime != "custom" && src == "" {
		showHelpAndExit(cmd)
	}

	var encoded string
	if runtime != "custom" {
		srcContent, err := ReadSourcesAsTar(src)
		if err != nil {
			fmt.Printf("%v\n", err)
			os.Exit(3)
		}
		encoded = base64.StdEncoding.EncodeToString(srcContent)
	} else {
		encoded = ""
	}

	// When signature is specified we build it from the input. Otherwise we defined an EMPTY signature (accepts and returns nothing)
	var sig *function.Signature = nil
	if (inputs != nil && len(inputs) > 0) || (outputs != nil && len(outputs) > 0) {
		s, err := buildSignature()
		if err != nil {
			errHelp := cmd.Help()
			if errHelp != nil {
				log.Errorf("failed to call cmd.Help: %v\n", errHelp)
				return
			}
			os.Exit(4)
		}
		sig = s
		fmt.Printf("Parsed signature: %v\n", s)
	} else {
		sig = function.NewSignature().Build()
	}

	request := function.Function{
		Name:            funcName,
		Handler:         handler,
		Runtime:         runtime,
		MaxConcurrency:  maxConcurrency,
		MemoryMB:        memory,
		CPUDemand:       cpuDemand,
		TarFunctionCode: encoded,
		CustomImage:     customImage,
		Signature:       sig,
	}
	requestBody, err := json.Marshal(request)
	if err != nil {
		showHelpAndExit(cmd)
	}

	apiName := "create"
	if update {
		apiName = "update"
	}

	url := fmt.Sprintf("http://%s:%d/%s", ServerConfig.Host, ServerConfig.Port, apiName)
	resp, err := utils.PostJson(url, requestBody)
	if err != nil {
		// TODO: check returned error code
		fmt.Printf("Creation request failed: %v\n", err)
		os.Exit(2)
	}
	utils.PrintJsonResponse(resp.Body)
}

func ReadSourcesAsTar(srcPath string) ([]byte, error) {
	// if we are on Windows, inverts the slash
	if IsWindows() {
		srcPath = strings.Replace(srcPath, "/", "\\", -1)
	}

	fileInfo, err := os.Stat(srcPath)
	if err != nil {
		return nil, fmt.Errorf("Missing source file")
	}

	var tarFileName string

	if fileInfo.IsDir() || !strings.HasSuffix(srcPath, ".tar") {
		// Creates a temporary dir (cross platform)
		file, err := os.CreateTemp("", "serverledgesource")
		if err != nil {
			return nil, err
		}
		defer func(file *os.File) {
			name := file.Name()
			err := file.Close()
			if err != nil {
				fmt.Printf("Error while closing file '%s': %v", name, err)
				os.Exit(1)
			}
			err = os.Remove(name)
			if err != nil {
				fmt.Printf("Error while trying to remove file '%s': %v", name, err)
				os.Exit(1)
			}
		}(file)

		err = utils.Tar(srcPath, file)
		if err != nil {
			fmt.Printf("Error while trying to tar file '%s'\n", srcPath)
			os.Exit(1)
		}
		tarFileName = file.Name()
	} else {
		// this is already a tar file
		tarFileName = srcPath
	}

	return os.ReadFile(tarFileName)
}

func deleteFunction(cmd *cobra.Command, args []string) {
	request := function.Function{Name: funcName}
	requestBody, err := json.Marshal(request)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(2)
	}

	url := fmt.Sprintf("http://%s:%d/delete", ServerConfig.Host, ServerConfig.Port)
	resp, err := utils.PostJson(url, requestBody)
	if err != nil {
		fmt.Printf("Deletion request failed: %v\n", err)
		os.Exit(2)
	}
	utils.PrintJsonResponse(resp.Body)
}

func listFunctions(cmd *cobra.Command, args []string) {
	url := fmt.Sprintf("http://%s:%d/function", ServerConfig.Host, ServerConfig.Port)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("List request failed: %v\n", err)
		os.Exit(2)
	}
	utils.PrintJsonResponse(resp.Body)
}

func getStatus(cmd *cobra.Command, args []string) {
	url := fmt.Sprintf("http://%s:%d/status", ServerConfig.Host, ServerConfig.Port)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("Invocation failed: %v", err)
		os.Exit(2)
	}
	utils.PrintJsonResponse(resp.Body)
}

func poll(cmd *cobra.Command, args []string) {
	if len(requestId) < 1 {
		showHelpAndExit(cmd)
	}

	url := fmt.Sprintf("http://%s:%d/poll/%s", ServerConfig.Host, ServerConfig.Port, requestId)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("Polling request failed: %v\n", err)
		os.Exit(2)
	}
	utils.PrintJsonResponse(resp.Body)
	fmt.Println()
}

func invokeWorkflow(cmd *cobra.Command, args []string) {
	if len(compName) < 1 {
		fmt.Printf("Invalid workflow name.\n")
		cmd.Help()
		os.Exit(1)
	}

	// Parse parameters
	paramsMap := make(map[string]interface{})

	// Parameters can be specified either via file ("--params_file") or via cli ("--param")
	if len(params) > 0 && len(paramsFile) > 0 {
		fmt.Println("Parameters must be specified using either --param OR --params_file")
		os.Exit(1)
	}
	if len(params) > 0 {
		for _, rawParam := range params {
			tokens := strings.Split(rawParam, ":")
			if len(tokens) < 2 {
				cmd.Help()
				return
			}
			paramsMap[tokens[0]] = strings.Join(tokens[1:], ":")
		}
	}
	if len(paramsFile) > 0 {
		jsonFile, err := os.Open(paramsFile)
		defer jsonFile.Close()
		byteValue, _ := io.ReadAll(jsonFile)
		err = json.Unmarshal(byteValue, &paramsMap)
		if err != nil {
			fmt.Printf("Could not parse JSON-encoded parameters from '%s'\n", paramsFile)
			os.Exit(1)
		}
	}

	request := client.WorkflowInvocationRequest{
		Params:          paramsMap,
		CanDoOffloading: true,
		Async:           asyncInvocation}
	invocationBody, err := json.Marshal(request)
	if err != nil {
		cmd.Help()
		os.Exit(1)
	}

	// Send invocation request
	url := fmt.Sprintf("http://%s:%d/workflow/invoke/%s", ServerConfig.Host, ServerConfig.Port, compName)
	resp, err := utils.PostJson(url, invocationBody)
	if err != nil {
		fmt.Println(err)
		if resp != nil {
			utils.PrintErrorResponse(resp.Body)
		}
		os.Exit(2)
	}
	utils.PrintJsonResponse(resp.Body)
}

func createWorkflow(cmd *cobra.Command, args []string) {
	if compName == "" || jsonSrc == "" {
		cmd.Help()
		os.Exit(1)
	}

	src, err := os.ReadFile(jsonSrc)
	if err != nil {
		fmt.Printf("Could not read source file: %s\n", jsonSrc)
		os.Exit(1)
	}
	encoded := base64.StdEncoding.EncodeToString(src)
	request := client.WorkflowCreationRequest{
		Name:   compName,
		ASLSrc: encoded}

	requestBody, err := json.Marshal(request)
	if err != nil {
		cmd.Help()
		os.Exit(3)
	}

	url := fmt.Sprintf("http://%s:%d/workflow/create", ServerConfig.Host, ServerConfig.Port)
	resp, err := utils.PostJson(url, requestBody)
	if err != nil {
		// TODO: check returned error code
		fmt.Printf("Creation request failed: %v\n", err)
		os.Exit(2)
	}
	utils.PrintJsonResponse(resp.Body)
}

func deleteWorkflow(cmd *cobra.Command, args []string) {
	if compName == "" {
		cmd.Help()
		os.Exit(1)
	}
	request := workflow.Workflow{
		Name: compName, // TODO: passing the name as a string is enough
	}
	requestBody, err := json.Marshal(request)
	if err != nil {
		fmt.Printf("Error: %v", err)
		os.Exit(2)
	}
	url := fmt.Sprintf("http://%s:%d/workflow/delete", ServerConfig.Host, ServerConfig.Port)
	resp, err := utils.PostJson(url, requestBody)
	if err != nil {
		fmt.Printf("Deletion request failed: %v\n", err)
		os.Exit(2)
	}
	utils.PrintJsonResponse(resp.Body)
}

func listWorkflows(cmd *cobra.Command, args []string) {
	url := fmt.Sprintf("http://%s:%d/workflow/list", ServerConfig.Host, ServerConfig.Port)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("List request failed: %v\n", err)
		os.Exit(2)
	}
	utils.PrintJsonResponse(resp.Body)
}

func IsWindows() bool {
	return os.PathSeparator == '\\' && os.PathListSeparator == ';'
}
