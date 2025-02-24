package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cornelk/hashmap"
	"github.com/grussorusso/serverledge/internal/client"
	"github.com/grussorusso/serverledge/internal/fc"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/node"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

// ===== Function Composition =====

func CreateFunctionCompositionFromASL(e echo.Context) error {
	var creationRequest client.CompositionCreationFromASLRequest
	var body []byte
	body, errReadBody := io.ReadAll(e.Request().Body)
	if errReadBody != nil {
		return errReadBody
	}

	err := json.Unmarshal(body, &creationRequest)
	if err != nil && err != io.EOF {
		log.Printf("Could not parse compose request - error during unmarshal: %v", err)
		return err
	}

	// checking if the function already exists. If exists we return an error
	_, found := fc.GetFC(creationRequest.Name)
	if found {
		log.Printf("Dropping request for already existing composition '%s'", creationRequest.Name)
		return e.JSON(http.StatusConflict, "composition already exists")
	}

	log.Printf("New request: creation of composition %s", creationRequest.Name)

	decodedSrc, err := base64.StdEncoding.DecodeString(creationRequest.ASLSrc)
	if err != nil {
		log.Printf("Could not decode composition source ASL: %v", err)
		return e.JSON(http.StatusBadRequest, "composition already exists")
	}

	comp, err := fc.FromASL(creationRequest.Name, decodedSrc[:])
	if err != nil {
		log.Printf("Could not parse composition from ASL: %v", err)
		return e.JSON(http.StatusBadRequest, "composition already exists")
	}

	err = comp.SaveToEtcd()
	if err != nil {
		log.Printf("Failed creation: %v", err)
		return e.JSON(http.StatusServiceUnavailable, "")
	}

	response := struct{ Created string }{comp.Name}
	return e.JSON(http.StatusOK, response)
}

func CreateFunctionComposition(e echo.Context) error {
	var comp fc.Dag
	// here we expect to receive the function composition struct already parsed from JSON/YAML
	var body []byte
	body, errReadBody := io.ReadAll(e.Request().Body)
	if errReadBody != nil {
		return errReadBody
	}

	err := json.Unmarshal(body, &comp)
	if err != nil && err != io.EOF {
		log.Printf("Could not parse composition request - error during unmarshal: %v", err)
		return err
	}
	// checking if the function already exists. If exists we return an error
	alreadyPresent := comp.Exists() // TODO: we would need a system-wide lock here...
	if alreadyPresent {
		log.Printf("Dropping request for already existing composition '%s'", comp.Name)
		return e.JSON(http.StatusConflict, "composition already exists")
	}

	log.Printf("New request: creation of composition %s", comp.Name)

	// Check that functions exist
	for _, fName := range comp.GetUniqueDagFunctions() {
		f, exists := function.GetFunction(fName)
		if !exists {
			log.Printf("Dropping request for composition with non-existing function '%s'", fName)
			return e.JSON(http.StatusBadRequest, "composition with non-existing function")
		}
		if f.Signature == nil {
			return e.JSON(http.StatusBadRequest, "function "+fName+"has nil signature")
		}
	}

	err = comp.SaveToEtcd()
	if err != nil {
		log.Printf("Failed creation: %v", err)
		return e.JSON(http.StatusServiceUnavailable, "")
	}
	response := struct{ Created string }{comp.Name}
	return e.JSON(http.StatusOK, response)
}

// GetFunctionCompositions handles a request to list the function compositions available in the system.
func GetFunctionCompositions(c echo.Context) error {
	list, err := fc.GetAllFC()
	if err != nil {
		return c.String(http.StatusServiceUnavailable, "")
	}
	return c.JSON(http.StatusOK, list)
}

// DeleteFunctionComposition handles a function deletion request.
func DeleteFunctionComposition(c echo.Context) error {
	var comp fc.Dag
	// here we only need the name of the function composition (and if all function should be deleted with it)
	err := json.NewDecoder(c.Request().Body).Decode(&comp)
	if err != nil && err != io.EOF {
		log.Printf("Could not parse delete request - error during decoding: %v", err)
		return err
	}

	composition, ok := fc.GetFC(comp.Name) // TODO: we would need a system-wide lock here...
	if !ok {
		log.Printf("Dropping request for non existing function '%s'", comp.Name)
		return c.JSON(http.StatusNotFound, "the request function composition to delete does not exist")
	}

	log.Printf("New request: deleting %s", composition.Name)
	err = composition.Delete()
	if err != nil {
		log.Printf("Failed deletion: %v", err)
		return c.JSON(http.StatusServiceUnavailable, "")
	}

	response := struct{ Deleted string }{composition.Name}
	return c.JSON(http.StatusOK, response)
}

// InvokeFunctionComposition handles a function composition invocation request.
func InvokeFunctionComposition(e echo.Context) error {
	// gets the command line param value for -fc (the composition name)
	fcName := e.Param("fc")
	funComp, ok := fc.GetFC(fcName)
	if !ok {
		log.Printf("Dropping request for unknown FC '%s'", fcName)
		return e.JSON(http.StatusNotFound, "function composition '"+fcName+"' does not exist")
	}

	// we use invocation request that is specific to function compositions
	var fcInvocationRequest client.CompositionInvocationRequest
	err := json.NewDecoder(e.Request().Body).Decode(&fcInvocationRequest)
	if err != nil && err != io.EOF {
		log.Printf("Could not parse invoke request - error during decoding: %v", err)
		return e.JSON(http.StatusInternalServerError, "failed to parse composition invocation request. Check parameters and composition definition")
	}
	// gets a fc.CompositionRequest from the pool goroutine-safe cache.
	fcReq := compositionRequestsPool.Get().(*fc.CompositionRequest) // A pointer *function.CompositionRequest will be created if does not exists, otherwise removed from the pool
	defer compositionRequestsPool.Put(fcReq)                        // at the end of the function, the function.CompositionRequest is added to the pool.
	fcReq.Fc = funComp
	fcReq.Params = fcInvocationRequest.Params
	fcReq.Arrival = time.Now()

	// instead of saving only one RequestQoS, we save a map with an entry for each function in the composition
	fcReq.RequestQoSMap = fcInvocationRequest.RequestQoSMap

	fcReq.CanDoOffloading = fcInvocationRequest.CanDoOffloading
	fcReq.Async = fcInvocationRequest.Async
	fcReq.ReqId = fmt.Sprintf("%v-%s%d", funComp.Name, node.NodeIdentifier[len(node.NodeIdentifier)-5:], fcReq.Arrival.Nanosecond())
	// init fields if possibly not overwritten later
	fcReq.ExecReport.Reports = hashmap.New[fc.ExecutionReportId, *function.ExecutionReport]() // make(map[fc.ExecutionReportId]*function.ExecutionReport)
	for nodeId := range funComp.Nodes {
		dagNode := funComp.Nodes[nodeId]
		execReportId := fc.CreateExecutionReportId(dagNode)
		fcReq.ExecReport.Reports.Set(execReportId, &function.ExecutionReport{
			OffloadLatency: 0,
			SchedAction:    "",
		})
	}

	if fcReq.Async {
		go fc.SubmitAsyncCompositionRequest(fcReq)
		return e.JSON(http.StatusOK, function.AsyncResponse{ReqId: fcReq.ReqId})
	}

	// sync execution
	err = fc.SubmitCompositionRequest(fcReq)

	if errors.Is(err, node.OutOfResourcesErr) {
		return e.String(http.StatusTooManyRequests, "")
	} else if err != nil {
		log.Printf("Invocation failed: %v", err)
		v := struct {
			Error    string
			Progress string
		}{
			Error:    err.Error(),
			Progress: fcReq.ExecReport.Progress.PrettyString(),
		}
		return e.JSON(http.StatusInternalServerError, v)
	} else {
		reports := make(map[string]*function.ExecutionReport)
		fcReq.ExecReport.Reports.Range(func(id fc.ExecutionReportId, report *function.ExecutionReport) bool {
			reports[string(id)] = report
			return true
		})

		return e.JSON(http.StatusOK, fc.CompositionResponse{
			Success:      true,
			Result:       fcReq.ExecReport.Result,
			Reports:      reports,
			ResponseTime: fcReq.ExecReport.ResponseTime,
		})
	}
}
