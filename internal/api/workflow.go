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
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/node"
	"github.com/grussorusso/serverledge/internal/workflow"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
)

func CreateWorkflowFromASL(e echo.Context) error {
	var creationRequest client.WorkflowCreationRequest
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
	_, found := workflow.GetFC(creationRequest.Name)
	if found {
		log.Printf("Dropping request for already existing workflow '%s'", creationRequest.Name)
		return e.JSON(http.StatusConflict, "workflow already exists")
	}

	log.Printf("New request: creation of workflow %s", creationRequest.Name)

	decodedSrc, err := base64.StdEncoding.DecodeString(creationRequest.ASLSrc)
	if err != nil {
		log.Printf("Could not decode workflow source ASL: %v", err)
		return e.JSON(http.StatusBadRequest, "workflow already exists")
	}

	comp, err := workflow.FromASL(creationRequest.Name, decodedSrc[:])
	if err != nil {
		log.Printf("Could not parse workflow from ASL: %v", err)
		return e.JSON(http.StatusBadRequest, "workflow already exists")
	}

	err = comp.SaveToEtcd()
	if err != nil {
		log.Printf("Failed creation: %v", err)
		return e.JSON(http.StatusServiceUnavailable, "")
	}

	response := struct{ Created string }{comp.Name}
	return e.JSON(http.StatusOK, response)
}

func CreateWorkflow(e echo.Context) error {
	var comp workflow.Workflow
	// here we expect to receive the function workflow struct already parsed from JSON/YAML
	var body []byte
	body, errReadBody := io.ReadAll(e.Request().Body)
	if errReadBody != nil {
		return errReadBody
	}

	err := json.Unmarshal(body, &comp)
	if err != nil && err != io.EOF {
		log.Printf("Could not parse workflow request - error during unmarshal: %v", err)
		return err
	}
	// checking if the function already exists. If exists we return an error
	alreadyPresent := comp.Exists() // TODO: we would need a system-wide lock here...
	if alreadyPresent {
		log.Printf("Dropping request for already existing workflow '%s'", comp.Name)
		return e.JSON(http.StatusConflict, "workflow already exists")
	}

	log.Printf("New request: creation of workflow %s", comp.Name)

	// Check that functions exist
	for _, fName := range comp.GetUniqueFunctions() {
		f, exists := function.GetFunction(fName)
		if !exists {
			log.Printf("Dropping request for workflow with non-existing function '%s'", fName)
			return e.JSON(http.StatusBadRequest, "workflow with non-existing function")
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

// GetWorkflows handles a request to list the function workflows available in the system.
func GetWorkflows(c echo.Context) error {
	list, err := workflow.GetAllFC()
	if err != nil {
		return c.String(http.StatusServiceUnavailable, "")
	}
	return c.JSON(http.StatusOK, list)
}

// DeleteWorkflow handles a function deletion request.
func DeleteWorkflow(c echo.Context) error {
	var comp workflow.Workflow
	// here we only need the name of the function workflow (and if all function should be deleted with it)
	err := json.NewDecoder(c.Request().Body).Decode(&comp)
	if err != nil && err != io.EOF {
		log.Printf("Could not parse delete request - error during decoding: %v", err)
		return err
	}

	workflow, ok := workflow.GetFC(comp.Name) // TODO: we would need a system-wide lock here...
	if !ok {
		log.Printf("Dropping request for non existing function '%s'", comp.Name)
		return c.JSON(http.StatusNotFound, "the request function workflow to delete does not exist")
	}

	log.Printf("New request: deleting %s", workflow.Name)
	err = workflow.Delete()
	if err != nil {
		log.Printf("Failed deletion: %v", err)
		return c.JSON(http.StatusServiceUnavailable, "")
	}

	response := struct{ Deleted string }{workflow.Name}
	return c.JSON(http.StatusOK, response)
}

// InvokeWorkflow handles a function workflow invocation request.
func InvokeWorkflow(e echo.Context) error {
	// gets the command line param value for -workflow (the workflow name)
	fcName := e.Param("workflow")
	funComp, ok := workflow.GetFC(fcName)
	if !ok {
		log.Printf("Dropping request for unknown FC '%s'", fcName)
		return e.JSON(http.StatusNotFound, "function workflow '"+fcName+"' does not exist")
	}

	// we use invocation request that is specific to function workflows
	var fcInvocationRequest client.WorkflowInvocationRequest
	err := json.NewDecoder(e.Request().Body).Decode(&fcInvocationRequest)
	if err != nil && err != io.EOF {
		log.Printf("Could not parse invoke request - error during decoding: %v", err)
		return e.JSON(http.StatusInternalServerError, "failed to parse workflow invocation request. Check parameters and workflow definition")
	}
	// gets a workflow.NewRequest from the pool goroutine-safe cache.
	fcReq := workflowInvocationRequestPool.Get().(*workflow.Request) // A pointer *function.NewRequest will be created if does not exists, otherwise removed from the pool
	defer workflowInvocationRequestPool.Put(fcReq)                   // at the end of the function, the function.NewRequest is added to the pool.
	fcReq.W = funComp
	fcReq.Params = fcInvocationRequest.Params
	fcReq.Arrival = time.Now()

	// instead of saving only one RequestQoS, we save a map with an entry for each function in the workflow
	fcReq.RequestQoSMap = fcInvocationRequest.RequestQoSMap

	fcReq.CanDoOffloading = fcInvocationRequest.CanDoOffloading
	fcReq.Async = fcInvocationRequest.Async
	fcReq.ReqId = fmt.Sprintf("%v-%s%d", funComp.Name, node.NodeIdentifier[len(node.NodeIdentifier)-5:], fcReq.Arrival.Nanosecond())
	// init fields if possibly not overwritten later
	fcReq.ExecReport.Reports = hashmap.New[workflow.ExecutionReportId, *function.ExecutionReport]() // make(map[workflow.ExecutionReportId]*function.ExecutionReport)
	for nodeId := range funComp.Nodes {
		task := funComp.Nodes[nodeId]
		execReportId := workflow.CreateExecutionReportId(task)
		fcReq.ExecReport.Reports.Set(execReportId, &function.ExecutionReport{
			OffloadLatency: 0,
			SchedAction:    "",
		})
	}

	if fcReq.Async {
		go workflow.SubmitAsyncWorkflowInvocationRequest(fcReq)
		return e.JSON(http.StatusOK, function.AsyncResponse{ReqId: fcReq.ReqId})
	}

	// sync execution
	err = workflow.SubmitWorkflowInvocationRequest(fcReq)

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
		fcReq.ExecReport.Reports.Range(func(id workflow.ExecutionReportId, report *function.ExecutionReport) bool {
			reports[string(id)] = report
			return true
		})

		return e.JSON(http.StatusOK, workflow.InvocationResponse{
			Success:      true,
			Result:       fcReq.ExecReport.Result,
			Reports:      reports,
			ResponseTime: fcReq.ExecReport.ResponseTime,
		})
	}
}
