package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/serverledge-faas/serverledge/internal/client"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/workflow"
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
	_, found := workflow.Get(creationRequest.Name)
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
		return e.JSON(http.StatusBadRequest, "ASL parsing failed")
	}

	err = comp.Save()
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

	err = comp.Save()
	if err != nil {
		log.Printf("Failed creation: %v", err)
		return e.JSON(http.StatusServiceUnavailable, "")
	}
	response := struct{ Created string }{comp.Name}
	return e.JSON(http.StatusOK, response)
}

// GetWorkflows handles a request to list the function workflows available in the system.
func GetWorkflows(c echo.Context) error {
	list, err := workflow.GetAllWorkflows()
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

	wflow, ok := workflow.Get(comp.Name) // TODO: we would need a system-wide lock here...
	if !ok {
		log.Printf("Dropping request for non existing function '%s'", comp.Name)
		return c.JSON(http.StatusNotFound, "the request function workflow to delete does not exist")
	}

	log.Printf("New request: deleting %s", wflow.Name)
	err = wflow.Delete()
	if err != nil {
		log.Printf("Failed deletion: %v", err)
		return c.JSON(http.StatusServiceUnavailable, "")
	}

	response := struct{ Deleted string }{wflow.Name}
	return c.JSON(http.StatusOK, response)
}

// ResumeWorkflow handles a workflow invocation resume request (workflow offloading).
func ResumeWorkflow(e echo.Context) error {
	workflowName := e.Param("workflow")
	wflow, ok := workflow.Get(workflowName)
	if !ok {
		log.Printf("Dropping request for unknown workflow '%s'", workflowName)
		return e.JSON(http.StatusNotFound, "function workflow '"+workflowName+"' does not exist")
	}

	var clientReq workflow.WorkflowInvocationResumeRequest
	err := json.NewDecoder(e.Request().Body).Decode(&clientReq)
	if err != nil && err != io.EOF {
		log.Printf("Could not parse invoke request - error during decoding: %v", err)
		return e.JSON(http.StatusInternalServerError, "failed to parse workflow invocation request. Check parameters and workflow definition")
	}

	req := workflowInvocationRequestPool.Get().(*workflow.Request)
	req.W = wflow
	req.Params = clientReq.Params
	req.Arrival = time.Now()
	req.QoS = clientReq.QoS
	req.CanDoOffloading = clientReq.CanDoOffloading
	req.Async = clientReq.Async
	req.Resuming = true
	req.Id = clientReq.ReqId
	req.ExecReport.Reports = map[string]*function.ExecutionReport{}

	if clientReq.Plan.ToExecute != nil {
		req.Plan = &workflow.OffloadingPlan{ToExecute: clientReq.Plan.ToExecute}
	} else {
		req.Plan = nil
	}

	log.Printf("Resuming workflow '%s'", workflowName)

	return handleWorkflowInvocation(e, req)
}

// InvokeWorkflow handles a function workflow invocation request.
func InvokeWorkflow(e echo.Context) error {
	workflowName := e.Param("workflow")
	wflow, ok := workflow.Get(workflowName)
	if !ok {
		log.Printf("Dropping request for unknown workflow '%s'", workflowName)
		return e.JSON(http.StatusNotFound, "function workflow '"+workflowName+"' does not exist")
	}

	var clientReq client.WorkflowInvocationRequest
	err := json.NewDecoder(e.Request().Body).Decode(&clientReq)
	if err != nil && err != io.EOF {
		log.Printf("Could not parse invoke request - error during decoding: %v", err)
		return e.JSON(http.StatusInternalServerError, "failed to parse workflow invocation request. Check parameters and workflow definition")
	}

	req := workflowInvocationRequestPool.Get().(*workflow.Request)
	req.W = wflow
	req.Params = clientReq.Params
	req.Arrival = time.Now()
	req.QoS = clientReq.QoS
	req.CanDoOffloading = clientReq.CanDoOffloading
	req.Async = clientReq.Async
	req.Plan = nil
	req.Resuming = false
	req.Id = fmt.Sprintf("%v-%s%d", wflow.Name, node.NodeIdentifier[len(node.NodeIdentifier)-5:], req.Arrival.Nanosecond())
	req.ExecReport.Reports = map[string]*function.ExecutionReport{}

	return handleWorkflowInvocation(e, req)
}

func handleWorkflowInvocation(e echo.Context, req *workflow.Request) error {

	if req.Async {
		go func() {
			errInvoke := req.W.Invoke(req)

			defer workflowInvocationRequestPool.Put(req)

			if errInvoke != nil {
				log.Printf("Invocation failed: %v", errInvoke)
				workflow.PublishAsyncInvocationResponse(req.Id, workflow.InvocationResponse{Success: false})
				return
			}

			log.Printf("Invocation succeeded. Publishing: %v", req.ExecReport)
			req.ExecReport.ResponseTime = time.Now().Sub(req.Arrival).Seconds()
			workflow.PublishAsyncInvocationResponse(req.Id, workflow.InvocationResponse{
				Success:      true,
				Result:       req.ExecReport.Result,
				Reports:      req.ExecReport.Reports,
				ResponseTime: req.ExecReport.ResponseTime,
			})
		}()

		return e.JSON(http.StatusOK, function.AsyncResponse{ReqId: req.Id})
	}

	// Synchronous execution of the workflow
	err := req.W.Invoke(req)

	defer workflowInvocationRequestPool.Put(req)

	if errors.Is(err, node.OutOfResourcesErr) {
		return e.String(http.StatusTooManyRequests, "")
	} else if err != nil {
		log.Printf("Invocation failed: %v", err)
		return e.JSON(http.StatusInternalServerError, err.Error())
	} else {
		req.ExecReport.ResponseTime = time.Now().Sub(req.Arrival).Seconds()

		return e.JSON(http.StatusOK, workflow.InvocationResponse{
			Success:      true,
			Result:       req.ExecReport.Result,
			Reports:      req.ExecReport.Reports,
			ResponseTime: req.ExecReport.ResponseTime,
		})
	}
}
