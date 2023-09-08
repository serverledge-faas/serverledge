package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/serverledge-faas/serverledge/internal/client"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/container"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"github.com/serverledge-faas/serverledge/internal/telemetry"
	"github.com/serverledge-faas/serverledge/internal/workflow"
	"github.com/serverledge-faas/serverledge/utils"
	"go.opentelemetry.io/otel/attribute"

	"github.com/labstack/echo/v4"
	"github.com/mikoim/go-loadavg"
	"github.com/serverledge-faas/serverledge/internal/scheduling"
)

var requestsPool = sync.Pool{
	New: func() any {
		return new(function.Request)
	},
}

var workflowInvocationRequestPool = sync.Pool{
	New: func() any {
		return new(workflow.Request)
	},
}

// GetFunctions handles a request to list the function available in the system.
func GetFunctions(c echo.Context) error {
	list, err := function.GetAll()
	if err != nil {
		return c.String(http.StatusServiceUnavailable, "")
	}
	return c.JSON(http.StatusOK, list)
}

// InvokeFunction handles a function invocation request.
func InvokeFunction(c echo.Context) error {
	funcName := c.Param("fun")
	fun, ok := function.GetFunction(funcName)
	if !ok {
		log.Printf("Dropping request for unknown fun '%s'\n", funcName)
		return c.String(http.StatusNotFound, "Function unknown")
	}

	var invocationRequest client.InvocationRequest
	err := json.NewDecoder(c.Request().Body).Decode(&invocationRequest)
	if err != nil && err != io.EOF {
		log.Printf("Could not parse request: %v\n", err)
		return fmt.Errorf("could not parse request: %v", err)
	}
	// gets a function.Request from the pool goroutine-safe cache.
	r := requestsPool.Get().(*function.Request) // function.Request will be created if does not exists, otherwise removed from the pool
	defer requestsPool.Put(r)                   // at the end of the function, the function.Request is added to the pool.
	r.Fun = fun
	r.Params = invocationRequest.Params
	r.Arrival = time.Now()
	r.MaxRespT = invocationRequest.QoSMaxRespT
	r.CanDoOffloading = invocationRequest.CanDoOffloading
	r.Async = invocationRequest.Async
	r.ReturnOutput = invocationRequest.ReturnOutput

	reqId := fmt.Sprintf("%s-%s%d", fun, node.NodeIdentifier[len(node.NodeIdentifier)-5:], r.Arrival.Nanosecond())
	r.Ctx = context.WithValue(context.Background(), "ReqId", reqId)

	// Tracing
	if telemetry.DefaultTracer != nil {
		ctx, span := telemetry.DefaultTracer.Start(r.Ctx, "invocation")
		r.Ctx = ctx
		span.SetAttributes(attribute.String("function", r.Fun.Name))
		defer span.End()
	}

	if r.Async {
		go scheduling.SubmitAsyncRequest(r)
		return c.JSON(http.StatusOK, function.AsyncResponse{ReqId: r.Id()})
	}

	executionReport, err := scheduling.SubmitRequest(r)

	if errors.Is(err, node.OutOfResourcesErr) {
		return c.String(http.StatusTooManyRequests, "")
	} else if err != nil {
		log.Printf("Invocation failed: %v\n", err)
		return c.String(http.StatusInternalServerError, "Node has not enough resources")
	} else {
		return c.JSON(http.StatusOK, function.Response{Success: true, ExecutionReport: executionReport})
	}
}

// PollAsyncResult checks for the result of an asynchronous invocation.
func PollAsyncResult(c echo.Context) error {
	reqId := c.Param("reqId")
	if len(reqId) < 0 {
		return c.String(http.StatusNotFound, "")
	}

	etcdClient, err := utils.GetEtcdClient()
	if err != nil {
		log.Println("Could not connect to Etcd")
		return c.String(http.StatusInternalServerError, "Failed to connect to the Global Registry")
	}

	ctx := context.Background()

	key := fmt.Sprintf("async/%s", reqId)
	res, err := etcdClient.Get(ctx, key)
	if err != nil {
		log.Println(err)
		return c.String(http.StatusInternalServerError, "Could not retrieve results")
	}

	if len(res.Kvs) == 1 {
		payload := res.Kvs[0].Value
		return c.JSONBlob(http.StatusOK, payload)
	} else {
		return c.String(http.StatusNotFound, "")
	}
}

// CreateFunction handles a function creation request.
func CreateFunction(c echo.Context) error {
	var f function.Function
	err := json.NewDecoder(c.Request().Body).Decode(&f)
	if err != nil && err != io.EOF {
		log.Printf("Could not parse request: %v\n", err)
		return err
	}

	_, ok := function.GetFunction(f.Name) // TODO: we would need a system-wide lock here...
	if ok {
		log.Printf("Dropping request for already existing function '%s'\n", f.Name)
		return c.String(http.StatusConflict, "")
	}

	log.Printf("New request: creation of %s\n", f.Name)

	// Check that the selected runtime exists
	if f.Runtime != container.CUSTOM_RUNTIME {
		_, ok := container.RuntimeToInfo[f.Runtime]
		if !ok {
			return c.JSON(http.StatusNotFound, "Invalid runtime.")
		}
	}

	err = f.SaveToEtcd()
	if err != nil {
		log.Printf("Failed creation: %v\n", err)
		return c.JSON(http.StatusServiceUnavailable, "")
	}
	response := struct{ Created string }{f.Name}
	return c.JSON(http.StatusOK, response)
}

// DeleteFunction handles a function deletion request.
func DeleteFunction(c echo.Context) error {
	var f function.Function
	err := json.NewDecoder(c.Request().Body).Decode(&f)
	if err != nil && err != io.EOF {
		log.Printf("Could not parse request: %v\n", err)
		return err
	}

	_, ok := function.GetFunction(f.Name) // TODO: we would need a system-wide lock here...
	if !ok {
		log.Printf("Dropping request for non existing function '%s'\n", f.Name)
		return c.String(http.StatusNotFound, "Unknown function")
	}

	log.Printf("New request: deleting %s\n", f.Name)
	err = f.Delete()
	if err != nil {
		log.Printf("Failed deletion: %v\n", err)
		return c.String(http.StatusServiceUnavailable, "")
	}

	// Delete local warm containers
	node.ShutdownWarmContainersFor(&f)

	response := struct{ Deleted string }{f.Name}
	return c.JSON(http.StatusOK, response)
}

func DecodeServiceClass(serviceClass string) (p function.ServiceClass) {
	if serviceClass == "low" {
		return function.LOW
	} else if serviceClass == "performance" {
		return function.HIGH_PERFORMANCE
	} else if serviceClass == "availability" {
		return function.HIGH_AVAILABILITY
	} else {
		return function.LOW
	}
}

// GetServerStatus simple api to check the current server status
func GetServerStatus(c echo.Context) error {
	node.Resources.RLock()
	defer node.Resources.RUnlock()

	portNumber := config.GetInt("api.port", 1323)

	address, err := utils.GetOutboundIp()
	url := fmt.Sprintf("http://%s:%d", address.String(), portNumber)

	loadAvg, err := loadavg.Parse()
	loadAvgValues := []float64{-1.0, -1.0, -1.0}
	if err == nil {
		loadAvgValues = []float64{loadAvg.LoadAverage1, loadAvg.LoadAverage5, loadAvg.LoadAverage10}
	}

	response := registration.StatusInformation{
		Url:                     url,
		AvailableWarmContainers: node.WarmStatus(),
		AvailableMemMB:          node.Resources.AvailableMemMB,
		AvailableCPUs:           node.Resources.AvailableCPUs,
		Coordinates:             *registration.Reg.Client.GetCoordinate(),
		LoadAvg:                 loadAvgValues,
	}

	return c.JSON(http.StatusOK, response)
}

// PrewarmFunction handles a prewarming request.
func PrewarmFunction(c echo.Context) error {
	var req client.PrewarmingRequest
	err := json.NewDecoder(c.Request().Body).Decode(&req)
	if err != nil && err != io.EOF {
		log.Printf("Could not parse request: %v\n", err)
		return err
	}

	fun, ok := function.GetFunction(req.Function)
	if !ok {
		log.Printf("Dropping request for unknown fun '%s'\n", req.Function)
		return c.String(http.StatusNotFound, "Function unknown")
	}

	count, err := node.PrewarmInstances(fun, req.Instances, req.ForceImagePull)

	if err != nil && !errors.Is(err, node.OutOfResourcesErr) {
		log.Printf("Failed prewarming: %v\n", err)
		return c.JSON(http.StatusServiceUnavailable, "")
	}
	response := struct{ Prewarmed int64 }{count}
	return c.JSON(http.StatusOK, response)
}
