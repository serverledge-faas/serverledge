package api

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/serverledge-faas/serverledge/internal/metrics"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/serverledge-faas/serverledge/internal/cache"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"github.com/serverledge-faas/serverledge/internal/scheduling"
)

func StartAPIServer(e *echo.Echo) {
	e.Use(middleware.Recover())

	// Routes
	e.POST("/invoke/:fun", InvokeFunction)
	e.POST("/create", CreateOrUpdateFunction)
	e.POST("/update", CreateOrUpdateFunction)
	e.POST("/delete", DeleteFunction)
	e.GET("/function", GetFunctions)
	e.GET("/poll/:reqId", PollAsyncResult)
	e.GET("/status", GetServerStatus)
	e.POST("/prewarm", PrewarmFunction)

	if config.GetBool(config.METRICS_ENABLED, false) {
		e.GET("/metrics", func(c echo.Context) error {
			metrics.ScrapingHandler.ServeHTTP(c.Response(), c.Request())
			return nil
		})
	}

	// Workflow routes
	e.POST("/workflow/invoke/:workflow", InvokeWorkflow)
	e.POST("/workflow/resume/:workflow", ResumeWorkflow)
	e.POST("/workflow/create", CreateWorkflowFromASL)
	e.POST("/workflow/import", CreateWorkflow)
	e.POST("/workflow/delete", DeleteWorkflow)
	e.GET("/workflow/list", GetWorkflows)

	// Start server
	portNumber := config.GetInt(config.API_PORT, 1323)
	e.HideBanner = true

	if err := e.Start(fmt.Sprintf(":%d", portNumber)); err != nil && !errors.Is(err, http.ErrServerClosed) {
		e.Logger.Fatal("shutting down the server")
	}
}

func CacheSetup() {
	//todo fix default values

	// setup cache space
	cache.Size = config.GetInt(config.CACHE_SIZE, 100)

	//setup cleanup interval
	d := config.GetInt(config.CACHE_CLEANUP, 60)
	interval := time.Duration(d)
	cache.CleanupInterval = interval * time.Second

	//setup default expiration time
	d = config.GetInt(config.CACHE_ITEM_EXPIRATION, 60)
	expirationInterval := time.Duration(d)
	cache.DefaultExp = expirationInterval * time.Second

	//cache first creation
	cache.GetCacheInstance()
}

func RegisterTerminationHandler(r *registration.Registry, e *echo.Echo) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt)

	go func() {
		select {
		case sig := <-c:
			fmt.Printf("Got %s signal. Terminating...\n", sig)
			node.ShutdownAllContainers()

			// deregister from etcd; server should be unreachable
			err := r.Deregister()
			if err != nil {
				log.Fatal(err)
			}

			//stop container janitor
			node.StopJanitor()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := e.Shutdown(ctx); err != nil {
				e.Logger.Fatal(err)
			}

			os.Exit(0)
		}
	}()
}

func CreateSchedulingPolicy() scheduling.Policy {
	policyConf := config.GetString(config.SCHEDULING_POLICY, "default")
	log.Printf("Configured scheduling policy: %s\n", policyConf)
	if policyConf == "cloudonly" {
		return &scheduling.CloudOnlyPolicy{}
	} else if policyConf == "edgecloud" {
		return &scheduling.CloudEdgePolicy{}
	} else if policyConf == "edgeonly" {
		return &scheduling.EdgePolicy{}
	} else { // default, localonly
		return &scheduling.DefaultLocalPolicy{}
	}
}
