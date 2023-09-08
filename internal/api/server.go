package api

import (
	"context"
	"errors"
	"fmt"
	"github.com/grussorusso/serverledge/internal/cache"
	"github.com/grussorusso/serverledge/internal/config"
	"github.com/grussorusso/serverledge/internal/node"
	"github.com/grussorusso/serverledge/internal/registration"
	"github.com/grussorusso/serverledge/internal/scheduling"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
)

func StartAPIServer(e *echo.Echo) {
	e.Use(middleware.Recover())

	// Routes
	e.POST("/invoke/:fun", InvokeFunction)
	e.POST("/create", CreateFunction)
	e.POST("/delete", DeleteFunction)
	e.GET("/function", GetFunctions)
	e.GET("/poll/:reqId", PollAsyncResult)
	e.GET("/status", GetServerStatus)
	// Function composition routes
	e.POST("/play/:fc", InvokeFunctionComposition)
	e.POST("/compose", CreateFunctionComposition)
	e.POST("/composeASL", CreateFunctionCompositionFromASL)
	e.POST("/uncompose", DeleteFunctionComposition)
	e.GET("/fc", GetFunctionCompositions)

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

	cache.Persist = config.GetBool(config.CACHE_PERSISTENCE, true)
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
	log.Printf("Configured policy: %s\n", policyConf)
	if policyConf == "cloudonly" {
		return &scheduling.CloudOnlyPolicy{}
	} else if policyConf == "edgecloud" {
		return &scheduling.CloudEdgePolicy{}
	} else if policyConf == "edgeonly" {
		return &scheduling.EdgePolicy{}
	} else if policyConf == "custom1" {
		return &scheduling.Custom1Policy{}
	} else { // default, localonly
		return &scheduling.DefaultLocalPolicy{}
	}
}
