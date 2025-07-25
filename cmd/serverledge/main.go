package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/serverledge-faas/serverledge/internal/api"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/metrics"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"github.com/serverledge-faas/serverledge/internal/scheduling"
	"github.com/serverledge-faas/serverledge/internal/telemetry"
	"github.com/serverledge-faas/serverledge/utils"

	"github.com/labstack/echo/v4"
)

func main() {
	configFileName := ""
	if len(os.Args) > 1 {
		configFileName = os.Args[1]
	}
	config.ReadConfiguration(configFileName)

	//setting up cache parameters
	api.CacheSetup()

	// register to etcd, this way server is visible to the others under a given local area
	registry := new(registration.Registry)
	isInCloud := config.GetBool(config.IS_IN_CLOUD, false)
	if isInCloud {
		registry.Area = "cloud/" + config.GetString(config.REGISTRY_AREA, "ROME")
	} else {
		registry.Area = config.GetString(config.REGISTRY_AREA, "ROME")
	}
	// before register checkout other servers into the local area
	//todo use this info later on; future work with active remote server selection
	_, err := registry.GetAll(true)
	if err != nil {
		log.Fatal(err)
	}

	defaultAddressStr := "127.0.0.1"
	address, err := utils.GetOutboundIp()
	if err == nil {
		defaultAddressStr = address.String()
	}
	registration.RegisteredLocalIP = config.GetString(config.API_IP, defaultAddressStr)

	myKey, err := registry.RegisterToEtcd()
	if err != nil {
		log.Fatal(err)
	}
	node.NodeIdentifier = myKey

	metrics.Init()

	if config.GetBool(config.TRACING_ENABLED, false) {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()

		tracesOutfile := config.GetString(config.TRACING_OUTFILE, "")
		if len(tracesOutfile) < 1 {
			tracesOutfile = fmt.Sprintf("traces-%s.json", time.Now().Format("20060102-150405"))
		}
		log.Printf("Enabling tracing to %s\n", tracesOutfile)
		otelShutdown, err := telemetry.SetupOTelSDK(ctx, tracesOutfile)
		if err != nil {
			log.Fatal(err)
		}
		// Handle shutdown properly so nothing leaks.
		defer func() {
			err = errors.Join(err, otelShutdown(context.Background()))
		}()
	}

	e := echo.New()

	// Register a signal handler to cleanup things on termination
	api.RegisterTerminationHandler(registry, e)

	schedulingPolicy := api.CreateSchedulingPolicy()
	go scheduling.Run(schedulingPolicy)

	if !isInCloud {
		err = registration.InitEdgeMonitoring(registry)
		if err != nil {
			log.Fatal(err)
		}
	}

	api.StartAPIServer(e)

}
