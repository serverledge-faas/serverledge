package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"golang.org/x/net/context"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/lb"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"github.com/serverledge-faas/serverledge/utils"
)

func registerTerminationHandler(e *echo.Echo) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt)

	go func() {
		select {
		case sig := <-c:
			fmt.Printf("Got %s signal. Terminating...\n", sig)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := e.Shutdown(ctx); err != nil {
				e.Logger.Fatal(err)
			}

			os.Exit(0)
		}
	}()
}

func main() {
	configFileName := ""
	if len(os.Args) > 1 {
		configFileName = os.Args[1]
	}
	config.ReadConfiguration(configFileName)

	// TODO: split Area in Region + Type (e.g., cloud/lb/edge)
	region := config.GetString(config.REGISTRY_AREA, "ROME")
	registry := &registration.Registry{Area: "lb/" + region}

	defaultAddressStr := "127.0.0.1"
	address, err := utils.GetOutboundIp()
	if err == nil {
		defaultAddressStr = address.String()
	}
	registration.RegisteredLocalIP = config.GetString(config.API_IP, defaultAddressStr)

	if _, err := registry.RegisterToEtcd(); err != nil {
		log.Printf("Could not register to Etcd: %v\n", err)
	}

	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Recover())

	// Register a signal handler to cleanup things on termination
	registerTerminationHandler(e)

	lb.StartReverseProxy(e, region)
}
