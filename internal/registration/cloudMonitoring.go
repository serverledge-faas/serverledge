package registration

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/serverledge-faas/serverledge/internal/config"
)

func InitCloudMonitoring() {
	Reg.CloudLatency = make(map[string]float64)
	//complete monitoring phase at startup
	monitorCloudLatency()
	go runCloudMonitor()
}

func runCloudMonitor() {
	cloudMonitorTicker := time.NewTicker(time.Duration(config.GetInt(config.REG_CLOUD_MONITORING_INTERVAL, 60)) * time.Second)
	for {
		select {
		case <-cloudMonitorTicker.C:
			monitorCloudLatency()
		}
	}
}

func tcpLatency(hostAndPort string) (float64, error) {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", hostAndPort, 3*time.Second)
	if err != nil {
		return 0, err
	}
	_ = conn.Close()
	return float64(time.Since(start).Milliseconds()), nil
}

func monitorCloudLatency() {
	region := config.GetString(config.REGISTRY_AREA, "ROME")
	cloudNodes, err := GetCloudNodes(region)
	if err != nil {
		log.Println(fmt.Printf("Error while retrieving cloud nodes: %v", err))
		return
	}

	if len(cloudNodes) == 0 {
		cloudAddress := config.GetString(config.CLOUD_URL, "")
		if cloudAddress != "" {
			cloudNodes["from-config"] = cloudAddress
		}
	}

	for _, url := range cloudNodes {
		// TODO: retrieve from offloadingLatency?
		latency, latencyError := tcpLatency(url)
		if latencyError != nil {
			log.Printf("Unable to compute latency towards %v: %v\n", url, latencyError)
			delete(Reg.CloudLatency, url)
			continue
		}
		Reg.CloudLatency[url] = latency
	}

	log.Printf("Map of cloud node latency at the end of monitoring: %v\n", Reg.CloudLatency)
}
