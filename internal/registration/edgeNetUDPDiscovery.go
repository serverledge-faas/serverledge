package registration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/utils"
)

// UDPStatusServer listen for incoming request from other edge-nodes which want to retrieve the status of this server
// this listener should be called asynchronously in the main function
func UDPStatusServer() {
	hostname, err := utils.GetOutboundIp()
	if err != nil {
		log.Fatal(err)
	}

	port := config.GetInt(config.LISTEN_UDP_PORT, 9876)
	address := fmt.Sprintf("%s:%d", hostname, port)
	udpAddr, err := net.ResolveUDPAddr("udp", address)

	if err != nil {
		log.Fatal(err)
	}
	// setup listener for incoming UDP connection
	udpConn, err := net.ListenUDP("udp", udpAddr)

	if err != nil {
		log.Fatal(err)
	}
	log.Printf("UDP server up and listening on port %d\n", port)

	defer func(udpConn *net.UDPConn) {
		err := udpConn.Close()
		if err != nil {
			log.Printf("Error while closing UDP connection: %s\n", err)
		}
	}(udpConn)

	for {
		// wait for UDP client to connect
		handleUDPConnection(udpConn)
	}

}

func handleUDPConnection(conn *net.UDPConn) {
	buffer := make([]byte, 1024)

	_, addr, err := conn.ReadFromUDP(buffer)
	if err != nil {
		return
	}
	//retrieve the current status
	msgStatus, err := getCurrentStatusInformation()
	if err != nil {
		log.Println(err)
		msgStatus = []byte("")
	}
	//send the infos back to the client edge-node
	_, err = conn.WriteToUDP(msgStatus, addr)
	if err != nil {
		log.Println(err)
	}
}

// TODO: this function should reuse the code in api.go for the /status API
func getCurrentStatusInformation() (status []byte, err error) {
	address, err := utils.GetOutboundIp()
	if err != nil {
		return []byte{}, err
	}

	portNumber := config.GetInt("api.port", 1323)
	url := fmt.Sprintf("http://%s:%d", address.String(), portNumber)
	response := StatusInformation{
		Url:                     url,
		AvailableWarmContainers: node.WarmStatus(),
		AvailableMemMB:          node.Resources.AvailableMemMB,
		AvailableCPUs:           node.Resources.AvailableCPUs,
		Coordinates:             *Reg.Client.GetCoordinate(),
	}

	return json.Marshal(response)

}

func statusInfoRequest(hostname string) (info *StatusInformation, duration time.Duration) {
	port := config.GetInt(config.LISTEN_UDP_PORT, 9876)
	address := fmt.Sprintf("%s:%d", hostname, port)

	remoteAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		log.Printf("Unreachable server %s\n", address)
		return nil, 0
	}

	udpConn, err := net.DialUDP("udp", nil, remoteAddr)
	if err != nil {
		log.Println(err)
		return nil, 0
	}
	defer func(udpConn *net.UDPConn) {
		err := udpConn.Close()
		if err != nil {
			log.Printf("Error while closing UDP connection: %s\n", err)
		}
	}(udpConn)

	// write a message to server, here 1 byte is enough
	message := []byte("A")
	sendingTime := time.Now()
	_, err = udpConn.Write(message)
	if err != nil {
		log.Println(err)
		return nil, 0
	}

	// receive message from server
	buffer := make([]byte, 1024)
	_, _, err = udpConn.ReadFromUDP(buffer)
	if err != nil {
		log.Println(err)
		return nil, 0
	}

	rtt := time.Now().Sub(sendingTime)
	buffer = bytes.Trim(buffer, "\x00")
	//unmarshal result
	var result StatusInformation
	err = json.Unmarshal(buffer, &result)
	if err != nil {
		fmt.Println("Can not unmarshal JSON")
		return nil, 0
	}
	return &result, rtt
}
