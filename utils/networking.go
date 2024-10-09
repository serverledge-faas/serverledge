package utils

import (
	"fmt"
	"net"
)

// GetOutboundIp retrieves the host ip address by Dialing with Google's DNS (cross-platform)
func GetOutboundIp() (net.IP, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return net.IP{}, fmt.Errorf("could not get UDP address - check internet connection: %v", err)
	}

	defer func() {
		err1 := conn.Close()
		if err1 != nil {
			panic(err1)
		}
	}()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP, nil
}
