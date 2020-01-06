package main

import (
	"fmt"
	"net"
	"strconv"
)

func parseConfig(a []SocketAddr, b []SocketAddr) {

	var host, port string

	for _, v := range a {
		host, port, _ = net.SplitHostPort(v.addr)
		if net.ParseIP(host) == nil {
			err := fmt.Errorf("Incorrectly configured address of Kafka broker ", host)
			configError(err)
		}
		p, _ := strconv.Atoi(port)
		if 0 >= p || p > 65535 {
			err := fmt.Errorf("Invalid port ", port)
			configError(err)
		}
	}

	for _, v := range b {
		host, port, _ = net.SplitHostPort(v.addr)
		if net.ParseIP(host) == nil {
			err := fmt.Errorf("Incorrectly configured address of Kafka broker ", host)
			configError(err)
		}
		r, _ := strconv.Atoi(port)
		if 0 >= r || r > 65535 {
			err := fmt.Errorf("Invalid port ", port)
			configError(err)
		}
	}

}
