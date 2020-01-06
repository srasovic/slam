package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

type ap struct {
	a string
	p string
	t string
}

type SocketAddr struct {
	addr  string
	proto string
	topic string
}

func getSocketValues(scanner *bufio.Scanner) ([]string, []string) {

	var Kafka []string
	var Listeners []string

	for scanner.Scan() {

		err := scanner.Err()
		if err == io.EOF {
			break
		}
		Addr := scanner.Text()
		if len(Addr) == 0 {
			continue
		}

		if strings.Contains(Addr, "LISTENERS") {
			Listeners, _ = getSocketValues(scanner)
			return Kafka, Listeners
		}
		Kafka = append(Kafka, Addr)
	}
	return Kafka, Listeners

}

func readConfigFile() ([]SocketAddr, []SocketAddr) {

	file, err := os.Open("./test.config")

	configError(err)

	defer file.Close()

	scanner := bufio.NewScanner(file)

	var Kafkas []SocketAddr
	var Listeners []SocketAddr

	for scanner.Scan() {

		line := scanner.Text()

		if strings.Contains(line, "KAFKA") {

			kafka, listeners := getSocketValues(scanner)
			numKafkas := len(kafka)
			numListeners := len(listeners)
			Kafkas = make([]SocketAddr, numKafkas)
			Listeners = make([]SocketAddr, numListeners)

			ap := make([]ap, numListeners)

			if len(kafka) == 0 || len(listeners) == 0 {
				err := errors.New("Malformed file. Please configure properly and try again")
				configError(err)
			}

			for i, v := range kafka {
				Kafkas[i].addr = v
				Kafkas[i].proto = "tcp"
			}

			for i, v := range listeners {
				var sep string
				if strings.Contains(v, " ") {
					sep = " "
				} else if strings.Contains(v, "\t") {
					sep = "\t"
				} else {
					continue
				}

				lline := strings.Split(v, sep)

				for _, v := range lline {

					if v == " " {
						continue
					} else if v == "tcp" || v == "udp" {
						ap[i].p = v
					} else if strings.Contains(v, "#") {
						ap[i].t = v
					} else {
						ap[i].a = v
					}

				}

				Listeners[i].addr = ap[i].a
				Listeners[i].proto = ap[i].p
				Listeners[i].topic = ap[i].t

			}

			for i, v := range ap {
				if v.p == "" || v.a == "" || v.t == "" {
					err := fmt.Errorf("Improperly configured listener on line %d", i+1)
					configError(err)
				}
			}

			if len(ap) > 5 {
				//fmt.Println("Here we should call docker API to spawn new instances of containers.")
			}

		}

	}

	if err := scanner.Err(); err != nil {
		err := errors.New("Fatal Error")
		configError(err)
	}
	return Kafkas, Listeners

}
