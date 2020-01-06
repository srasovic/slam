package main

import (
	"log"
)

func PrintStructs(a []SocketAddr, b []SocketAddr) {

	if debug {
		log.Println("Starting SLAM daemon with the following configuration:\n")
		log.Println("Kafka address:")
		for _, v := range a {
			log.Println(v)
		}
		log.Println("\nListener addresses:")
		for _, v := range b {
			log.Println(v)
		}
		log.Println()
	} else {
		log.Println("Starting SLAM daemon\n...")
	}

}
