package main

import (
	"log"
	"os"
	"runtime"

	"github.com/pborman/getopt"
)

var w *os.File
var debug bool

func init() {

	var err error
	//oldout := os.Stdout // keep backup of the real stdout
	//olderr := os.Stderr
	w, err = os.OpenFile("/var/log/slam.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

	os.Stdout = w
	os.Stderr = w

	checkError(err)

}

func main() {

	defer func() {
		err := recover().(error)
		checkError(err)
	}()

	optName := getopt.BoolLong("debug", 0, "", "Debug")
	optHelp := getopt.BoolLong("help", 0, "Help")
	getopt.Parse()

	if *optHelp {
		getopt.Usage()
		os.Exit(0)
	}

	if *optName {
		debug = true
	}

	kafkas, listeners := readConfigFile()

	parseConfig(kafkas, listeners)

	log.Println("SLAM daemon started. Logging to /var/log/slam.log")

	log.SetOutput(w)

	PrintStructs(kafkas, listeners)

	listenerSocketProcessor(kafkas, listeners)

}

func checkError(err error) {
	if err != nil {
		log.Println("Unrecovered Error:")
		log.Printf("%s\n", err)
		log.Println("Stack Trace:")
		buf := make([]byte, 4096)
		buf = buf[:runtime.Stack(buf, true)]
		log.Printf("%s\n", buf)
		os.Exit(1)
	}
}

func configError(err error) {
	if err != nil {
		log.Println("Configuration Error:")
		log.Printf("%s\n", err)
		os.Exit(1)
	}
}
