package main

import (
	"flag"
	"os"
	"strconv"
	"time"
	"www/microservice"
)

func main() {

	//name := flag.String("name", "ms", "Name of the service")

	hostname, _ := os.Hostname()
	name := flag.String("name", "ms:"+hostname+":"+strconv.Itoa(time.Now().Nanosecond()), "Name of the service")
	host := flag.String("host", "0.0.0.0", "IP address that service listening to")
	port := flag.Int("port", 1234, "Port that service listening to")

	rqsize := flag.Int("rqsize", 1, "The maxsize of receive queue")
	dqsize := flag.Int("dqsize", 1, "The maxsize of delivery queue")

	maxpworkernum := flag.Int("ptnum", 1, "The max number of process threads")
	maxdworkernum := flag.Int("dtnum", 1, "The max number of delivery threads")

	failretry := flag.Int("retry", 0, "The retry number before failure")
	monitorwindow := flag.Int("monwindow", 1, "The monitoring window size")

	flag.Parse()

	// // params := make(map[string]interface{})
	// // params["name"] = *name
	// // params["host"] = *host
	// // params["port"] = *port

	params := map[string]interface{}{
		"name":   *name,
		"host":   *host,
		"port":   *port,
		"rqsize": *rqsize, "maxpworkernum": *maxpworkernum,
		"dqsize": *dqsize, "maxdworkernum": *maxdworkernum,
		"failretry":     *failretry,
		"monitorwindow": *monitorwindow,
	}

	ms := microservice.New(params)
	ms.Run()

	//syslogcollector := syslogcollector.New(params)
	//syslogcollector.Run()
	// x_values := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	// y_values := []float64{1, 1, 2, 4, 5, 5, 7, 10}
	// z_values := []float64{1, 1.5, 1.4, 1, 2, 3.5, 4, 9}

	// a, b, alpha, beta := microservice.Regression(x_values, y_values, z_values, 200, 0.05)

	// fmt.Printf("a: %f\n", a)
	// fmt.Printf("b: %f\n", b)
	// fmt.Printf("alpha: %f\n", alpha)
	// fmt.Printf("beta: %f\n", beta)
}
