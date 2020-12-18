package metrics

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const metricsPort = ":9100"

func Expose() {
	http.Handle("/metrics", promhttp.Handler())
	log.Printf("Serving metrics on %s", metricsPort)
	_ = http.ListenAndServe(metricsPort, nil)
}
