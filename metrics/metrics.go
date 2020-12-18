package metrics

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const metricsPort = ":9100"

var Quantiles = map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}

func Expose() {
	http.Handle("/metrics", promhttp.Handler())
	log.Printf("Serving metrics on %s", metricsPort)
	_ = http.ListenAndServe(metricsPort, nil)
}
