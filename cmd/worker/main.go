package main

import (
	"log"
	"net/http"

	"omarkhd/litess/server"
)

const (
	port = ":3000"
)

func main() {
	ws := server.NewWorkerServer()
	log.Printf("Starting worker on port %s", port)
	log.Fatal(http.ListenAndServe(port, ws.Mux()))
}
