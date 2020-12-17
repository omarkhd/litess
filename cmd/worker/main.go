package main

import (
	"log"
	"net/http"

	"omarkhd/litess/db"
	"omarkhd/litess/server"
)

const (
	port = ":3000"
)

func main() {
	ng, err := db.New()
	if err != nil {
		log.Fatal(err.Error())
	}
	ws := server.NewWorkerServer(ng)
	log.Printf("Starting worker on port %s", port)
	log.Fatal(http.ListenAndServe(port, ws.Mux()))
}
