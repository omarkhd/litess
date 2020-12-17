package server

import (
	"net/http"

	"github.com/gorilla/mux"
)

const (
	rootEndpoint = "/"
)

type workerServer struct {
	mux *mux.Router
}

func NewWorkerServer() *workerServer {
	ws := &workerServer{
		mux: mux.NewRouter(),
	}
	ws.wire()
	return ws
}

func (ws *workerServer) root(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}

func (ws *workerServer) wire() {
	ws.mux.HandleFunc(rootEndpoint, ws.root)
}

func (ws *workerServer) Mux() *mux.Router {
	return ws.mux
}
