package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"omarkhd/litess/db"
)

const (
	rootEndpoint  = "/"
	execEndpoint  = rootEndpoint + "exec"
	queryEndpoint = rootEndpoint + "query"

	contentTypeHeader = "Content-Type"
	jsonContentType   = "application/json"

	jsonEncoderPrefix = ""
	jsonEncoderIndent = "    "
)

type workerServer struct {
	mux *mux.Router
	ng  db.Engine
}

func NewWorkerServer(ng db.Engine) *workerServer {
	ws := &workerServer{
		mux: mux.NewRouter(),
		ng:  ng,
	}
	ws.wire()
	return ws
}

func (ws *workerServer) wire() {
	ws.mux.HandleFunc(execEndpoint, ws.exec).Methods(http.MethodPost)
	ws.mux.HandleFunc(queryEndpoint, ws.query).Methods(http.MethodPost)
}

func (ws *workerServer) Mux() *mux.Router {
	return ws.mux
}

func (ws *workerServer) exec(w http.ResponseWriter, r *http.Request) {
	wreq, err := ws.workerRequest(r)
	if err != nil {
		ws.error(w, err, http.StatusBadRequest)
		return
	}
	dbr, err := ws.ng.Exec(r.Context(), wreq.SQL)
	if err != nil {
		ws.error(w, err, http.StatusInternalServerError)
		return
	}
	wres := workerResponse{}
	wres.RowsAffected, _ = dbr.RowsAffected()
	wres.LastInsertId, _ = dbr.LastInsertId()
	w.Header().Set(contentTypeHeader, jsonContentType)
	encoder := ws.encoder(w)
	_ = encoder.Encode(wres)
}

func (ws *workerServer) query(w http.ResponseWriter, r *http.Request) {
	wreq, err := ws.workerRequest(r)
	if err != nil {
		ws.error(w, err, http.StatusBadRequest)
		return
	}
	dbr, err := ws.ng.Query(context.Background(), wreq.SQL)
	if err != nil {
		ws.error(w, err, http.StatusInternalServerError)
		return
	}
	wres := workerResponse{}
	for dbr.Next() {
		wres.RowsAffected += 1
	}
	w.Header().Set(contentTypeHeader, jsonContentType)
	encoder := ws.encoder(w)
	_ = encoder.Encode(wres)
}

func (ws *workerServer) workerRequest(r *http.Request) (workerRequest, error) {
	var wr workerRequest
	if err := json.NewDecoder(r.Body).Decode(&wr); err != nil {
		return wr, err
	}
	return wr, nil
}

func (ws *workerServer) error(w http.ResponseWriter, err error, code int) {
	if err != nil {
		w.Header().Set(contentTypeHeader, jsonContentType)
		encoder := ws.encoder(w)
		w.WriteHeader(code)
		_ = encoder.Encode(&workerResponse{
			Error: err.Error(),
		})
	}
}

func (ws *workerServer) encoder(w io.Writer) *json.Encoder {
	encoder := json.NewEncoder(w)
	encoder.SetIndent(jsonEncoderPrefix, jsonEncoderIndent)
	return encoder
}

type workerRequest struct {
	SQL string `json:"sql"`
}

type workerResponse struct {
	RowsAffected int64  `json:"rows_affected"`
	LastInsertId int64  `json:"last_insert_id"`
	Error        string `json:"error"`
}
