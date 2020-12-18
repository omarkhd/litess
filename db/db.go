package db

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	_ "modernc.org/sqlite"
	"omarkhd/litess/metrics"
)

const (
	driverName     = "sqlite"
	dataSourceName = "file::memory:?cache=shared"
)

var (
	dbQueriesSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "db_queries",
		Objectives: metrics.Quantiles,
	}, []string{"procedure"})
	dbErrorsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "db_errors",
	}, []string{"procedure"})
)

func init() {
	prometheus.MustRegister(dbQueriesSummary)
	prometheus.MustRegister(dbErrorsCounter)
}

type Engine interface {
	Exec(context.Context, string) (sql.Result, error)
	Query(context.Context, string) (*sql.Rows, error)
}

func New() (Engine, error) {
	d, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return &liteEngine{db: d}, nil
}

type liteEngine struct {
	db *sql.DB
}

func (le *liteEngine) Exec(ctx context.Context, q string) (sql.Result, error) {
	log.Print(q)
	labels := map[string]string{
		"procedure": "exec",
	}
	summary := dbQueriesSummary.With(labels)
	start := time.Now().UnixNano()
	defer func() {
		summary.Observe(float64(time.Now().UnixNano() - start))
	}()
	r, err := le.db.ExecContext(ctx, q)
	if err != nil {
		dbErrorsCounter.With(labels).Inc()
		return nil, err
	}
	return r, nil
}

func (le *liteEngine) Query(ctx context.Context, q string) (*sql.Rows, error) {
	log.Print(q)
	labels := map[string]string{
		"procedure": "query",
	}
	summary := dbQueriesSummary.With(labels)
	start := time.Now().UnixNano()
	defer func() {
		summary.Observe(float64(time.Now().UnixNano() - start))
	}()
	r, err := le.db.QueryContext(ctx, q)
	if err != nil {
		dbErrorsCounter.With(labels).Inc()
		return nil, err
	}
	return r, nil
}
