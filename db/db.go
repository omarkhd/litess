package db

import (
	"context"
	"database/sql"
	"log"

	_ "modernc.org/sqlite"
)

const (
	driverName     = "sqlite"
	dataSourceName = "db.sqlite"
)

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
	r, err := le.db.ExecContext(ctx, q)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (le *liteEngine) Query(ctx context.Context, q string) (*sql.Rows, error) {
	log.Print(q)
	r, err := le.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	return r, nil
}
