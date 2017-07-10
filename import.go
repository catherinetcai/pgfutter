package main

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

type Import struct {
	txn   *sql.Tx
	stmts []*sql.Stmt
}

func NewCSVImport(db *sql.DB, schema string, tableName string, columns []string) (*Import, error) {

	table, err := createTable(db, schema, tableName, columns)
	if err != nil {
		return nil, err
	}

	_, err = table.Exec()
	if err != nil {
		return nil, err
	}

	return newImport(db, schema, tableName, columns)
}

func NewJSONImport(db *sql.DB, schema string, tableName string, column string, dataType string) (*Import, error) {

	table, err := createJSONTable(db, schema, tableName, column, dataType)
	if err != nil {
		return nil, err
	}

	_, err = table.Exec()
	if err != nil {
		return nil, err
	}

	return newImport(db, schema, tableName, []string{column})
}

func newImport(db *sql.DB, schema string, tableName string, columns []string) (*Import, error) {

	txn, err := db.Begin()
	if err != nil {
		return nil, err
	}

	// Chunk slices of 100s
	var chunked [][]string
	chunkSize := 100
	for i := 0; i < len(columns); i += chunkSize {
		end := i + chunkSize
		if end > len(columns) {
			end = len(columns)
		}
		chunked = append(chunked, columns[i:end])
	}
	var stmts []*sql.Stmt
	for _, columns := range chunked {
		stmt, err := txn.Prepare(pq.CopyInSchema(schema, tableName, columns...))
		if err != nil {
			fmt.Println(err)
			continue
		}
		stmts = append(stmts, stmt)
	}

	return &Import{txn, stmts}, nil
}

func (i *Import) AddRow(columns ...interface{}) error {
	for _, stmt := range i.stmts {
		_, err := stmt.Exec(columns...)
		if err != nil {
			return err
		}
	}
	return nil
}

func (i *Import) Commit() error {
	for _, stmt := range i.stmts {
		_, err := stmt.Exec()
		if err != nil {
			return err
		}
		_ = stmt.Close()
	}
	return i.txn.Commit()
}
