package main

import (
	"database/sql"
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/lib/pq"
)

type Import struct {
	txn  *sql.Tx
	stmt *sql.Stmt
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
	fmt.Println("Full schema...", schema)
	stmt, err := txn.Prepare(pq.CopyInSchema(schema, tableName, columns...))
	spew.Dump("Full statement...", stmt)
	if err != nil {
		return nil, err
	}
	return &Import{txn, stmt}, nil
}

func (i *Import) AddRow(columns ...interface{}) error {
	_, err := i.stmt.Exec(columns...)
	if err != nil {
		fmt.Println("Error execing statement against column: ", columns, "Error: ", err.Error())
	}
	return err
}

func (i *Import) Commit() error {
	_, err := i.stmt.Exec()
	if err != nil {
		return err
	}
	_ = i.stmt.Close()
	return i.txn.Commit()
}
