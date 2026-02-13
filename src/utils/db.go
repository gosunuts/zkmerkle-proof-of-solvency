package utils

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// DB is a wrapper around sql.DB with additional functionality
type DB struct {
	*sql.DB
	maxExecutionTime time.Duration
}

// NewDB creates a new database connection
func NewDB(dataSource string) (*DB, error) {
	db, err := sql.Open("mysql", dataSource)
	if err != nil {
		return nil, err
	}

	// Set connection pool settings
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Hour)

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &DB{
		DB:               db,
		maxExecutionTime: 10000 * time.Second,
	}, nil
}

// SetMaxExecutionTime sets the maximum execution time for queries
func (db *DB) SetMaxExecutionTime(seconds int) {
	db.maxExecutionTime = time.Duration(seconds) * time.Second
}

// ExecWithTimeout executes a query with timeout hint
func (db *DB) ExecWithTimeout(query string, args ...interface{}) (sql.Result, error) {
	// Add max execution time hint for MySQL
	timeoutQuery := fmt.Sprintf("/*+ MAX_EXECUTION_TIME(%d) */ %s", int(db.maxExecutionTime.Milliseconds()), query)
	return db.DB.Exec(timeoutQuery, args...)
}

// QueryWithTimeout executes a query with timeout hint
func (db *DB) QueryWithTimeout(query string, args ...interface{}) (*sql.Rows, error) {
	// Add max execution time hint for MySQL
	timeoutQuery := fmt.Sprintf("/*+ MAX_EXECUTION_TIME(%d) */ %s", int(db.maxExecutionTime.Milliseconds()), query)
	return db.DB.Query(timeoutQuery, args...)
}

// QueryRowWithTimeout executes a query with timeout hint
func (db *DB) QueryRowWithTimeout(query string, args ...interface{}) *sql.Row {
	// Add max execution time hint for MySQL
	timeoutQuery := fmt.Sprintf("/*+ MAX_EXECUTION_TIME(%d) */ %s", int(db.maxExecutionTime.Milliseconds()), query)
	return db.DB.QueryRow(timeoutQuery, args...)
}

// Transaction represents a database transaction
type Transaction struct {
	*sql.Tx
}

// BeginTransaction starts a new transaction
func (db *DB) BeginTransaction() (*Transaction, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &Transaction{Tx: tx}, nil
}

// Close closes the database connection
func (db *DB) Close() error {
	return db.DB.Close()
}
