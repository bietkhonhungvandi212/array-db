package util

import (
	"fmt"
	"time"
)

// PageID represents a unique page identifier
type PageID uint64

// PageSize represents the standard page size (4KB)
const PageSize = 4096

// TransactionID represents a unique transaction identifier
type TransactionID uint64

// Timestamp represents a logical timestamp for MVCC
type Timestamp uint64

// ErrorType represents different types of database errors
type ErrorType int

const (
	ErrTypeNotFound ErrorType = iota
	ErrTypeInvalidKey
	ErrTypeInvalidValue
	ErrTypeTransactionAborted
	ErrTypeIOError
	ErrTypeCorruption
)

// DatabaseError represents a database-specific error
type DatabaseError struct {
	Type    ErrorType
	Message string
	Cause   error
	Context map[string]interface{}
}

func (e *DatabaseError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("ArrayDB Error [%d]: %s (caused by: %v)", e.Type, e.Message, e.Cause)
	}
	return fmt.Sprintf("ArrayDB Error [%d]: %s", e.Type, e.Message)
}

// NewDatabaseError creates a new database error
func NewDatabaseError(errType ErrorType, message string, cause error) *DatabaseError {
	return &DatabaseError{
		Type:    errType,
		Message: message,
		Cause:   cause,
		Context: make(map[string]interface{}),
	}
}

// Options represents database configuration options
type Options struct {
	Path               string
	PageSize           int
	BufferPoolSize     int
	SyncWrites         bool
	ReadOnly           bool
	MaxOpenFiles       int
	CompactionInterval time.Duration
}

// DefaultOptions returns default database options
func DefaultOptions() Options {
	return Options{
		PageSize:           PageSize,
		BufferPoolSize:     1000, // 4MB default buffer pool
		SyncWrites:         false,
		ReadOnly:           false,
		MaxOpenFiles:       1000,
		CompactionInterval: 30 * time.Minute,
	}
}
