package file_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/bietkhonhungvandi212/array-db/internal/storage/file"
	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
	"github.com/stretchr/testify/assert"
)

// Helper function to create a test page

// Helper to generate binary data for testing
func generateBinaryData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256) // Pattern: 0, 1, 2, ..., 255
	}
	return data
}

func TestNewFileManager(t *testing.T) {
	tests := []struct {
		name          string
		initialPages  int
		expectedError error
		shouldSucceed bool
	}{
		{
			name:          "Valid creation with 1 page",
			initialPages:  1,
			expectedError: nil,
			shouldSucceed: true,
		},
		{
			name:          "Valid creation with 10 pages",
			initialPages:  10,
			expectedError: nil,
			shouldSucceed: true,
		},
		{
			name:          "Zero pages",
			initialPages:  0,
			expectedError: util.ErrInvalidInitialPages,
			shouldSucceed: false,
		},
		{
			name:          "Large but valid page count",
			initialPages:  1000,
			expectedError: nil,
			shouldSucceed: true,
		},
		{
			name:          "Exceeds max map size",
			initialPages:  int(util.MAX_MAP_SIZE/int64(util.PageSize)) + 1,
			expectedError: util.ErrMaxMapSizeExceeded,
			shouldSucceed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, cleanup := util.CreateTempFile(t)
			defer cleanup()

			fm, err := file.NewFileManager(path, tt.initialPages)
			if tt.shouldSucceed {
				assert.NoError(t, err, "NewFileManager failed")
				assert.NotNil(t, fm, "Expected valid FileManager")
				assert.Equal(t, int64(tt.initialPages)*int64(util.PageSize), fm.Size, "FileManager size mismatch")
				_, err := os.Stat(path)
				assert.NoError(t, err, "Expected file to exist")
				assert.NoError(t, fm.Close(), "Close failed")
			} else {
				assert.Error(t, err, "Expected error but got success")
				if tt.expectedError != nil {
					assert.Contains(t, err.Error(), tt.expectedError.Error(), "Wrong error type")
				}
				if fm != nil {
					fm.Close()
				}
			}
		})
	}
}

func TestFileManagerReadWrite(t *testing.T) {
	tests := []struct {
		name          string
		initialPages  int
		pageID        util.PageID
		data          []byte
		prepareData   func(t *testing.T, fm *file.FileManager, p *page.Page)
		expectedError error
		shouldSucceed bool
	}{
		{
			name:          "Valid read-write with text data",
			initialPages:  1,
			pageID:        0,
			data:          []byte("test instructor record: ID=12345, name=John Doe"),
			prepareData:   nil,
			expectedError: nil,
			shouldSucceed: true,
		},
		{
			name:          "Valid read-write with binary data",
			initialPages:  1,
			pageID:        0,
			data:          generateBinaryData(100),
			prepareData:   nil,
			expectedError: nil,
			shouldSucceed: true,
		},
		{
			name:         "Checksum mismatch",
			initialPages: 1,
			pageID:       0,
			data:         []byte("test data"),
			prepareData: func(t *testing.T, fm *file.FileManager, p *page.Page) {
				if err := fm.WritePage(p); err != nil {
					t.Fatalf("WritePage: %v", err)
				}
				fm.Data[page.HEADER_SIZE] ^= 0xFF // Corrupt first data byte
			},
			expectedError: util.ErrChecksumMismatch,
			shouldSucceed: false,
		},
		{
			name:         "Read from resized file",
			initialPages: 1,
			pageID:       2,
			data:         []byte("resized page data"),
			prepareData: func(t *testing.T, fm *file.FileManager, p *page.Page) {
				if err := fm.WritePage(p); err != nil {
					t.Fatalf("WritePage: %v", err)
				}
			},
			expectedError: nil,
			shouldSucceed: true,
		},
		// {
		// 	name:          "Out of bounds pageID",
		// 	initialPages:  1,
		// 	pageID:        4096,
		// 	data:          []byte("test data"),
		// 	prepareData:   nil,
		// 	expectedError: util.ErrPageOutOfBounds,
		// 	shouldSucceed: false,
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, cleanup := util.CreateTempFile(t)
			defer cleanup()

			fm, err := file.NewFileManager(path, tt.initialPages)
			if err != nil {
				if tt.shouldSucceed {
					t.Fatalf("NewFileManager: %v", err)
				}
				return
			}
			defer fm.Close()

			p := page.CreateTestPage(tt.pageID, tt.data)
			p.Header.SetDirtyFlag()

			if tt.prepareData != nil {
				tt.prepareData(t, fm, p)
			} else {
				err = fm.WritePage(p)
				if !tt.shouldSucceed {
					assert.Error(t, err)
					return
				}
				assert.NoError(t, err, "WritePage failed")
			}

			p2, err := fm.ReadPage(tt.pageID)
			if tt.shouldSucceed {
				assert.NoError(t, err, "ReadPage failed")
				assert.NotNil(t, p2, "Expected valid page but got nil")
				assert.Equal(t, p.Header.PageID, p2.Header.PageID, "PageID mismatch")
				assert.Equal(t, p.Header.Flags, p2.Header.Flags, "Flags mismatch")
				assert.True(t, bytes.Equal(p.Data[:], p2.Data[:]), "Data mismatch")
			} else {
				assert.Error(t, err, "Expected error but got success")
				assert.Nil(t, p2, "Expected nil page on error")
				if tt.expectedError != nil {
					assert.Contains(t, err.Error(), tt.expectedError.Error(), "Wrong error type")
				}
			}
		})
	}
}
