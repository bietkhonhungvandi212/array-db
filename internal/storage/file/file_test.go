package file

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
	"github.com/stretchr/testify/assert"
)

// Helper function to create a temporary test file
func createTempFile(t *testing.T) (string, func()) {
	t.Helper()
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_db.dat")

	cleanup := func() {
		os.Remove(tempFile)
	}

	return tempFile, cleanup
}

// Helper function to create a test page
func createTestPage(pageID util.PageID, data []byte) *page.Page {
	p := &page.Page{
		Header: page.PageHeader{
			PageID: pageID,
			Flags:  0,
		},
	}

	// Fill the data array with test data
	copy(p.Data[:], data)

	return p
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
			name:          "Invalid negative pages",
			initialPages:  -1,
			expectedError: util.ErrInvalidInitialPages,
			shouldSucceed: false,
		},
		{
			name:          "Zero pages (edge case)",
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempFile, cleanup := createTempFile(t)
			defer cleanup()

			fm, err := NewFileManager(tempFile, tt.initialPages)

			if tt.shouldSucceed {
				if err != nil {
					t.Fatalf("Expected success but got error: %v", err)
				}
				if fm == nil {
					t.Fatal("Expected valid FileManager but got nil")
				}

				// Verify the file was created with correct size
				expectedSize := int64(tt.initialPages) * int64(util.PageSize)
				if fm.Size != expectedSize {
					t.Errorf("Expected size %d but got %d", expectedSize, fm.Size)
				}

				// Verify the file exists
				if _, err := os.Stat(tempFile); os.IsNotExist(err) {
					t.Error("Expected file to exist but it doesn't")
				}

				// Clean up
				fm.Close()
			} else {
				if err == nil {
					if fm != nil {
						fm.Close()
					}
					t.Fatal("Expected error but got success")
				}
				if tt.expectedError != nil && err != tt.expectedError {
					t.Errorf("Expected error %v but got %v", tt.expectedError, err)
				}
			}
		})
	}
}

func TestNewFileManager_ExceedsMaxMapSize(t *testing.T) {
	tempFile, cleanup := createTempFile(t)
	defer cleanup()

	// Calculate pages that would exceed MAX_MAP_SIZE
	exceedingPages := int(MAX_MAP_SIZE/int64(util.PageSize)) + 1

	fm, err := NewFileManager(tempFile, exceedingPages)

	if err != util.ErrMaxMapSizeExceeded {
		if fm != nil {
			fm.Close()
		}
		t.Errorf("Expected ErrMaxMapSizeExceeded but got %v", err)
	}
}

func TestFileManagerReadWrite(t *testing.T) {
	tests := []struct {
		name          string
		initialPages  int
		pageID        util.PageID
		data          []byte                              // Data to write to Page.Data
		prepareData   func(fm *FileManager, p *page.Page) // Optional data corruption
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
			data:          generateBinaryData(100), // Binary pattern
			prepareData:   nil,
			expectedError: nil,
			shouldSucceed: true,
		},
		{
			name:         "Checksum mismatch",
			initialPages: 1,
			pageID:       0,
			data:         []byte("test data"),
			prepareData: func(fm *FileManager, p *page.Page) {
				if err := fm.WritePage(p); err != nil {
					t.Fatalf("Write fail: %v", err)
				} // Triggers resize
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
			prepareData: func(fm *FileManager, p *page.Page) {
				if err := fm.WritePage(p); err != nil {
					t.Fatalf("Write fail: %v", err)
				} // Triggers resize
			},
			expectedError: nil,
			shouldSucceed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, cleanup := createTempFile(t)
			defer cleanup()

			fm, err := NewFileManager(path, tt.initialPages)
			if err != nil {
				if tt.shouldSucceed {
					t.Fatalf("NewFileManager: %v", err)
				}
				return
			}
			defer fm.Close()

			// Prepare page
			p := &page.Page{Header: page.PageHeader{PageID: tt.pageID}}
			if len(tt.data) > len(p.Data) {
				t.Fatalf("Test data too large: %d bytes, max %d", len(tt.data), len(p.Data))
			}
			copy(p.Data[:], tt.data)
			p.Header.SetDirtyFlag()
			// Write page and optionally corrupt
			if tt.prepareData != nil {
				tt.prepareData(fm, p)
			} else {
				err = fm.WritePage(p)
				if !tt.shouldSucceed {
					assert.Error(t, err)
					return
				}
				assert.NoError(t, err, "WritePage failed")
			}

			// Read and verify
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

// Helper to generate binary data for testing
func generateBinaryData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256) // Pattern: 0, 1, 2, ..., 255
	}
	return data
}
