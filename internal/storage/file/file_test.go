package file

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
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
