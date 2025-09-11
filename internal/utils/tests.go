package util

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func CreateTempFile(t *testing.T) (string, func()) {
	t.Helper()
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, fmt.Sprintf("arraydb-test-%d.dat", rand.Intn(100)+10))
	return tempFile, func() {
		os.Remove(tempFile)
	}
}
