package file

import (
	"os"
	"sync"

	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

/**
* This module is used to read and write data from / to disk
* we will map the file to memory in disk that facilitate accessility to disk
**/
type FileManager struct {
	file *os.File
	data []byte
	size int64

	rwlock sync.Mutex
}

// When read from disk -> Deseialize the data to page.Page
// When write to disk -> Serialize the data to []byte and store them in disk by offset

/* READ FILE */
func (file *FileManager) ReadPage(pageId util.PageID) (*page.Page, error) {
	return nil, nil
}
