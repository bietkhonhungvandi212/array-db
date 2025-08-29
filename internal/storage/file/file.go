package file

import (
	"errors"
	"os"
	"sync"
	"syscall"
	"unsafe"

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

func NewFileManager(path string, initialPages int) (*FileManager, error) {
	if initialPages < 0 {
		return nil, errors.New("Initial Pages must be greater than 0")
	}

	initialSize := int64(initialPages) * int64(util.PageSize)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 066)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if err := f.Truncate(initialSize); err != nil {
		return nil, err
	}

	h, err := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil, syscall.PAGE_READWRITE, 0, uint32(initialSize), nil)
	if err != nil {
		return nil, err
	}
	ptr, err := syscall.MapViewOfFile(h, syscall.FILE_MAP_WRITE, 0, 0, uintptr(initialSize))
	if err != nil {
		return nil, err
	}

	data := (*[1 << 30]byte)(unsafe.Pointer(ptr))[:initialSize:initialSize]
	return &FileManager{file: f, data: data, size: initialSize}, nil
}

/* READ FILE */
func (file *FileManager) ReadPage(pageId util.PageID) (*page.Page, error) {
	return nil, nil
}
