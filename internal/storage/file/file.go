package file

import (
	"errors"
	"os"
	"syscall"
	"unsafe"

	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

/**
* This module is used to read and write data from / to disk
* we will map the file to memory in disk that facilitate accessility to disk
**/
const MAX_MAP_SIZE = 1 << 28 // 256MB limit
type FileManager struct {
	file    *os.File
	data    []byte
	size    int64
	mapping syscall.Handle
}

func NewFileManager(path string, initialPages int) (*FileManager, error) {
	if initialPages < 0 {
		return nil, util.ErrInvalidInitialPages
	}

	initialSize := int64(initialPages) * int64(util.PageSize)
	if initialSize > MAX_MAP_SIZE {
		return nil, util.ErrMaxMapSizeExceeded
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	if err := f.Truncate(initialSize); err != nil {
		f.Close()
		return nil, err
	}

	h, err := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil, syscall.PAGE_READWRITE, 0, uint32(initialSize), nil)
	if err != nil {
		f.Close()
		return nil, err
	}

	ptr, err := syscall.MapViewOfFile(h, syscall.FILE_MAP_WRITE, 0, 0, uintptr(initialSize))
	if err != nil {
		f.Close()
		return nil, err
	}

	data := (*[1 << 30]byte)(unsafe.Pointer(ptr))[:initialSize:initialSize]
	return &FileManager{file: f, data: data, size: initialSize, mapping: h}, nil
}

/**
* CLOSE FUNCTION
**/
func (fm *FileManager) Close() error {
	var err error
	if fm.data != nil {
		if e := syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(&fm.data[0]))); e != nil {
			err = errors.Join(err, e)
		}
		fm.data = nil
	}
	if fm.mapping != 0 {
		if e := syscall.CloseHandle(fm.mapping); e != nil {
			err = errors.Join(err, e)
		}
		fm.mapping = 0
	}
	if fm.file != nil {
		if e := fm.file.Close(); e != nil {
			err = errors.Join(err, e)
		}
		fm.file = nil
	}

	return err
}

// When read from disk -> Deseialize the data to page.Page
// When write to disk -> Serialize the data to []byte and store them in disk by offset
/* READ FILE */
func (file *FileManager) ReadPage(pageId util.PageID) (*page.Page, error) {
	return nil, nil
}
