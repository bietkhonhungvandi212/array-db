package file

import (
	"errors"
	"fmt"
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
	File    *os.File
	Data    []byte
	Size    int64
	Mapping syscall.Handle
}

func NewFileManager(path string, initialPages int) (*FileManager, error) {
	if initialPages <= 0 {
		return nil, util.ErrInvalidInitialPages
	}

	initialSize := int64(initialPages) * int64(util.PageSize)
	if initialSize > MAX_MAP_SIZE {
		return nil, util.ErrMaxMapSizeExceeded
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	if err := f.Truncate(initialSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("truncate: %w", err)
	}

	h, err := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil, syscall.PAGE_READWRITE, 0, uint32(initialSize), nil)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("map view: %w", err)
	}

	ptr, err := syscall.MapViewOfFile(h, syscall.FILE_MAP_WRITE, 0, 0, uintptr(initialSize))
	if err != nil {
		if err := syscall.CloseHandle(h); err != nil {
			return nil, err
		}

		f.Close()
		return nil, err
	}

	data := (*[MAX_MAP_SIZE]byte)(unsafe.Pointer(ptr))[:initialSize:initialSize]
	return &FileManager{File: f, Data: data, Size: initialSize, Mapping: h}, nil
}

// When read from disk -> Deseialize the data to page.Page
/* READ FILE */
func (file *FileManager) ReadPage(pageId util.PageID) (*page.Page, error) {
	offset := int64(pageId) * int64(util.PageSize)
	if offset+util.PageSize > file.Size {
		return nil, util.ErrPageOutOfBounds
	}

	page, err := page.Deserialize(file.Data[offset : offset+int64(util.PageSize)])
	if err != nil {
		return nil, fmt.Errorf("deserialize page %d: %w", pageId, err)
	}

	return page, nil
}

// When write to disk -> Serialize the data to []byte and store them in disk by offset
/* WRITE FILE */
func (fm *FileManager) WritePage(p *page.Page) error {
	offset := int64(p.Header.PageID) * int64(util.PageSize)
	if offset+int64(util.PageSize) > fm.Size {
		newSize := max(fm.Size*2, offset+int64(util.PageSize))
		if newSize > MAX_MAP_SIZE {
			return util.ErrMaxMapSizeExceeded
		}

		if err := fm.File.Truncate(newSize); err != nil {
			return fmt.Errorf("truncate to %d: %w", newSize, err)
		}
		if err := syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(&fm.Data[0]))); err != nil {
			return fmt.Errorf("unmap during resize: %w", err)
		}
		if err := syscall.CloseHandle(fm.Mapping); err != nil {
			return fmt.Errorf("close handle during resize: %w", err)
		}
		h, err := syscall.CreateFileMapping(syscall.Handle(fm.File.Fd()), nil, syscall.PAGE_READWRITE, 0, uint32(newSize), nil)
		if err != nil {
			return fmt.Errorf("create mapping during resize: %w", err)
		}
		ptr, err := syscall.MapViewOfFile(h, syscall.FILE_MAP_WRITE, 0, 0, uintptr(newSize))
		if err != nil {
			return fmt.Errorf("map view during resize: %w", err)
		}
		fm.Data = (*[MAX_MAP_SIZE]byte)(unsafe.Pointer(ptr))[:newSize:newSize]
		fm.Size = newSize
		fm.Mapping = h
	}

	copy(fm.Data[offset:], p.Serialize())
	return nil
}

/**
* CLOSE FUNCTION
**/
func (fm *FileManager) Close() error {
	if fm == nil {
		return nil // Idempotent
	}
	var err error
	if fm.Data != nil {
		if e := syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(&fm.Data[0]))); e != nil {
			err = errors.Join(err, fmt.Errorf("unmap: %w", e))
		}
		fm.Data = nil
	}
	if fm.Mapping != 0 {
		if e := syscall.CloseHandle(fm.Mapping); e != nil {
			err = errors.Join(err, fmt.Errorf("close mapping: %w", e))
		}
		fm.Mapping = 0
	}
	if fm.File != nil {
		if e := fm.File.Sync(); e != nil {
			err = errors.Join(err, fmt.Errorf("sync file: %w", e))
		}
		if e := fm.File.Close(); e != nil {
			err = errors.Join(err, fmt.Errorf("close file: %w", e))
		}
		fm.File = nil
	}
	return err
}
