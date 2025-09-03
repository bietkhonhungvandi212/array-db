package file

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

/**
* This module is used to read and write data from / to disk
* we will map the file to memory in disk that facilitate accessility to disk
**/
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

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	fm := &FileManager{File: f}

	if err := mmap(fm, initialSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("map file fail: %w", err)
	}

	return fm, nil
}

// When read from disk -> Deseialize the data to page.Page
/* READ FILE */
func (fm *FileManager) ReadPage(pageId util.PageID) (*page.Page, error) {
	offset := int64(pageId) * int64(util.PageSize)
	if offset+util.PageSize > fm.Size {
		return nil, util.ErrPageOutOfBounds
	}

	page, err := page.Deserialize(fm.Data[offset : offset+int64(util.PageSize)])
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
		if newSize > util.MAX_MAP_SIZE {
			return util.ErrMaxMapSizeExceeded
		}

		if err := munmap(fm); err != nil {
			return fmt.Errorf("[WritePage] unmap file fail: %w", err)
		}

		if err := mmap(fm, newSize); err != nil {
			return fmt.Errorf("[WritePage] map file fail: %w", err)
		}
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
	if err := munmap(fm); err != nil {
		return fmt.Errorf("[close] unmap file fail: %w", err)
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
