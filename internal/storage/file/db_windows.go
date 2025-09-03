//go:build windows

package file

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"

	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

// Base on: https://github.com/etcd-io/bbolt/blob/main/bolt_windows.go

func mmap(fm *FileManager, size int64) error {
	if fm.File == nil {
		return util.ErrFileManagerNil
	}
	if size <= 0 {
		return util.ErrInvalidInitialPages
	}
	if size > util.MAX_MAP_SIZE {
		return util.ErrMaxMapSizeExceeded
	}

	if err := fm.File.Truncate(size); err != nil {
		return fmt.Errorf("truncate to %d: %w", size, err)
	}
	sizehi := uint32(size >> 32)
	sizelo := uint32(size)
	h, err := syscall.CreateFileMapping(syscall.Handle(fm.File.Fd()), nil, syscall.PAGE_READWRITE, sizehi, sizelo, nil)
	if err != nil {
		return fmt.Errorf("create mapping: %w", err)
	}
	ptr, err := syscall.MapViewOfFile(h, syscall.FILE_MAP_WRITE, 0, 0, uintptr(size))
	if err != nil {
		if err := syscall.CloseHandle(h); err != nil {
			return os.NewSyscallError("CloseHandle", err)
		}
		return fmt.Errorf("map view: %w", err)
	}
	fm.Data = (*[util.MAX_MAP_SIZE]byte)(unsafe.Pointer(ptr))[:size:size]
	fm.Size = size
	return nil
}

// munmap unmaps a pointer from a file.
func munmap(fm *FileManager) error {
	if fm.File == nil {
		return util.ErrFileManagerNil
	}

	if fm.Data == nil {
		return nil
	}

	addr := uintptr(unsafe.Pointer(&fm.Data[0]))
	var err error
	if e := syscall.UnmapViewOfFile(addr); e != nil {
		err = fmt.Errorf("unmap: %w", e)
	}

	fm.Data = nil
	fm.Size = 0
	return err
}
