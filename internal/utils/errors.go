package util

import "errors"

var (
	ErrInvalidPageId         = errors.New("invalid page id")
	ErrInvalidPageSize       = errors.New("invalid page size")
	ErrChecksumMismatch      = errors.New("checksum mismatch")
	ErrInvalidInitialPages   = errors.New("initial pages must be positive")
	ErrMaxMapSizeExceeded    = errors.New("initial size exceeds maximum mapping size")
	ErrPageAlreadyDirty      = errors.New("page is already dirty")
	ErrPageNotDirty          = errors.New("page is not dirty")
	ErrPageAlreadyPinned     = errors.New("page is already pinned")
	ErrPageNotPinned         = errors.New("page is not pinned")
	ErrPageOffsetOutOfBounds = errors.New("page offset out of bounds")
	ErrPageOutOfBounds       = errors.New("page out of bounds")
	ErrInvalidOffset         = errors.New("invalid offset or size")
	ErrFileManagerNil        = errors.New("file manager is nil")
	ErrFileDataNil           = errors.New("file data is nil")
	ErrInvalidPoolSize       = errors.New("invalid pool size")
	ErrOutBoundOfFrame       = errors.New("frame idx out of bound")
	ErrInvalidEviction       = errors.New("invalid eviction")
	ErrNoFreeFrame           = errors.New("no free frames")
)
