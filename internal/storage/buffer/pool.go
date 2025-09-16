package buffer

import (
	"fmt"
	"sync"

	"github.com/bietkhonhungvandi212/array-db/internal/storage/file"
	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

// BufferPool manages the buffer pool with a pluggable replacer.
type BufferPool struct {
	poolSize int               // Total frames
	fm       *file.FileManager // File manager for I/O
	rs       *ReplacerShared
	replacer Replacer // Pluggable replacement policy

	muTable sync.RWMutex // Lock table lookup
}

// NewBufferPool initializes the buffer pool with a replacer.
func NewBufferPool(size int, fm *file.FileManager, replacer Replacer, shared *ReplacerShared) *BufferPool {
	if size <= 0 {
		panic(util.ErrInvalidPoolSize)
	}
	bp := &BufferPool{
		fm:       fm,
		poolSize: size,
		rs:       shared,
		replacer: replacer,
	}

	return bp
}

// AllocateFrame delegates eviction to the replacer.
func (bp *BufferPool) AllocateFrame(pageId util.PageID) (*page.Page, error) {
	bp.muTable.RLock()
	if idx, ok := bp.rs.pageToIdx[pageId]; ok {
		bp.muTable.RUnlock()
		return bp.replacer.GetPage(idx)
	}

	readPage, err := bp.fm.ReadPage(pageId)
	if err != nil {
		return nil, err
	}

	idx, err := bp.replacer.RequestFree(readPage, bp.fm)
	if err != nil {
		return nil, err
	}
	fmt.Println(idx)

	return readPage, nil
}

// PinFrame delegates to replacer.
func (bp *BufferPool) PinFrame(pageId util.PageID) error {
	return bp.replacer.Pin(pageId)
}

// UnpinFrame delegates to replacer.
func (bp *BufferPool) UnpinFrame(pageId util.PageID, isDirty bool) error {
	return bp.replacer.Unpin(pageId, isDirty)
}
