package buffer

import (
	"github.com/bietkhonhungvandi212/array-db/internal/storage/file"
	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

// BufferPool manages the buffer pool with a pluggable replacer.
type BufferPool struct {
	fm       *file.FileManager // File manager for I/O
	rs       *ReplacerShared
	replacer Replacer // Pluggable replacement policy
}

// NewBufferPool initializes the buffer pool with a replacer.
func NewBufferPool(fm *file.FileManager, replacer Replacer, shared *ReplacerShared) *BufferPool {
	bp := &BufferPool{
		fm:       fm,
		rs:       shared,
		replacer: replacer,
	}

	return bp
}

// AllocateFrame delegates eviction to the replacer.
func (bp *BufferPool) AllocateFrame(pageId util.PageID) (*page.Page, error) {
	// Page not in buffer, need to load from disk
	readPage, err := bp.fm.ReadPage(pageId)
	if err != nil {
		return nil, err
	}

	// Get the page into buffer and pin it
	if err := bp.replacer.RequestFree(readPage, bp.fm); err != nil {
		return nil, err
	}

	return readPage, nil
}

// Get and pin page
func (bp *BufferPool) GetPage(pageId util.PageID) (*page.Page, error) {
	return bp.replacer.GetPage(pageId)
}

// UnpinFrame delegates to replacer.
func (bp *BufferPool) Release(pageId util.PageID, isDirty bool) error {
	return bp.replacer.Unpin(pageId, isDirty)
}
