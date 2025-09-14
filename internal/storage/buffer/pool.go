package buffer

import (
	"fmt"

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
	if idx, ok := bp.rs.pageToIdx[pageId]; ok {
		return bp.replacer.GetPage(idx)
	}

	freeIdx := bp.rs.allocFromFree()
	if freeIdx == -1 {
		rmIdx, err := bp.replacer.Evict()
		if err != nil {
			return nil, err
		}
		freeIdx = rmIdx
	}

	readPage, err := bp.fm.ReadPage(pageId)
	if err != nil {
		bp.rs.returnFrameToFree(freeIdx)
		return nil, err
	}

	bp.rs.pageToIdx[pageId] = freeIdx

	if err := bp.replacer.PutPage(freeIdx, readPage); err != nil {
		return nil, fmt.Errorf("[AllocateFrame] Put page fail: %w", err)
	}

	// NOTE: The pin should be managed explicitly by transations
	// if err := bp.PinFrame(freeIdx); err != nil {
	// 	bp.rs.removePageMapping(pageId)
	// 	if err := bp.replacer.ResetFrameByIdx(freeIdx); err != nil {
	// 		return nil, fmt.Errorf("[AllocateFrame] reset frame fail: %w", err)
	// 	}
	// 	bp.rs.returnFrameToFree(freeIdx)
	// 	return nil, err
	// }

	return readPage, nil
}

// PinFrame delegates to replacer.
func (bp *BufferPool) PinFrame(frameIdx int) error {
	return bp.replacer.Pin(frameIdx)
}

// UnpinFrame delegates to replacer.
func (bp *BufferPool) UnpinFrame(idx int, isDirty bool) error {
	return bp.replacer.Unpin(idx, isDirty)
}

func (bp *BufferPool) MarkDirty(idx int) error {
	return bp.replacer.Dirty(idx)
}
