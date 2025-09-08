package buffer

import (
	"github.com/bietkhonhungvandi212/array-db/internal/storage/file"
	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

type BufferPool struct {
	frames     []page.Page         // Holds page.Page (4KB)
	pageToIdx  map[util.PageID]int // Map the pageId to index
	pinCounts  []int32
	dirtyFlags []bool
	nextFree   []int // Free list for allocation
	nextLRU    []int // Forward links for LRU
	prevLRU    []int // Backward links for LRU
	freeHead   int   // Head of free list
	lruHead    int   // Head of LRU (evict first)
	lruTail    int   // Tail of LRU (most recent)
	poolSize   int   // Total frames
	file.Filer
}

func NewBufferPool(size int, filer file.Filer) *BufferPool {
	if size <= 0 {
		panic(util.ErrInvalidPoolSize)
	}

	bp := BufferPool{
		frames:     make([]page.Page, size),
		pageToIdx:  make(map[util.PageID]int, size),
		pinCounts:  make([]int32, size),
		dirtyFlags: make([]bool, size),
		nextFree:   make([]int, size),
		nextLRU:    make([]int, size),
		prevLRU:    make([]int, size),
		freeHead:   0,
		lruHead:    -1,
		lruTail:    -1,
		poolSize:   size,
		Filer:      filer,
	}

	for i := range size {
		bp.nextFree[i] = i + 1
		bp.dirtyFlags[i] = false
		bp.nextLRU[i] = -1
		bp.prevLRU[i] = -1
	}
	bp.nextFree[size-1] = -1

	return &bp
}

func (bp *BufferPool) Pin(frameIdx int) {
}

func (bp *BufferPool) isFrameDirty(frameIdx int) (bool, error) {
	if frameIdx >= bp.poolSize || frameIdx < 0 {
		return false, util.ErrOutBoundOfFrame
	}

	return bp.frames[frameIdx].Header.IsDirty(), nil
}

func (bp *BufferPool) isFramePinned(frameIdx int) (bool, error) {
	if frameIdx >= bp.poolSize || frameIdx < 0 {
		return false, util.ErrOutBoundOfFrame
	}

	return bp.frames[frameIdx].Header.IsPinned(), nil
}
