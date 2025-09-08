package buffer

import (
	"sync"

	"github.com/bietkhonhungvandi212/array-db/internal/storage/file"
	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

type BufferPool struct {
	frames      []page.Page         // Holds page.Page (4KB)
	fileToFrame map[util.PageID]int //Map the pageId to index
	nextFree    []int               // Free list for allocation
	nextLRU     []int               // Forward links for LRU
	prevLRU     []int               // Backward links for LRU
	freeHead    int                 // Head of free list
	lruHead     int                 // Head of LRU (evict first)
	lruTail     int                 // Tail of LRU (most recent)
	poolSize    int                 // Total frames
	file.Filer

	mu sync.Mutex // For thread safety
}

func newBufferPool(size int, filer file.Filer) *BufferPool {
	if size <= 0 {
		panic(util.ErrInvalidPoolSize)
	}

	bp := BufferPool{
		frames:      make([]page.Page, size),
		fileToFrame: make(map[util.PageID]int, size),
		nextFree:    make([]int, size),
		nextLRU:     make([]int, size),
		prevLRU:     make([]int, size),
		freeHead:    0,
		lruHead:     -1,
		lruTail:     -1,
		poolSize:    size,
		Filer:       filer,
	}

	for i := range size {
		if i == size-1 {
			bp.nextFree[i] = -1
		} else {
			bp.nextFree[i] = i + 1
		}

		bp.nextLRU[i] = -1
		bp.prevLRU[i] = -1
	}

	return &bp
}

func (bp *BufferPool) isFrameDirty(frameIdx int) bool {
	return bp.frames[frameIdx].Header.IsDirty()
}

func (bp *BufferPool) isFramePinned(frameIdx int) bool {
	return bp.frames[frameIdx].Header.IsPinned()
}
