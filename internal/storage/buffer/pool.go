package buffer

import (
	"sync"

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

	mu sync.Mutex
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

// DRAFT
func (bp *BufferPool) GetPage(pageID util.PageID) (*page.Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if frameIdx, exists := bp.pageToIdx[pageID]; exists {
		err := bp.moveToTail(frameIdx) // Internal method
		if err != nil {
			return nil, err
		}

		return &bp.frames[frameIdx], nil
	}
	// ... handle cache miss

	return nil, nil
}

// ===================== HELPER FUNCTION =====================
func (this *BufferPool) moveToTail(frameIdx int) error {
	if err := this.removeFromLRU(frameIdx); err != nil {
		return err
	}
	return this.addToTail(frameIdx)
}

func (this *BufferPool) addToTail(frameIdx int) error {
	if frameIdx >= this.poolSize || frameIdx < 0 {
		return util.ErrOutBoundOfFrame
	}

	tmp := this.lruTail
	this.lruTail = frameIdx
	this.prevLRU[frameIdx] = tmp
	this.nextLRU[frameIdx] = -1

	if tmp != -1 {
		this.nextLRU[tmp] = frameIdx
	}

	if this.lruHead == -1 {
		this.lruHead = frameIdx
	}

	return nil
}

func (this *BufferPool) removeFromLRU(frameIdx int) error {
	if frameIdx >= this.poolSize || frameIdx < 0 {
		return util.ErrOutBoundOfFrame
	}

	if this.lruHead == -1 || (this.nextLRU[frameIdx] == -1 && this.prevLRU[frameIdx] == -1 && this.lruHead != frameIdx) {
		return util.ErrInvalidEviction
	}

	// single node
	if this.lruHead == frameIdx && this.lruTail == frameIdx {
		this.lruHead = -1
		this.lruTail = -1

		return nil
	}

	// Update head
	if this.prevLRU[frameIdx] == -1 {
		nextIdx := this.nextLRU[frameIdx]
		this.lruHead = nextIdx
		this.nextLRU[frameIdx] = -1
		this.prevLRU[nextIdx] = -1

		return nil
	}

	// Update tail
	if this.nextLRU[frameIdx] == -1 {
		prevIdx := this.prevLRU[frameIdx]
		this.lruTail = prevIdx
		this.nextLRU[prevIdx] = -1
		this.prevLRU[frameIdx] = -1

		return nil
	}

	// node.Next.Prev = node.Prev
	// node.Prev.Next = node.Next
	this.nextLRU[this.prevLRU[frameIdx]] = this.nextLRU[frameIdx]
	this.prevLRU[this.nextLRU[frameIdx]] = this.prevLRU[frameIdx]

	this.nextLRU[frameIdx] = -1
	this.prevLRU[frameIdx] = -1

	return nil
}

func (this *BufferPool) allocFromFree() int {
	if this.freeHead == -1 {
		return -1
	}

	freeIdx := this.freeHead
	this.freeHead = this.nextFree[freeIdx]
	this.nextFree[freeIdx] = -1

	return freeIdx
}
