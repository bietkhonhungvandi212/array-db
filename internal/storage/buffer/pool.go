package buffer

import (
	"fmt"

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
	fm         *file.FileManager
}

func NewBufferPool(size int, filer *file.FileManager) *BufferPool {
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
		fm:         filer,
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

/* EVICT LRU */
func (this *BufferPool) EvictFromLRU() (int, error) {
	if this.lruHead == -1 {
		return -1, util.ErrNoFreeFrame // This mark empty LRU
	}

	currentIdx := this.lruHead
	for this.pinCounts[currentIdx] != -1 {
		if this.pinCounts[currentIdx] == 0 && this.frames[currentIdx].Header.IsPinned() {
			//handle dirty pages
			if this.dirtyFlags[currentIdx] {
				if err := this.fm.WritePage(&this.frames[currentIdx]); err != nil {
					return -1, fmt.Errorf("[pool] [EvictFromLRU] flush failed: %w", err)
				}

				if err := this.frames[currentIdx].Header.ClearDirtyFlag(); err != nil {
					return -1, fmt.Errorf("clear dirty failed: %w", err)
				}

				this.dirtyFlags[currentIdx] = false
			}

			this.removeLRUByIndex(currentIdx)
			delete(this.pageToIdx, this.frames[currentIdx].Header.PageID)

			return currentIdx, nil

		}
		currentIdx = this.nextLRU[currentIdx]
	}

	return -1, util.ErrNoFreeFrame
}

// DRAFT
func (this *BufferPool) GetPage(pageID util.PageID) (*page.Page, error) {
	if frameIdx, exists := this.pageToIdx[pageID]; exists {
		this.moveToTail(frameIdx)

		return &this.frames[frameIdx], nil
	}

	// ... handle cache miss
	return nil, nil
}

// ===================== HELPER FUNCTION =====================
func (this *BufferPool) moveToTail(frameIdx int) {
	this.removeLRUByIndex(frameIdx)
	this.addToTail(frameIdx)
}

func (this *BufferPool) addToTail(frameIdx int) {
	if frameIdx >= this.poolSize || frameIdx < 0 {
		panic(fmt.Sprintf("[pool] [addToTail] frame index out of bound: %d", frameIdx))
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
}

func (this *BufferPool) removeLRUByIndex(frameIdx int) {
	if frameIdx >= this.poolSize || frameIdx < 0 {
		panic(fmt.Sprintf("[pool] [removeFromLRU] frame index out of bound: %d", frameIdx))
	}

	if this.lruHead == -1 || (this.nextLRU[frameIdx] == -1 && this.prevLRU[frameIdx] == -1 && this.lruHead != frameIdx) {
		panic(fmt.Sprintf("[pool] [removeFromLRU] frame index %d is invalid ", frameIdx))
	}

	prev := this.prevLRU[frameIdx]
	next := this.nextLRU[frameIdx]
	isHead := (prev == -1)
	isTail := (next == -1)

	switch {
	case isHead && isTail:
		// Case 1: Single node (both head and tail)
		this.lruHead = -1
		this.lruTail = -1
	case isHead && !isTail:
		// Case 2: Head node (has next, no prev)
		this.lruHead = next
		this.prevLRU[next] = -1
	case !isHead && isTail:
		// Case 3: Tail node (has prev, no next)
		this.lruTail = prev
		this.nextLRU[prev] = -1
	case !isHead && !isTail:
		// Case 4: Middle node (has both prev and next)
		this.nextLRU[prev] = next
		this.prevLRU[next] = prev
	default:
		// This should never happen due to validation above
		panic(fmt.Sprintf("[pool] [removeFromLRU] unexpected state for frame %d", frameIdx))
	}

	// Clear the removed node's links
	this.nextLRU[frameIdx] = -1
	this.prevLRU[frameIdx] = -1
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
