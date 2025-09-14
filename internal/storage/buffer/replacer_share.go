package buffer

import (
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

// ReplacerShared provides common state and methods for replacement policies.
type ReplacerShared struct {
	pageToIdx map[util.PageID]int // Map PageID to frame index
	nextFree  []int               // Free list for allocation
	freeHead  int                 // Head of free list
	lruHead   int                 // Head of LRU (evict first)
	lruTail   int                 // Tail of LRU (most recent)
	poolSize  int                 // Total frames
}

// NewReplacerShared initializes the shared replacer state.
func NewReplacerShared(size int) *ReplacerShared {
	if size <= 0 {
		panic(util.ErrInvalidPoolSize)
	}
	rs := &ReplacerShared{
		pageToIdx: make(map[util.PageID]int, size),
		nextFree:  make([]int, size),
		freeHead:  0,
		poolSize:  size,
		lruHead:   -1,
		lruTail:   -1,
	}
	for i := 0; i < size; i++ {
		rs.nextFree[i] = i + 1
	}
	rs.nextFree[size-1] = -1
	return rs
}

// allocFromFree allocates a free frame index.
func (rs *ReplacerShared) allocFromFree() int {
	if rs.freeHead == -1 {
		return -1
	}
	freeIdx := rs.freeHead
	rs.freeHead = rs.nextFree[freeIdx]
	rs.nextFree[freeIdx] = -1
	return freeIdx
}

// returnFrameToFree returns a frame to the free list.
func (rs *ReplacerShared) returnFrameToFree(frameIdx int) {
	rs.nextFree[frameIdx] = rs.freeHead
	rs.freeHead = frameIdx
}

// removePageMapping removes a page from the pageToIdx map.
func (rs *ReplacerShared) removePageMapping(pageId util.PageID) {
	delete(rs.pageToIdx, pageId)
}

func (rs *ReplacerShared) getMap() map[util.PageID]int {
	return rs.pageToIdx
}
