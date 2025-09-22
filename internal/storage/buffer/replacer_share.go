package buffer

import (
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

// ReplacerShared provides common state and methods for replacement policies.
type ReplacerShared struct {
	pageToIdx map[util.PageID]int // Map PageID to frame index
	poolSize  int                 // Total frames
}

// NewReplacerShared initializes the shared replacer state.
func NewReplacerShared(size int) *ReplacerShared {
	if size <= 0 {
		panic(util.ErrInvalidPoolSize)
	}
	rs := &ReplacerShared{
		pageToIdx: make(map[util.PageID]int, size),
		poolSize:  size,
	}
	return rs
}

// removePageMapping removes a page from the pageToIdx map.
func (rs *ReplacerShared) removePageMapping(pageId util.PageID) {
	delete(rs.pageToIdx, pageId)
}

func (rs *ReplacerShared) getMap() map[util.PageID]int {
	return rs.pageToIdx
}

func (lr *ReplacerShared) Size() int {
	return lr.poolSize
}
