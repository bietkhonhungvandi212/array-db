package buffer

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

type LRUDesc struct {
	page      *page.Page
	nextIdx   int
	prevIdx   int
	pinCounts int32
	dirty     bool
}

type LRUReplacer struct {
	frames []*LRUDesc // Holds page.Page (4KB)
	*ReplacerShared
	lruHead  int   // Head of LRU (evict first)
	lruTail  int   // Tail of LRU (most recent)
	nextFree []int // Free list for allocation
	freeHead int   // Head of free list
}

func (lr *LRUReplacer) Init(size int, replacerShared *ReplacerShared) {
	if size <= 0 {
		panic(util.ErrInvalidPoolSize)
	}

	lr.frames = make([]*LRUDesc, size)
	lr.ReplacerShared = replacerShared
	lr.lruHead = -1
	lr.lruTail = -1
	lr.nextFree = make([]int, size)

	lr.freeHead = 0
	for i := 0; i < size; i++ {
		lr.nextFree[i] = i + 1
	}
	lr.nextFree[size-1] = -1
}

func (lr *LRUReplacer) RequestFree() (int, error) {
	freeIdx := lr.allocFromFree()
	if freeIdx == -1 {
		rmIdx, err := lr.Evict()
		if err != nil {
			return -1, err
		}
		freeIdx = rmIdx
	}

	return freeIdx, nil
}

// Pin marks a frame as pinned (cannot be evicted)
func (lr *LRUReplacer) Pin(frameIdx int) error {
	if frameIdx >= lr.poolSize || frameIdx < 0 {
		return fmt.Errorf("invalid frame index %d", frameIdx)
	}
	node := lr.frames[frameIdx]
	if node == nil {
		return fmt.Errorf("frame %d is not allocated", frameIdx)
	}

	newCount := atomic.AddInt32(&node.pinCounts, 1)
	if newCount == 1 && !node.page.Header.IsPinned() {
		node.page.Header.SetPinnedFlag()
	}
	return nil
}

// Unpin marks a frame as unpinned (can be evicted) and optionally dirty
func (lr *LRUReplacer) Unpin(frameIdx int, isDirty bool) error {
	if frameIdx >= lr.poolSize || frameIdx < 0 {
		return fmt.Errorf("invalid frame index %d", frameIdx)
	}
	node := lr.frames[frameIdx]
	if node == nil {
		return fmt.Errorf("frame %d is not allocated", frameIdx)
	}
	if node.pinCounts <= 0 {
		return fmt.Errorf("frame %d is not pinned", frameIdx)
	}
	node.pinCounts--
	if node.pinCounts == 0 {
		_ = node.page.Header.ClearPinnedFlag()
	}
	if isDirty {
		node.dirty = true
	}
	return nil
}

func (this *LRUReplacer) GetPage(frameIdx int) (*page.Page, error) {
	node := this.frames[frameIdx]
	if node == nil {
		return nil, util.ErrPageIdExistedInBuffer
	}

	return node.page, nil
}

func (this *LRUReplacer) PutPage(frameIdx int, page *page.Page) error {
	if frameIdx >= this.poolSize || frameIdx < 0 {
		return fmt.Errorf("invalid frame index %d", frameIdx)
	}

	return this.addToTail(frameIdx, page)
}

func (lr *LRUReplacer) GetPinCount(frameIdx int) (int32, error) {
	if frameIdx >= lr.poolSize || frameIdx < 0 {
		return -1, fmt.Errorf("invalid frame index %d", frameIdx)
	}

	return lr.frames[frameIdx].pinCounts, nil
}

func (lr *LRUReplacer) Dirty(frameIdx int) error {
	if frameIdx >= lr.poolSize || frameIdx < 0 {
		return fmt.Errorf("invalid frame index %d", frameIdx)
	}

	node := lr.frames[frameIdx]
	if node == nil {
		return fmt.Errorf("frame %d is not allocated", frameIdx)
	}

	if !node.page.Header.IsDirty() {
		node.page.Header.SetDirtyFlag()
	}

	return nil
}

func (lr *LRUReplacer) IsDirty(frameIdx int) (bool, error) {
	if frameIdx >= lr.poolSize || frameIdx < 0 {
		return false, fmt.Errorf("invalid frame index %d", frameIdx)
	}
	node := lr.frames[frameIdx]

	return node.dirty, nil
}

func (this *LRUReplacer) ResetBuffer() {
	// Clear all frames
	for i := 0; i < this.Size(); i++ {
		this.frames[i] = nil
	}

	// Clear page mappings
	this.pageToIdx = make(map[util.PageID]int)
	// Reset free list
	this.freeHead = 0
	for i := 0; i < this.poolSize; i++ {
		this.nextFree[i] = i + 1
	}
	this.nextFree[this.poolSize-1] = -1
	// Reset LRU state
	this.lruHead = -1
	this.lruTail = -1
}

func (lr *LRUReplacer) addToTail(frameIdx int, page *page.Page) error {
	tmp := lr.lruTail
	lr.lruTail = frameIdx
	node := &LRUDesc{
		page:      page,
		prevIdx:   tmp,
		nextIdx:   -1,
		pinCounts: 0,
		dirty:     false,
	}
	if tmp != -1 {
		prevNode := lr.frames[tmp]
		if prevNode == nil {
			return util.ErrPageNotFound
		}
		prevNode.nextIdx = frameIdx
	}
	if lr.lruHead == -1 {
		lr.lruHead = frameIdx
	}
	lr.frames[frameIdx] = node

	return nil
}

func (lr *LRUReplacer) removeLRUByIndex(frameIdx int) error {
	if frameIdx >= lr.poolSize || frameIdx < 0 {
		return fmt.Errorf("invalid frame index %d", frameIdx)
	}
	node := lr.frames[frameIdx]
	if lr.lruHead == -1 || node == nil || (node.nextIdx == -1 && node.prevIdx == -1 && lr.lruHead != frameIdx) {
		return fmt.Errorf("invalid LRU state for frame %d", frameIdx)
	}
	prev := node.prevIdx
	next := node.nextIdx
	isHead := prev == -1
	isTail := next == -1

	switch {
	case isHead && isTail:
		// Only one node in the list
		lr.lruHead = -1
		lr.lruTail = -1
	case isHead && !isTail:
		// Removing head, next becomes new head
		lr.lruHead = next
		lr.frames[next].prevIdx = -1
	case !isHead && isTail:
		// Removing tail, prev becomes new tail
		lr.lruTail = prev
		lr.frames[prev].nextIdx = -1
	case !isHead && !isTail:
		// Removing middle node, connect prev and next
		lr.frames[prev].nextIdx = next
		lr.frames[next].prevIdx = prev
	}

	// Clear the removed node's links
	node.nextIdx = -1
	node.prevIdx = -1
	return nil
}

// allocFromFree allocates a free frame index.
func (lr *LRUReplacer) allocFromFree() int {
	if lr.freeHead == -1 {
		return -1
	}
	freeIdx := lr.freeHead
	lr.freeHead = lr.nextFree[freeIdx]
	lr.nextFree[freeIdx] = -1
	return freeIdx
}

// returnFrameToFree returns a frame to the free list.
func (lr *LRUReplacer) returnFrameToFree(frameIdx int) {
	lr.nextFree[frameIdx] = lr.freeHead
	lr.freeHead = frameIdx
}

func (lr *LRUReplacer) Evict() (int, error) {
	current := lr.lruHead
	for current != -1 {
		node := lr.frames[current]
		if atomic.LoadInt32(&node.pinCounts) == 0 {
			if err := lr.removeLRUByIndex(current); err != nil {
				return -1, err
			}
			// TODO: Flush the dirty page
			lr.removePageMapping(node.page.Header.PageID)
			return current, nil
		}
		current = node.nextIdx
	}
	return -1, errors.New("[Evict LRU] no evictable frame")
}
