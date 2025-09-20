package buffer

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/bietkhonhungvandi212/array-db/internal/storage/file"
	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

type ClockDesc struct {
	page       atomic.Pointer[page.Page]
	usageCount int32 // TODO: shoud have the max usage for efficient clock hand
	refCount   int32
	dirty      atomic.Bool
}

type ClockReplacer struct {
	frames []*ClockDesc // Holds page.Page (4KB)
	*ReplacerShared
	nextVictimIdx int32
	maxLoop       int

	muP sync.Mutex
}

func (this *ClockReplacer) Init(size int, maxLoop int, replacerShared *ReplacerShared) {
	this.frames = make([]*ClockDesc, size)
	this.ReplacerShared = replacerShared
	this.nextVictimIdx = -1
	this.maxLoop = maxLoop

	for i := 0; i < size; i++ {
		this.frames[i] = &ClockDesc{
			usageCount: 0,
			refCount:   0,
			dirty:      atomic.Bool{},
		}
	}
}

func (this *ClockReplacer) RequestFree(page *page.Page, fm *file.FileManager) error {
	poolSize := int32(this.poolSize)
	for {
		// Atomically advance clock hand and get current position
		victimIdx := atomic.AddInt32(&this.nextVictimIdx, 1) % poolSize

		desc := this.frames[victimIdx]
		refCount := atomic.LoadInt32(&desc.refCount)

		if refCount > 0 {
			continue
		}

		usageCount := atomic.LoadInt32(&desc.usageCount)
		if usageCount > 0 {
			atomic.AddInt32(&desc.usageCount, -1)
			continue
		}

		// this.muP.Lock()
		// defer this.muP.Unlock()
		if _, exist := this.pageToIdx[page.Header.PageID]; exist {
			return nil
		}

		frameIdx := int(victimIdx)
		// to avoid race condition, store refcount -1 as sentinel
		if atomic.CompareAndSwapInt32(&desc.refCount, 0, -1) {
			dirty := desc.dirty.Load()
			if dirty {
				page := desc.page.Load()
				if err := fm.WritePage(page); err != nil {
					return err
				}
				if err := this.putPage(frameIdx, page); err != nil {
					return fmt.Errorf("[AllocateFrame] Put page fail: %w", err)
				}

			}

			// Only delete from pageToIdx if there's actually a page in this frame
			existingPage := desc.page.Load()
			if existingPage != nil {
				delete(this.pageToIdx, existingPage.Header.PageID)
			}
			this.pageToIdx[page.Header.PageID] = frameIdx
			desc.page.Store(page)
			desc.dirty.Store(false)
			atomic.StoreInt32(&desc.usageCount, 0)
			atomic.StoreInt32(&desc.refCount, 0)

			return nil
		}
	}
}

func (this *ClockReplacer) Pin(pageId util.PageID) error {
	// this.muP.Lock()
	// defer this.muP.Unlock()
	frameIdx, exist := this.pageToIdx[pageId]
	if !exist {
		return util.ErrPageNotFound
	}

	node := this.frames[frameIdx]
	if atomic.LoadInt32(&node.refCount) < 0 {
		return util.ErrPageEvicted
	}

	page := node.page.Load()
	if page == nil {
		return fmt.Errorf("frame %d is not allocated", frameIdx)
	}

	// Verify the page ID still matches (could have been replaced)
	if page.Header.PageID != pageId {
		return util.ErrPageMissed
	}

	atomic.AddInt32(&node.refCount, 1)
	if !node.page.Load().Header.IsPinned() {
		node.page.Load().Header.SetPinnedFlag()
	}
	// Successfully updated, now handle page header

	if current := atomic.LoadInt32(&node.usageCount); current < int32(this.maxLoop) {
		atomic.AddInt32(&node.usageCount, 1)
	}

	return nil
}

func (this *ClockReplacer) Unpin(pageId util.PageID, isDirty bool) error {
	this.muP.Lock()
	defer this.muP.Unlock()
	frameIdx, exist := this.pageToIdx[pageId]
	if !exist {
		return util.ErrPageNotFound
	}

	if frameIdx >= this.poolSize || frameIdx < 0 {
		return fmt.Errorf("invalid frame index %d", frameIdx)
	}

	node := this.frames[frameIdx]
	page := node.page.Load()
	if page == nil {
		return fmt.Errorf("frame %d is not allocated", frameIdx)
	}

	// Handle dirty flag first (while still pinned)
	if isDirty {
		node.dirty.Store(true)
		page.Header.SetDirtyFlag()
	}

	// Atomically decrement reference count
	newCount := atomic.AddInt32(&node.refCount, -1)
	if newCount < 0 {
		// Restore count and return error - negative counts are invalid here
		atomic.AddInt32(&node.refCount, 1)
		return fmt.Errorf("frame %d was not pinned (refCount was %d)", frameIdx, newCount+1)
	}

	if newCount == 0 {
		_ = page.Header.ClearPinnedFlag()
	}

	return nil
}

func (this *ClockReplacer) GetPinCount(frameIdx int) (int32, error) {
	if frameIdx >= this.poolSize || frameIdx < 0 {
		return 0, fmt.Errorf("invalid frame index %d", frameIdx)
	}

	return atomic.LoadInt32(&this.frames[frameIdx].refCount), nil
}

func (this *ClockReplacer) GetPage(frameIdx int) (*page.Page, error) {
	node := this.frames[frameIdx]
	page := node.page.Load()
	if page == nil {
		return nil, util.ErrPageIdExistedInBuffer
	}

	return node.page.Load(), nil
}

func (this *ClockReplacer) putPage(frameIdx int, page *page.Page) error {
	if frameIdx >= this.poolSize || frameIdx < 0 {
		return fmt.Errorf("invalid frame index %d", frameIdx)
	}

	desc := &ClockDesc{
		usageCount: 0,
		refCount:   0,
		dirty:      atomic.Bool{},
	}
	desc.page.Store(page)
	this.frames[frameIdx] = desc

	return nil
}

func (this *ClockReplacer) ResetBuffer() {
	// Clear page mappings
	this.pageToIdx = make(map[util.PageID]int)

	// Reset clock hand position
	this.nextVictimIdx = -1

	// Reset all frames to initial state
	for i := 0; i < this.poolSize; i++ {
		this.frames[i] = &ClockDesc{
			usageCount: 0,
			refCount:   0,
			dirty:      atomic.Bool{},
		}
	}
}
