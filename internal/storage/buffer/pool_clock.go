package buffer

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/bietkhonhungvandi212/array-db/internal/storage/file"
	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

type ClockDesc struct {
	page       atomic.Pointer[page.Page]
	usageCount int32
	refCount   int32
	dirty      atomic.Bool

	muPin sync.Mutex
}

type ClockReplacer struct {
	frames []*ClockDesc // Holds page.Page (4KB)
	*ReplacerShared
	nextVictimIdx int32
	maxLoop       int

	muLookup sync.Mutex
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

		if refCount := atomic.LoadInt32(&desc.refCount); refCount != 0 {
			continue
		}

		if usageCount := atomic.LoadInt32(&desc.usageCount); usageCount > 0 {
			atomic.AddInt32(&desc.usageCount, -1)
			continue
		}

		this.muLookup.Lock()
		defer this.muLookup.Unlock()
		if frameIdx, exist := this.pageToIdx[page.Header.PageID]; exist {
			if err := this.Pin(frameIdx); err != nil {
				return err
			}
			return nil
		}

		frameIdx := int(victimIdx)
		// to avoid race condition, store refcount math.MinInt32 as sentinel
		if atomic.CompareAndSwapInt32(&desc.refCount, 0, math.MinInt32) {
			dirty := desc.dirty.Load()
			if dirty {
				page := desc.page.Load()
				if err := fm.WritePage(page); err != nil {
					atomic.StoreInt32(&desc.refCount, 0)
					return err
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
			atomic.StoreInt32(&desc.usageCount, 1)
			atomic.StoreInt32(&desc.refCount, 1)

			desc.muPin.Lock()
			desc.page.Load().Header.SetPinnedFlag()
			desc.muPin.Unlock()

			return nil
		}
	}
}

func (this *ClockReplacer) Pin(frameIdx int) error {
	node := this.frames[frameIdx]
	if nev := atomic.AddInt32(&node.refCount, 1) < 0; nev {
		return util.ErrPageEvicted
	}

	page := node.page.Load()
	if page == nil {
		return fmt.Errorf("frame %d is not allocated", frameIdx)
	}

	node.muPin.Lock()
	if !node.page.Load().Header.IsPinned() {
		node.page.Load().Header.SetPinnedFlag()
	}
	node.muPin.Unlock()
	// Successfully updated, now handle page header

	if current := atomic.LoadInt32(&node.usageCount); current < int32(this.maxLoop) {
		atomic.AddInt32(&node.usageCount, 1)
	}

	return nil
}

func (this *ClockReplacer) Unpin(pageId util.PageID, isDirty bool) error {
	this.muLookup.Lock()
	frameIdx, exist := this.pageToIdx[pageId]
	if !exist {
		this.muLookup.Unlock()
		return util.ErrPageNotFound
	}
	this.muLookup.Unlock()

	node := this.frames[frameIdx]
	page := node.page.Load()
	if page == nil {
		return fmt.Errorf("frame %d is not allocated", frameIdx)
	}

	// Handle dirty flag first (while still pinned)
	if isDirty {
		node.dirty.Store(true)
		node.muPin.Lock()
		page.Header.SetDirtyFlag()
		node.muPin.Unlock()
	}

	if current := atomic.LoadInt32(&node.refCount); current <= 0 {
		return fmt.Errorf("frame %d was not pinned", frameIdx)
	}

	if newCount := atomic.AddInt32(&node.refCount, -1); newCount == 0 {
		node.muPin.Lock()
		page.Header.ClearPinnedFlag()
		node.muPin.Unlock()
	}

	return nil
}

func (this *ClockReplacer) GetPinCount(frameIdx int) (int32, error) {
	if frameIdx >= this.poolSize || frameIdx < 0 {
		return 0, fmt.Errorf("invalid frame index %d", frameIdx)
	}

	return atomic.LoadInt32(&this.frames[frameIdx].refCount), nil
}

func (this *ClockReplacer) GetPage(pageId util.PageID) (*page.Page, error) {
	this.muLookup.Lock()
	defer this.muLookup.Unlock()
	frameIdx, exist := this.pageToIdx[pageId]
	if !exist {
		return nil, util.ErrPageNotFound
	}

	if err := this.Pin(frameIdx); err != nil {
		return nil, err
	}
	node := this.frames[frameIdx]
	page := node.page.Load()
	if page == nil {
		return nil, fmt.Errorf("page id %d not found", pageId)
	}

	return node.page.Load(), nil
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
