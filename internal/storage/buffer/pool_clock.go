package buffer

import (
	"errors"
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
	this.nextVictimIdx = 0
	this.maxLoop = maxLoop

	for i := 0; i < size; i++ {
		this.frames[i] = &ClockDesc{
			usageCount: 0,
			refCount:   0,
			dirty:      atomic.Bool{},
		}
	}
}

func (this *ClockReplacer) RequestFree(page *page.Page, fm *file.FileManager) (int, error) {
	poolSize := int32(this.poolSize)
	for range poolSize * int32(this.maxLoop) {
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

		// to avoid race condition, store refcount -1 as sentinel
		this.muP.Lock()
		defer this.muP.Unlock()
		if idx, exist := this.pageToIdx[page.Header.PageID]; exist {
			return idx, nil
		}

		if atomic.CompareAndSwapInt32(&desc.refCount, 0, -1) {
			dirty := desc.dirty.Load()
			if dirty {
				page := desc.page.Load()
				desc.dirty.Store(false)
				if err := fm.WritePage(page); err != nil {
					desc.dirty.Store(true) // rollback
					return -1, err
				}
				frameIdx := int(victimIdx)
				this.pageToIdx[page.Header.PageID] = frameIdx
				if err := this.putPage(frameIdx, page); err != nil {
					return -1, fmt.Errorf("[AllocateFrame] Put page fail: %w", err)
				}
			}
		}
	}

	return -1, errors.New("can not find the victim with maxLoop")
}

func (this *ClockReplacer) Pin(pageId util.PageID) error {
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

	for {
		current := atomic.LoadInt32(&node.refCount)
		var newVal int32

		if current >= 0 {
			newVal = current + 1
		} else {
			return fmt.Errorf("invalid refCount state: %d", current)
		}

		if atomic.CompareAndSwapInt32(&node.refCount, current, newVal) {
			// Successfully updated, now handle page header
			if newVal == 1 {
				page.Header.SetPinnedFlag()
			}
			break
		}
		// CAS failed, retry
	}

	if current := atomic.LoadInt32(&node.usageCount); current < int32(this.maxLoop) {
		atomic.AddInt32(&node.usageCount, 1)
	}
	return nil
}

func (this *ClockReplacer) Unpin(frameIdx int, isDirty bool) error {
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
