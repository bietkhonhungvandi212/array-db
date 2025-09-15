package buffer

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
)

type ClockDesc struct {
	page       *page.Page
	usageCount int32 //TODO: shoud have the max usage for efficient clock hand
	refCount   int32
	dirty      int32
}

type ClockReplacer struct {
	frames []*ClockDesc // Holds page.Page (4KB)
	*ReplacerShared
	nextVictimIdx int
	maxLoop       int
}

func (this *ClockReplacer) Init(size int, maxLoop int, replacerShared *ReplacerShared) {
	this.frames = make([]*ClockDesc, size)
	this.ReplacerShared = replacerShared
	this.nextVictimIdx = 0
	this.maxLoop = maxLoop

}

func (this *ClockReplacer) RequestFrame() (int, error) {
	var victimIdx int
	for range this.maxLoop * 2 {
		victimIdx = this.nextVictimIdx
		if this.frames[victimIdx] == nil {
			this.nextVictimIdx = (this.nextVictimIdx + 1) % this.poolSize
			return victimIdx, nil
		}

		desc := this.frames[victimIdx]
		if !desc.page.Header.IsPinned() {
			if desc.usageCount == 0 {
				this.nextVictimIdx = (this.nextVictimIdx + 1) % this.poolSize
				return victimIdx, nil
			} else {
				desc.usageCount--
			}
		}

		this.nextVictimIdx = (this.nextVictimIdx + 1) % this.poolSize
	}

	return -1, errors.New("can not find the victim with maxLoop")
}

func (this *ClockReplacer) Pin(frameIdx int) error {
	if frameIdx >= this.poolSize || frameIdx < 0 {
		return fmt.Errorf("invalid frame index %d", frameIdx)
	}

	node := this.frames[frameIdx]
	if node == nil {
		return fmt.Errorf("frame %d is not allocated", frameIdx)
	}

	newCount := atomic.AddInt32(&node.refCount, 1)
	if newCount == 1 {
		if node.page.Header.IsPinned() {
			return errors.New("the page have been pinned")
		}
		node.page.Header.SetPinnedFlag()
	}

	if atomic.LoadInt32(&node.usageCount) < int32(this.maxLoop) {
		atomic.AddInt32(&node.usageCount, 1)
	}
	return nil
}

func (this *ClockReplacer) Unpin(frameIdx int, isDirty bool) error {
	if frameIdx >= this.poolSize || frameIdx < 0 {
		return fmt.Errorf("invalid frame index %d", frameIdx)
	}

	node := this.frames[frameIdx]
	if node == nil {
		return fmt.Errorf("frame %d is not allocated", frameIdx)
	}

	newCount := atomic.AddInt32(&node.refCount, -1)
	if newCount == 0 {
		if !node.page.Header.IsPinned() {
			return errors.New("the page have been unpinned")
		}

		node.page.Header.ClearPinnedFlag()
	}

	return nil
}

func (this *ClockReplacer) GetPinCount(frameIdx int) (int32, error) {
	if frameIdx >= this.poolSize || frameIdx < 0 {
		return 0, fmt.Errorf("invalid frame index %d", frameIdx)
	}

	return this.frames[frameIdx].refCount, nil
}

func (this *ClockReplacer) Dirty(frameIdx int) error {
	if frameIdx >= this.poolSize || frameIdx < 0 {
		return fmt.Errorf("invalid frame index %d", frameIdx)
	}

	node := this.frames[frameIdx]
	if node == nil {
		return fmt.Errorf("frame %d is not allocated", frameIdx)
	}

	if atomic.LoadInt32(&node.dirty) == 0 {
		if node.page.Header.IsDirty() {
			return errors.New("the page have been dirty")
		}

		atomic.AddInt32(&node.dirty, 1)
		node.page.Header.SetDirtyFlag()
	}

	return nil
}

func (this *ClockReplacer) IsDirty(frameIdx int) (bool, error) {
	if frameIdx >= this.poolSize || frameIdx < 0 {
		return false, fmt.Errorf("invalid frame index %d", frameIdx)
	}
	node := this.frames[frameIdx]

	return atomic.LoadInt32(&node.dirty) != 0, nil
}

// GetPage(frameIdx int) (*page.Page, error)
// PutPage(frameIdx int, page *page.Page) error
// ResetFrameByIdx(frameIdx int) error
// Size() int
// ResetBuffer() // for testing purpose
