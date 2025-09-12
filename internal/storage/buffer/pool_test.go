package buffer

import (
	"fmt"
	"testing"

	"github.com/bietkhonhungvandi212/array-db/internal/storage/file"
	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
	"github.com/stretchr/testify/assert"
)

var initialPages int

func init() {
	initialPages = 1
}

func TestNewBufferPool(t *testing.T) {
	t.Run("ValidSize", func(t *testing.T) {
		path, cleanup := util.CreateTempFile(t)
		defer cleanup()
		fm, err := file.NewFileManager(path, 1)
		assert.NoError(t, err, "create FileManager")
		defer fm.Close()

		size := 100
		bp := NewBufferPool(size, fm)
		assert.Equal(t, size, len(bp.frames), "frames length")
		assert.Equal(t, size, len(bp.pinCounts), "pinCounts length")
		assert.Equal(t, size, len(bp.dirtyFlags), "dirtyFlags length")
		assert.Equal(t, size, len(bp.nextFree), "nextFree length")
		assert.Equal(t, size, len(bp.nextLRU), "nextLRU length")
		assert.Equal(t, size, len(bp.prevLRU), "prevLRU length")
		assert.Equal(t, 0, bp.freeHead, "freeHead")
		assert.Equal(t, -1, bp.lruHead, "lruHead")
		assert.Equal(t, -1, bp.lruTail, "lruTail")
		assert.Equal(t, fm, bp.fm, "Filer")

		// Free list: 0→1→...→size-1→-1
		idx := bp.freeHead
		for i := 0; i < size; i++ {
			assert.Equal(t, i, idx, "free list at %d", i)
			idx = bp.nextFree[idx]
		}
		assert.Equal(t, -1, idx, "free list end")

		// State consistency
		for i := 0; i < size; i++ {
			assert.Equal(t, int32(0), bp.pinCounts[i], "pinCounts[%d]", i)
			assert.False(t, bp.dirtyFlags[i], "dirtyFlags[%d]", i)
		}

		// pageToIdx empty
		assert.Empty(t, bp.pageToIdx, "pageToIdx should be empty")
	})

	t.Run("ZeroSize", func(t *testing.T) {
		defer func() { recover() }()
		NewBufferPool(0, nil)
		t.Fatal("expected panic for size=0")
	})
}

func TestAllocFromFree(t *testing.T) {
	path, cleanup := util.CreateTempFile(t)
	defer cleanup()
	fm, err := file.NewFileManager(path, 1)
	assert.NoError(t, err, "create FileManager")
	defer fm.Close()

	bp := NewBufferPool(4, fm)
	t.Run("AllocateAll", func(t *testing.T) {
		for i := 0; i < 4; i++ {
			idx := bp.allocFromFree()
			assert.Equal(t, i, idx, "alloc index")
			nextIdx := i + 1
			if nextIdx == 4 {
				nextIdx = -1 //
			}
			assert.Equal(t, nextIdx, bp.freeHead, "freeHead")
		}
		assert.Equal(t, -1, bp.allocFromFree(), "empty free list")
	})
}

// Helper function to reset LRU state
func resetLRU(bp *BufferPool) {
	bp.lruHead = -1
	bp.lruTail = -1
	for i := 0; i < bp.poolSize; i++ {
		bp.nextLRU[i] = -1
		bp.prevLRU[i] = -1
	}
}

func TestLRUOperations(t *testing.T) {
	path, cleanup := util.CreateTempFile(t)
	defer cleanup()
	fm, err := file.NewFileManager(path, 1)
	assert.NoError(t, err, "create FileManager")
	defer fm.Close()
	bp := NewBufferPool(4, fm)

	t.Run("AddToTail_Err", func(t *testing.T) {
		assert.Error(t, bp.addToTail(-1), "add to tail err")
	})
	t.Run("AddToTail_NoErr", func(t *testing.T) {
		resetLRU(bp) // Clean slate

		assert.NoError(t, bp.addToTail(0))
		assert.Equal(t, 0, bp.lruHead, "[0] lruHead")
		assert.Equal(t, 0, bp.lruTail, "[0] lruTail")
		assert.Equal(t, -1, bp.nextLRU[0], "[0] next lru -1")
		assert.Equal(t, -1, bp.prevLRU[0], "[0] prev lru -1")

		assert.NoError(t, bp.addToTail(1))
		assert.Equal(t, 0, bp.lruHead, "[1] lruHead")
		assert.Equal(t, 1, bp.lruTail, "[1] lruTail")
		assert.Equal(t, 1, bp.nextLRU[0], "nextLRU[0]")
		assert.Equal(t, -1, bp.prevLRU[0], "prevLRU[0]")
		assert.Equal(t, 0, bp.prevLRU[1], "prevLRU[1]")
		assert.Equal(t, -1, bp.nextLRU[1], "nextLRU[1]")
	})
	t.Run("RemoveFromLRU_Err", func(t *testing.T) {
		assert.Error(t, bp.removeLRUByIndex(5), "remove idx out bound")
		assert.Error(t, bp.removeLRUByIndex(-1), "remove negative idx")
	})
	t.Run("RemoveFromLRU", func(t *testing.T) {
		resetLRU(bp) // Clean slate

		// Setup LRU chain: 0 ↔ 1 ↔ 2
		assert.NoError(t, bp.addToTail(0), "add 0")
		assert.NoError(t, bp.addToTail(1), "add 1")
		assert.NoError(t, bp.addToTail(2), "add 2")

		// Verify initial setup
		assert.Equal(t, 0, bp.lruHead, "initial lruHead")
		assert.Equal(t, 2, bp.lruTail, "initial lruTail")

		// Remove middle node (1): 0 ↔ 2
		err := bp.removeLRUByIndex(1)
		assert.NoError(t, err, "remove middle")
		assert.Equal(t, 0, bp.lruHead, "lruHead after remove middle")
		assert.Equal(t, 2, bp.lruTail, "lruTail after remove middle")
		assert.Equal(t, 2, bp.nextLRU[0], "nextLRU[0]")
		assert.Equal(t, 0, bp.prevLRU[2], "prevLRU[2]")

		// Remove head (0): just 2
		assert.NoError(t, bp.removeLRUByIndex(0), "remove head")
		assert.Equal(t, 2, bp.lruHead, "lruHead after remove head")
		assert.Equal(t, 2, bp.lruTail, "lruTail after remove head")
		assert.Equal(t, -1, bp.nextLRU[2], "nextLRU[2] at single frame")
		assert.Equal(t, -1, bp.prevLRU[2], "prevLRU[2] at single frame")
	})
}

func TestAllocateFrame(t *testing.T) {
	path, cleanup := util.CreateTempFile(t)
	defer cleanup()
	fm, err := file.NewFileManager(path, 5)
	assert.NoError(t, err, "create FileManager")
	defer fm.Close()
	bp := NewBufferPool(3, fm)

	// Create test pages on disk first
	for i := util.PageID(0); i < 5; i++ {
		testPage := &page.Page{
			Header: page.PageHeader{PageID: i},
		}
		testData := fmt.Sprintf("Page %d test data", i)
		copy(testPage.Data[:], []byte(testData))
		assert.NoError(t, fm.WritePage(testPage), "write test page %d", i)
	}

	t.Run("AllocateFrame_CacheHit", func(t *testing.T) {
		resetLRU(bp)
		resetBufferPool(bp)

		// First allocation - cache miss
		page1, err := bp.AllocateFrame(0)
		assert.NoError(t, err, "first allocation")
		assert.NotNil(t, page1, "page should not be nil")
		assert.Equal(t, util.PageID(0), page1.Header.PageID, "correct page ID")

		// Verify page is in buffer
		frameIdx, exists := bp.pageToIdx[0]
		assert.True(t, exists, "page should be in pageToIdx")
		assert.Equal(t, page1, bp.frames[frameIdx], "page should be in frames")

		// Second allocation - cache hit
		page1Again, err := bp.AllocateFrame(0)
		assert.NoError(t, err, "cache hit allocation")
		assert.Equal(t, page1, page1Again, "should return same page instance")

		// Verify buffer state unchanged
		assert.Equal(t, frameIdx, bp.pageToIdx[0], "frame index unchanged")
	})

	t.Run("AllocateFrame_FreeFrames", func(t *testing.T) {
		resetLRU(bp)
		resetBufferPool(bp)

		// Allocate pages to fill buffer pool (size=3)
		for i := 0; i < 3; i++ {
			pageID := util.PageID(i)
			page, err := bp.AllocateFrame(pageID)
			assert.NoError(t, err, "allocate page %d", pageID)

			// Verify allocation
			assert.Equal(t, pageID, page.Header.PageID, "correct page ID %d", pageID)
			frameIdx, exists := bp.pageToIdx[pageID]
			assert.True(t, exists, "page %d in pageToIdx", pageID)
			assert.Equal(t, page, bp.frames[frameIdx], "page %d in frames", pageID)
		}

		// Verify buffer pool is full
		assert.Equal(t, 3, len(bp.pageToIdx), "buffer pool should be full")
		assert.Equal(t, -1, bp.allocFromFree(), "no free frames left")
	})

	t.Run("AllocateFrame_EvictionRequired", func(t *testing.T) {
		resetLRU(bp)
		resetBufferPool(bp)

		// Fill buffer pool and establish LRU order
		for i := util.PageID(0); i < 3; i++ {
			page, err := bp.AllocateFrame(i)

			assert.NoError(t, err, "allocate page %d", i)
			assert.NotNil(t, page, "page after allocate not nil %d", i)
		}

		// Verify LRU order: 0 (head) ↔ 1 ↔ 2 (tail)
		assert.Equal(t, bp.pageToIdx[0], bp.lruHead, "page 0 is LRU head")
		assert.Equal(t, bp.pageToIdx[2], bp.lruTail, "page 2 is LRU tail")

		// Allocate new page - should evict page 0 (LRU head)
		page3, err := bp.AllocateFrame(3)
		assert.NoError(t, err, "allocate page 3 with eviction")

		assert.NotNil(t, page3, "page 3 should not be nil")
		assert.Equal(t, util.PageID(3), page3.Header.PageID, "correct page ID")

		// Verify page 0 was evicted
		_, exists := bp.pageToIdx[0]
		assert.False(t, exists, "page 0 should be evicted")

		// Verify page 3 is in buffer
		frameIdx3, exists3 := bp.pageToIdx[3]
		assert.True(t, exists3, "page 3 should be in buffer")
		assert.Equal(t, page3, bp.frames[frameIdx3], "page 3 in frames")
		assert.Equal(t, frameIdx3, bp.lruTail, "lrutail should point to page 3")

		// Verify other pages still in buffer
		assert.Contains(t, bp.pageToIdx, util.PageID(1), "page 1 still in buffer")
		frameIdx1 := bp.pageToIdx[1]
		assert.Equal(t, frameIdx1, bp.lruHead, "lruHead should point to page 1")
		assert.Contains(t, bp.pageToIdx, util.PageID(2), "page 2 still in buffer")
	})
	t.Run("AllocateFrame_DiskReadError", func(t *testing.T) {
		resetLRU(bp)
		resetBufferPool(bp)

		// Try to read non-existent page
		page, err := bp.AllocateFrame(999)
		assert.Error(t, err, "should error for non-existent page")
		assert.Nil(t, page, "page should be nil on error")

		// Verify page not in buffer
		_, exists := bp.pageToIdx[999]
		assert.False(t, exists, "failed page should not be in buffer")

		// Verify free frame was returned
		assert.NotEqual(t, -1, bp.allocFromFree(), "free frame should be available")
	})
	t.Run("AllocateFrame_EvictionError", func(t *testing.T) {
		resetLRU(bp)
		resetBufferPool(bp)

		// Fill buffer pool
		for i := util.PageID(0); i < 3; i++ {
			page, err1 := bp.AllocateFrame(i)
			assert.NoError(t, err1, "allocate page %d", i)
			err2 := bp.PinFrame(page.Header.PageID)

			// Pin after allocation
			assert.NoError(t, err2, "Pin after Allocation")
		}

		// Try to allocate new page - should fail (no evictable pages)
		page4th, err1 := bp.AllocateFrame(4)
		assert.Error(t, err1, "should error when no frames can be evicted because of full pin")
		assert.Nil(t, page4th, "page should be nil on eviction error")

		// Verify page 4 not in buffer
		_, exists := bp.pageToIdx[4]
		assert.False(t, exists, "page 4 should not be in buffer")
	})
	t.Run("AllocateFrame_DataIntegrity", func(t *testing.T) {
		resetLRU(bp)
		resetBufferPool(bp)

		// Allocate page and verify data content
		page0, err := bp.AllocateFrame(0)
		assert.NoError(t, err, "allocate page 0")

		// Check data content matches what we wrote to disk
		expectedData := "Page 0 test data"
		actualData := string(page0.Data[:len(expectedData)])
		assert.Equal(t, expectedData, actualData, "page data should match disk content")

		// Allocate different page
		page1, err := bp.AllocateFrame(1)
		assert.NoError(t, err, "allocate page 1")

		expectedData1 := "Page 1 test data"
		actualData1 := string(page1.Data[:len(expectedData1)])
		assert.Equal(t, expectedData1, actualData1, "page 1 data should match disk content")

		// Verify pages are different instances
		assert.NotEqual(t, page0, page1, "different pages should be different instances")
	})
	t.Run("AllocateFrame_FrameReset", func(t *testing.T) {
		resetLRU(bp)
		resetBufferPool(bp)

		// Allocate a page
		_, err := bp.AllocateFrame(1)
		assert.NoError(t, err, "allocate page 1")
		frameIdx := bp.pageToIdx[1]

		// Manually dirty the frame
		errPin1 := bp.PinFrame(1)
		assert.NoError(t, errPin1, "Pin for page 1 should not err")
		errDir1 := bp.MarkDirty(1)
		assert.NoError(t, errDir1, "Mark dirty for page 1 should not err")

		// Remove from buffer manually to test frame reset
		delete(bp.pageToIdx, 1)
		bp.returnFrameToFree(frameIdx)

		// Allocate different page to same frame
		page2, err := bp.AllocateFrame(2)
		assert.NoError(t, err, "allocate page 2")
		newFrameIdx := bp.pageToIdx[2]

		// Verify frame was reset (should be same frame but clean state)
		assert.Equal(t, frameIdx, newFrameIdx, "should reuse same frame")
		assert.Equal(t, int32(0), bp.pinCounts[frameIdx], "pin count reset")
		assert.False(t, bp.dirtyFlags[frameIdx], "dirty flag reset")
		assert.False(t, page2.Header.IsPinned(), "page header pin flag reset")
		assert.False(t, page2.Header.IsDirty(), "page header dirty flag reset")
	})
}

// Helper function to reset buffer pool state
func resetBufferPool(bp *BufferPool) {
	// Clear pageToIdx mapping
	for k := range bp.pageToIdx {
		delete(bp.pageToIdx, k)
	}

	// Reset all frames to nil
	for i := range bp.frames {
		bp.frames[i] = nil
	}

	// Reset pin counts and dirty flags
	for i := range bp.pinCounts {
		bp.pinCounts[i] = 0
		bp.dirtyFlags[i] = false
	}

	// Reset free list
	bp.freeHead = 0
	for i := 0; i < bp.poolSize-1; i++ {
		bp.nextFree[i] = i + 1
	}
	bp.nextFree[bp.poolSize-1] = -1
}
