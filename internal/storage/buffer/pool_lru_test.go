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
		fm, err := file.NewFileManager(path, util.PageSize)
		assert.NoError(t, err, "create FileManager")
		defer fm.Close()

		size := 100

		// Create shared state and LRU replacer
		shared := NewReplacerShared(size)
		replacer := &LRUReplacer{}
		replacer.Init(size, shared)

		// Create buffer pool with LRU replacer
		bp := NewBufferPool(size, fm, replacer, shared)

		// Test BufferPool structure
		assert.Equal(t, size, bp.poolSize, "pool size should be matched")
		assert.Equal(t, fm, bp.fm, "file manager should be matched")
		assert.NotNil(t, bp.replacer, "replacer")
		assert.NotNil(t, bp.rs, "replacer shared")

		// Test ReplacerShared state
		assert.Equal(t, size, len(replacer.nextFree), "nextFree length")
		assert.Equal(t, 0, replacer.freeHead, "freeHead should be at first index 0")
		assert.Equal(t, -1, replacer.lruHead, "lruHead should be -1")
		assert.Equal(t, -1, replacer.lruTail, "lruTail should be -1")
		assert.Equal(t, size, replacer.poolSize, "replacer pool size")

		// Test LRU replacer state
		assert.Equal(t, size, len(replacer.frames), "frames length")
		assert.Equal(t, size, replacer.Size(), "replacer size")

		// Free list: 0→1→...→size-1→-1
		idx := replacer.freeHead
		for i := 0; i < size; i++ {
			assert.Equal(t, i, idx, "free list at %d", i)
			idx = replacer.nextFree[idx]
		}
		assert.Equal(t, -1, idx, "free list end")

		// State consistency
		assert.Empty(t, shared.pageToIdx, "pageToIdx empty")
		for i := 0; i < size; i++ {
			assert.Nil(t, replacer.frames[i], "frame[%d] should be nil", i)
		}
	})

	t.Run("ZeroSize", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic for size=0")
			}
		}()
		shared := NewReplacerShared(0)
		replacer := &LRUReplacer{}
		replacer.Init(0, shared)
		NewBufferPool(0, nil, replacer, shared)
		t.Fatal("expected panic for size=0")
	})

	t.Run("SingleFrame", func(t *testing.T) {
		path, cleanup := util.CreateTempFile(t)
		defer cleanup()
		fm, err := file.NewFileManager(path, util.PageSize)
		assert.NoError(t, err)
		defer fm.Close()

		size := 1
		shared := NewReplacerShared(size)
		replacer := &LRUReplacer{}
		replacer.Init(size, shared)
		bp := NewBufferPool(size, fm, replacer, shared)

		assert.Equal(t, 0, replacer.freeHead, "freeHead")
		assert.Equal(t, -1, replacer.nextFree[0], "nextFree[0]")
		assert.Equal(t, -1, replacer.lruHead, "lruHead should be -1")
		assert.Equal(t, -1, replacer.lruTail, "lruTail should be -1")
		assert.Equal(t, size, bp.poolSize, "pool size")
	})
}

func TestAllocateFrame(t *testing.T) {
	path, cleanup := util.CreateTempFile(t)
	defer cleanup()
	fm, err := file.NewFileManager(path, 5)
	assert.NoError(t, err, "create FileManager")
	defer fm.Close()

	// Create shared state and LRU replacer
	shared := NewReplacerShared(3)
	replacer := &LRUReplacer{}
	replacer.Init(3, shared)
	bp := NewBufferPool(3, fm, replacer, shared)

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
		// Reset state
		replacer.ResetBuffer()

		// First allocation - cache miss
		page1, err := bp.AllocateFrame(0)
		assert.NoError(t, err, "first allocation")
		assert.NotNil(t, page1, "page should not be nil")
		assert.Equal(t, util.PageID(0), page1.Header.PageID, "correct page ID")

		// Verify page is in buffer
		frameIdx, exists := shared.pageToIdx[0]
		assert.True(t, exists, "page should be in pageToIdx")

		// Get page from replacer to verify it's stored correctly
		storedPage, err := replacer.GetPage(frameIdx)
		assert.NoError(t, err, "get page from replacer")
		assert.Equal(t, page1, storedPage, "page should be in replacer frames")

		// Second allocation - cache hit
		page1Again, err := bp.AllocateFrame(0)
		assert.NoError(t, err, "cache hit allocation")
		assert.Equal(t, page1, page1Again, "should return same page instance")

		// Verify buffer state unchanged
		assert.Equal(t, frameIdx, shared.pageToIdx[0], "frame index unchanged")
	})

	t.Run("AllocateFrame_FreeFrames", func(t *testing.T) {
		// Reset state
		replacer.ResetBuffer()

		// Allocate pages to fill buffer pool (size=3)
		for i := 0; i < 3; i++ {
			pageID := util.PageID(i)
			page, err := bp.AllocateFrame(pageID)
			assert.NoError(t, err, "allocate page %d", pageID)

			// Verify allocation
			assert.Equal(t, pageID, page.Header.PageID, "correct page ID %d", pageID)
			frameIdx, exists := bp.rs.pageToIdx[pageID]
			assert.True(t, exists, "page %d in pageToIdx", pageID)
			// Verify page is stored in replacer
			storedPage, err := replacer.GetPage(frameIdx)
			assert.NoError(t, err, "get page %d from replacer", pageID)
			assert.Equal(t, page, storedPage, "page %d in replacer frames", pageID)
		}

		// Verify buffer pool is full
		assert.Equal(t, 3, len(shared.pageToIdx), "buffer pool should be full")
		assert.Equal(t, -1, replacer.allocFromFree(), "no free frames left")
		assert.Equal(t, replacer.lruHead, 0, "lruHead should be at index 0")
		assert.Equal(t, replacer.lruTail, 2, "lruTail should be at index 2")
		assert.Equal(t, replacer.freeHead, -1, "freeHead should be empty and -1")
	})

	t.Run("AllocateFrame_EvictionRequired", func(t *testing.T) {
		// Reset state
		replacer.ResetBuffer()

		// Fill buffer pool and establish LRU order
		for i := util.PageID(0); i < 3; i++ {
			page, err := bp.AllocateFrame(i)
			assert.NoError(t, err, "allocate page %d", i)
			assert.NotNil(t, page, "page after allocate not nil %d", i)
		}

		// Verify LRU order: 0 (head) ↔ 1 ↔ 2 (tail)
		frameIdx0 := shared.pageToIdx[0]
		frameIdx2 := shared.pageToIdx[2]
		assert.Equal(t, frameIdx0, replacer.lruHead, "page 0 should be LRU head")
		assert.Equal(t, frameIdx2, replacer.lruTail, "page 2 should be LRU tail")

		// Allocate new page - should evict page 0 (LRU head)
		page3, err := bp.AllocateFrame(3)
		assert.NoError(t, err, "allocate page 3 with eviction")

		assert.NotNil(t, page3, "page 3 should not be nil")
		assert.Equal(t, util.PageID(3), page3.Header.PageID, "correct page ID")

		// Verify page 0 was evicted
		_, exists := shared.pageToIdx[0]
		assert.False(t, exists, "page 0 should be evicted")

		// Verify page 3 is in buffer
		frameIdx3, exists3 := shared.pageToIdx[3]
		assert.True(t, exists3, "page 3 should be in buffer")

		// Verify page 3 is stored in replacer
		storedPage3, err := replacer.GetPage(frameIdx3)
		assert.NoError(t, err, "get page 3 from replacer")
		assert.Equal(t, page3, storedPage3, "page 3 in replacer frames")
		assert.Equal(t, frameIdx3, replacer.lruTail, "lruTail should point to page 3")

		// Verify other pages still in buffer
		assert.Contains(t, shared.pageToIdx, util.PageID(1), "page 1 still in buffer")
		frameIdx1 := shared.pageToIdx[1]
		assert.Equal(t, frameIdx1, replacer.lruHead, "lruHead should point to page 1")
		assert.Contains(t, shared.pageToIdx, util.PageID(2), "page 2 still in buffer")
	})
	t.Run("AllocateFrame_DiskReadError", func(t *testing.T) {
		replacer.ResetBuffer()

		// Try to read non-existent page
		page, err := bp.AllocateFrame(999)
		assert.Error(t, err, "should error for non-existent page")
		assert.Nil(t, page, "page should be nil on error")

		// Verify page not in buffer
		_, exists := bp.rs.pageToIdx[999]
		assert.False(t, exists, "failed page should not be in buffer")

		// Verify free frame was returned
		assert.Equal(t, 0, replacer.freeHead, "free frame should be available")
		assert.Equal(t, -1, replacer.lruHead, "lruHead should be kept -1")
		assert.Equal(t, -1, replacer.lruTail, "lruTail should be kept -1")
	})
	t.Run("AllocateFrame_EvictionError", func(t *testing.T) {
		replacer.ResetBuffer()

		// Fill buffer pool
		for i := util.PageID(0); i < 3; i++ {
			page, err1 := bp.AllocateFrame(i)
			assert.NoError(t, err1, "allocate page %d", i)
			err2 := bp.PinFrame(bp.rs.pageToIdx[page.Header.PageID])

			// Pin after allocation
			assert.NoError(t, err2, "Pin after Allocation")
		}

		// Try to allocate new page - should fail (no evictable pages)
		page4th, err1 := bp.AllocateFrame(4)
		assert.Error(t, err1, "should error when no frames can be evicted because of full pin")
		assert.Nil(t, page4th, "page should be nil on eviction error")

		// Verify page 4 not in buffer
		_, exists := bp.rs.pageToIdx[4]
		assert.False(t, exists, "page 4 should not be in buffer")
	})
	t.Run("AllocateFrame_DataIntegrity", func(t *testing.T) {
		replacer.ResetBuffer()

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
		replacer.ResetBuffer()

		// Allocate a page
		_, err := bp.AllocateFrame(1)
		assert.NoError(t, err, "allocate page 1")
		frameIdx := bp.rs.pageToIdx[1]

		// Manually dirty the frame
		errPin1 := bp.PinFrame(frameIdx)
		assert.NoError(t, errPin1, "Pin for page 1 should not err")
		errDir1 := bp.MarkDirty(frameIdx)
		assert.NoError(t, errDir1, "Mark dirty for page 1 should not err")

		// Remove from buffer manually to test frame reset
		delete(bp.rs.pageToIdx, 1)
		replacer.returnFrameToFree(frameIdx)

		// Allocate different page to same frame
		page2, err := bp.AllocateFrame(2)
		assert.NoError(t, err, "allocate page 2")
		newFrameIdx := bp.rs.pageToIdx[2]

		// Verify frame was reset (should be same frame but clean state)
		assert.Equal(t, frameIdx, newFrameIdx, "should reuse same frame")
		count, errPin := bp.replacer.GetPinCount(newFrameIdx)
		if errPin != nil {
			assert.NoError(t, errPin1, "get pin count should not return error")
		}

		assert.Equal(t, int32(0), count, "pin count reset")
		dirty, errDir := bp.replacer.IsDirty(newFrameIdx)
		if errDir != nil {
			assert.NoError(t, errDir, "get dirty should not return error")
		}
		assert.False(t, dirty, "dirty flag reset")
		assert.False(t, page2.Header.IsPinned(), "page header pin flag reset")
		assert.False(t, page2.Header.IsDirty(), "page header dirty flag reset")
	})
}
