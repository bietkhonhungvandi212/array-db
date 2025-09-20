package buffer

import (
	"fmt"
	"testing"

	"github.com/bietkhonhungvandi212/array-db/internal/storage/file"
	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
	"github.com/stretchr/testify/assert"
)

func TestNewBufferPoolClock(t *testing.T) {
	t.Run("ValidSize", func(t *testing.T) {
		path, cleanup := util.CreateTempFile(t)
		defer cleanup()
		fm, err := file.NewFileManager(path, 10)
		assert.NoError(t, err, "create FileManager")
		defer fm.Close()

		// Prepare
		size := 100
		maxLoop := 3
		shared := NewReplacerShared(size)
		replacer := &ClockReplacer{}
		replacer.Init(size, maxLoop, shared)

		// Create buffer pool with LRU replacer
		bp := NewBufferPool(fm, replacer, shared)

		// Test BufferPool structure
		assert.Equal(t, size, bp.rs.poolSize, "pool size should be matched")
		assert.Equal(t, fm, bp.fm, "file manager should be matched")
		assert.NotNil(t, bp.replacer, "replacer")
		assert.NotNil(t, bp.rs, "replacer shared")

		// Test clock replacer state
		assert.Equal(t, size, len(replacer.frames), "frames should be matched size")
		assert.Equal(t, maxLoop, replacer.maxLoop, "maxLoop should be matched")
		assert.Equal(t, int32(-1), replacer.nextVictimIdx, "nextVictimIdx")

		// frames
		for i := 0; i < size; i++ {
			assert.Equal(t, int32(0), replacer.frames[i].refCount, "refCount initialed with 0 at %d", i)
			assert.Equal(t, int32(0), replacer.frames[i].usageCount, "usageCount initialed with 0 at %d", i)
			assert.False(t, replacer.frames[i].dirty.Load(), "dirty initialed with false at %d", i)
		}

		// State consistency
		assert.Empty(t, shared.pageToIdx, "pageToIdx empty")
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
		NewBufferPool(nil, replacer, shared)
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
		bp := NewBufferPool(fm, replacer, shared)

		assert.Equal(t, 0, replacer.freeHead, "freeHead")
		assert.Equal(t, -1, replacer.nextFree[0], "nextFree[0]")
		assert.Equal(t, -1, replacer.lruHead, "lruHead should be -1")
		assert.Equal(t, -1, replacer.lruTail, "lruTail should be -1")
		assert.Equal(t, size, bp.rs.poolSize, "pool size")
	})
}

func TestAllocateFrameClock(t *testing.T) {
	path, cleanup := util.CreateTempFile(t)
	defer cleanup()
	fm, err := file.NewFileManager(path, 5)
	assert.NoError(t, err, "create FileManager")
	defer fm.Close()

	// Create shared state and LRU replacer
	size := 3
	maxLoop := 3
	shared := NewReplacerShared(size)
	replacer := &ClockReplacer{}
	replacer.Init(size, maxLoop, shared)

	bp := NewBufferPool(fm, replacer, shared)

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

	t.Run("AllocateFrame_FullBuffer", func(t *testing.T) {
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
			assert.Equal(t, int32(1), replacer.frames[frameIdx].refCount, "refCount %d in replacer frames should be 1", pageID)
			assert.Equal(t, int32(1), replacer.frames[frameIdx].usageCount, "usageCount %d in replacer frames should be 1", pageID)
			assert.False(t, replacer.frames[frameIdx].dirty.Load(), "dirty flag %d in replacer frames should be false", pageID)
		}

		// Verify buffer pool is full
		assert.Equal(t, int(3), len(shared.pageToIdx), "buffer pool should be full")
		assert.Equal(t, int32(2), replacer.nextVictimIdx, "victim current must be at index 3")
	})
	t.Run("AllocateFrame_EvictionRequired", func(t *testing.T) {
		// Reset state
		replacer.ResetBuffer()

		// Fill buffer pool
		for i := util.PageID(0); i < 3; i++ {
			page, err := bp.AllocateFrame(i)
			assert.NoError(t, err, "allocate page %d", i)
			assert.NotNil(t, page, "page after allocate not nil %d", i)

			// Test in one loop first
			err1 := bp.UnpinFrame(i, false)
			assert.NoError(t, err1, "page after allocating should be unpined without err")

		}

		// Allocate new page - should evict page 0 (current victim)
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

		// Verify other pages still in buffer
		assert.Contains(t, shared.pageToIdx, util.PageID(1), "page 1 still in buffer")
		assert.Contains(t, shared.pageToIdx, util.PageID(2), "page 2 still in buffer")
	})
	t.Run("AllocateFrame_VictimLoop", func(t *testing.T) {
		// Reset state
		replacer.ResetBuffer()

		// Fill buffer pool
		for i := util.PageID(0); i < 3; i++ {
			page, err := bp.AllocateFrame(i)
			assert.NoError(t, err, "allocate page %d", i)
			assert.NotNil(t, page, "page after allocate not nil %d", i)
			err2 := bp.UnpinFrame(i, false)
			assert.NoError(t, err2, "unpinned after allocate must not be err")
		}

		// Setup Pin page to increment usage count
		for i := 0; i < 3; i++ {
			idx := i % 3
			err1 := bp.PinFrame(util.PageID(idx))
			assert.NoError(t, err1, "pin should not err")
			err2 := bp.UnpinFrame(util.PageID(idx), false)
			assert.NoError(t, err2, "unpin should not err")
		}

		// Allocate new page - should evict page 0 (current victim)
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

		assert.Contains(t, shared.pageToIdx, util.PageID(1), "page 1 still in buffer")
		assert.Contains(t, shared.pageToIdx, util.PageID(2), "page 2 still in buffer")

		node1Idx := shared.pageToIdx[util.PageID(1)]
		assert.Equal(t, int32(0), replacer.frames[node1Idx].usageCount, "usageCount of page id 1 must be 0 after eviction")
		assert.Equal(t, int32(0), replacer.frames[node1Idx].refCount, "refCount of page id 1 must be 0 after eviction")
		node2Idx := shared.pageToIdx[util.PageID(2)]
		assert.Equal(t, int32(0), replacer.frames[node2Idx].usageCount, "usageCount of page id 2 must be 0 after eviction")
		assert.Equal(t, int32(0), replacer.frames[node2Idx].refCount, "refCount of page id 2 must be 0 after eviction")
	})
}
