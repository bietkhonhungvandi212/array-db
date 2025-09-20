package buffer

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
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
		replacer := &ClockReplacer{}
		maxLoop := 0
		replacer.Init(0, maxLoop, shared)
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
		replacer := &ClockReplacer{}
		maxLoop := 1
		replacer.Init(size, maxLoop, shared)
		bp := NewBufferPool(fm, replacer, shared)

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
		storedPage, err := replacer.GetPage(0)
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
			storedPage, err := replacer.GetPage(pageID)
			assert.NoError(t, err, "get page %d from replacer", pageID)
			assert.Equal(t, page, storedPage, "page %d in replacer frames", pageID)
			assert.Equal(t, int32(1), replacer.frames[frameIdx].refCount, "refCount %d in replacer frames should be 1", pageID)
			assert.Equal(t, int32(1), replacer.frames[frameIdx].usageCount, "usageCount %d in replacer frames should be 1", pageID)
			assert.False(t, replacer.frames[frameIdx].dirty.Load(), "dirty flag %d in replacer frames should be false", pageID)
		}

		// Verify buffer pool is full
		assert.Equal(t, int(3), len(shared.pageToIdx), "buffer pool should be full")
		assert.Equal(t, int32(2), replacer.nextVictimIdx, "victim current must be at index 2")
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
		_, exists3 := shared.pageToIdx[3]
		assert.True(t, exists3, "page 3 should be in buffer")

		// Verify page 3 is stored in replacer
		storedPage3, err := replacer.GetPage(3)
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
		_, exists3 := shared.pageToIdx[3]
		assert.True(t, exists3, "page 3 should be in buffer")

		// Verify page 3 is stored in replacer
		storedPage3, err := replacer.GetPage(3)
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

func TestBufferPoolClockConcurrency(t *testing.T) {
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

	t.Run("ClockBufferConcurrency_HitCache", func(t *testing.T) {
		// Reset state
		replacer.ResetBuffer()

		// Fill buffer pool with initial pages
		for i := util.PageID(0); i < 3; i++ {
			page, err := bp.AllocateFrame(i)
			assert.NoError(t, err, "allocate page %d", i)
			assert.NotNil(t, page, "page after allocate not nil %d", i)
			err2 := bp.UnpinFrame(i, false)
			assert.NoError(t, err2, "unpinned after allocate must not be err")
		}

		// Generate goroutine for hit cache concurrently
		numGoroutines := 10
		targetPageId := util.PageID(1)

		var wg sync.WaitGroup
		results := make([]*page.Page, numGoroutines)
		errors := make([]error, numGoroutines)
		for i := range numGoroutines {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				page, err := bp.AllocateFrame(targetPageId)
				results[index] = page
				errors[index] = err

				// Unpin after allocation to allow other goroutines to access
				if err == nil && page != nil {
					err1 := bp.UnpinFrame(targetPageId, false)
					assert.NoError(t, err1, "unpinned after allocation concurently should not throw error")
				}
			}(i)
		}

		wg.Wait()
		// Verify all goroutines succeeded
		var firstPage *page.Page
		for i := 0; i < numGoroutines; i++ {
			assert.NoError(t, errors[i], "goroutine %d should not have error", i)
			assert.NotNil(t, results[i], "goroutine %d should get a valid page", i)

			if firstPage == nil {
				firstPage = results[i]
			} else {
				// All goroutines should get the same page instance (cache hit)
				assert.Equal(t, firstPage, results[i], "goroutine %d should get same page instance", i)
			}

			// Verify correct page ID
			assert.Equal(t, targetPageId, results[i].Header.PageID, "goroutine %d should get correct page ID", i)
		}

		// Verify the page is still in the buffer pool
		_, exists := shared.pageToIdx[targetPageId]
		assert.True(t, exists, "page should still be in buffer pool")

		// Verify frame state
		storedPage, err := replacer.GetPage(targetPageId)
		assert.NoError(t, err, "should be able to get page from replacer")
		assert.Equal(t, firstPage, storedPage, "stored page should match")
	})
}

func TestBufferPoolClockConcurrencyAllocation(t *testing.T) {
	path, cleanup := util.CreateTempFile(t)
	defer cleanup()
	numOfPages := 20
	fm, err := file.NewFileManager(path, numOfPages)
	assert.NoError(t, err, "create FileManager")
	defer fm.Close()

	// Create shared state and LRU replacer
	size := numOfPages / 2
	maxLoop := 3
	shared := NewReplacerShared(size)
	replacer := &ClockReplacer{}
	replacer.Init(size, maxLoop, shared)

	bp := NewBufferPool(fm, replacer, shared)

	// Create test pages on disk first
	for i := util.PageID(0); i < util.PageID(numOfPages); i++ {
		testPage := &page.Page{
			Header: page.PageHeader{PageID: i},
		}
		testData := fmt.Sprintf("Page %d test data", i)
		copy(testPage.Data[:], []byte(testData))
		assert.NoError(t, fm.WritePage(testPage), "write test page %d", i)
	}

	t.Run("ClockBufferConcurrency_RequestFree", func(t *testing.T) {
		// Reset state
		replacer.ResetBuffer()

		// Fill concurrently buffer pool
		var wg sync.WaitGroup
		for i := 0; i < size; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				pageId := util.PageID(index)
				page, err := bp.AllocateFrame(pageId)
				assert.NoError(t, err, "allocate page %d", i)
				assert.NotNil(t, page, "page after allocate not nil %d", i)
				err2 := bp.UnpinFrame(pageId, false)
				assert.NoError(t, err2, "unpinned after allocate must not be err")
			}(i)
		}

		wg.Wait()

		// Verify len of buffer
		assert.Equal(t, size, len(shared.pageToIdx), "buffer should be full with len %d", size)
		// Verify the allocation properly
		for i := range size {
			pageId := util.PageID(i)
			assert.Contains(t, shared.pageToIdx, pageId, "page id %d should be in the buffer", pageId)
			frameIdx := shared.pageToIdx[pageId]
			node := replacer.frames[frameIdx]
			assert.Equal(t, int32(0), node.refCount, "refCount at index %d after allocate must be 0", i)
			assert.Equal(t, int32(1), node.usageCount, "usageCount index %d after allocate must be 1", i)
			assert.NotNil(t, node.page.Load(), "the page at node index %d should not be nil", i)
			assert.Equal(t, pageId, node.page.Load().Header.PageID, "the page at node index %d should not be nil", i)
		}

		assert.Equal(t, int32(9), replacer.nextVictimIdx, "victim current must be at index 9")
	})

	t.Run("ClockBufferConcurrency_EvictionAndClockLogic", func(t *testing.T) {
		// Reset state
		replacer.ResetBuffer()

		// Fill buffer pool to capacity (10 pages in buffer of size 10)
		var wg sync.WaitGroup
		for i := 0; i < size; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				pageId := util.PageID(index)
				page, err := bp.AllocateFrame(pageId)
				assert.NoError(t, err, "allocate page %d", index)
				assert.NotNil(t, page, "page after allocate not nil %d", index)
				err2 := bp.UnpinFrame(pageId, false)
				assert.NoError(t, err2, "unpinned after allocate must not be err")
			}(i)
		}
		wg.Wait()

		// Verify buffer is full
		assert.Equal(t, size, len(shared.pageToIdx), "buffer should be full")

		// Now pin and unpin some pages to increase their usage count
		// This tests the clock algorithm's usage count behavior
		pinnedPages := map[util.PageID]bool{}
		var wgAccess sync.WaitGroup
		var muPinnedPages sync.Mutex
		for range 10 {
			wgAccess.Go(func() {
				pageId := util.PageID(rand.Intn(10))
				muPinnedPages.Lock()
				pinnedPages[pageId] = true
				muPinnedPages.Unlock()

				err := bp.PinFrame(pageId)
				assert.NoError(t, err, "pin page %d", pageId)
				err = bp.UnpinFrame(pageId, false)
				assert.NoError(t, err, "unpin page %d", pageId)
			})
		}

		wgAccess.Wait()

		// Verify usage counts increased for pinned pages
		for pageId := range pinnedPages {
			frameIdx := shared.pageToIdx[pageId]
			node := replacer.frames[frameIdx]
			usageCount := atomic.LoadInt32(&node.usageCount)
			assert.Greater(t, usageCount, int32(1), "page %d should have higher usage count", pageId)
		}

		// Now try to allocate new pages concurrently (should trigger eviction)
		// This tests concurrent eviction behavior
		var evictionWg sync.WaitGroup
		newPages := map[util.PageID]bool{}
		var muNewPages sync.Mutex
		for range 20 {
			evictionWg.Go(func() {
				pageId := util.PageID(rand.Intn(10) + 10)
				muNewPages.Lock()
				newPages[pageId] = true
				muNewPages.Unlock()
				page, err := bp.AllocateFrame(pageId)
				assert.NoError(t, err, "allocate new page %d should succeed", pageId)
				assert.NotNil(t, page, "new page %d should not be nil", pageId)
				assert.Equal(t, pageId, page.Header.PageID, "correct page ID")
				err2 := bp.UnpinFrame(pageId, false)
				assert.NoError(t, err2, "unpin new page %d", pageId)
			})
		}
		evictionWg.Wait()

		// Verify buffer is still at capacity
		assert.Equal(t, size, len(shared.pageToIdx), "buffer should still be full after eviction")

		// Verify new pages are in buffer
		for newPageId := range newPages {
			assert.Contains(t, shared.pageToIdx, newPageId, "new page %d should be in buffer", newPageId)
		}

		// Verify clock hand advanced
		assert.Greater(t, replacer.nextVictimIdx, int32(9), "clock hand should have advanced during eviction")
	})
}
