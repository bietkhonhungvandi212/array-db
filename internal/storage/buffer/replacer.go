package buffer

import "github.com/bietkhonhungvandi212/array-db/internal/storage/page"

// Replacer defines the contract for page replacement policies.
type Replacer interface {
	// Request a frame for allocating and evict if needed. returns an evictable frame index, or error if none.
	RequestFree() (int, error)
	Pin(frameIdx int) error
	Unpin(frameIdx int, isDirty bool) error
	GetPinCount(frameIdx int) (int32, error)
	Dirty(frameIdx int) error
	IsDirty(frameIdx int) (bool, error)
	GetPage(frameIdx int) (*page.Page, error)
	PutPage(frameIdx int, page *page.Page) error
	ResetFrameByIdx(frameIdx int) error
	Size() int
	ResetBuffer() // for testing purpose
}
