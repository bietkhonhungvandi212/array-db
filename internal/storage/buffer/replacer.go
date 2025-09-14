package buffer

import "github.com/bietkhonhungvandi212/array-db/internal/storage/page"

// Replacer defines the contract for page replacement policies.
type Replacer interface {
	// Init initializes the replacer with shared state and additional policy-specific data.
	Init(size int, replacerShared *ReplacerShared)
	// Evict selects and returns an evictable frame index, or error if none.
	Evict() (int, error)
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
