package buffer

import (
	"github.com/bietkhonhungvandi212/array-db/internal/storage/file"
	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

// Replacer defines the contract for page replacement policies.
type Replacer interface {
	// Request a frame for allocating and evict if needed. returns an evictable frame index, or error if none.
	RequestFree(page *page.Page, fm *file.FileManager) (int, error)
	Pin(page util.PageID) error
	Unpin(page util.PageID, isDirty bool) error
	GetPinCount(frameIdx int) (int32, error)
	GetPage(frameIdx int) (*page.Page, error)
	ResetBuffer() // for testing purpose
}
