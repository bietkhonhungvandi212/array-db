package main

import (
	"fmt"

	"github.com/bietkhonhungvandi212/array-db/internal/storage/page"
)

func main() {
	// Create a page
	p := &page.Page{Header: page.PageHeader{PageID: 1}}
	copy(p.Data[:10], []byte("test data"))

	// Set flags
	p.Header.SetDirtyFlag()
	p.Header.SetPinnedFlag()

	// Serialize
	data := p.Serialize()
	fmt.Printf("Data as string: %q\n", string(data[16:26]))

	fmt.Printf("Serialized page: %d bytes, PageID=%d, Dirty=%v, Pinned=%v\n",
		len(data), p.Header.PageID, p.Header.IsDirty(), p.Header.IsPinned())
}
