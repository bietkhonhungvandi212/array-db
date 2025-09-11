package page

import (
	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

func CreateTestPage(pageID util.PageID, data []byte) *Page {
	p := &Page{
		Header: PageHeader{
			PageID: pageID,
			Flags:  0,
		},
	}
	if len(data) > len(p.Data) {
		data = data[:len(p.Data)] // Truncate to fit
	}
	copy(p.Data[:], data)
	return p
}
