package page

import util "github.com/bietkhonhungvandi212/array-db/internal/utils"

const (
	HEADER_SIZE = 16 // Size of PageHeader struct: PageID(8) + Checksum(4) + Flags(2) + padding(2)
)

type Page struct {
	Header PageHeader
	Data   [util.PageSize - HEADER_SIZE]byte
}

type PageHeader struct {
	PageID   util.PageID
	Checksum uint32
	Flags    uint16
}
