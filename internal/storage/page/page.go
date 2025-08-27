package page

import (
	"encoding/binary"

	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

const (
	HEADER_SIZE = 16 // Size of PageHeader struct: PageID(8) + Checksum(4) + Flags(2) + padding(2)
)

// Page is block that read/write from disk
type Page struct {
	Header PageHeader
	Data   [util.PageSize - HEADER_SIZE]byte
}

type PageHeader struct {
	PageID   util.PageID // 8 bytes
	Checksum uint32      // 4 bytes
	Flags    uint16      // 2 bytes
	_        uint16      //2 bytes (padding)
}

// Serialize packs the page into a byte slice for writing
func (p *Page) Serialize() []byte {
	buf := make([]byte, util.PageSize)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(p.Header.PageID))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(p.Header.Checksum))
	binary.LittleEndian.PutUint16(buf[12:14], uint16(p.Header.Flags))

	copy(buf[HEADER_SIZE:], p.Data[:])

	//TODO: Calculate checksum

	return nil
}

// Deserialize unpacks from bytes, validates checksum
func Deserialize(data []byte) (*Page, error) {
	return nil, nil
}
