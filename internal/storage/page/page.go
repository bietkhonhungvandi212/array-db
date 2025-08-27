package page

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	util "github.com/bietkhonhungvandi212/array-db/internal/utils"
)

const (
	HEADER_SIZE = 16 // Size of PageHeader struct: PageID(8) + Checksum(4) + Flags(2) + padding(2)
	DIRTY_FLAG  = 1 << 0
	PINNED_FLAG = 1 << 1
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
	binary.LittleEndian.PutUint16(buf[12:14], uint16(p.Header.Flags))

	// Copy data
	copy(buf[HEADER_SIZE:], p.Data[:])

	// Calculate checksum over PageID + Flags + Data (excluding checksum field)
	checkSumByte := make([]byte, 0)
	checkSumByte = append(checkSumByte, buf[0:8]...)
	checkSumByte = append(checkSumByte, buf[12:]...)

	checksum := crc32.ChecksumIEEE(checkSumByte)
	binary.LittleEndian.PutUint32(buf[8:12], checksum)

	return buf
}

// Deserialize unpacks from bytes, validates checksum
func Deserialize(data []byte) (*Page, error) {
	return nil, nil
}

func (p *PageHeader) SetDirtyFlag() {
	p.Flags |= DIRTY_FLAG
}

func (p *PageHeader) ClearDirtyFlag() error {
	if p.Flags&DIRTY_FLAG == 0 {
		return errors.New("page is not dirty")
	}
	p.Flags &^= DIRTY_FLAG
	return nil
}

func (p *PageHeader) IsDirty() bool {
	return p.Flags&DIRTY_FLAG != 0
}

func (p *PageHeader) SetPinnedFlag() {
	p.Flags |= PINNED_FLAG
}

func (p *PageHeader) ClearPinnedFlag() error {
	if p.Flags&PINNED_FLAG == 0 {
		return errors.New("page is not pinned")
	}
	p.Flags &^= PINNED_FLAG
	return nil
}

func (p *PageHeader) IsPinned() bool {
	return p.Flags&PINNED_FLAG != 0
}
