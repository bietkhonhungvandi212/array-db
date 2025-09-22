package page

import (
	"encoding/binary"
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
	_        uint16      // 2 bytes (padding)
}

// Serialize packs the page into a byte slice for writing
func (p *Page) Serialize() []byte {
	buf := make([]byte, util.PageSize)
	// Write header fields
	binary.LittleEndian.PutUint64(buf[0:8], uint64(p.Header.PageID))
	binary.LittleEndian.PutUint16(buf[12:14], p.Header.Flags)
	binary.LittleEndian.PutUint16(buf[14:16], 0) // Padding
	// Write data
	copy(buf[HEADER_SIZE:], p.Data[:])
	// Compute checksum over PageID + Flags + Data (excluding checksum field)
	h := crc32.NewIEEE()
	h.Write(buf[0:8])
	h.Write(buf[12:])
	p.Header.Checksum = h.Sum32()
	binary.LittleEndian.PutUint32(buf[8:12], p.Header.Checksum)
	return buf
}

// Deserialize unpacks from bytes, validates checksum
func Deserialize(data []byte) (*Page, error) {
	if len(data) != util.PageSize {
		return nil, util.ErrInvalidPageSize
	}

	// stored Checksum
	pageChecksum := binary.LittleEndian.Uint32(data[8:12])

	// calculated Checksum
	checksumByte := make([]byte, 0)
	checksumByte = append(checksumByte, data[0:8]...)
	checksumByte = append(checksumByte, data[12:]...)
	checksum := crc32.ChecksumIEEE(checksumByte)

	if checksum != pageChecksum {
		return nil, util.ErrChecksumMismatch
	}

	var page Page
	page.Header.PageID = util.PageID(binary.LittleEndian.Uint64(data[0:8]))
	page.Header.Checksum = checksum
	page.Header.Flags = binary.LittleEndian.Uint16(data[12:14])

	copy(page.Data[:], data[HEADER_SIZE:])

	return &page, nil
}

func (p *PageHeader) SetDirtyFlag() {
	p.Flags |= DIRTY_FLAG
}

func (p *PageHeader) ClearDirtyFlag() error {
	if p.Flags&DIRTY_FLAG == 0 {
		return util.ErrPageNotDirty
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

func (p *PageHeader) ClearPinnedFlag() {
	p.Flags &^= PINNED_FLAG
}

func (p *PageHeader) IsPinned() bool {
	return p.Flags&PINNED_FLAG != 0
}
