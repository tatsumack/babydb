package main

import (
	"bytes"
	"encoding/binary"
	"unsafe"

	"golang.org/x/xerrors"
)

type SlotID uint16

type SlotHeader struct {
	SlotNum         uint16
	FreeSpaceOffset uint16
}

type Pointer struct {
	Offset uint16
	Len    uint16
}

func (p *Pointer) Range() (start, end uint16) {
	start = p.Offset
	end = start + p.Len
	return start, end
}

type Slot struct {
	header *SlotHeader
	Body   Page
}

var pointerSize int

func init() {
	pointerSize = int(unsafe.Sizeof(Pointer{}))
}

func NewSlot(page Page) *Slot {
	return &Slot{
		header: &SlotHeader{
			SlotNum:         0,
			FreeSpaceOffset: uint16(len(page)),
		},
		Body: page,
	}
}

func (s *Slot) Capacity() int {
	return len(s.Body)
}

func (s *Slot) SlotNum() uint16 {
	return s.header.SlotNum
}

func (s *Slot) PointerSize() uint16 {
	return s.SlotNum() * uint16(unsafe.Sizeof(Pointer{}))
}

func (s *Slot) FreeSpace() uint16 {
	return s.header.FreeSpaceOffset - s.PointerSize()
}

func (s *Slot) Data(pointer *Pointer) []byte {
	start, end := pointer.Range()
	return s.Body[start:end]
}

func (s *Slot) Pointers() ([]Pointer, error) {
	var pointers []Pointer
	for i := 0; i < int(s.SlotNum()); i++ {
		var pointer Pointer
		buf := bytes.NewBuffer(s.Body[i*pointerSize : (i+1)*pointerSize])
		if err := binary.Read(buf, binary.LittleEndian, &pointer); err != nil {
			return nil, xerrors.Errorf("failed to decode: %w", err)
		}
		//fmt.Printf("index: %d, offset: %d, len %d\n", i, pointer.Offset, pointer.Len)
		pointers = append(pointers, pointer)
	}
	return pointers, nil
}

func (s *Slot) Insert(index SlotID, newLen uint16) error {
	if s.FreeSpace() < uint16(pointerSize)+newLen {
		return xerrors.Errorf("no available space")
	}
	oldSlotNum := s.header.SlotNum
	s.header.FreeSpaceOffset -= newLen
	s.header.SlotNum++
	freeSpaceOffset := s.header.FreeSpaceOffset
	src := make([]byte, int(oldSlotNum)*pointerSize-int(index)*pointerSize)
	copy(src, s.Body[int(index)*pointerSize:int(oldSlotNum)*pointerSize])
	for i := 0; i < len(src); i++ {
		s.Body[int(index+1)*pointerSize+i] = src[i]
	}
	pointer := Pointer{
		Offset: freeSpaceOffset,
		Len:    newLen,
	}
	buf := bytes.NewBuffer(nil)
	if err := binary.Write(buf, binary.LittleEndian, &pointer); err != nil {
		return xerrors.Errorf("failed to encode: %w", err)
	}
	for i := 0; i < pointerSize; i++ {
		s.Body[int(index)*pointerSize+i] = buf.Bytes()[i]
	}
	return nil
}

func (s *Slot) Remove(index SlotID) error {
	if err := s.Resize(index, 0); err != nil {
		return xerrors.Errorf(": %w", err)
	}
	src := make([]byte, len(s.Body))
	copy(src, s.Body)
	for i := int(index) + 1; i < int(s.SlotNum()); i++ {
		for j := 0; j < pointerSize; j++ {
			s.Body[(i-1)*pointerSize+j] = src[i*pointerSize+j]
		}
	}
	s.header.SlotNum--
	return nil
}

func (s *Slot) Resize(index SlotID, newLen uint16) error {
	pointers, err := s.Pointers()
	if err != nil {
		return xerrors.Errorf("%w", err)
	}
	oldLen := pointers[index].Len
	diff := int(newLen) - int(oldLen)
	if diff == 0 {
		return nil
	}
	if diff > int(s.FreeSpace()) {
		return xerrors.New("no available space")
	}
	freeSpaceOffset := s.header.FreeSpaceOffset
	oldOffset := pointers[index].Offset
	newFreeSpaceOffset := uint16(int(freeSpaceOffset) - diff)
	s.header.FreeSpaceOffset = newFreeSpaceOffset

	src := make([]byte, oldOffset-freeSpaceOffset)
	copy(src, s.Body[freeSpaceOffset:oldOffset])
	for i := 0; i < int(oldOffset-freeSpaceOffset); i++ {
		s.Body[int(newFreeSpaceOffset)+i] = src[i]
	}
	for i, p := range pointers {
		if p.Offset > oldOffset {
			continue
		}
		p.Offset = uint16(int(p.Offset) - diff)
		buf := bytes.NewBuffer(nil)
		if err := binary.Write(buf, binary.LittleEndian, &p); err != nil {
			return xerrors.Errorf("failed to encode: %w", err)
		}
		for j := 0; j < pointerSize; j++ {
			s.Body[i*pointerSize+j] = buf.Bytes()[j]
		}
	}

	pointer := pointers[index]
	pointer.Len = newLen
	if newLen == 0 {
		pointer.Offset = newFreeSpaceOffset
	}
	buf := bytes.NewBuffer(nil)
	if err := binary.Write(buf, binary.LittleEndian, &pointer); err != nil {
		return xerrors.Errorf("failed to encode: %w", err)
	}
	for j := 0; j < pointerSize; j++ {
		s.Body[int(index)*pointerSize+j] = buf.Bytes()[j]
	}

	return nil
}

func (s *Slot) Fetch(index SlotID) ([]byte, error) {
	pointers, err := s.Pointers()
	if err != nil {
		return nil, xerrors.Errorf(": %w", err)
	}
	return s.Data(&pointers[index]), nil
}

func (s *Slot) Set(index SlotID, buf []byte) error {
	pointers, err := s.Pointers()
	if err != nil {
		return xerrors.Errorf(": %w", err)
	}
	start, _ := pointers[index].Range()
	for i := 0; i < len(buf); i++ {
		s.Body[int(start)+i] = buf[i]
	}
	return nil
}
