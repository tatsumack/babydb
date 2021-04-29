package main

import (
	"strconv"

	"golang.org/x/xerrors"
)

type BranchHeader struct {
	RightChild PageID
}

type Branch struct {
	Header BranchHeader
	Body   *Slot
}

func NewBranch(page Page, key []byte, leftChild PageID, rightChild PageID) (*Branch, error) {
	b := Branch{
		Header: BranchHeader{
			RightChild: rightChild,
		},
		Body: NewSlot(page),
	}
	if err := b.Insert(0, key, leftChild); err != nil {
		return nil, xerrors.Errorf(": %w", err)
	}
	return &b, nil
}

func (b *Branch) PairsNum() uint16 {
	return b.Body.SlotNum()
}

func (b *Branch) SearchSlotID(key []byte) (SlotID, bool, error) {
	left := 0
	right := int(b.PairsNum())
	keyStr := string(key)
	for left < right {
		mid := (left + right) / 2
		p, err := b.PairsAt(SlotID(mid))
		if err != nil {
			return 0, false, err
		}
		pKeyStr := string(p.Key)
		if pKeyStr < keyStr {
			left = mid + 1
		} else if pKeyStr > keyStr {
			right = mid
		} else {
			return SlotID(mid), true, nil
		}
	}
	return SlotID(left), false, nil
}

func (b *Branch) PairsAt(slotID SlotID) (*Pair, error) {
	bytes, err := b.Body.Fetch(slotID)
	if err != nil {
		return nil, err
	}
	return PairFromBytes(bytes)
}

func (b *Branch) SearchChild(key []byte) (PageID, error) {
	index, err := b.SearchChildIndex(key)
	if err != nil {
		return 0, xerrors.Errorf(": %w", err)
	}
	return b.ChildAt(index)
}

func (b *Branch) SearchChildIndex(key []byte) (SlotID, error) {
	slotID, ok, err := b.SearchSlotID(key)
	if err != nil {
		return 0, err
	}
	if ok {
		return SlotID(int(slotID) + 1), nil
	}
	return slotID, nil
}

func (b *Branch) ChildAt(childIndex SlotID) (PageID, error) {
	if uint16(childIndex) == b.PairsNum() {
		return b.Header.RightChild, nil
	}
	p, err := b.PairsAt(childIndex)
	if err != nil {
		return PageIDInvalid, xerrors.Errorf(": %w", err)
	}
	value, err := strconv.ParseUint(string(p.Value), 10, 16)
	if err != nil {
		return PageIDInvalid, xerrors.Errorf(": %w", err)
	}
	return PageID(value), nil
}

func (b *Branch) MaxPairSize() uint16 {
	return uint16(b.Body.Capacity()/2 - pointerSize)
}

func (b *Branch) FillRightChild() error {
	lastID := b.PairsNum() - 1
	p, err := b.PairsAt(SlotID(lastID))
	if err != nil {
		return xerrors.Errorf(": %w", err)
	}
	value, err := strconv.ParseUint(string(p.Value), 10, 16)
	if err != nil {
		return xerrors.Errorf(": %w", err)
	}
	rightChild := PageID(value)
	if err := b.Body.Remove(SlotID(lastID)); err != nil {
		return xerrors.Errorf(": %w", err)
	}
	b.Header.RightChild = rightChild
	return nil
}

func (b *Branch) Insert(slotID SlotID, key []byte, pageID PageID) error {
	p := Pair{
		Key:   key,
		Value: []byte(strconv.Itoa(int(pageID))),
	}
	bytes, err := p.ToBytes()
	if err != nil {
		return xerrors.Errorf(": %w", err)
	}
	if len(bytes) > int(b.MaxPairSize()) {
		return xerrors.New("over max pair size")
	}
	if err := b.Body.Insert(slotID, uint16(len(bytes))); err != nil {
		return xerrors.Errorf(": %w", err)
	}
	if err := b.Body.Set(slotID, bytes); err != nil {
		return xerrors.Errorf(": %w", err)
	}
	return nil
}

func (b *Branch) IsHalfFull() bool {
	return 2*int(b.Body.FreeSpace()) < b.Body.Capacity()
}

func (b *Branch) SplitInsert(newBranch *Branch, newKey []byte, newPageID PageID) ([]byte, error) {
	newKeyStr := string(newKey)
	for {
		if newBranch.IsHalfFull() {
			index, ok, err := b.SearchSlotID(newKey)
			if err != nil {
				return nil, xerrors.Errorf(": %w", err)
			}
			if ok {
				return nil, xerrors.Errorf("key must be unique: %s", string(newKey))
			}
			if err := b.Insert(index, newKey, newPageID); err != nil {
				return nil, xerrors.Errorf(": %w", err)
			}
			break
		}
		p, err := b.PairsAt(0)
		if err != nil {
			return nil, xerrors.Errorf(": %w", err)
		}
		if string(p.Key) < newKeyStr {
			if err := b.Transfer(newBranch); err != nil {
				return nil, xerrors.Errorf(": %w", err)
			}
		} else {
			if err := newBranch.Insert(SlotID(newBranch.PairsNum()), newKey, newPageID); err != nil {
				return nil, xerrors.Errorf(": %w", err)
			}
			for !newBranch.IsHalfFull() {
				if b.PairsNum() == 1 {
					break
				}
				if err := b.Transfer(newBranch); err != nil {
					return nil, xerrors.Errorf(": %w", err)
				}
			}
			break
		}
	}
	if err := newBranch.FillRightChild(); err != nil {
		return nil, xerrors.Errorf(": %w", err)
	}
	p, err := b.PairsAt(0)
	if err != nil {
		return nil, xerrors.Errorf(": %w", err)
	}
	return p.Key, nil
}

func (b *Branch) Transfer(dest *Branch) error {
	nextIndex := SlotID(dest.PairsNum())
	bytes, err := b.Body.Fetch(0)
	if err != nil {
		return xerrors.Errorf(": %w", err)
	}
	if err := dest.Body.Insert(nextIndex, uint16(len(bytes))); err != nil {
		return xerrors.Errorf(": %w", err)
	}
	if err := dest.Body.Set(nextIndex, bytes); err != nil {
		return xerrors.Errorf(": %w", err)
	}
	if err := b.Body.Remove(0); err != nil {
		return xerrors.Errorf(": %w", err)
	}
	return nil
}
