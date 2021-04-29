package main

import "golang.org/x/xerrors"

type LeafHeader struct {
	PrevPageID PageID
	NextPageID PageID
}

type Leaf struct {
	Header *LeafHeader
	Body   *Slot
}

func NewLeaf(page Page) *Leaf {
	return &Leaf{
		Header: &LeafHeader{
			PrevPageID: PageIDInvalid,
			NextPageID: PageIDInvalid,
		},
		Body: NewSlot(page),
	}
}

func (l *Leaf) PrevPageID() PageID {
	return l.Header.PrevPageID
}

func (l *Leaf) SetPrevPageID(pageID PageID) {
	l.Header.PrevPageID = pageID
}

func (l *Leaf) NextPageID() PageID {
	return l.Header.NextPageID
}

func (l *Leaf) SetNextPageID(pageID PageID) {
	l.Header.NextPageID = pageID
}

func (l *Leaf) PairsNum() uint16 {
	return l.Body.SlotNum()
}

func (l *Leaf) SearchSlotID(key []byte) (SlotID, bool, error) {
	left := 0
	right := int(l.PairsNum())
	keyStr := string(key)
	for left < right {
		mid := (left + right) / 2
		p, err := l.PairsAt(SlotID(mid))
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

func (l *Leaf) PairsAt(slotID SlotID) (*Pair, error) {
	bytes, err := l.Body.Fetch(slotID)
	if err != nil {
		return nil, err
	}
	return PairFromBytes(bytes)
}

func (l *Leaf) MaxPairSize() uint16 {
	return uint16(l.Body.Capacity()/2 - pointerSize)
}

func (l *Leaf) Insert(slotID SlotID, key, value []byte) error {
	pair := Pair{
		Key:   key,
		Value: value,
	}
	buf, err := pair.ToBytes()
	if err != nil {
		return err
	}
	if err := l.Body.Insert(slotID, uint16(len(buf))); err != nil {
		return err
	}
	if err := l.Body.Set(slotID, buf); err != nil {
		return err
	}
	return nil
}

func (l *Leaf) IsHalfFull() bool {
	return 2*int(l.Body.FreeSpace()) < l.Body.Capacity()
}

func (l *Leaf) SplitInsert(newLeaf *Leaf, newKey, newValue []byte) ([]byte, error) {
	for {
		if newLeaf.IsHalfFull() {
			index, ok, err := l.SearchSlotID(newKey)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, xerrors.Errorf("key not found: %s", newKey)
			}
			if err := l.Insert(index, newKey, newValue); err != nil {
				return nil, err
			}
			break
		}
		pair, err := l.PairsAt(0)
		if err != nil {
			return nil, err
		}
		if string(pair.Key) <= string(newKey) {
			if err := l.Transfer(newLeaf); err != nil {
				return nil, err
			}
		} else {
			if err := newLeaf.Insert(SlotID(newLeaf.PairsNum()), newKey, newValue); err != nil {
				return nil, err
			}
			for !newLeaf.IsHalfFull() {
				if err := l.Transfer(newLeaf); err != nil {
					return nil, err
				}
			}
			break
		}
	}
	p, err := l.PairsAt(0)
	if err != nil {
		return nil, err
	}
	return p.Key, nil
}

func (l *Leaf) Transfer(dest *Leaf) error {
	nextIndex := dest.PairsNum()
	buf, err := l.Body.Fetch(0)
	if err != nil {
		return xerrors.Errorf(": %w", err)
	}
	if err := dest.Body.Insert(SlotID(nextIndex), uint16(len(buf))); err != nil {
		return xerrors.Errorf(": %w", err)
	}
	if err := dest.Body.Set(SlotID(nextIndex), buf); err != nil {
		return xerrors.Errorf(": %w", err)
	}
	if err := l.Body.Remove(0); err != nil {
		return xerrors.Errorf(": %w", err)
	}
	return nil
}
