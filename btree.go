package main

import (
	"bytes"
	"encoding/gob"

	"golang.org/x/xerrors"
)

type Pair struct {
	Key   []byte
	Value []byte
}

func PairFromBytes(src []byte) (*Pair, error) {
	var p Pair
	buf := bytes.NewBuffer(src)
	if err := gob.NewDecoder(buf).Decode(&p); err != nil {
		return nil, xerrors.Errorf("failed to decode: %w", err)
	}
	return &p, nil
}

func (p *Pair) ToBytes() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if err := gob.NewEncoder(buf).Encode(p); err != nil {
		return nil, xerrors.Errorf("failed to encode: %w", err)
	}
	return buf.Bytes(), nil
}
