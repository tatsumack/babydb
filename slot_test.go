package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlot_Insert(t *testing.T) {
	t.Parallel()
	page := make([]byte, 128)
	slot := NewSlot(page)
	insert := func(s *Slot, index SlotID, buf []byte) {
		assert.NoError(t, s.Insert(index, uint16(len(buf))))
		assert.NoError(t, s.Set(index, buf))
	}
	update := func(s *Slot, index SlotID, buf []byte) {
		assert.NoError(t, s.Resize(index, uint16(len(buf))))
		assert.NoError(t, s.Set(index, buf))
	}
	push := func(s *Slot, buf []byte) {
		index := s.SlotNum()
		insert(s, SlotID(index), buf)
	}
	push(slot, []byte("hello"))
	push(slot, []byte("world"))
	got, err := slot.Fetch(0)
	assert.NoError(t, err)
	assert.Equal(t, got, []byte("hello"))
	got, err = slot.Fetch(1)
	assert.NoError(t, err)
	assert.Equal(t, got, []byte("world"))

	insert(slot, 1, []byte(", "))
	push(slot, []byte("!"))
	got, err = slot.Fetch(0)
	assert.NoError(t, err)
	assert.Equal(t, got, []byte("hello"))
	got, err = slot.Fetch(1)
	assert.NoError(t, err)
	assert.Equal(t, got, []byte(", "))
	got, err = slot.Fetch(2)
	assert.NoError(t, err)
	assert.Equal(t, got, []byte("world"))
	got, err = slot.Fetch(3)
	assert.NoError(t, err)
	assert.Equal(t, got, []byte("!"))

	update(slot, 3, []byte("!!!!!!!"))
	got, err = slot.Fetch(3)
	assert.NoError(t, err)
	assert.Equal(t, got, []byte("!!!!!!!"))

	update(slot, 1, []byte("helloooo"))
	got, err = slot.Fetch(1)
	assert.NoError(t, err)
	assert.Equal(t, got, []byte("helloooo"))
}
