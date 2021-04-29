package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLeaf_Insert(t *testing.T) {
	t.Parallel()

	page := make([]byte, 300)
	leaf := NewLeaf(page)

	id, _, err := leaf.SearchSlotID([]byte("deadbeef"))
	assert.NoError(t, err)
	assert.Equal(t, SlotID(0), id)

	assert.NoError(t, leaf.Insert(id, []byte("deadbeef"), []byte("world")))
	pair, err := leaf.PairsAt(0)
	assert.NoError(t, err)
	assert.Equal(t, []byte("deadbeef"), pair.Key)

	id, _, err = leaf.SearchSlotID([]byte("facebook"))
	assert.NoError(t, err)
	assert.Equal(t, SlotID(1), id)
	assert.NoError(t, leaf.Insert(id, []byte("facebook"), []byte("!")))

	pair, err = leaf.PairsAt(0)
	assert.NoError(t, err)
	assert.Equal(t, []byte("deadbeef"), pair.Key)
	pair, err = leaf.PairsAt(1)
	assert.NoError(t, err)
	assert.Equal(t, []byte("facebook"), pair.Key)

	id, _, err = leaf.SearchSlotID([]byte("beefdead"))
	assert.NoError(t, err)
	assert.Equal(t, SlotID(0), id)
	assert.NoError(t, leaf.Insert(id, []byte("beefdead"), []byte("hello")))

	pair, err = leaf.PairsAt(0)
	assert.NoError(t, err)
	assert.Equal(t, []byte("beefdead"), pair.Key)
	pair, err = leaf.PairsAt(1)
	assert.NoError(t, err)
	assert.Equal(t, []byte("deadbeef"), pair.Key)
	pair, err = leaf.PairsAt(2)
	assert.NoError(t, err)
	assert.Equal(t, []byte("facebook"), pair.Key)
}

func TestLeaf_SplitInsert(t *testing.T) {
	t.Parallel()

	page := make([]byte, 300)
	leaf := NewLeaf(page)

	id, _, err := leaf.SearchSlotID([]byte("deadbeef"))
	assert.NoError(t, err)
	assert.NoError(t, leaf.Insert(id, []byte("deadbeef"), []byte("world")))
	id, _, err = leaf.SearchSlotID([]byte("facebook"))
	assert.NoError(t, err)
	assert.NoError(t, leaf.Insert(id, []byte("facebook"), []byte("!")))
	id, _, err = leaf.SearchSlotID([]byte("beefdead"))
	assert.NoError(t, err)
	assert.NoError(t, leaf.Insert(id, []byte("beefdead"), []byte("hello")))

	newPage := make([]byte, 300)
	newLeaf := NewLeaf(newPage)
	_, err = leaf.SplitInsert(newLeaf, []byte("beefdead"), []byte("hello"))
	assert.NoError(t, err)

}
