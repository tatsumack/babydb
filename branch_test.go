package main

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBranch_Insert(t *testing.T) {
	t.Parallel()
	bytes := make([]byte, 500)
	result := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(result, 5)
	branch, err := NewBranch(bytes, result, PageID(1), PageID(2))
	assert.NoError(t, err)
	binary.PutVarint(result, 8)
	assert.NoError(t, branch.Insert(1, result, PageID(3)))
	binary.PutVarint(result, 11)
	assert.NoError(t, branch.Insert(2, result, PageID(4)))

	type test struct {
		input    int64
		expected int
	}
	tests := []test{
		{
			input:    1,
			expected: 1,
		},
		{
			input:    5,
			expected: 3,
		},
		{
			input:    6,
			expected: 3,
		},
		{
			input:    8,
			expected: 4,
		},
		{
			input:    10,
			expected: 4,
		},
		{
			input:    11,
			expected: 2,
		},
		{
			input:    12,
			expected: 2,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("input %d", tt.input), func(t *testing.T) {
			t.Parallel()
			binary.PutVarint(result, tt.input)
			p, err := branch.SearchChild(result)
			assert.NoError(t, err)
			assert.Equal(t, PageID(tt.expected), p)
		})
	}
}

func TestBranch_SplitInsert(t *testing.T) {
	t.Parallel()
	bytes := make([]byte, 200)
	result := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(result, 5)
	branch, err := NewBranch(bytes, result, PageID(1), PageID(2))
	assert.NoError(t, err)
	binary.PutVarint(result, 8)
	assert.NoError(t, branch.Insert(1, result, PageID(3)))
	binary.PutVarint(result, 11)
	assert.NoError(t, branch.Insert(2, result, PageID(4)))

	bytes2 := make([]byte, 200)
	branch2 := &Branch{
		Body: NewSlot(bytes2),
	}
	binary.PutVarint(result, 10)
	midKey, err := branch.SplitInsert(branch2, result, PageID(5))
	assert.NoError(t, err)
	assert.Equal(t, result, midKey)

	assert.Equal(t, uint16(2), branch.PairsNum())
	assert.Equal(t, uint16(1), branch2.PairsNum())

	type test struct {
		input    int64
		expected int
	}
	tests := []test{
		{
			input:    1,
			expected: 1,
		},
		{
			input:    5,
			expected: 3,
		},
		{
			input:    6,
			expected: 3,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("input %d", tt.input), func(t *testing.T) {
			t.Parallel()
			binary.PutVarint(result, tt.input)
			p, err := branch2.SearchChild(result)
			assert.NoError(t, err)
			assert.Equal(t, PageID(tt.expected), p)
		})
	}

	tests = []test{
		{
			input:    9,
			expected: 5,
		},
		{
			input:    10,
			expected: 4,
		},
		{
			input:    11,
			expected: 2,
		},
		{
			input:    12,
			expected: 2,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("input %d", tt.input), func(t *testing.T) {
			t.Parallel()
			binary.PutVarint(result, tt.input)
			p, err := branch.SearchChild(result)
			assert.NoError(t, err)
			assert.Equal(t, PageID(tt.expected), p)
		})
	}
}
