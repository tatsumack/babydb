package main

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufferPoolManager_CreatePage(t *testing.T) {
	tests := []struct {
		name    string
		want    *Buffer
		wantErr bool
	}{
		{
			name: "valid",
			want: &Buffer{
				PageID:     PageID(1),
				Page:       make([]byte, PageSize),
				IsDirty:    true,
				UsageCount: 1,
				mux:        sync.Mutex{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.CreateTemp(os.TempDir(), "babydb-disk")
			assert.NoError(t, err)
			defer func() { assert.NoError(t, os.Remove(f.Name())) }()

			b := bufferPoolManager(t, f, 1)

			got, err := b.CreatePage()
			if (err != nil) != tt.wantErr {
				t.Errorf("CreatePage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBufferPoolManager_Evict(t *testing.T) {
	t.Parallel()
	f, err := os.CreateTemp(os.TempDir(), "babydb-disk")
	assert.NoError(t, err)
	defer func() { assert.NoError(t, os.Remove(f.Name())) }()

	b1 := make([]byte, PageSize)
	copy(b1, "hello")

	b2 := make([]byte, PageSize)
	copy(b2, "world")

	b := bufferPoolManager(t, f, 1)

	buffer, err := b.CreatePage()
	assert.NoError(t, err)
	buffer.Page = b1
	buffer.IsDirty = true
	pageID1 := buffer.PageID
	buffer.Close()

	buffer, err = b.FetchPage(pageID1)
	assert.NoError(t, err)
	assert.Equal(t, Page(b1), buffer.Page)
	buffer.Close()

	buffer, err = b.CreatePage()
	assert.NoError(t, err)
	buffer.Page = b2
	buffer.IsDirty = true
	pageID2 := buffer.PageID
	buffer.Close()

	buffer, err = b.FetchPage(pageID2)
	assert.NoError(t, err)
	assert.Equal(t, Page(b2), buffer.Page)
	buffer.Close()

	buffer, err = b.FetchPage(pageID1)
	assert.NoError(t, err)
	assert.Equal(t, Page(b1), buffer.Page)
	buffer.Close()
}

func bufferPoolManager(t *testing.T, f *os.File, poolSize uint64) *BufferPoolManager {
	d, err := NewDiskManager(f.Name())
	assert.NoError(t, err)
	return NewBufferPoolManager(d, NewBufferPool(poolSize))
}
