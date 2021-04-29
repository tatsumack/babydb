package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDiskManager_AllocatePage(t *testing.T) {
	t.Parallel()
	type fields struct {
		nextPageID PageID
	}
	tests := []struct {
		name   string
		fields fields
		want   PageID
	}{
		{
			name:   "valid",
			fields: fields{nextPageID: 0},
			want:   1,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f, err := os.CreateTemp(os.TempDir(), "babydb-disk")
			assert.NoError(t, err)
			defer func() { assert.NoError(t, os.Remove(f.Name())) }()

			d, err := NewDiskManager(f.Name())
			assert.NoError(t, err)

			got := d.AllocatePage()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDiskManager_WriteAndReadPage(t *testing.T) {
	t.Parallel()
	type args struct {
		pageID PageID
		bytes  []byte
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "valid",
			args: args{pageID: PageID(0), bytes: []byte("This is a test.")},
			want: []byte("This is a test."),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f, err := os.CreateTemp(os.TempDir(), "babydb-disk")
			fmt.Println(f.Name())
			assert.NoError(t, err)
			defer func() { assert.NoError(t, os.Remove(f.Name())) }()

			d, err := NewDiskManager(f.Name())
			assert.NoError(t, err)

			err = d.WritePage(tt.args.pageID, tt.args.bytes)
			assert.NoError(t, err)

			buf := make([]byte, len(tt.args.bytes))
			err = d.ReadPage(tt.args.pageID, buf)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, buf)
		})
	}
}
