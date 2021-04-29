package main

import (
	"io"
	"os"

	"golang.org/x/xerrors"
)

type PageID uint64

const PageSize uint64 = 4096

type DiskManager struct {
	heapFile   *os.File
	nextPageID PageID
}

func NewDiskManager(filePath string) (*DiskManager, error) {
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &DiskManager{
		heapFile: f,
	}, nil
}

func (d *DiskManager) AllocatePage() PageID {
	d.nextPageID++
	return d.nextPageID
}

func (d *DiskManager) WritePage(pageID PageID, buf []byte) error {
	offset := int64(PageSize * uint64(pageID))
	if _, err := d.heapFile.Seek(offset, 0); err != nil {
		return xerrors.Errorf("%w", err)
	}
	if _, err := d.heapFile.WriteAt(buf, offset); err != nil {
		return xerrors.Errorf("%w", err)
	}
	return nil
}

func (d *DiskManager) ReadPage(pageID PageID, buf []byte) error {
	offset := int64(PageSize * uint64(pageID))
	if _, err := d.heapFile.Seek(offset, 0); err != nil {
		return xerrors.Errorf("%w", err)
	}
	if _, err := d.heapFile.ReadAt(buf, offset); err != nil && err != io.EOF {
		return xerrors.Errorf("%w", err)
	}
	return nil
}
