package main

import (
	"sync"

	"golang.org/x/xerrors"
)

type BufferID int64

const BufferIDNone BufferID = -1

type Page []byte

type Buffer struct {
	PageID     PageID
	Page       Page
	IsDirty    bool
	UsageCount uint64
	mux        sync.Mutex
}

func (b *Buffer) Close() {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.UsageCount--
}

type BufferPool struct {
	buffers      []*Buffer
	nextVictimID BufferID
}

func NewBufferPool(poolSize uint64) *BufferPool {
	buffers := make([]*Buffer, poolSize)
	for i := uint64(0); i < poolSize; i++ {
		b := &Buffer{}
		buffers[i] = b
	}
	return &BufferPool{
		buffers:      buffers,
		nextVictimID: 0,
	}
}

func (b *BufferPool) Size() int {
	return len(b.buffers)
}

func (b *BufferPool) Evict() BufferID {
	for i := 0; i < b.Size(); i++ {
		victimID := b.nextVictimID
		buffer := b.FetchBuffer(victimID)
		if buffer.UsageCount == 0 {
			return victimID
		}
		b.incrementVictimID()
	}
	return BufferIDNone
}

func (b *BufferPool) FetchBuffer(bufferID BufferID) *Buffer {
	buffer := b.buffers[bufferID]
	return buffer
}

func (b *BufferPool) incrementVictimID() {
	b.nextVictimID = BufferID(int(b.nextVictimID+1) % b.Size())
}

type BufferPoolManager struct {
	disk      *DiskManager
	pool      *BufferPool
	pageTable map[PageID]BufferID
}

func NewBufferPoolManager(disk *DiskManager, pool *BufferPool) *BufferPoolManager {
	return &BufferPoolManager{
		disk:      disk,
		pool:      pool,
		pageTable: make(map[PageID]BufferID),
	}
}

func (b *BufferPoolManager) CreatePage() (*Buffer, error) {
	bufferID := b.pool.Evict()
	if bufferID == BufferIDNone {
		return nil, xerrors.Errorf("failed to evict buffer")
	}

	buffer := b.pool.FetchBuffer(bufferID)
	evictPageID := buffer.PageID
	if buffer.IsDirty {
		err := b.disk.WritePage(evictPageID, buffer.Page)
		if err != nil {
			return nil, xerrors.Errorf("failed to write page: %w", err)
		}
	}

	delete(b.pageTable, evictPageID)
	pageID := b.disk.AllocatePage()
	b.pageTable[pageID] = bufferID

	buffer.PageID = pageID
	buffer.UsageCount = 1
	buffer.IsDirty = true
	buffer.Page = make([]byte, PageSize)
	return buffer, nil
}

func (b *BufferPoolManager) FetchPage(pageID PageID) (*Buffer, error) {
	bufferID, ok := b.pageTable[pageID]
	if ok {
		buffer := b.pool.FetchBuffer(bufferID)
		buffer.UsageCount++
		return buffer, nil
	}

	bufferID = b.pool.Evict()
	if bufferID == BufferIDNone {
		return nil, xerrors.Errorf("failed to evict buffer")
	}

	buffer := b.pool.FetchBuffer(bufferID)
	evictPageID := buffer.PageID
	if buffer.IsDirty {
		err := b.disk.WritePage(evictPageID, buffer.Page)
		if err != nil {
			return nil, xerrors.Errorf("failed to write page: %w", err)
		}
	}

	buffer.PageID = pageID
	buffer.IsDirty = false
	err := b.disk.ReadPage(pageID, buffer.Page)
	if err != nil {
		return nil, xerrors.Errorf("failed to read page: %w", err)
	}

	delete(b.pageTable, evictPageID)
	b.pageTable[pageID] = bufferID

	return buffer, nil
}
