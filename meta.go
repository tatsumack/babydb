package main

type MetaHeader struct {
	RootPageID PageID
}

type Meta struct {
	Header MetaHeader
	page   Page
}

func NewMeta(page Page) *Meta {
	return &Meta{
		page: page,
	}
}
