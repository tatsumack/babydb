package main

type NodeType uint8

const (
	NodeTypeLeaf NodeType = iota + 1
	NodeTypeBranch
)

type NodeHeader struct {
	NodeType NodeType
}

type Node struct {
	Header NodeHeader
	Body   Page
}

func NewNode(page Page) *Node {
	return &Node{
		Body: page,
	}
}

func (n *Node) InitializeAsLeaf() {
	n.Header.NodeType = NodeTypeLeaf
}

func (n *Node) InitializeAsBranch() {
	n.Header.NodeType = NodeTypeBranch
}
