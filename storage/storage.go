package storage

import (
	"errors"
)

// Based on on a b-tree from here: https://www.cloudcentric.dev/implementing-a-b-tree-in-go/
// TODO need to implement delete method
// TODO Thread safety will be needed for cleaning out old data
/*
1. result == 0 if left and right are equal
1. -1 if left < right
1. +1 if left > right
*/
type keyCompareFunc func(left int, right int) int
type Item[T any] struct {
	Key  int
	Data T
}

type Storage[T any] struct {
	root   *btNode[T] //first node
	degree int        // range of keys per child
}

func New[T any]() Storage[T] {
	degree := 2
	return Storage[T]{
		root:   nil,
		degree: degree,
	}
}

func (s *Storage[T]) Insert(item Item[T], cmpr keyCompareFunc) error {
	if item.Key == 0 {
		// because in go defaults for int are 0, so we dont want to
		// return a default data struct since we re-use memory in some cases
		return errors.New("Unsupported use of key 0")
	}
	if s.root == nil {
		s.root = &btNode[T]{}
	}
	if s.root.numItems >= computeMaxItems(s.degree) {
		newRoot := &btNode[T]{} 
		midItem, newNode := s.root.splitChildren(s.degree)
		newRoot.insertItemAtPosition(midItem, 0, s.degree)
		newRoot.insertChildAtPosition(s.root, 0, s.degree)
		newRoot.insertChildAtPosition(newNode, 1, s.degree)
		s.root = newRoot
	}
	s.root.insert(s.degree, item, cmpr)
	return nil
}

func (s *Storage[T]) Search(targetKey int, cmpr keyCompareFunc) (Item[T], error) {
	if targetKey == 0 {
		// because in go defaults for int are 0, so we dont want to
		// return a default data struct since we re-use memory in some cases
		return Item[T]{}, errors.New("Unsupported use of key 0")
	}
	if s.root != nil {
		for n := s.root; n != nil; {
			pos, fnd := n.searchNode(targetKey, cmpr)
			if fnd == true {
				return n.items[pos], nil
			}
			n = n.children[pos]
		}
	}
	return Item[T]{}, errors.New("Not Found")
}

type stack[T any] []*btNode[T]

func (s stack[T]) push(node *btNode[T]) stack[T] {
	return append(s, node)
}

func (s stack[T]) pop(node *btNode[T]) (*btNode[T], stack[T]) {
	st := s[:len(s)-1]
	bt := s[len(s)-1]
	return bt, st
}

/*
DFS traverse
*/
type DoPerNode[T any] func(Item[T])

func (s *Storage[T]) Traverse(visitFn DoPerNode[T]) error {
	if s.root != nil {
		var visited stack[T]
		s.root.traverseNode(visitFn, visited)
	}
	return nil
}

const (
	degree = 3
)

type btNode[T any] struct {
	nodeId      int
	numItems    int
	items       [2*degree - 1]Item[T]
	children    [2 * degree]*btNode[T]
	numChildren int
}

func computeMaxItems(degree int) int {
	return 2*degree - 1
}

func computeMaxChildren(degree int) int {
	return 2 * degree
}

func computeMinItems(degree int) int {
	return degree - 1
}

// assumes leafs are always new nodes
func (n *btNode[T]) isLeaf() bool {
	return n.numChildren == 0
}

func (n *btNode[T]) incrementChildCount() {
	n.numChildren++
}

func (n *btNode[T]) incrementItemCount() {
	n.numItems++
}

func (n *btNode[T]) decrementItemCount() {
	n.numItems--
}

func (n *btNode[T]) decrementChildCount() {
	n.numChildren--
}

// return the total keys for current node
func (n *btNode[T]) getNumOfKeys() int {
	return len(n.items)
}

/*
1. Middle item gets moved to parent node
2. Take whats left after mid and move to a new btNode[T]
3. Link new child node to its parent node, in the middle position
*/
func (n *btNode[T]) splitChildren(degree int) (Item[T], *btNode[T]) {
	middleIndex := computeMinItems(degree)
	item := n.items[middleIndex]
	newNode := &btNode[T]{}
	copy(newNode.items[:], n.items[middleIndex+1:])
	newNode.numItems = computeMinItems(degree)

	if n.isLeaf() == false { // the chidlren need to go to the new parent node
		copy(newNode.children[:], n.children[middleIndex+1:])
		newNode.numChildren = computeMinItems(degree) + 1
	}

	// reset the n node
	for i, l := middleIndex, n.numItems; i < l; i++ {
		n.items[i] = Item[T]{} // return everything to default state!
		n.decrementItemCount()
		if n.isLeaf() == false {
			n.children[i+1] = nil
			n.decrementChildCount()
		}
	}
	return item, newNode
}

func (n *btNode[T]) insert(
	degree int,
	item Item[T],
	cmpr keyCompareFunc) bool {
	pos, found := n.searchNode(item.Key, cmpr)
	if found { // TODO if we find the item we may want to allow for an update rather than a replace
		n.items[pos] = item
		return false
	}

	if n.isLeaf() == true {
		n.insertItemAtPosition(item, pos, degree)
		return true
	}

	if n.children[pos].numItems >= computeMaxItems(degree) {
		midItem, newNode := n.children[pos].splitChildren(degree)
		n.insertItemAtPosition(midItem, pos, degree)
		n.insertChildAtPosition(newNode, pos+1, degree)
		switch cmp := cmpr(item.Key, n.items[pos].Key); {
		case cmp < 0: // we stay to the left
		case cmp > 0: // we move to the right
			pos++
		default: // mid position
			n.items[pos] = item
			return true
		}
	}
	return n.children[pos].insert(degree, item, cmpr)

}

func (n *btNode[T]) insertItemAtPosition(item Item[T], pos int, degree int) {
	if pos < n.numItems {
		copy(n.items[pos+1:n.numItems+1], n.items[pos:n.numItems])
	}
	n.items[pos] = item
	n.incrementItemCount()
}

func (n *btNode[T]) insertChildAtPosition(children *btNode[T], pos int, degree int) {
	if pos < n.numChildren {
		copy(n.children[pos+1:n.numChildren+1], n.children[pos:n.numChildren])
	}
	n.children[pos] = children
	n.incrementChildCount()
}

// search the current nodes children for our element
func (n *btNode[T]) searchNode(key int, cmpr keyCompareFunc) (int, bool) {
	low, high := 0, n.numItems
	var mid int
	for low < high {
		mid = (low + high) / 2
		cmp := cmpr(key, n.items[mid].Key)
		switch {
		case cmp > 0:
			low = mid + 1
		case cmp < 0:
			high = mid
		case cmp == 0:
			return mid, true
		}
	}
	return low, false
}

func (n *btNode[T]) traverseNode(visitFn DoPerNode[T], visited stack[T]) {
    visited.push(n) // TODO: we are not doing this currently, 
                    // we are just doing it as we see them and checking for children
    for _, item := range n.items {
        if item.Key != 0 { // because 0 is default and we are not using ptrs
            visitFn(item)
        }
    }
	for _, child := range n.children {
		if child != nil {
			child.traverseNode(visitFn, visited)
		}
	}
}
