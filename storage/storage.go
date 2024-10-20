package storage

import (
	"errors"
	"fmt"
	"sync"
)

/*
   Credit to this site for the b-tree implementation:
   https://www.cloudcentric.dev/implementing-a-b-tree-in-go/
*/
// TODO need to implement delete method
// TODO Thread safety will be needed for cleaning out old data
/*
   1. result == 0 if left and right are equal
   1. -1 if left < right
   1. +1 if left > right
*/

type Storage[T any] struct {
	root   *btNode[T] // first node
	degree int        // range of keys per child
	mutex  sync.Mutex
}

func New[T any]() Storage[T] {
	degree := 2
	return Storage[T]{
		root:   nil,
		degree: degree,
	}
}

func (s *Storage[T]) Insert(item Item[T], cmpr keyCompareFunc) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
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
	s.mutex.Lock()
	defer s.mutex.Unlock()
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

func (s *Storage[T]) Delete(targetKey int, cmpr keyCompareFunc) (Item[T], error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.root == nil {
		return Item[T]{}, errors.New("Storage is not initilized, add items into the storage before deletetion")
	}
	deleteItem, err := s.root.remove(targetKey, cmpr, false)
	if s.root.numItems == 0 {
		if s.root.isLeaf() {
			s.root = nil
		} else {
			s.root = s.root.children[0]
		}
	}
	if err != nil {
		return Item[T]{}, nil
	}
	return deleteItem, nil
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
func (s *Storage[T]) Traverse(visitFn DoPerNode[T]) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
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

func (left *btNode[T]) combineItemCountIntoLeft(right *btNode[T]) {
	left.numItems += right.numItems
}

func (left *btNode[T]) combineChildrenCountIntoLeft(right *btNode[T]) {
	left.numChildren += right.numChildren
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
	if found {
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

func (n *btNode[T]) removeItemAtPosition(pos int) Item[T] {
	rmItem := n.items[pos]
	n.items[pos] = Item[T]{}
	if lastPos := n.numItems - 1; pos < lastPos {
		copy(n.items[pos:lastPos], n.items[pos+1:lastPos+1])
		n.items[lastPos] = Item[T]{}
	}
	n.numItems--
	return rmItem
}

func (n *btNode[T]) removeChildAtPos(pos int) *btNode[T] {
	rmChild := n.children[pos]
	n.children[pos] = nil

	if lastPos := n.numChildren - 1; pos < lastPos {
		copy(n.children[pos:lastPos], n.children[pos+1:lastPos+1])
		n.children[lastPos] = nil
	}
	return rmChild
}

func (n *btNode[T]) fillChildrenAtPos(pos int) {
	switch {
	// borrow from right nodes
	case pos < 0 && n.children[pos-1].numItems > computeMinItems(degree):
		{
			left, right := n.children[pos-1], n.children[pos]
			copy(right.items[1:right.numItems+1], right.items[:right.numItems])
			right.items[0] = n.items[pos-1]
			right.incrementItemCount()
			if !right.isLeaf() {
				right.insertChildAtPosition(left.removeChildAtPos(left.numChildren-1), 0, degree)
			}
		}
		// borrow from left nodes
	case pos < n.numChildren-1 && n.children[pos+1].numItems > computeMinItems(degree):
		{
			left, right := n.children[pos], n.children[pos+1]
			left.items[left.numItems] = n.items[pos]
			left.incrementItemCount()
			if !left.isLeaf() {
				left.insertChildAtPosition(right.removeChildAtPos(0), left.numChildren, degree)
			}
			n.items[pos] = right.removeItemAtPosition(0)
		}
	default: //merge nodes
		{
			if pos >= n.numItems {
				pos = n.numItems - 1
			}
			left, right := n.children[pos], n.children[pos+1]
			left.items[left.numItems] = n.removeItemAtPosition(pos)
			left.incrementItemCount()

			copy(left.items[left.numItems:], right.items[:right.numItems])
			left.combineItemCountIntoLeft(right)
			if !left.isLeaf() {
				copy(left.children[left.numChildren:], right.children[:right.numChildren])
				left.combineChildrenCountIntoLeft(right)
			}
			n.removeChildAtPos(pos + 1)
			right = nil
		}
	}
}

func (n *btNode[T]) remove(key int, cmpr keyCompareFunc, isSeekingSuccessor bool) (Item[T], error) {
	pos, found := n.searchNode(key, cmpr)
	var nxtNode *btNode[T]
	if found == true {
		if n.isLeaf() {
			return n.removeItemAtPosition(pos), nil
		}
		nxtNode, isSeekingSuccessor = n.children[pos+1], true
	} else {
		nxtNode = n.children[pos]
	}
	if n.isLeaf() && isSeekingSuccessor {
		return n.removeItemAtPosition(0), nil
	}

	if nxtNode == nil {
		return Item[T]{}, errors.New(fmt.Sprintf("Item with key %d not found", key))
	}

	deleteItem, err := nxtNode.remove(key, cmpr, isSeekingSuccessor)
	if found && isSeekingSuccessor {
		n.items[pos] = deleteItem
	}

	if nxtNode.numItems < computeMinItems(degree) {
		if found && isSeekingSuccessor {
			n.fillChildrenAtPos(pos + 1)
		} else {
			n.fillChildrenAtPos(pos)
		}
	}

	return deleteItem, err
}
