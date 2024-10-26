package storage

import (
	"errors"
	"sync"
)

type MapStorage[T any] struct {
	data  map[string]MapItem[T]
	mutex sync.Mutex
}

func NewMapStorage[T any]() MapStorage[T] {
	return MapStorage[T]{
		data: make(map[string]MapItem[T]),
	}
}

func (s *MapStorage[T]) Insert(item MapItem[T], cmpr keyCompareFunc) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if item.Key == "" {
		return errors.New("Key for new item cannoy be \"\" must be a valid string")
	}
	s.data[item.Key] = item
	return nil
}

func (s *MapStorage[T]) Search(targetKey string, cmpr keyCompareFunc) (MapItem[T], error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	i, ok := s.data[targetKey]
	if ok == false {
		return MapItem[T]{}, errors.New("Not Found")
	}
	return i, nil
}

// TODO we are not deleting?
func (s *MapStorage[T]) Delete(targetKey string, cmpr keyCompareFunc) (MapItem[T], error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	i, ok := s.data[targetKey]
	if ok == true {
		delete(s.data, targetKey)
	}
	return i, nil
}

func (s *MapStorage[T]) Traverse(visitFn DoPerEntry[T]) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for _, value := range s.data {
		visitFn(value)
	}
	return nil
}
