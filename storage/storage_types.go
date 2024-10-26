package storage

type keyCompareFunc func(left int, right int) int

type Item[T any] struct {
	Key  int
	Data T
}

type DoPerNode[T any] func(Item[T])

type MapItem[T any] struct {
	Key  string
	Data T
}
type DoPerEntry[T any] func(MapItem[T])

