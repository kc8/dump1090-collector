package storage

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

func simpleKeyCompare(a int, b int) int {
	if a == b {
		return 0
	}
	if a < b {
		return -1
	}
	return 1
}

func TestCreateInsertBSTree(t *testing.T) {
	storage := New[[]byte]()
	item := Item[[]byte]{
		Key:  1,
		Data: make([]byte, 1),
	}
	storage.Insert(item, simpleKeyCompare)
}

func TestCreateInsertSingleItem(t *testing.T) {
	storage := New[[]byte]()
	key := 1
	const testByte byte = 10
	item := Item[[]byte]{
		Key:  key,
		Data: []byte{testByte},
	}
	storage.Insert(item, simpleKeyCompare)
	item, err := storage.Search(key, simpleKeyCompare)
	if err != nil {
		t.Fatalf("Got error when searching for item w/ key %d. Error: %q", key, err)
	}
	if item.Key != key {
	}
	if item.Data[0] != 10 {
		t.Fatalf("Got wrong item back key %d. byte: %d", key, item.Data[0])
	}
}

func TestCreateInsertMultiItem(t *testing.T) {
	item1 := Item[[]byte]{
		Key:  1,
		Data: []byte{99},
	}
	item2 := Item[[]byte]{
		Key:  2,
		Data: []byte{109},
	}
	item3 := Item[[]byte]{
		Key:  3,
		Data: []byte{111},
	}
	storage := New[[]byte]()
	storage.Insert(item1, simpleKeyCompare)
	storage.Insert(item2, simpleKeyCompare)
	storage.Insert(item3, simpleKeyCompare)

	findFirstItem, err := storage.Search(1, simpleKeyCompare)
	if err != nil {
		t.Fatalf("Got error when searching for item w/ key %d. Error: %q", findFirstItem.Key, err)
	}
	if findFirstItem.Key != 1 {
	}
	if findFirstItem.Data[0] != 99 {
		t.Fatalf("Got wrong item back key %d. byte: %d", findFirstItem.Key, findFirstItem.Data[0])
	}

	findSecondItem, err := storage.Search(2, simpleKeyCompare)
	if err != nil {
		t.Fatalf("Got error when searching for item w/ key %d. Error: %q", findSecondItem.Key, err)
	}
	if findSecondItem.Key != 2 {
		t.Fatalf("Got wrong key back: key %d", findFirstItem.Key)
	}
	if findSecondItem.Data[0] != 109 {
		t.Fatalf("Got wrong item back key %d. byte: %d", findSecondItem.Key, findSecondItem.Data[0])
	}

	findThirdItem, err := storage.Search(3, simpleKeyCompare)
	if err != nil {
		t.Fatalf("Got error when searching for item w/ key %d. Error: %q", findThirdItem.Key, err)
	}
	if findThirdItem.Key != 3 {
		t.Fatalf("Got wrong key back: key %d", findSecondItem.Key)
	}
	if findThirdItem.Data[0] != 111 {
		t.Fatalf("Got wrong item back key %d. byte: %d", findThirdItem.Key, findThirdItem.Data[0])
	}
}

/*
- Test that we can at least add 100 values into the b-tree 10 times
this should not fail or else there is something wrong in the data structure
- The idea behind randomness is that it should not fail in these cases as it simulatees
real world randomness
*/
func TestAlotOfInput(t *testing.T) {
	for i := 0; i < 10; i++ {
		sampleSize := 100
		data := make(map[int]int)
		storage := New[int]()

		for i := 0; i < sampleSize; i++ {
			Key := rand.IntN(1000)
			if Key == 0 {
				continue
			}
			Data := rand.IntN(1000)
			item := Item[int]{
				Key,
				Data,
			}
			storage.Insert(item, simpleKeyCompare)
			data[Key] = Data
		}
		for k, v := range data {
			item, err := storage.Search(k, simpleKeyCompare)
			if err != nil {
				t.Fatalf("Got error when searching for item w/ key %d. Error: %q", k, err)
			}
			if item.Key != k {
				t.Fatalf("Got wrong key back: key %d expeect %d", item.Key, k)
			}
			if item.Data != v {
				t.Fatalf("Got wrong item back data %d. expected: %d", item.Data, v)
			}
		}
	}
}

func TestTraverse(t *testing.T) {
	item1 := Item[[]byte]{
		Key:  1,
		Data: []byte{99},
	}
	item2 := Item[[]byte]{
		Key:  2,
		Data: []byte{109},
	}
	item3 := Item[[]byte]{
		Key:  3,
		Data: []byte{111},
	}
	item4 := Item[[]byte]{
		Key:  4,
		Data: []byte{112},
	}
	item5 := Item[[]byte]{
		Key:  5,
		Data: []byte{113},
	}
	item6 := Item[[]byte]{
		Key:  -1,
		Data: []byte{114},
	}
	item7 := Item[[]byte]{
		Key:  6,
		Data: []byte{115},
	}
	storage := New[[]byte]()
	storage.Insert(item1, simpleKeyCompare)
	storage.Insert(item2, simpleKeyCompare)
	storage.Insert(item3, simpleKeyCompare)
	storage.Insert(item4, simpleKeyCompare)
	storage.Insert(item5, simpleKeyCompare)
	storage.Insert(item6, simpleKeyCompare)
	storage.Insert(item7, simpleKeyCompare)

	var allItems []byte
	answer := []byte{99, 109, 111, 112, 113, 114, 115}
	visited := func(item Item[[]byte]) {
		allItems = append(allItems, item.Data[0])
	}
	err := storage.Traverse(visited)
	if err != nil {
		t.Fatalf("Failed with traversal error: %q", err)
	}
	if len(allItems) != len(answer) {
		t.Fatalf("Got wrong length of items %d was supposed to be %d", len(allItems), len(answer))
	}
	failedList := []string{}
	for _, a := range answer {
		if slices.Contains(allItems, a) == false {
			failedList = append(failedList, fmt.Sprintf("Failed to find element in storage element: %d", a))
		}
	}
	if len(failedList) > 0 {
		sBuilder := strings.Builder{}
		for _, f := range failedList {
			sBuilder.WriteString(fmt.Sprint(f + "\n"))
		}
		t.Fatalf(sBuilder.String())
	}
}

func TestDelete(t *testing.T) {
	item1 := Item[[]byte]{
		Key:  1,
		Data: []byte{99},
	}
	item2 := Item[[]byte]{
		Key:  2,
		Data: []byte{109},
	}
	item3 := Item[[]byte]{
		Key:  3,
		Data: []byte{111},
	}
	item4 := Item[[]byte]{
		Key:  4,
		Data: []byte{112},
	}
	item5 := Item[[]byte]{
		Key:  5,
		Data: []byte{113},
	}
	item6 := Item[[]byte]{
		Key:  -1,
		Data: []byte{114},
	}
	item7 := Item[[]byte]{
		Key:  6,
		Data: []byte{115},
	}
	storage := New[[]byte]()
	storage.Insert(item1, simpleKeyCompare)
	storage.Insert(item2, simpleKeyCompare)
	storage.Insert(item3, simpleKeyCompare)
	storage.Insert(item4, simpleKeyCompare)
	storage.Insert(item5, simpleKeyCompare)
	storage.Insert(item6, simpleKeyCompare)
	storage.Insert(item7, simpleKeyCompare)

	deleteItem, delErr := storage.Delete(item3.Key, simpleKeyCompare)
	if delErr != nil {
		t.Fatalf(fmt.Sprintf("Failed to delete with error: %s", delErr.Error()))
	}
	if deleteItem.Data[0] != item3.Data[0] {
		t.Fatalf("Did not get byte 109 back in delted Item got: %d", deleteItem.Data[0])
	}

	var allItems []byte
	answer := []byte{99, 109, 112, 113, 114, 115}
	visited := func(item Item[[]byte]) {
		allItems = append(allItems, item.Data[0])
	}
	err := storage.Traverse(visited)
	if err != nil {
		t.Fatalf("Failed with traversal error: %q", err)
	}
	if len(allItems) != len(answer) {
		t.Fatalf("Got wrong length of items %d was supposed to be %d", len(allItems), len(answer))
	}
	failedList := []string{}
	for _, a := range answer {
		if slices.Contains(allItems, a) == false {
			failedList = append(failedList, fmt.Sprintf("Failed to find element in storage element: %d", a))
		}
	}
	if len(failedList) > 0 {
		sBuilder := strings.Builder{}
		for _, f := range failedList {
			sBuilder.WriteString(fmt.Sprint(f + "\n"))
		}
		t.Fatalf(sBuilder.String())
	}
}

func TestCreateInsertDeleteSingleItem(t *testing.T) {
	storage := New[[]byte]()
	key := 1
	const testByte byte = 10
	item := Item[[]byte]{
		Key:  key,
		Data: []byte{testByte},
	}
	storage.Insert(item, simpleKeyCompare)

	deleteItem, delErr := storage.Delete(item.Key, simpleKeyCompare)
	if deleteItem.Key != key {
		t.Fatalf("Delete key did not match original key w/ key %d", key)
	}
	if delErr != nil {
		t.Fatalf("Delete key returned error key w/ key %d Error: %q", key, delErr)
	}
	if deleteItem.Key != key {
		t.Fatalf("Got wrong key item back key %d. byte: %d", key, deleteItem.Data)
	}
	if deleteItem.Data[0] != 10 {
		t.Fatalf("Got wrong item back key %d. byte: %d", key, deleteItem.Data[0])
	}

    // item should not exist
	sitem, err := storage.Search(key, simpleKeyCompare)
	if err == nil {
		t.Fatalf("Did not Got error the search failed after delete w/ key %d. Error: %q", key, err)
	} 
	if sitem.Key != 0 {
		t.Fatalf("Got wrong key item back key %d. byte: %d", key, sitem.Data)
	}
}

func TestDeleteThenReInsert(t *testing.T) {
	item1 := Item[[]byte]{
		Key:  1,
		Data: []byte{99},
	}
	item2 := Item[[]byte]{
		Key:  2,
		Data: []byte{109},
	}
	item3 := Item[[]byte]{
		Key:  3,
		Data: []byte{111},
	}
	item4 := Item[[]byte]{
		Key:  4,
		Data: []byte{112},
	}
	item5 := Item[[]byte]{
		Key:  5,
		Data: []byte{113},
	}
	item6 := Item[[]byte]{
		Key:  -1,
		Data: []byte{114},
	}
	item7 := Item[[]byte]{
		Key:  6,
		Data: []byte{115},
	}
	storage := New[[]byte]()
	storage.Insert(item1, simpleKeyCompare)
	storage.Insert(item2, simpleKeyCompare)
	storage.Insert(item3, simpleKeyCompare)
	storage.Insert(item4, simpleKeyCompare)
	storage.Insert(item5, simpleKeyCompare)
	storage.Insert(item6, simpleKeyCompare)
	storage.Insert(item7, simpleKeyCompare)

	deleteItem, delErr := storage.Delete(item3.Key, simpleKeyCompare)
	if delErr != nil {
		t.Fatalf(fmt.Sprintf("Failed to delete with error: %s", delErr.Error()))
	}
	if deleteItem.Data[0] != item3.Data[0] {
		t.Fatalf("Did not get byte 109 back in delted Item got: %d", deleteItem.Data[0])
	}
	deleteItem4, delErr := storage.Delete(item4.Key, simpleKeyCompare)
	if delErr != nil {
		t.Fatalf(fmt.Sprintf("Failed to delete with error: %s", delErr.Error()))
	}
	if deleteItem4.Data[0] != item4.Data[0] {
		t.Fatalf("Did not get byte 109 back in delted Item got: %d", deleteItem4.Data[0])
	}

    // re-insert deleteed item
	storage.Insert(item3, simpleKeyCompare)
	item10 := Item[[]byte]{
		Key:  -2,
		Data: []byte{116},
	}
    // insert new item
	storage.Insert(item10, simpleKeyCompare)

	var allItems []byte
	answer := []byte{99, 111, 109, 113, 114, 115, 116}
	visited := func(item Item[[]byte]) {
		allItems = append(allItems, item.Data[0])
	}
	err := storage.Traverse(visited)
	if err != nil {
		t.Fatalf("Failed with traversal error: %q", err)
	}
	if len(allItems) != len(answer) {
		t.Fatalf("Got wrong length of items %d was supposed to be %d", len(allItems), len(answer))
	}
	failedList := []string{}
	for _, a := range answer {
		if slices.Contains(allItems, a) == false {
			failedList = append(failedList, fmt.Sprintf("Failed to find element in storage element: %d", a))
		}
	}
	if len(failedList) > 0 {
		sBuilder := strings.Builder{}
		for _, f := range failedList {
			sBuilder.WriteString(fmt.Sprint(f + "\n"))
		}
		t.Fatalf(sBuilder.String())
	}
}
