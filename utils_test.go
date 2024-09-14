package main 

import "testing"

func TestSimpleId(t *testing.T) {
    key := simpleKey("string")
    if key < 0 {
        t.Fatalf("Key was not > 0 got: %d", key)
    }
}
