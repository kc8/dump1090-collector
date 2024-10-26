package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
)

type isFound func(arr []byte, index int, key byte) bool

// return index
func binSearch(arr []byte, key byte, low int, high int, found isFound) int {
	mid := low + (high-low)/2

	if low < high {
		//if arr[mid] == key {
		if found(arr, mid, key) == true {
			return mid
		} else if arr[mid] < key {
			return binSearch(arr, key, mid+1, high, found)
		} else {
			return binSearch(arr, key, low, mid-1, found)
		}
	}
	return -1
}

type level string

const (
	INFO  level = "INFO"
	ERROR level = "ERROR"
	WARN  level = "WARN"
	FATAL level = "FATAL"
)

func Log(msg string, l level) {
	log.Printf("[%s]: %s\n", l, msg)
	if l == "FATAL" {
		log.Fatalf("[%s]: %s\n", l, msg)
	}
}

type aircraftDataResp struct {
	Prefix string `json:"prefix"`
	Number string `json:"number"`
}

/*
fullUri is the complete uri
*/
func getAircraftMetaData(fullUri string) (*aircraftDataResp, error) {
	req, err := http.NewRequest(http.MethodGet, fullUri, nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("Unexpected result from server got response code: %d", res.StatusCode))
	}
	var formatted aircraftDataResp
	if err := json.NewDecoder(res.Body).Decode(&formatted); err != nil {
		return nil, err
	}
	return &formatted, nil
}

// NOTE: Simple key will not gaurentee a unique key!
func simpleKey(s string) int {
	var result int
	for _, c := range s {
		result += int(byte(c))
	}
	return result
}

func simpleKeyCompare(a int, b int) int {
	if a == b {
		return 0
	}
	if a < b {
		return -1
	}
	return 1
}

func floatCompare(right, left, epsilon float32) bool {
	a := float64(right)
	b := float64(left)
	e := float64(epsilon)
	if a == b {
		return true
	}
	return (math.Abs(a-b) / (math.Abs(a) + math.Abs(b))) < e
}
