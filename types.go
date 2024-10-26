package main

import (
	"encoding/json"
)

type Nullable[T any] struct {
	Value T
	Valid bool
     maybeErr error 
}

type CordinatesOverTime struct {
	Lat          float32 `json:"lat"`
	Long         float32 `json:"long"`
	TimestampUTC int64   `json:"timestamp"`
}

func (c *CordinatesOverTime) Scan(value interface{}) error {
	return json.Unmarshal([]byte(value.(string)), c)
}

func (c *CordinatesOverTime) Value() ([]byte, error) {
	b, err := json.Marshal(c)
	return b, err
}

type DataOverTime[T int | float32] struct {
	Data         T     `json:"value"`
	TimestampUTC int64 `json:"timestamp"`
}

func (c *DataOverTime[T]) Scan(value interface{}) error {
	return json.Unmarshal([]byte(value.(string)), c)
}

func (c *DataOverTime[T]) Value() ([]byte, error) {
	b, err := json.Marshal(c)
	return b, err
}

type CollectedData struct {
	// Aggregated values over time
	LastSeen    int64
	FirstSeen   int64
	MsgCount    uint64
	Coordinates []CordinatesOverTime

	Icao         string
	TailNumber   string
	Altitude     []DataOverTime[float32] `json:"altitude"`
	GroundSpeed  []DataOverTime[float32] `json:"groundSpeed"`
	HeadingTrack []DataOverTime[int]     `json:"headingTrack"`
	VerticalRate []DataOverTime[float32] `json:"verticalRate"`
	SquawkCode   []DataOverTime[int]     `json:"squawkCode"`
	Emergency    Nullable[int]
}

func convertDataOverTimeToJson[T float32 | int](data []DataOverTime[T]) ([]byte, error) {
	b, err := json.Marshal(data)
	return b, err
}

func convertCordinatesOverTimeToJson(data []CordinatesOverTime) ([]byte, error) {
	b, err := json.Marshal(data)
	return b, err
}
