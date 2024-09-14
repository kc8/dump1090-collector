package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	LINE_FEED_ENDING byte = 0x0a
)

type ADBSParseError struct {
	FullMessage   string
	UnderlyingError   error
	IndexPosition int
}

func (e ADBSParseError) Error() string {
	return fmt.Sprintf(
		"Full ADBS MESSAGE %s \n Indexed Position: %d \n Underlying Error %s\n",
		e.FullMessage,
		e.IndexPosition,
		e.UnderlyingError)
}

type FormattedAdbsMsg struct {
	MessageType       string
	TransmissionType  string // unused
	SessionID         string // unused
	AircraftId        string // unused
	AircraftICAOAddr  string // in hex
	FlightRecordNumer string // unused

	GeneratedTimestamp time.Time
	LoggedTimestamp    time.Time

	CallsignFlightNum string
	Altitude          Nullable[float32]
	GroundSpeed       Nullable[float32]
	HeadingTrack      Nullable[int]
	Lat               Nullable[float32]
	Long              Nullable[float32]
	VerticalRate      Nullable[float32]
	SquawkCode        Nullable[int]
	SquawkChange      Nullable[int]
	Emergency         Nullable[int]
	TransponderIdent  Nullable[int]
	IsOnGround        Nullable[int]
}

func getTimeStamp(dateStamp string, timeStamp string) (*time.Time, error) {
	gDate, tStampErr := time.Parse("2006/01/02", dateStamp)
	gTime, tStampErr := time.Parse("15:04:05.000", timeStamp)
	if tStampErr != nil {
		return nil, tStampErr
	}
	generatedTimestamp := time.Date(
		gDate.Year(),
		gDate.Month(),
		gDate.Day(),
		gTime.Hour(),
		gTime.Minute(),
		gTime.Second(),
		gTime.Nanosecond(),
		gDate.Location())
	return &generatedTimestamp, nil
}

func parseStringToFloatWith0AsInvalid(txt string) (Nullable[float32], error) {
	if txt == "" {
		return Nullable[float32]{Valid: false}, nil
	}
	altitude, altErr := strconv.ParseFloat(txt, 32)
	if altErr != nil {
		return Nullable[float32]{Value: 0, Valid: false}, altErr
	}
	return Nullable[float32]{Value: float32(altitude), Valid: true}, nil
}

func parseStringToIntWith0AsInvalid(txt string) (Nullable[int], error) {
	if txt == "" {
		return Nullable[int]{Valid: false}, nil
	}
	altitude, altErr := strconv.ParseInt(txt, 10, 32)
	if altErr != nil {
		return Nullable[int]{Value: 0, Valid: false}, altErr
	}
	return Nullable[int]{Value: int(altitude), Valid: true}, nil
}

func ParseCSVFormat(msg []byte) (*FormattedAdbsMsg, error) {
	stringSplit := strings.Split(string(msg), ",")

	generatedTimestamp, tStampErr := getTimeStamp(stringSplit[6], stringSplit[7])
	if tStampErr != nil {
		return nil, ADBSParseError{
			FullMessage:   string(msg),
			UnderlyingError:   tStampErr,
			IndexPosition: 6,
		}
	}
	loggedTimestamp, tStampErr := getTimeStamp(stringSplit[8], stringSplit[9])
	if tStampErr != nil {
		return nil, ADBSParseError{
			FullMessage:   string(msg),
			UnderlyingError:   tStampErr,
			IndexPosition: 8,
		}
	}

	altitude, err := parseStringToFloatWith0AsInvalid(stringSplit[11])
	if err != nil {
		return nil, ADBSParseError{
			FullMessage:   string(msg),
			UnderlyingError:   err,
			IndexPosition: 11,
		}
	}
	groundSpeed, err := parseStringToFloatWith0AsInvalid(stringSplit[12])
	if err != nil {
		return nil, ADBSParseError{
			FullMessage:   string(msg),
			UnderlyingError:   err,
			IndexPosition: 12,
		}
	}
	headingTrack, err := parseStringToIntWith0AsInvalid(stringSplit[13])
	if err != nil {
		return nil, ADBSParseError{
			FullMessage:   string(msg),
			UnderlyingError:   err,
			IndexPosition: 13,
		}
	}
	lat, err := parseStringToFloatWith0AsInvalid(stringSplit[14])
	if err != nil {
		return nil, ADBSParseError{
			FullMessage:   string(msg),
			UnderlyingError:   err,
			IndexPosition: 14,
		}
	}
	long, err := parseStringToFloatWith0AsInvalid(stringSplit[15])
	if err != nil {
		return nil, ADBSParseError{
			FullMessage:   string(msg),
			UnderlyingError:   err,
			IndexPosition: 15,
		}
	}
	vertRate, err := parseStringToFloatWith0AsInvalid(stringSplit[16])
	if err != nil {
		return nil, ADBSParseError{
			FullMessage:   string(msg),
			UnderlyingError:   err,
			IndexPosition: 16,
		}
	}
	squawk, err := parseStringToIntWith0AsInvalid(stringSplit[17])
	if err != nil {
		return nil, ADBSParseError{
			FullMessage:   string(msg),
			UnderlyingError:   err,
			IndexPosition: 17,
		}
	}
	squawkChange, err := parseStringToIntWith0AsInvalid(stringSplit[18])
	if err != nil {
		return nil, ADBSParseError{
			FullMessage:   string(msg),
			UnderlyingError:   err,
			IndexPosition: 18,
		}
	}
	emergency, err := parseStringToIntWith0AsInvalid(stringSplit[19])
	if err != nil {
		return nil, ADBSParseError{
			FullMessage:   string(msg),
			UnderlyingError:   err,
			IndexPosition: 19,
		}
	}
	transPonderIdent, err := parseStringToIntWith0AsInvalid(stringSplit[20])
	if err != nil {
		return nil, ADBSParseError{
			FullMessage:   string(msg),
			UnderlyingError:   err,
			IndexPosition: 20,
		}
	}
	// the slice at the end is because the buffer is filled with junk after the 21s index
	isOnGround, err := parseStringToIntWith0AsInvalid(stringSplit[21][:0])
	if err != nil {
		return nil, ADBSParseError{
			FullMessage:   string(msg),
			UnderlyingError:   err,
			IndexPosition: 21,
		}
	}

	result := FormattedAdbsMsg{
		MessageType:       stringSplit[0],
		TransmissionType:  stringSplit[1],
		SessionID:         stringSplit[2],
		AircraftId:        stringSplit[3],
		AircraftICAOAddr:  stringSplit[4],
		FlightRecordNumer: stringSplit[5],

		GeneratedTimestamp: *generatedTimestamp,

		LoggedTimestamp:   *loggedTimestamp,
		CallsignFlightNum: stringSplit[10],
		Altitude:          altitude,
		GroundSpeed:       groundSpeed,
		HeadingTrack:      headingTrack,
		Lat:               lat,
		Long:              long,
		VerticalRate:      vertRate,
		SquawkCode:        squawk,
		SquawkChange:      squawkChange,
		Emergency:         emergency,
		TransponderIdent:  transPonderIdent,
		IsOnGround:        isOnGround,
	}
	return &result, nil
}
