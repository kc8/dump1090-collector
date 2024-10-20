package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	//"sync"
	"time"

	storage "github.com/kc8/dump-1090-aggergator/storage"
	database "github.com/kc8/dump-1090-aggergator/storage/database"
)

var (
	FIVE_SECOND_DEADLINE = time.Now().Add(5 * time.Second)
	TEN_SECOND_DEADLINE  = time.Now().Add(10 * time.Second)
	ONE_SEOCND_DEADLINE  = time.Now().Add(1 * time.Second)

	sto        = storage.NewMapStorage[CollectedData]()
	lookupAddr = flag.String("lookupAddr", "", "FQDN to lookup translations and other metdata")
)

func main() {
	var (
		addr                   = flag.String("addr", "", "Adress of piaware")
		port                   = flag.String("port", "30003", "Port for CSV protocol")
		dbLocation             = flag.String("dbLoc", "", "Path to the sqlite4 database location Example: /home/user/Documents")
		dbFileName             = flag.String("dbFilename", "dump1090reader.db", "Override filename of sqlite3 database example: dump1090reader.db")
		flightSessionLen int64 = 7200000
	)
	flag.Int64Var(&flightSessionLen, "flightSessionDur", 7200000, "MS for how long a flight session is default: 2 hours  7,200,000 ms")
	flag.Parse()
	dbInstance, dbCreateErr := database.New(*dbFileName, **&dbLocation)
	if dbCreateErr != nil {
		Log(fmt.Sprintf("Could not open database: %q", dbCreateErr), ERROR)
		panic("Database was not open: pancing")
	}
	errI := dbInstance.TestConnnection()
	if errI != nil {
		Log(fmt.Sprintf("Failed to test conn to db %q", errI), ERROR)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan bool)
	defer close(done)

	if *addr == "" {
		flag.PrintDefaults()
		os.Exit(-1)
	}
	if *lookupAddr == "" {
		flag.PrintDefaults()
		os.Exit(-1)
	}

	go func() {
		s := make(chan os.Signal, 1)
		signal.Notify(s, os.Interrupt)
		<-s
		signal.Reset()
		done <- true
	}()

	findChannel := make(chan Nullable[storage.MapItem[CollectedData]])
	queue := NewQueue(&sto)
	go queue.run(findChannel)
	go readData(ctx, *addr, *port, *lookupAddr, done, queue, findChannel)
	go scanForEntryIntoDB(ctx, dbInstance, &sto, done, flightSessionLen, queue)
	<-done
}

func generateConnection(ctx context.Context, host string, port string) (net.Conn, error) {
	address := fmt.Sprintf("%s:%s", host, port)
	dialer := net.Dialer{}
	dial, dialErr := dialer.DialContext(ctx, "tcp", address)
	if dialErr != nil {
		return nil, dialErr
	}
	return dial, nil
}

func createNewDataEntry(rawAircraft *FormattedAdbsMsg) storage.MapItem[CollectedData] {
	// TODO I want to try and cache this
	currentKey := rawAircraft.AircraftICAOAddr
	addr := fmt.Sprintf(
		"http://%s/icaoTranslate?icao=%s",
		*lookupAddr,
		rawAircraft.AircraftICAOAddr)

	resp, err := getAircraftMetaData(addr)
	if err != nil {
		Log(fmt.Sprintf("Failed to look up aircraft info due to %s", err.Error()), WARN)
	}
	tailNum := ""
	if resp.Number != "" {
		tailNum = fmt.Sprintf("%s%s", resp.Prefix, resp.Number)
	}
	Log(fmt.Sprintf("Missed %s, adding", rawAircraft.AircraftICAOAddr), INFO)
	item := storage.MapItem[CollectedData]{
		Key: currentKey,
		Data: CollectedData{
			FirstSeen:  time.Now().UTC().UnixMilli(),
			Icao:       rawAircraft.AircraftICAOAddr,
			TailNumber: tailNum,
			MsgCount:   1,
		},
	}
	return item
}

func updateEntry(value storage.MapItem[CollectedData], result *FormattedAdbsMsg) storage.MapItem[CollectedData] {
    newValue := value // We are making copies
	currentTimeStamp := time.Now().UTC().UnixMilli()
	newValue.Data.LastSeen = currentTimeStamp
	newValue.Data.MsgCount++

	if result.Lat.Valid == true && result.Long.Valid == true {
		if len(value.Data.Coordinates) == 0 {
			newValue.Data.Coordinates = append(
				newValue.Data.Coordinates,
				CordinatesOverTime{
					Lat:          result.Lat.Value,
					Long:         result.Long.Value,
					TimestampUTC: currentTimeStamp,
				})
		}
		if len(value.Data.Coordinates) >= 1 && (floatCompare(value.Data.Coordinates[len(value.Data.Coordinates)-1].Lat, result.Lat.Value, 0.01) ||
			floatCompare(value.Data.Coordinates[len(value.Data.Coordinates)-1].Long, result.Long.Value, 0.01)) {
			newValue.Data.Coordinates = append(
				newValue.Data.Coordinates,
				CordinatesOverTime{
					Lat:          result.Lat.Value,
					Long:         result.Long.Value,
					TimestampUTC: currentTimeStamp,
				})
		}
	}
	if result.Altitude.Valid == true {
		if len(value.Data.Altitude) == 0 {
			newValue.Data.Altitude = append(
				newValue.Data.Altitude,
				DataOverTime[float32]{
					Data:         result.Altitude.Value,
					TimestampUTC: currentTimeStamp,
				})
		}
		if len(value.Data.Altitude) >= 1 && floatCompare(value.Data.Altitude[len(value.Data.Altitude)-1].Data, result.Altitude.Value, 0.01) &&
			floatCompare(value.Data.Altitude[len(value.Data.Altitude)-1].Data, result.Altitude.Value, 0.01) {
			newValue.Data.Altitude = append(
				newValue.Data.Altitude,
				DataOverTime[float32]{
					Data:         result.Altitude.Value,
					TimestampUTC: currentTimeStamp,
				})
		}

	}
	if result.GroundSpeed.Valid == true {
		if len(value.Data.GroundSpeed) == 0 {
			newValue.Data.GroundSpeed = append(
				newValue.Data.GroundSpeed,
				DataOverTime[float32]{
					Data:         result.GroundSpeed.Value,
					TimestampUTC: currentTimeStamp,
				})
		}
		if len(value.Data.GroundSpeed) >= 1 && floatCompare(value.Data.GroundSpeed[len(value.Data.GroundSpeed)-1].Data, result.GroundSpeed.Value, 0.01) &&
			floatCompare(value.Data.GroundSpeed[len(value.Data.GroundSpeed)-1].Data, result.GroundSpeed.Value, 0.01) {
			newValue.Data.GroundSpeed = append(
				newValue.Data.GroundSpeed,
				DataOverTime[float32]{
					Data:         result.GroundSpeed.Value,
					TimestampUTC: currentTimeStamp,
				})
		}
	}
	if result.HeadingTrack.Valid == true {
		if len(value.Data.HeadingTrack) == 0 {
			newValue.Data.HeadingTrack = append(
				newValue.Data.HeadingTrack,
				DataOverTime[int]{
					Data:         result.HeadingTrack.Value,
					TimestampUTC: currentTimeStamp,
				})
		}
		if len(value.Data.HeadingTrack) >= 1 && value.Data.HeadingTrack[len(value.Data.HeadingTrack)-1].Data != result.HeadingTrack.Value {
			newValue.Data.HeadingTrack = append(
				newValue.Data.HeadingTrack,
				DataOverTime[int]{
					Data:         result.HeadingTrack.Value,
					TimestampUTC: currentTimeStamp,
				})
		}
	}
	if result.VerticalRate.Valid == true {
		if len(value.Data.VerticalRate) == 0 {
			newValue.Data.VerticalRate = append(
				newValue.Data.VerticalRate,
				DataOverTime[float32]{
					Data:         result.VerticalRate.Value,
					TimestampUTC: currentTimeStamp,
				})
		}
		if len(value.Data.VerticalRate) >= 1 && floatCompare(value.Data.VerticalRate[len(value.Data.VerticalRate)-1].Data, result.VerticalRate.Value, 0.01) &&
			floatCompare(value.Data.VerticalRate[len(value.Data.VerticalRate)-1].Data, result.VerticalRate.Value, 0.01) {
			newValue.Data.VerticalRate = append(
				newValue.Data.VerticalRate,
				DataOverTime[float32]{
					Data:         result.VerticalRate.Value,
					TimestampUTC: currentTimeStamp,
				})
		}
	}
	if result.SquawkCode.Valid == true {
		if len(value.Data.SquawkCode) == 0 {
			newValue.Data.SquawkCode = append(
				newValue.Data.SquawkCode,
				DataOverTime[int]{
					Data:         result.SquawkCode.Value,
					TimestampUTC: currentTimeStamp,
				})
		}
		if len(value.Data.SquawkCode) >= 1 && value.Data.SquawkCode[len(value.Data.SquawkCode)-1].Data != result.SquawkCode.Value {
			newValue.Data.SquawkCode = append(
				newValue.Data.SquawkCode,
				DataOverTime[int]{
					Data:         result.SquawkCode.Value,
					TimestampUTC: currentTimeStamp,
				})
		}
	}
	if result.Emergency.Valid == true {
		newValue.Data.Emergency = result.Emergency
	}
    return newValue
}

func readData(
	ctx context.Context,
	host string,
	port string,
	lookupAddr string,
	done chan bool,
	itemQueue *modifyStoQueue,
	fndChan chan Nullable[storage.MapItem[CollectedData]]) {
	tempBuf := make([]byte, 1)
	currentMsg := make([]byte, 128)

	dial, dialErr := generateConnection(ctx, host, port)
	if dialErr != nil {
		Log(fmt.Sprintf("Failed to dial connection to %s:%s. Due to %s", host, port, dialErr.Error()), FATAL)
	}
	Log(fmt.Sprintf("Success dialing connection to %s:%s", host, port), INFO)

	for {
		select {
		case <-done:
			// TODO wont prevent read from empty channels if the dial gets changed from under this func
			Log(fmt.Sprintf("Exiting Application"), INFO)
			return
		default:
			tempBuf[0] = 0
			pos := 0

			for tempBuf[0] != LINE_FEED_ENDING {
				n, readFromErr := dial.Read(tempBuf)
				if tempBuf[0] == LINE_FEED_ENDING {
					continue
				}
				if readFromErr != nil {
					// TODO we get some kind of EOF due to an un-discovered reason, so we just redial for now
					Log(fmt.Sprintf("Failed to read from connection due to %s", readFromErr.Error()), WARN)
					closeErr := dial.Close()
					if closeErr != nil {
						Log(fmt.Sprintf("Failed to close dialer due to %s for connection %s:%s", closeErr.Error(), host, port), ERROR)
					}
					Log(fmt.Sprintf("Success in closing connection to %s:%s", host, port), INFO)
					dial, dialErr = generateConnection(ctx, host, port)
					if dialErr != nil {
						Log(fmt.Sprintf("Failed to redial connection to %s:%s. Due to %s", host, port, dialErr.Error()), FATAL)
					}
					Log(fmt.Sprintf("Success re-dailing connection to %s:%s", host, port), INFO)
					tempBuf[0] = 0
					pos = 0
					continue
				}
				if n > 0 {
					currentMsg[pos] = tempBuf[0]
					pos++
					dial.SetDeadline(time.Now().Add(5 * time.Second))
				} else {
					Log(fmt.Sprintf("No data read from connection"), INFO)
				}
			}
			pos = 0
			result, err := ParseCSVFormat(currentMsg)
			if err != nil {
				Log(fmt.Sprintf("Failed to correctly parse from connection due to: %s", err.Error()), ERROR)
			}
			//currentKey := result.AircraftICAOAddr

			//if value, err := sto.Search(currentKey, simpleKeyCompare); err != nil {
			// itemQueue.addSearch(currentKey)
            itemQueue.updateOrAdd(result)
			/*item := <-fndChan

			findErr := item.maybeErr
			if findErr != nil && item.Valid == false {
				if findErr.Error() == "Not Found" {
					// TODO I want to try and cache this
					addr := fmt.Sprintf("http://%s/icaoTranslate?icao=%s", lookupAddr, result.AircraftICAOAddr)
					resp, err := getAircraftMetaData(addr)
					if err != nil {
						Log(fmt.Sprintf("Failed to look up aircraft info due to %s", err.Error()), WARN)
					}
					tailNum := ""
					if resp.Number != "" {
						tailNum = fmt.Sprintf("%s%s", resp.Prefix, resp.Number)
					}
					Log(fmt.Sprintf("Missed %s, adding", result.AircraftICAOAddr), INFO)
					item := storage.MapItem[CollectedData]{
						Key: currentKey,
						Data: CollectedData{
							FirstSeen:  time.Now().UTC().UnixMilli(),
							Icao:       result.AircraftICAOAddr,
							TailNumber: tailNum,
							MsgCount:   1,
						},
					}
					itemQueue.append(item)
					//if err := sto.Insert(item, simpleKeyCompare); err != nil {
					//	Log(err.Error(), ERROR)
					//}
				} else {
					Log(fmt.Sprintf("Search error encountered %s ", findErr), ERROR)
				}
			} else {
				value := item.Value
				// Make a copy to update
				newValue := value
				currentTimeStamp := time.Now().UTC().UnixMilli()
				newValue.Data.LastSeen = currentTimeStamp
				newValue.Data.MsgCount++

				if result.Lat.Valid == true && result.Long.Valid == true {
					if len(value.Data.Coordinates) == 0 {
						newValue.Data.Coordinates = append(
							newValue.Data.Coordinates,
							CordinatesOverTime{
								Lat:          result.Lat.Value,
								Long:         result.Long.Value,
								TimestampUTC: currentTimeStamp,
							})
					}
					if len(value.Data.Coordinates) >= 1 && (floatCompare(value.Data.Coordinates[len(value.Data.Coordinates)-1].Lat, result.Lat.Value, 0.01) ||
						floatCompare(value.Data.Coordinates[len(value.Data.Coordinates)-1].Long, result.Long.Value, 0.01)) {
						newValue.Data.Coordinates = append(
							newValue.Data.Coordinates,
							CordinatesOverTime{
								Lat:          result.Lat.Value,
								Long:         result.Long.Value,
								TimestampUTC: currentTimeStamp,
							})
					}
				}
				if result.Altitude.Valid == true {
					if len(value.Data.Altitude) == 0 {
						newValue.Data.Altitude = append(
							newValue.Data.Altitude,
							DataOverTime[float32]{
								Data:         result.Altitude.Value,
								TimestampUTC: currentTimeStamp,
							})
					}
					if len(value.Data.Altitude) >= 1 && floatCompare(value.Data.Altitude[len(value.Data.Altitude)-1].Data, result.Altitude.Value, 0.01) &&
						floatCompare(value.Data.Altitude[len(value.Data.Altitude)-1].Data, result.Altitude.Value, 0.01) {
						newValue.Data.Altitude = append(
							newValue.Data.Altitude,
							DataOverTime[float32]{
								Data:         result.Altitude.Value,
								TimestampUTC: currentTimeStamp,
							})
					}

				}
				if result.GroundSpeed.Valid == true {
					if len(value.Data.GroundSpeed) == 0 {
						newValue.Data.GroundSpeed = append(
							newValue.Data.GroundSpeed,
							DataOverTime[float32]{
								Data:         result.GroundSpeed.Value,
								TimestampUTC: currentTimeStamp,
							})
					}
					if len(value.Data.GroundSpeed) >= 1 && floatCompare(value.Data.GroundSpeed[len(value.Data.GroundSpeed)-1].Data, result.GroundSpeed.Value, 0.01) &&
						floatCompare(value.Data.GroundSpeed[len(value.Data.GroundSpeed)-1].Data, result.GroundSpeed.Value, 0.01) {
						newValue.Data.GroundSpeed = append(
							newValue.Data.GroundSpeed,
							DataOverTime[float32]{
								Data:         result.GroundSpeed.Value,
								TimestampUTC: currentTimeStamp,
							})
					}
				}
				if result.HeadingTrack.Valid == true {
					if len(value.Data.HeadingTrack) == 0 {
						newValue.Data.HeadingTrack = append(
							newValue.Data.HeadingTrack,
							DataOverTime[int]{
								Data:         result.HeadingTrack.Value,
								TimestampUTC: currentTimeStamp,
							})
					}
					if len(value.Data.HeadingTrack) >= 1 && value.Data.HeadingTrack[len(value.Data.HeadingTrack)-1].Data != result.HeadingTrack.Value {
						newValue.Data.HeadingTrack = append(
							newValue.Data.HeadingTrack,
							DataOverTime[int]{
								Data:         result.HeadingTrack.Value,
								TimestampUTC: currentTimeStamp,
							})
					}
				}
				if result.VerticalRate.Valid == true {
					if len(value.Data.VerticalRate) == 0 {
						newValue.Data.VerticalRate = append(
							newValue.Data.VerticalRate,
							DataOverTime[float32]{
								Data:         result.VerticalRate.Value,
								TimestampUTC: currentTimeStamp,
							})
					}
					if len(value.Data.VerticalRate) >= 1 && floatCompare(value.Data.VerticalRate[len(value.Data.VerticalRate)-1].Data, result.VerticalRate.Value, 0.01) &&
						floatCompare(value.Data.VerticalRate[len(value.Data.VerticalRate)-1].Data, result.VerticalRate.Value, 0.01) {
						newValue.Data.VerticalRate = append(
							newValue.Data.VerticalRate,
							DataOverTime[float32]{
								Data:         result.VerticalRate.Value,
								TimestampUTC: currentTimeStamp,
							})
					}
				}
				if result.SquawkCode.Valid == true {
					if len(value.Data.SquawkCode) == 0 {
						newValue.Data.SquawkCode = append(
							newValue.Data.SquawkCode,
							DataOverTime[int]{
								Data:         result.SquawkCode.Value,
								TimestampUTC: currentTimeStamp,
							})
					}
					if len(value.Data.SquawkCode) >= 1 && value.Data.SquawkCode[len(value.Data.SquawkCode)-1].Data != result.SquawkCode.Value {
						newValue.Data.SquawkCode = append(
							newValue.Data.SquawkCode,
							DataOverTime[int]{
								Data:         result.SquawkCode.Value,
								TimestampUTC: currentTimeStamp,
							})
					}
				}
				if result.Emergency.Valid == true {
					newValue.Data.Emergency = result.Emergency
				}

				itemQueue.append(newValue)*/
				/*if err := sto.Insert(newValue, simpleKeyCompare); err != nil {
					Log(err.Error(), ERROR)
				}*/
			}
		}

		// reset buffer
		for i := range currentMsg {
			currentMsg[i] = 0
		}
	//}
}

var CURRENT_TICK int = 0

func tick(ticker *time.Ticker, stop chan bool) {
	for {
		select {
		case <-stop:
			ticker.Stop()
			return
		case t := <-ticker.C:
			CURRENT_TICK = t.Second()
		}
	}
}

func scanForEntryIntoDB(
	ctx context.Context,
	db *database.Db,
	sto *storage.MapStorage[CollectedData],
	done chan bool,
	sessionLen int64,
	itemQueue *modifyStoQueue) {
	ticker := time.NewTicker(time.Second * 5)

	go tick(ticker, done)
	for {
		// nodesToDeleteFromSto := make([]int, 0)
		<-ticker.C
		Log("Checking for Aircraft to add to the database", INFO)
		doPerNode := func(item storage.MapItem[CollectedData]) {
			now := time.Now().UTC().UnixMilli()
			then := time.UnixMilli(item.Data.LastSeen).UTC().UnixMilli()
			if (now - then) >= 10000 {
				Log(fmt.Sprintf("Add storage %d getRidOfAt : %d", time.Now().UTC().UnixMilli(), (now-then)), INFO)
				headingTrack := traveseTheData[int](item.Data.HeadingTrack)
				altitude := traveseTheData[float32](item.Data.Altitude)
				groundSpeed := traveseTheData[float32](item.Data.GroundSpeed)
				verticalRate := traveseTheData[float32](item.Data.VerticalRate)
				squawkCode := traveseTheData[int](item.Data.SquawkCode)
				cordinates := traverseCordinatesOverTime(item.Data.Coordinates)

				insertErr := db.Insert(
					ctx,
					item.Data.LastSeen,
					item.Data.FirstSeen,
					item.Data.MsgCount,
					cordinates,
					item.Data.Icao,
					item.Data.TailNumber,
					altitude,
					groundSpeed,
					headingTrack,
					verticalRate,
					squawkCode,
					item.Data.Emergency.Value)
				if insertErr != nil {
					Log(fmt.Sprintf("Could not insert aircraft into db: %s", insertErr), ERROR)
				} else {
					Log(fmt.Sprintf("Marking entry from storage %s tailNumber: %s for deletion", item.Data.Icao, item.Data.TailNumber), INFO)
					// nodesToDeleteFromSto = append(nodesToDeleteFromSto, item.Key)
					itemQueue.delete(item)
				}
			}
		}
		itemQueue.checkForReadyToDelete(doPerNode)
	}
}

func traverseCordinatesOverTime(data []CordinatesOverTime) []byte {
	tempArr := make([]CordinatesOverTime, 0)
	for _, a := range data {
		tempArr = append(tempArr, a)
	}
	result, err := convertCordinatesOverTimeToJson(tempArr)
	if err != nil {
		Log(fmt.Sprintf("Failed to convert to json %s", err.Error()), ERROR)
		return nil
	}
	return result
}

func traveseTheData[T float32 | int](data []DataOverTime[T]) []byte {
	tempArr := make([]DataOverTime[T], 0)
	for _, a := range data {
		tempArr = append(tempArr, a)
	}
	result, err := convertDataOverTimeToJson(tempArr)
	if err != nil {
		Log(fmt.Sprintf("Failed to convert to json %s", err.Error()), ERROR)
		return nil
	}
	return result
}
