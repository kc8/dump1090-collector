package database

import (
	"context"
	sql "database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

// reference to db must always be ptr
type Db struct {
	databaseCon *sql.DB
	mutex       sync.Mutex
}

const (
	table_name = "aircraftData"
)

func New(sqlLitefilename string, sqlitePath string) (*Db, error) {
	dbName := func() string {
		if strings.Contains(*&sqlLitefilename, "/") {
			return sqlLitefilename
		}
		return "/" + sqlLitefilename
	}()
	fullDbPath := fmt.Sprintf("%s%s", sqlitePath, dbName)
	checkOrCreateEr := checkOrCreateDbFile(sqlitePath, fullDbPath)
	if checkOrCreateEr != nil {
		return nil, checkOrCreateEr
	}
	dbInstance, dbOpenErr := sql.Open("sqlite3", fullDbPath)
	if dbOpenErr != nil {
		return nil, dbOpenErr
	}
	result := &Db{
		databaseCon: dbInstance,
	}
	if err := result.TestConnnection(); err != nil {
		return nil, err
	}
	createTblErr := result.createTable()
	if createTblErr != nil {
		cleanResultErr := result.Clean()
		if cleanResultErr != nil {
			return nil, errors.New(fmt.Sprintf("[ERROR] Failed creating aircraft table and closing db due to various errors: %s AND %s", cleanResultErr.Error(), createTblErr.Error()))
		}
		return nil, errors.New(fmt.Sprintf("Failed to create Db due to: %s", createTblErr.Error()))
	}

	return result, nil
}

func checkOrCreateDbFile(dbPath string, fullDbPath string) error {
	_, erroPath := os.Stat(fullDbPath)
	_ = erroPath
	if erroPath != nil {
		mkErr := os.MkdirAll(dbPath, 755)
		if mkErr != nil {
			return mkErr
		}
		f, createErr := os.Create(fullDbPath)
		if mkErr != nil {
			return createErr
		}
		return f.Close()
	}
	return nil
}

func (d *Db) Clean() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	err := d.databaseCon.Close()
	if err != nil {
		return err
	}
	return nil
}

func (d *Db) Insert(ctx context.Context,
	lastSeen    int64,
	firstSeen   int64,
	msgCount    uint64,
	coordinates []byte,
	icao         string,
	tailNumber   string,
	altitude     []byte,
	groundSpeed  []byte,
	headingTrack []byte, 
	verticalRate []byte,
	squawkCode   []byte, 
	emergency    int) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	tx, dbbeginErr := d.databaseCon.BeginTx(ctx, nil)
	if dbbeginErr != nil {
		return dbbeginErr
	}
	defer tx.Commit()

	insertStatement := `
    insert into aircraftData(
        icao,
        firstSeen,
        lastSeen,
        msgCount,
        emergency,
        location,
        altitude,
        groundSpeed,
        headingTrack,
        squawkCode,
        verticalRate
    )
    values (
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?
    );
    `
	stmt, prepErr := tx.Prepare(insertStatement)
	if prepErr != nil {
		tx.Rollback()
		return prepErr
	}
	defer stmt.Close()
	exec, execErr := stmt.Exec(
       icao,
       firstSeen,
       lastSeen,
       msgCount,
       emergency,
       coordinates,
       altitude,
       groundSpeed,
       headingTrack,
       squawkCode,
       verticalRate)
	if execErr != nil {
		tx.Rollback()
		return execErr
	}
	numRowsEffected, err := exec.RowsAffected()
	if err != nil {
		tx.Rollback()
		return err
	}
	if numRowsEffected <= 0 {
		tx.Rollback()
		return errors.New(fmt.Sprintf("Expected to moodify more than 1 row but modified %d instead", numRowsEffected))
	}
	return nil
}

// TODO cordinate are location, remove cordinate 
func (d *Db) createTable() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	const CREATE_TABLE = `CREATE TABLE IF NOT EXISTS aircraftData (
        "icao" VARCHAR(64),
        "firstSeen" UNSIGNED BIG INT, 
        "lastSeen" UNSIGNED BIG INT, 
        "msgCount" UNSIGNED BIG INT,
        "emergency" BOOLEAN,
        "cordinate" jsonb,
        "location" jsonb,
        "altitude" jsonb,
        "groundSpeed" jsonb,
        "headingTrack" jsonb,
        "verticalRate" jsonb,
        "squawkCode" jsonb
        );
        `

	tx, dbbeginErr := d.databaseCon.Begin()
	defer tx.Commit()
	if dbbeginErr != nil {
		return dbbeginErr
	}
	exec, execErr := tx.Exec(CREATE_TABLE)
	if execErr != nil {
		tx.Rollback()
		return execErr
	}
	_, err := exec.RowsAffected()
	if err != nil {
		tx.Rollback()
		return err
	}
	return nil
}

func (d *Db) TestConnnection() error {
	return d.databaseCon.Ping()
}
