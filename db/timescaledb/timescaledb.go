package timescaledb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"owl_server/models"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

const USER = "morel"
type TimescaleDB struct {
	dbPool *pgxpool.Pool
}

type ConnectionConfig struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Host string `json:"host"`
	Port string `json:"port"`
	Database string `json:"database"`
	SSL bool `json:"ssl"`
}

func getConnectionString() (string, error) {
	configString, err := os.ReadFile("connectionConfigs/timescaledbConnectionConfig.json")
	if err != nil {
		return "", fmt.Errorf("unable to retrieve connection string. Underlying error: %s", err.Error())
	}
	var config ConnectionConfig
	err = json.Unmarshal(configString, &config)
	if err != nil {
		return "", fmt.Errorf("unable to retrieve connection string. Underlying error: %s", err.Error())
	}
	var ssl string
	if config.SSL {
		ssl = "?sslmode=require"
	} else {
		ssl = ""
	}
	return "postgres://" + config.Username + ":" + url.QueryEscape(config.Password) + "@" + config.Host + ":" + config.Port + "/" + config.Database + ssl, nil
}

func (db *TimescaleDB) Connect() error {
	connectionString, err := getConnectionString()
	if err != nil {
		return err
	}
	dbPool, err := pgxpool.Connect(context.Background(), connectionString)
	if err != nil {
		return err
	}
	db.dbPool = dbPool
	return nil
}

func (db *TimescaleDB) Disconnect() error {
	if db.dbPool == nil {
		return nil
	}
	log.Printf("Disconnecting from the database...")
	db.dbPool.Close()
	log.Printf("Disconnected successfully.")
	return nil
}

func (db *TimescaleDB) CreateTables() error {
	ctx := context.Background()

	// Create EVENT table
	_, err := db.dbPool.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            event_name TEXT NOT NULL,
            creation_time TIMESTAMPTZ NULL,
            event_result TEXT NULL
        )
    `)
	if err != nil {
		return err
	}

	// Create STEPS table
    _, err = db.dbPool.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS steps (
            step_id TEXT PRIMARY KEY,
            step_name TEXT NOT NULL,
            event_id TEXT REFERENCES events(event_id),
            creation_time TIMESTAMPTZ NULL,
            step_number INTEGER NOT NULL
        )
    `)
    if err != nil {
        return err
    }

	// Create LABELS table
    _, err = db.dbPool.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS labels (
            label_id TEXT PRIMARY KEY,
            step_id TEXT REFERENCES steps(step_id),
            key TEXT NOT NULL,
            value TEXT NOT NULL
        )
    `)
    if err != nil {
        return err
    }

	// Convert STEPS table to a hypertable
	// Skip this for now, getting the error:
	// cannot create a unique index without the column "creation_time" (used in partitioning) (SQLSTATE TS103)
    // _, err = db.dbPool.Exec(ctx, `
    //     SELECT create_hypertable('steps', 'creation_time', if_not_exists => TRUE)
    // `)
	// if err != nil {
	// 	return err
	// }

	return nil
}

func (db *TimescaleDB) InsertUpdate(update models.Update) error {
	if db.dbPool == nil {
		return fmt.Errorf("database is disconnected")
	}
	switch update.UpdateType {
	case models.UPDATE_TYPE_START:
		return db.insertStartUpdate(update)
	case models.UPDATE_TYPE_STEP:
		return db.insertStepUpdate(update)
	case models.UPDATE_TYPE_LABEL:
		return db.insertLabelUpdate(update)
	case models.UPDATE_TYPE_END:
		return db.insertEndUpdate(update)
	default:
		return fmt.Errorf("unknown update: %v", update.UpdateType)
	}
}

func (db *TimescaleDB) createEvent(eventName string, eventID string, creationTime int64) error {
	ctx := context.Background()
	dbEventId := getDBEventID(eventName, eventID)
	if creationTime <= 0 {
		_, err := db.dbPool.Exec(ctx, `
		INSERT INTO events (event_id, event_name)
		VALUES ($1, $2)
		`, dbEventId, eventName)
		return err
	} else {
		_, err := db.dbPool.Exec(ctx, `
		INSERT INTO events (event_id, event_name, creation_time)
		VALUES ($1, $2, $3)
		ON CONFLICT (event_id)
		DO UPDATE SET creation_time = EXCLUDED.creation_time
		`, dbEventId, eventName, getConvertedTimestamp(creationTime))
		return err
	}
}

func (db *TimescaleDB) insertStartUpdate(update models.Update) error {
	return db.createEvent(update.EventName, update.EventId, update.Timestamp)
}

func (db *TimescaleDB) createStep(eventName string, eventID string, stepName string, stepNumber int, timestamp int64) error {
	ctx := context.Background()

	// Check if event exists. If not, create it
	dbEventId := getDBEventID(eventName, eventID)
	var exists bool
	err := db.dbPool.QueryRow(ctx, `
		SELECT EXISTS ( SELECT 1 FROM events WHERE event_id = $1 )
		`, dbEventId).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		// Create event
		err = db.createEvent(eventName, eventID, -1)
		if err != nil {
			return err
		}
	}

	// Once events exist, create step
	stepID := getStepID(eventName, eventID, stepName, stepNumber)
	if timestamp <= 0 {
		_, err = db.dbPool.Exec(ctx, `
			INSERT INTO steps (step_id, step_name, event_id, step_number)
			VALUES ($1, $2, $3, $4)
		`, stepID, stepName, dbEventId, stepNumber)
	} else {
		_, err = db.dbPool.Exec(ctx, `
		INSERT INTO steps (step_id, step_name, event_id, creation_time, step_number)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (step_id)
		DO UPDATE SET creation_time = EXCLUDED.creation_time
		`, stepID, stepName, dbEventId, getConvertedTimestamp(timestamp), stepNumber)
	}
	return err
}

func (db *TimescaleDB) insertStepUpdate(update models.Update) error {
	return db.createStep(update.EventName, update.EventId, update.StepName, update.StepNumber, update.Timestamp)
}

func (db *TimescaleDB) insertLabelUpdate(update models.Update) error {
	// Check if step exists. If not, create it
	labelID := getLabelID(update.EventName, update.EventId, update.StepName, update.StepNumber, update.LabelKey)
	stepID := getStepID(update.EventName, update.EventId, update.StepName, update.StepNumber)
	ctx := context.Background()
	var exists bool
	err := db.dbPool.QueryRow(ctx, `
		SELECT EXISTS ( SELECT 1 FROM steps WHERE step_id = $1 )
		`, stepID).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		// Create step
		err = db.createStep(update.EventName, update.EventId, update.StepName, update.StepNumber, -1)
		if err != nil {
			return err
		}
	}

	// Now insert the label
	_, err = db.dbPool.Exec(ctx, `
		INSERT INTO labels (label_id, step_id, key, value)
        VALUES ($1, $2, $3, $4)
		ON CONFLICT (label_id)
		DO UPDATE SET value = EXCLUDED.value
    `, labelID, stepID, update.LabelKey, update.LabelVal)
	return err
}

func (db *TimescaleDB) insertEndUpdate(update models.Update) error {
	// Update the event with the result
	ctx := context.Background()
	eventID := getDBEventID(update.EventName, update.EventId)
    _, err := db.dbPool.Exec(ctx, `
        UPDATE events
        SET event_result = $1
        WHERE event_id = $2
    `, update.Result, eventID)
    if err != nil {
		return err
	}

	// Insert the end step
	return db.createStep(update.EventName, update.EventId, "end", update.StepNumber, update.Timestamp)
}

// Converts an eventID to a db event ID
// an eventID is simply a UUID generated on the client
// A DBEventID is a combination of that UUID, the event name and the user
// 	The DBEventID will be used as a key in the events database
func getDBEventID(eventName string, eventID string) string {
	return fmt.Sprintf("%s-%s-%s", USER, eventName, eventID)
}

// Returns the stepID, which will be used as key in the steps database
func getStepID(eventName string, eventID string, stepName string, stepNumber int) string {
	dbEventId := getDBEventID(eventName, eventID)
	return fmt.Sprintf("%s-%s-%d", dbEventId, stepName, stepNumber)
}

// Returns the labelID, which will be used as key in the labels database
func getLabelID(eventName string, eventID string, stepName string, stepNumber int, labelKey string) string{
	stepID := getStepID(eventName, eventID, stepName, stepNumber)
	return fmt.Sprintf("%s-%s", stepID, labelKey)
}

func getConvertedTimestamp(timestamp int64) string {
	referenceDate := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
    t := referenceDate.Add(time.Duration(timestamp) * time.Millisecond)
	// Format the time for TimescaleDB (RFC3339 format)
    formattedTime := t.Format(time.RFC3339)
	return formattedTime
}