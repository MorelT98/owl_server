package db

import (
	"owl_server/models"
)

// Interface for databases
type DB interface {
	// Connects to the given database
	Connect() error

	// Inserts the given update in the database.
	// Returns an error if the insertion fails
	InsertUpdate(update models.Update) error

	// Disconnects from the database.
	Disconnect() error
}