package handlers

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"owl_server/db"
	"owl_server/db/mongodb"
	"owl_server/models"
)

// Handler for post requests.
// Parses the response body as an array of models.Update
// objects, and forwards each update to the database
// to save them.
func PostUpdates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// Parsing logic
	var updates []models.Update

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = json.Unmarshal(body, &updates)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// db logic
	var database db.DB = &mongodb.MongoDB{}
	err = database.Connect()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer func() {
		err := database.Disconnect()
		if err != nil {
			log.Fatalf("error while saving update: %s", err)
		}
	}()

	for _, update := range updates {
		err := database.InsertUpdate(update)
		if err != nil {
			log.Printf("error while saving update: %s\n", err)
		}
	}

	w.WriteHeader(http.StatusCreated)
}