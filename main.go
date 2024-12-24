package main

import (
	"fmt"
	"log"
	"net/http"
	"owl_server/db/timescaledb"
	"owl_server/handlers"
)

const PORT int = 3030

func main() {
	var database = &timescaledb.TimescaleDB{}
	log.Printf("Connecting to the timescaledb database...")
	err := database.Connect()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Connected successfully. Creating tables (if needed)...")
	err = database.CreateTables()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Tables created! (Or they already existed.)")

	http.HandleFunc("/receive", handlers.PostUpdates)
	log.Printf("Owl server listening on port %v", PORT)
	
	var port = fmt.Sprintf(":%d", PORT)
	var error = http.ListenAndServe(port, nil)
	if error != nil {
		log.Fatal(error)
	}
}