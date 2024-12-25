package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"owl_server/db/timescaledb"
	"owl_server/handlers"
	"syscall"
)

const PORT int = 3030
var database *timescaledb.TimescaleDB

func main() {
	database = &timescaledb.TimescaleDB{}
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
	go gracefulShutdown()

	http.HandleFunc("/receive", handlers.PostUpdates)
	log.Printf("Owl server listening on port %v", PORT)
	
	var port = fmt.Sprintf(":%d", PORT)
	var error = http.ListenAndServe(port, nil)
	if error != nil {
		log.Fatal(error)
	}
}

func gracefulShutdown() {
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    s := <-quit
	fmt.Println("Closing application", s)
	database.Disconnect()
    os.Exit(0)
}