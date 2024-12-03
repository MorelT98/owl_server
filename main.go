package main

import(
	"fmt"
	"log"
	"net/http"
	"owl_server/handlers"
)

const PORT int = 3000

func main() {
	http.HandleFunc("/receive", handlers.PostUpdates)
	log.Printf("Owl server listening on port %v", PORT)
	
	var port = fmt.Sprintf(":%d", PORT)
	var error = http.ListenAndServe(port, nil)
	if error != nil {
		log.Fatal(error)
	}
}