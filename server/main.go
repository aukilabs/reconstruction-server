package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"reflect"

	"github.com/go-chi/chi"
)

func main() {

	// Configure logging to include file name, line number, and timestamp
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	apiKey := flag.String("api-key", "", "API key for the server")
	port := flag.String("port", ":8080", "Port to run the server on")
	flag.Parse()

	if apiKey == nil || *apiKey == "" {
		log.Fatal("API key is required")
	}
	// create a new router
	r := chi.NewRouter()

	// Endpoint for triggering refinement jobs (from DMT)
	r.Post("/jobs", func(w http.ResponseWriter, r *http.Request) {
		log.Println("[POST] /jobs endpoint called")

		debug, _ := httputil.DumpRequest(r, true)
		log.Printf("%s", debug)

		if apiKey != nil && *apiKey != "" {
			inApiKey := r.Header.Get("X-API-Key")
			if inApiKey != *apiKey {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}

		reqBodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			log.Print("Failed to read request body for job request")
			log.Print(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBodyString := string(reqBodyBytes)
		log.Printf("Request body: %s", reqBodyString)

		j, err := CreateJob("jobs", reqBodyString)
		if err != nil {
			log.Print("Job failed with error: ", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		go func(j job) {
			executeJob(&j)
		}(*j)

		w.WriteHeader(http.StatusOK)
	})

	r.Get("/jobs", func(w http.ResponseWriter, r *http.Request) {
		log.Println("[GET] /jobs endpoint called")

		jobList := jobs.List()
		log.Println("Number of jobs to list: ", len(jobList), " type: ", reflect.TypeOf(jobList))

		//debug, _ := httputil.DumpRequest(r, true)
		//log.Printf("%s", debug)

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		//dummy := map[string]string{"hello": "world"}
		//err := encoder.Encode(dummy)

		err := encoder.Encode(jobList)
		if err != nil {
			log.Printf("Error encoding jobs list: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		log.Println("Jobs list returned successfully")
	})
	// start the server
	log.Print("Server running on ", *port)
	log.Fatal(http.ListenAndServe(*port, r))
}
