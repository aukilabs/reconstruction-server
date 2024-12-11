package main

import (
	"encoding/json"
	"flag"
	"io"

	"net/http"
	"net/http/httputil"
	"reflect"
	"sync"

	"github.com/aukilabs/go-tooling/pkg/errors"
	"github.com/aukilabs/go-tooling/pkg/logs"
	"github.com/go-chi/chi"
)

// Add a global variable to track if a job is in progress
var (
	jobInProgress bool
	jobMutex      sync.Mutex
)

func main() {

	apiKey := flag.String("api-key", "", "API key for the server")
	port := flag.String("port", ":8080", "Port to run the server on")
	loglevel := flag.String("log", "info", "Log Level")
	flag.Parse()

	// Configure logging to include file name, line number, and timestamp
	// log.SetFlags(log.Lshortfile | log.LstdFlags)
	logs.SetLevel(logs.ParseLevel(*loglevel))
	logs.Encoder = json.Marshal

	if apiKey == nil || *apiKey == "" {
		logs.Fatal(errors.New("API key is required"))
		// log.Fatal("API key is required")
	}
	// create a new router
	r := chi.NewRouter()

	// Endpoint for triggering refinement jobs (from DMT)
	r.Post("/jobs", func(w http.ResponseWriter, r *http.Request) {
		logs.Info("[POST] /jobs endpoint called")
		// log.Println("[POST] /jobs endpoint called")

		debug, _ := httputil.DumpRequest(r, true)
		logs.Debug(debug)
		// log.Printf("%s", debug)

		if apiKey != nil && *apiKey != "" {
			inApiKey := r.Header.Get("X-API-Key")
			if inApiKey != *apiKey {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}

		// Check if a job is already in progress
		jobMutex.Lock()
		if jobInProgress {
			jobMutex.Unlock()
			logs.Info("Job already in progress, rejecting incoming job request.")
			// log.Println("Job already in progress, rejecting incoming job request.")
			http.Error(w, "Reconstruction server is busy processing another job", http.StatusServiceUnavailable)
			return
		}
		jobInProgress = true
		jobMutex.Unlock()

		reqBodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			logs.Error(errors.New("Failed to read request body for job request: " + err.Error()))
			// log.Print("Failed to read request body for job request")
			// log.Print(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			jobMutex.Lock()
			jobInProgress = false
			jobMutex.Unlock()
			return
		}

		reqBodyString := string(reqBodyBytes)
		logs.Infof("Request body: %s", reqBodyString)
		// log.Printf("Request body: %s", reqBodyString)

		j, err := CreateJobMetadata("jobs", reqBodyString)
		if err != nil {
			logs.Error(errors.New("Job creation failed with error: " + err.Error()))
			// log.Print("Job creation failed with error: ", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			jobMutex.Lock()
			jobInProgress = false
			jobMutex.Unlock()
			return
		}

		go func(j job) {
			defer func() {
				jobMutex.Lock()
				jobInProgress = false
				jobMutex.Unlock()
			}()
			executeJob(&j)
		}(*j)

		w.WriteHeader(http.StatusOK)
	})

	r.Get("/jobs", func(w http.ResponseWriter, r *http.Request) {
		logs.Info("[GET] /jobs endpoint called")
		// log.Println("[GET] /jobs endpoint called")

		jobList := jobs.List()
		logs.Info("Number of jobs to list: ", len(jobList), " type: ", reflect.TypeOf(jobList))
		// log.Println("Number of jobs to list: ", len(jobList), " type: ", reflect.TypeOf(jobList))

		//debug, _ := httputil.DumpRequest(r, true)
		//log.Printf("%s", debug)

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		//dummy := map[string]string{"hello": "world"}
		//err := encoder.Encode(dummy)

		err := encoder.Encode(jobList)
		if err != nil {
			logs.Error(errors.Newf("Error encoding jobs list: %v", err))
			// log.Printf("Error encoding jobs list: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		logs.Info("Jobs list returned successfully")
	})
	// start the server
	logs.Info("Server running on ", *port)
	logs.Fatal(http.ListenAndServe(*port, r))
}
