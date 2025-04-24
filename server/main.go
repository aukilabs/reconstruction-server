package main

import (
	"encoding/json"
	"flag"
	"io"
	"os"

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
	loglevel := flag.String("log-level", "info", "Log Level")
	numCpuWorkers := flag.Int("cpu-workers", 2, "Number of CPU workers for local refinement")
	jobRequestPath := flag.String("job-request", "", "Path to job request JSON file to process a single job")
	flag.Parse()

	// Configure logging to include file name, line number, and timestamp
	logs.SetLevel(logs.ParseLevel(*loglevel))
	logs.Encoder = json.Marshal

	// If job request file is provided, process single job
	if *jobRequestPath != "" {
		logs.Info("Processing single job from request file: ", *jobRequestPath)

		// Read job request file
		reqBytes, err := os.ReadFile(*jobRequestPath)
		if err != nil {
			logs.Fatal(errors.Newf("Failed to read job request file: %v", err))
		}

		// Create job metadata
		j, err := CreateJobMetadata("jobs", string(reqBytes), "localhost") // Using localhost since we're running locally
		if err != nil {
			logs.Fatal(errors.Newf("Job creation failed: %v", err))
		}

		// Execute job
		executeJob(j, *numCpuWorkers)
		return // Exit after processing single job
	}

	// Normal server mode
	if apiKey == nil || *apiKey == "" {
		logs.Fatal(errors.New("API key is required"))
	}
	// create a new router
	r := chi.NewRouter()

	// Endpoint for triggering refinement jobs (from DMT)
	r.Post("/jobs", func(w http.ResponseWriter, r *http.Request) {
		logs.Info("[POST] /jobs endpoint called")

		debug, _ := httputil.DumpRequest(r, true)
		logs.Debug(debug)

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
			http.Error(w, "Reconstruction server is busy processing another job", http.StatusServiceUnavailable)
			return
		}
		jobInProgress = true
		jobMutex.Unlock()

		// Read request body
		reqBodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			logs.Error(errors.New("Failed to read request body for job request: " + err.Error()))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			jobMutex.Lock()
			jobInProgress = false
			jobMutex.Unlock()
			return
		}
		reqBodyString := string(reqBodyBytes)
		logs.Infof("Request body: %s", reqBodyString)

		reconstructionServerURL := r.Host

		// Create job metadata
		j, err := CreateJobMetadata("jobs", reqBodyString, reconstructionServerURL)
		if err != nil {
			logs.Error(errors.New("Job creation failed with error: " + err.Error()))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			jobMutex.Lock()
			jobInProgress = false
			jobMutex.Unlock()
			return
		}

		// Execute Job
		go func(j job) {
			defer func() {
				jobMutex.Lock()
				jobInProgress = false
				jobMutex.Unlock()
			}()
			executeJob(&j, *numCpuWorkers)
		}(*j)

		w.WriteHeader(http.StatusOK)
	})

	// Endpoint for fetching current job list
	r.Get("/jobs", func(w http.ResponseWriter, r *http.Request) {
		logs.Info("[GET] /jobs endpoint called")

		debug, _ := httputil.DumpRequest(r, true)
		logs.Debug(debug)

		jobList := jobs.List()
		logs.Info("Number of jobs to list: ", len(jobList), " type: ", reflect.TypeOf(jobList))

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")

		err := encoder.Encode(jobList)
		if err != nil {
			logs.Error(errors.Newf("Error encoding jobs list: %v", err))
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		logs.Info("Jobs list returned successfully")
	})
	// start the server
	logs.Info("Server running on ", *port)
	logs.Fatal(http.ListenAndServe(*port, r))
}
