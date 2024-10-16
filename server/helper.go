package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	//"errors"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lestrrat-go/jwx/jwt"
)

type jobList struct {
	lock sync.RWMutex
	list map[string]job
}

func (js *jobList) AddJob(j *job) {
	js.lock.Lock()
	defer js.lock.Unlock()

	js.list[j.ID] = *j
}

func (js *jobList) UpdateJob(id string, status string) {
	js.lock.Lock()
	defer js.lock.Unlock()

	if j, ok := js.list[id]; ok {
		j.Status = status
		js.list[id] = j
	}
}

func (js *jobList) List() []job {
	js.lock.RLock()
	defer js.lock.RUnlock()

	log.Println("job list count: ", len(js.list))
	log.Println("job list null? ", js.list == nil)

	var jobs []job
	for _, j := range js.list {
		jobs = append(jobs, j)
		log.Println("APPEND! new job list count: ", len(jobs))
	}

	log.Println("job list null? ", js.list == nil)
	return jobs
}

var jobs = jobList{
	lock: sync.RWMutex{},
	list: map[string]job{},
}

type job struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
	DataIDs         []string  `json:"data_ids"`
	DomainID        string    `json:"domain_id"`
	JobPath         string    `json:"job_path"`
	ProcessingType  string    `json:"processing_type"`
	Status          string    `json:"status"`
	CreatedAt       time.Time `json:"created_at"`
	AccessToken     string    `json:"-"`
	DomainServerURL string    `json:"domain_server_url"`
}

type JobRequestData struct {
	DataIDs         []string `json:"data_ids"`
	DomainID        string   `json:"domain_id"`
	AccessToken     string   `json:"access_token"`
	ProcessingType  string   `json:"processing_type"`
	DomainServerURL string   `json:"domain_server_url"` // Optional. Default: "issuer" of the incoming request, since jobs are triggered via the domain server.
}

type DomainDataMetadata struct {
	ID       string `json:"id"`
	DomainID string `json:"domain_id"`
	Token    string `json:"token"`
	EditableDomainDataMetadata
}

type EditableDomainDataMetadata struct {
	Name     string `json:"name"`
	DataType string `json:"data_type"`
}

type DomainData struct {
	DomainDataMetadata ``
	Data               io.ReadCloser `json:"-"`
}

var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")

var MaxBytesError = &http.MaxBytesError{}

func escapeQuotes(s string) string {
	return quoteEscaper.Replace(s)
}
func WriteDomainData(mw *multipart.Writer, data *DomainData) error {
	h := make(textproto.MIMEHeader)
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Disposition",
		fmt.Sprintf(
			`form-data; name="%s"; data-type="%s"; id="%s"; domain-id="%s"`,
			escapeQuotes(data.Name),
			escapeQuotes(data.DataType),
			escapeQuotes(data.ID),
			escapeQuotes(data.DomainID),
		),
	)
	fw, err := mw.CreatePart(h)
	if err != nil {
		return err
	}
	_, err = io.Copy(fw, data.Data)
	return err
}

func UploadDomainDataToDomain(j *job) error {
	// upload all files from j.OutputPath to the domain server
	outputPath := path.Join(j.JobPath, "refined", "global")
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		return err
	}

	expected_outputs := map[string]struct {
		name     string
		dataType string
	}{
		"refined_manifest.json": {
			name:     "refined_manifest",
			dataType: "refined_manifest_json",
		},
		"RefinedPointCloud.ply": {
			name:     "refined_pointcloud",
			dataType: "refined_pointcloud_ply",
		},
		"UnrefinedPointCloud.ply": {
			name:     "unrefined_pointcloud",
			dataType: "unrefined_pointcloud_ply",
		},
	}

	r, w := io.Pipe()
	mw := multipart.NewWriter(w)
	go func() {
		defer w.Close()
		for outputFile, outputData := range expected_outputs {
			f, err := os.Open(path.Join(outputPath, outputFile))
			if err != nil {
				log.Print(err)
				continue
			}

			nameSuffix := j.CreatedAt.Format("2006-01-02_15-04-05")
			if err := WriteDomainData(mw, &DomainData{
				DomainDataMetadata: DomainDataMetadata{
					EditableDomainDataMetadata: EditableDomainDataMetadata{
						Name:     outputData.name + "_" + nameSuffix,
						DataType: outputData.dataType,
					},
					DomainID: j.DomainID,
				},
				Data: f,
			}); err != nil {
				log.Print(err)
			}
		}
		mw.Close()
	}()

	req, err := http.NewRequest(http.MethodPost, j.DomainServerURL+"/api/v1/domains/"+j.DomainID+"/data", r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+j.AccessToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("domain server returned status %d", resp.StatusCode)
	}
	return nil
}

func DownloadDomainDataFromDomain(ctx context.Context, j *job, ids ...string) error {

	log.Printf("downloading %d data from domain %s", len(ids), j.DomainID)
	if len(ids) == 0 {
		return errors.New("no data ids provided")
	}

	scan_data_ids := strings.Join(ids, ",")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.DomainServerURL+"/api/v1/domains/"+j.DomainID+"/data?ids="+scan_data_ids, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", "Bearer "+j.AccessToken)
	req.Header.Add("Accept", "multipart/form-data")

	log.Println("Downloading data from domain, request:\n", req)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("domain server returned status %d", resp.StatusCode)
	}

	//if err := os.MkdirAll(path.Join(j.JobPath, "Frames"), 0755); err != nil {
	//	return err
	//}

	_, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return err
	}
	var i int
	mr := multipart.NewReader(resp.Body, params["boundary"])
	for {
		part, err := mr.NextPart()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		meta, err := ReadDomainDataMetadata(part)
		if err != nil {
			return err
		}

		// For now match multiple data objects from same scan by the timestamp in the name:
		// dmt_manifest_2024-09-27_01-13-50
		// dmt_pointcloud_2024-09-27_01-13-50
		// dmt_arposes_2024-09-27_01-13-50
		// dmt_portal_detections_2024-09-27_01-13-50
		// dmt_intrinsics_2024-09-27_01-13-50
		// dmt_frames_2024-09-27_01-13-50

		dateTimeRegex := regexp.MustCompile(`\d{4}-\d{2}-\d{2}[_-]\d{2}-\d{2}-\d{2}`)
		scanFolderName := ""
		if match := dateTimeRegex.FindString(meta.Name); match != "" {
			scanFolderName = match
		}

		var fileName string
		switch meta.DataType {
		case "dmt_manifest_json":
			fileName = "Manifest.json"
		case "dmt_featurepoints_ply", "dmt_pointcloud_ply":
			fileName = "FeaturePoints.ply"
		case "dmt_arposes_csv":
			fileName = "ARposes.csv"
		case "dmt_portal_detections_csv", "dmt_observations_csv":
			fileName = "PortalDetections.csv"
		case "dmt_intrinsics_csv", "dmt_cameraintrinsics_csv":
			fileName = "CameraIntrinsics.csv"
		case "dmt_frames_csv":
			fileName = "Frames.csv"
		case "dmt_gyro_csv":
			fileName = "Gyro.csv"
		case "dmt_accel_csv":
			fileName = "Accel.csv"
		case "dmt_gyroaccel_csv":
			fileName = "gyro_accel.csv"
		case "dmt_recording_mp4":
			fileName = "Frames.mp4"
		default:
			log.Printf("unknown domain data type: %s", meta.DataType)
			fileName = meta.Name + "." + meta.DataType
		}

		scanFolder := path.Join(j.JobPath, "datasets", scanFolderName)
		if err := os.MkdirAll(scanFolder, 0755); err != nil {
			return err
		}

		f, err := os.Create(path.Join(scanFolder, fileName))
		if err != nil {
			return err
		}
		defer f.Close()

		if _, err := io.Copy(f, part); err != nil {
			return err
		}

		i++
	}
	log.Printf("downloaded %d data objects from domain %s", i, j.DomainID)
	return nil
}

func ParseFramesCsv(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// frames.csv is a csv file has two columns, the first column is the timestamp, and the second column is the data id
	// we need to extract the data ids from the second column
	var ids []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ",")
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid line: %s", line)
		}
		ids = append(ids, parts[1])
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

func ReadDispositionParams(part *multipart.Part) (map[string]string, error) {
	d := part.Header.Get("Content-Disposition")
	var err error
	_, dispositionParams, err := mime.ParseMediaType(d)
	if err != nil {
		return nil, err
	}
	return dispositionParams, nil
}

func ReadDomainDataMetadata(part *multipart.Part) (*DomainDataMetadata, error) {
	dispositionParams, err := ReadDispositionParams(part)
	if err != nil {
		return nil, err
	}
	return &DomainDataMetadata{
		EditableDomainDataMetadata: EditableDomainDataMetadata{
			Name:     dispositionParams["name"],
			DataType: dispositionParams["data-type"],
		},
		DomainID: dispositionParams["domain-id"],
		Token:    dispositionParams["access-token"],
	}, nil
}

func ReadJobRequestFromJson(requestJson string) (*JobRequestData, error) {

	var jobRequest JobRequestData
	if err := json.Unmarshal([]byte(requestJson), &jobRequest); err != nil {
		return nil, err
	}

	// Debug printing the extracted metadata
	log.Printf("Parsed Metadata:\n")
	log.Printf("Data IDs: %s\n", jobRequest.DataIDs)
	log.Printf("DomainID: %s\n", jobRequest.DomainID)
	log.Printf("Processing Type: %s\n", jobRequest.ProcessingType)
	log.Printf("Access Token: %s\n", jobRequest.AccessToken)
	log.Printf("Domain Server URL: %s\n", jobRequest.DomainServerURL)

	return &jobRequest, nil
}

func CreateJob(dirPath string, requestJson string) (*job, error) {

	log.Println("Will mkdir path ", dirPath)
	if err := os.MkdirAll(dirPath, 0750); err != nil {
		return nil, err
	}

	log.Println("Refinement job requested")
	jobRequest, err := ReadJobRequestFromJson(requestJson)

	if err != nil {
		return nil, err
	}

	log.Println("Parsing domain access token: ", jobRequest.AccessToken)
	t, err := jwt.ParseString(jobRequest.AccessToken, jwt.WithValidate(false))
	if err != nil {
		log.Println("Error parsing domain access token from job request: ", err)
		return nil, err
	}

	domainServerURL := jobRequest.DomainServerURL
	if domainServerURL == "" {
		domainServerURL = t.Issuer()
		if domainServerURL == "" {
			return nil, errors.New("domain server URL is not set in job request or domain access token")
		}
		log.Println("Using domain server URL from domain access token: ", domainServerURL)

	} else {
		log.Println("Using domain server URL from job request: ", domainServerURL)
	}

	startTime := time.Now()
	jobID := uuid.NewString()
	jobName := "job_" + jobID

	j := job{
		CreatedAt:       startTime,
		ID:              jobID,
		Name:            jobName,
		DomainID:        jobRequest.DomainID,
		DataIDs:         jobRequest.DataIDs,
		ProcessingType:  jobRequest.ProcessingType,
		Status:          "started",
		DomainServerURL: domainServerURL,
		AccessToken:     jobRequest.AccessToken,
	}
	j.JobPath = path.Join(dirPath, jobRequest.DomainID, jobName)

	if err := os.MkdirAll(j.JobPath, 0755); err != nil {
		return nil, err
	}

	f, err := os.Create(path.Join(j.JobPath, "jobrequest"+j.ID))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// write the requestJson to the file for later checking
	if _, err := f.WriteString(requestJson); err != nil {
		return nil, err
	}

	//dataString := buf.String()
	log.Println("Data File:", f.Name())

	//destPath, err := unzipFile(f.Name(), path.Join(dirPath, "datasets"))
	//if err != nil {
	//	return nil, err
	//}

	log.Println("Downloading data from domain")
	if err := DownloadDomainDataFromDomain(context.Background(), &j, jobRequest.DataIDs...); err != nil {
		return nil, err
	}
	log.Println("Download succeeded")

	log.Println("Adding job to job list")
	jobs.AddJob(&j)
	log.Println("Job added to job list")

	return &j, nil
}

func executeJob(j *job) {
	refinementPython := "main.py"

	jobRootPath := path.Join(j.JobPath) // Parent of 'datasets' folder. Output will be under 'refined' subfolder.
	outputPath := path.Join(j.JobPath, "refined")
	logFilePath := path.Join(j.JobPath, "log.txt")

	params := []string{refinementPython, j.ProcessingType, jobRootPath, outputPath}

	datasetsRootPath := path.Join(jobRootPath, "datasets")
	if allScanFolders, err := os.ReadDir(datasetsRootPath); err != nil {
		log.Printf("job %s failed to read input directory: %s", j.ID, err)
		jobs.UpdateJob(j.ID, "failed")
		return
	} else {
		log.Printf("job %s read %d scan folders", j.ID, len(allScanFolders))
		for _, folder := range allScanFolders {
			params = append(params, folder.Name())
		}
	}

	startTime := time.Now()
	cmd := exec.Command("python3", params...)
	// Create log file
	logFile, err := os.Create(logFilePath)
	if err != nil {
		log.Printf("job %s failed to create log file: %s", j.ID, err)
		jobs.UpdateJob(j.ID, "failed")
		return
	}
	defer logFile.Close()

	// Write to both log file and stdout/stderr
	stdoutWriter := io.MultiWriter(logFile, os.Stdout)
	stderrWriter := io.MultiWriter(logFile, os.Stderr)
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrWriter

	log.Printf("job %s started, logging to %s", j.ID, logFilePath)

	// Run the refinement python
	if err := cmd.Start(); err != nil {
		log.Printf("job %s failed to start: %s", j.ID, err)
		jobs.UpdateJob(j.ID, "failed")
		return
	}

	if err := cmd.Wait(); err != nil {
		log.Printf("job %s failed: %s", j.ID, err)
		jobs.UpdateJob(j.ID, "failed")
		return
	}

	log.Printf("Refinement python script for job %s finished.", j.ID)
	timeTaken := time.Since(startTime)
	log.Printf("Refinement algorithm took %s", timeTaken)

	log.Printf("Going to upload results to domain %s", j.DomainID)

	if err := UploadDomainDataToDomain(j); err != nil {
		log.Printf("job %s failed to upload data: %s", j.ID, err)
		jobs.UpdateJob(j.ID, "failed")
		return
	}

	// remove the job directory (disable for now)
	// TODO: keep scan inputs downloaded in some local cache while still setting up the domain.
	/*if err := os.RemoveAll(j.JobPath); err != nil {
		log.Printf("job %s failed to remove output directory: %s", j.ID, err)
	}
	*/

	log.Printf("job %s succeeded!", j.ID)
	jobs.UpdateJob(j.ID, "succeeded")
}

// unzipFile unzips a file to a destination directory
/*
func unzipFile(zipPath, destDir string) (string, error) {
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return "", err
	}
	destPath := ""
	zipReader, err := zip.OpenReader(zipPath)
	if err != nil {
		return destPath, err
	}
	defer zipReader.Close()

	if len(zipReader.File) == 0 {
		return destPath, fmt.Errorf("zip file is empty")
	}

	destPath = path.Join(destDir, path.Base(path.Dir(zipReader.File[0].Name)))

	for _, file := range zipReader.File {
		filePath := filepath.Join(destDir, file.Name)
		if file.FileInfo().IsDir() { // if the file is a directory, create it
			os.MkdirAll(destPath, os.ModePerm)
			continue
		}

		if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
			return destPath, err
		}

		destFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
		if err != nil {
			return destPath, err
		}

		srcFile, err := file.Open()
		if err != nil {
			destFile.Close()
			return destPath, err
		}

		if _, err := io.Copy(destFile, srcFile); err != nil {
			destFile.Close()
			srcFile.Close()
			return destPath, err
		}

		destFile.Close()
		srcFile.Close()
	}

	return destPath, nil
}
*/
