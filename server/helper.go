package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"

	"fmt"
	"io"

	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aukilabs/go-tooling/pkg/errors"
	"github.com/aukilabs/go-tooling/pkg/logs"
	"github.com/google/uuid"
	"github.com/lestrrat-go/jwx/jwt"
)

type ExpectedOutput struct {
	FilePath string // relative to job folder
	Name     string
	DataType string
}

type jobList struct {
	lock sync.RWMutex
	list map[string]job
}

func (js *jobList) AddJob(j *job) {
	js.lock.Lock()
	defer js.lock.Unlock()

	js.list[j.ID] = *j
}

func WriteJobManifestFile(j *job, status string) {
	if status == "failed" {
		/*
			outputCount, err := UploadRefinedOutputsToDomain(j)
			if err != nil {
				log.Printf("job %s failed inside 'UpdateJobManifestFile', couldn't upload refined outputs: %s", j.ID, err)
			}
			if outputCount == 0 {
				log.Printf("job %s python produced no refined outputs. Upload basic failed manifest instead.", j.ID)
			}
		*/
		WriteFailedJobManifestFile(j, "Reconstruction job script failed")
	} else if status == "processing" {
		progress := 0
		WriteJobManifestFileHelper(j, status, progress, "Request received by reconstruction server")
	} else {
		return
	}

	err := UploadJobManifestToDomain(j)
	if err != nil {
		logs.WithTag("job_id", j.ID).
			WithTag("domain_id", j.DomainID).
			Error(errors.New("failed to upload job manifest to domain").Wrap(err))
	}
}

func (js *jobList) UpdateJob(id string, status string) {
	js.lock.Lock()
	j, ok := js.list[id]
	if ok {
		j.Status = status
		js.list[id] = j
	}
	js.lock.Unlock()

	if ok {
		WriteJobManifestFile(&j, status)
	}
}

func (js *jobList) List() []job {
	js.lock.RLock()
	defer js.lock.RUnlock()

	logs.Info("job list count: ", len(js.list))
	logs.Info("job list null? ", js.list == nil)
	// log.Println("job list count: ", len(js.list))
	// log.Println("job list null? ", js.list == nil)

	var jobs []job
	for _, j := range js.list {
		jobs = append(jobs, j)
		logs.Info("APPEND! new job list count: ", len(jobs))
		// log.Println("APPEND! new job list count: ", len(jobs))
	}
	logs.Info("job list null? ", js.list == nil)
	// log.Println("job list null? ", js.list == nil)
	return jobs
}

var jobs = jobList{
	lock: sync.RWMutex{},
	list: map[string]job{},
}

type job struct {
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	DataIDs         []string          `json:"data_ids"`
	DomainID        string            `json:"domain_id"`
	JobPath         string            `json:"job_path"`
	ProcessingType  string            `json:"processing_type"`
	Status          string            `json:"status"`
	UploadedDataIDs map[string]string `json:"-"`
	CreatedAt       time.Time         `json:"created_at"`
	AccessToken     string            `json:"-"`
	DomainServerURL string            `json:"domain_server_url"`
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

type PostDomainDataResponse struct {
	Data []DomainDataMetadata `json:"data"`
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

func WriteFailedJobManifestFile(j *job, errorMessage string) error {
	pythonSnippet := `
from utils.data_utils import save_failed_manifest_json; 
save_failed_manifest_json('` + j.JobPath + `/job_manifest.json', '` + errorMessage + `')
`
	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Infof("writing failed manifest, error message: %s", errorMessage)

	cmd := exec.Command("python3", "-c", pythonSnippet)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func WriteJobManifestFileHelper(j *job, status string, progress int, statusDetails string) error {
	pythonSnippet := `
from utils.data_utils import save_manifest_json;
save_manifest_json({},
	'` + j.JobPath + `/job_manifest.json',
	jobStatus='` + status + `',
	jobProgress=` + strconv.Itoa(progress) + `,
	jobStatusDetails='` + statusDetails + `'
)`

	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Info("Writing manifest with status: ", status, ", progress: ", progress, ", status details: ", statusDetails)

	cmd := exec.Command("python3", "-c", pythonSnippet)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func UploadJobManifestToDomain(j *job) error {
	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Infof("Upload job manifest to domain, for job")

	output := ExpectedOutput{
		FilePath: "job_manifest.json",
		Name:     "refined_manifest",
		DataType: "refined_manifest_json",
	}

	return UploadOutputToDomain(j, output)
}

func UploadRefinedOutputsToDomain(j *job) (int, error) {
	refinedOutput := path.Join("refined", "global")
	expectedOutputs := []ExpectedOutput{
		{
			FilePath: path.Join(refinedOutput, "refined_manifest.json"),
			Name:     "refined_manifest",
			DataType: "refined_manifest_json",
		},
		{
			FilePath: path.Join(refinedOutput, "RefinedPointCloud.ply"),
			Name:     "refined_pointcloud",
			DataType: "refined_pointcloud_ply",
		},
		{
			// The unrefined point cloud after just basic stitch from overlap QR codes
			// Not really useful to apps, but for debugging the refinement
			FilePath: path.Join(refinedOutput, "BasicStitchPointCloud.ply"),
			Name:     "unrefined_pointcloud",
			DataType: "unrefined_pointcloud_ply",
		},
		/*{
			FilePath: path.Join(refinedOutput, "occlusion", "meshes.obj"),
			Name:     "occlusionmesh_v1",
			DataType: "obj",
		},*/
	}

	outputCount := 0
	for _, output := range expectedOutputs {
		if _, err := os.Stat(path.Join(j.JobPath, output.FilePath)); !os.IsNotExist(err) {
			outputCount++
		}
	}

	// Upload manifest using PUT since it already exists from start of the job
	if err := UploadOutputToDomain(j, expectedOutputs[0]); err != nil {
		return outputCount, errors.New("failed to upload refined manifest to domain").Wrap(err)
	}

	if err := UploadOutputsToDomain(j, expectedOutputs[1:]); err != nil {
		return outputCount, errors.New("failed to upload refined outputs to domain").Wrap(err)
	}

	return outputCount, nil
}

func UploadOutputsToDomain(j *job, expectedOutputs []ExpectedOutput) error {
	firstErr := error(nil)
	for _, output := range expectedOutputs {
		if err := UploadOutputToDomain(j, output); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func UploadOutputToDomain(j *job, output ExpectedOutput) error {
	outputPath := j.JobPath
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		return err
	}

	f, err := os.Open(path.Join(outputPath, output.FilePath))
	if err != nil {
		return fmt.Errorf("failed to open output file %s: %s", output.FilePath, err.Error())
	}
	defer f.Close()

	nameSuffix := j.CreatedAt.Format("2006-01-02_15-04-05")
	domainData := DomainData{
		DomainDataMetadata: DomainDataMetadata{
			EditableDomainDataMetadata: EditableDomainDataMetadata{
				Name:     output.Name + "_" + nameSuffix,
				DataType: output.DataType,
			},
			DomainID: j.DomainID,
		},
		Data: f,
	}

	httpMethod := http.MethodPost
	alreadyUploadedID := j.UploadedDataIDs[output.Name+"."+output.DataType]
	if alreadyUploadedID != "" {
		logs.WithTag("job_id", j.ID).
			WithTag("domain_id", j.DomainID).
			Infof("%s.%s already uploaded. Updating it instead.", output.Name, output.DataType)
		domainData.ID = alreadyUploadedID
		httpMethod = http.MethodPut
	}

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	if err := WriteDomainData(writer, &domainData); err != nil {
		return fmt.Errorf("failed to write domain data to message body: %s", err.Error())
	}

	if err := writer.Close(); err != nil {
		return err
	}

	reqUrl := j.DomainServerURL + "/api/v1/domains/" + j.DomainID + "/data"
	req, err := http.NewRequest(httpMethod, reqUrl, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+j.AccessToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("domain server returned status %d", resp.StatusCode)
	}

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Infof("Uploaded domain data! response: %s", string(responseBody))
	var parsedResp PostDomainDataResponse
	if err := json.Unmarshal(responseBody, &parsedResp); err != nil {
		return err
	}
	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Infof("Uploaded domain data! parsed response: %+v", parsedResp)
	j.UploadedDataIDs[output.Name+"."+output.DataType] = parsedResp.Data[0].ID
	return nil
}

func DownloadDomainDataFromDomain(ctx context.Context, j *job, ids ...string) error {

	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Infof("downloading %d data from domain", len(ids))
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

	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Info("Downloading data from domain, request:\n", req)

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
			logs.Infof("unknown domain data type: %s", meta.DataType)
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
	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Infof("downloaded %d data objects from domain", i)
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
	}, nil
}

func ReadJobRequestFromJson(requestJson string) (*JobRequestData, error) {

	var jobRequest JobRequestData
	if err := json.Unmarshal([]byte(requestJson), &jobRequest); err != nil {
		return nil, err
	}

	// Debug printing the extracted metadata
	logs.Debug("Parsed Metadata:\n")
	logs.Debug("Data IDs: %s\n", jobRequest.DataIDs)
	logs.Debug("DomainID: %s\n", jobRequest.DomainID)
	logs.Debug("Processing Type: %s\n", jobRequest.ProcessingType)
	logs.Debug("Access Token: %s\n", jobRequest.AccessToken)
	logs.Debug("Domain Server URL: %s\n", jobRequest.DomainServerURL)

	return &jobRequest, nil
}

func CreateJobMetadata(dirPath string, requestJson string) (*job, error) {

	logs.Info("Will mkdir path ", dirPath)
	if err := os.MkdirAll(dirPath, 0750); err != nil {
		return nil, err
	}

	logs.Info("Refinement job requested")
	jobRequest, err := ReadJobRequestFromJson(requestJson)

	if err != nil {
		return nil, err
	}

	logs.WithTag("domain_id", jobRequest.DomainID).
		Info("Parsing domain access token: ", jobRequest.AccessToken)
	t, err := jwt.ParseString(jobRequest.AccessToken, jwt.WithValidate(false))
	if err != nil {
		return nil, errors.New("Error parsing domain access token from job request").
			WithTag("domain_id", jobRequest.DomainID).Wrap(err)
	}

	domainServerURL := jobRequest.DomainServerURL
	if domainServerURL == "" {
		domainServerURL = t.Issuer()
		if domainServerURL == "" {
			return nil, errors.New("domain server URL is not set in job request or domain access token")
		}
		logs.WithTag("domain_id", jobRequest.DomainID).
			Info("Using domain server URL from domain access token: ", domainServerURL)

	} else {
		logs.WithTag("domain_id", jobRequest.DomainID).
			Info("Using domain server URL from job request: ", domainServerURL)
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
		UploadedDataIDs: map[string]string{},
	}
	j.JobPath = path.Join(dirPath, jobRequest.DomainID, jobName)

	if err := os.MkdirAll(j.JobPath, 0755); err != nil {
		return nil, errors.New("failed to create job directory").Wrap(err).
			WithTag("job_id", j.ID).
			WithTag("domain_id", j.DomainID)
	}

	f, err := os.Create(path.Join(j.JobPath, "jobrequest"+j.ID))
	if err != nil {
		return nil, errors.New("failed to create jobrequest file").Wrap(err).
			WithTag("job_id", j.ID).
			WithTag("domain_id", j.DomainID)
	}
	defer f.Close()

	// write the requestJson to the file for later checking
	if _, err := f.WriteString(requestJson); err != nil {
		return nil, errors.New("failed to write jobrequest file").Wrap(err).
			WithTag("job_id", j.ID).
			WithTag("domain_id", j.DomainID)
	}

	//dataString := buf.String()
	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Info("Data File:", f.Name())

	//destPath, err := unzipFile(f.Name(), path.Join(dirPath, "datasets"))
	//if err != nil {
	//	return nil, err
	//}

	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Info("Adding job to job list")
	jobs.AddJob(&j)
	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Info("Job added to job list")

	return &j, nil
}

func executeJob(j *job) {

	// Write in-progress manifest as soon as job starts.
	// DMT uses this to show job status to the user.
	WriteJobManifestFile(j, "processing")

	// Download domain data in batches of 20 ids
	batchSize := 20
	for i := 0; i < len(j.DataIDs); i += batchSize {
		end := i + batchSize
		if end > len(j.DataIDs) {
			end = len(j.DataIDs)
		}
		batch := j.DataIDs[i:end]

		if err := DownloadDomainDataFromDomain(context.Background(), j, batch...); err != nil {
			logs.WithTag("job_id", j.ID).
				WithTag("domain_id", j.DomainID).
				Error(errors.Newf("failed to download data batch %d-%d", i, end).Wrap(err))
			jobs.UpdateJob(j.ID, "failed")
			return
		}
	}

	refinementPython := "main.py"

	jobRootPath := path.Join(j.JobPath) // Parent of 'datasets' folder. Output will be under 'refined' subfolder.
	outputPath := path.Join(j.JobPath, "refined")
	logFilePath := path.Join(j.JobPath, "log.txt")

	params := []string{
		refinementPython,
		"--mode", j.ProcessingType,
		"--job_root_path", jobRootPath,
		"--output", outputPath,
		"--domain_id", j.DomainID,
		"--job_id", j.Name,
		"--scans"}

	datasetsRootPath := path.Join(jobRootPath, "datasets")
	if allScanFolders, err := os.ReadDir(datasetsRootPath); err != nil {
		logs.WithTag("job_id", j.ID).
			WithTag("domain_id", j.DomainID).
			Error(errors.Newf("failed to to read input directory").Wrap(err))
		jobs.UpdateJob(j.ID, "failed")
		return
	} else {
		logs.WithTag("job_id", j.ID).
			WithTag("domain_id", j.DomainID).
			Infof("read %d scan folders", len(allScanFolders))
		for _, folder := range allScanFolders {
			params = append(params, folder.Name())
		}
	}

	startTime := time.Now()
	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Debugf("executing main.py with params: %s", params)
	cmd := exec.Command("python3", params...)
	// Create log file
	logFile, err := os.Create(logFilePath)
	if err != nil {
		logs.WithTag("job_id", j.ID).
			WithTag("domain_id", j.DomainID).
			Error(errors.Newf("failed to create log file").Wrap(err))
		jobs.UpdateJob(j.ID, "failed")
		return
	}
	defer logFile.Close()

	// Write to both log file and stdout/stderr
	stdoutWriter := io.MultiWriter(logFile, os.Stdout)
	stderrWriter := io.MultiWriter(logFile, os.Stderr)
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrWriter

	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Infof("started, logging to %s", logFilePath)

	// Run the refinement python
	if err := cmd.Start(); err != nil {
		logs.WithTag("job_id", j.ID).
			WithTag("domain_id", j.DomainID).
			Error(errors.Newf("job failed to start").Wrap(err))
		jobs.UpdateJob(j.ID, "failed")
		return
	}
	// Monitor progress in a separate goroutine
	progressDone := make(chan bool)
	go func() {
		datasetsPath := path.Join(jobRootPath, "datasets")
		refinedPath := path.Join(jobRootPath, "refined", "local")

		for {
			select {
			case <-progressDone:
				return
			default:
				time.Sleep(10 * time.Second)

				// Get total number of datasets
				datasetFolders, err := os.ReadDir(datasetsPath)
				if err != nil {
					logs.WithTag("job_id", j.ID).
						WithTag("domain_id", j.DomainID).
						Error(errors.Newf("Error reading datasets directory").Wrap(err))
					return
				}
				totalCount := len(datasetFolders)

				// Check number of completed datasets by matching dataset names
				refinedCount := 0
				for _, dataset := range datasetFolders {
					if _, err := os.Stat(path.Join(refinedPath, dataset.Name())); err == nil {
						refinedCount++
					}
				}
				refinedCount-- // Last folder created is still refining, remove it
				if refinedCount < 0 {
					refinedCount = 0 // Just in case
				}
				progress := int((float64(refinedCount) / float64(totalCount)) * 100)
				if progress > 100 {
					progress = 100
				}

				// Update manifest with current progress
				statusText := fmt.Sprintf("Processed %d of %d scans", refinedCount, totalCount)
				logs.WithTag("job_id", j.ID).
					WithTag("domain_id", j.DomainID).
					Infof("progress: %d%% - %s", progress, statusText)

				err = WriteJobManifestFileHelper(j, "processing", progress, statusText)
				if err != nil {
					logs.WithTag("job_id", j.ID).
						WithTag("domain_id", j.DomainID).
						Error(errors.Newf("failed to write job manifest").Wrap(err))
				}

				err = UploadJobManifestToDomain(j)
				if err != nil {
					logs.WithTag("job_id", j.ID).
						WithTag("domain_id", j.DomainID).
						Error(errors.Newf("failed to upload job manifest").Wrap(err))
				}

				time.Sleep(10 * time.Second)
			}
		}
	}()

	if err := cmd.Wait(); err != nil {
		progressDone <- true
		logs.WithTag("job_id", j.ID).
			WithTag("domain_id", j.DomainID).
			Error(errors.Newf("job failed").Wrap(err))
		jobs.UpdateJob(j.ID, "failed")
		return
	}
	progressDone <- true

	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Infof("Refinement python script finished.")

	timeTaken := time.Since(startTime)
	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Infof("Refinement algorithm took %s", timeTaken)

	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Infof("Going to upload results to domain %s", j.DomainID)

	if _, err := UploadRefinedOutputsToDomain(j); err != nil {
		logs.WithTag("job_id", j.ID).
			WithTag("domain_id", j.DomainID).
			Error(errors.New("failed to upload refined outputs to domain").Wrap(err))
		jobs.UpdateJob(j.ID, "failed")
		return
	}

	// remove the job directory (disable for now)
	// TODO: keep scan inputs downloaded in some local cache while still setting up the domain.
	/*if err := os.RemoveAll(j.JobPath); err != nil {
		log.Printf("job %s failed to remove output directory: %s", j.ID, err)
	}
	*/

	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Infof("job succeeded!")
	jobs.UpdateJob(j.ID, "succeeded")
}
