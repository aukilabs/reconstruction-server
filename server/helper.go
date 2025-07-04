package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"sort"

	"fmt"
	"io"

	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"os/exec"
	"path"
	"path/filepath"
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

const RefinedManifestDataName = "refined_manifest"
const RefinedManifestDataType = "refined_manifest_json"

type ExpectedOutput struct {
	FilePath string // relative to job folder
	Name     string
	DataType string
	Optional bool
}

type jobList struct {
	lock sync.RWMutex
	list map[string]*job
}

func (js *jobList) AddJob(j *job) {
	js.lock.Lock()
	defer js.lock.Unlock()

	js.list[j.ID] = j
}

func ParseStatusFromManifest(manifestPath string) (string, error) {
	content, err := os.ReadFile(manifestPath)
	if err != nil {
		return "", err
	}

	var parsedManifest map[string]interface{}
	if err := json.Unmarshal(content, &parsedManifest); err != nil {
		return "", err
	}

	status, ok := parsedManifest["jobStatus"].(string)
	if !ok {
		return "", fmt.Errorf("cannot parse jobStatus in existing manifest json file: %s", manifestPath)
	}
	return status, nil
}

func WriteJobManifestFile(j *job, status string) {
	switch status {
	case "failed":
		// If python script has already written a manifest with status "failed", don't overwrite it
		statusFromManifest, err := ParseStatusFromManifest(path.Join(j.JobPath, "job_manifest.json"))

		if err == nil && statusFromManifest == "failed" {
			logs.WithTag("job_id", j.ID).
				WithTag("domain_id", j.DomainID).
				Infof("job %s python script has already written a failed manifest, won't overwrite.", j.ID)
		} else {
			WriteFailedJobManifestFile(j, "Reconstruction job script failed")
		}

	case "processing":
		progress := 0
		WriteJobManifestFileHelper(j, status, progress, "Request received by reconstruction server")
	default:
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
	}
	js.lock.Unlock()

	if ok {
		WriteJobManifestFile(j, status)
	}
}

func (js *jobList) List() []*job {
	js.lock.RLock()
	defer js.lock.RUnlock()

	logs.Info("job list count: ", len(js.list))
	logs.Info("job list null? ", js.list == nil)
	// log.Println("job list count: ", len(js.list))
	// log.Println("job list null? ", js.list == nil)

	var jobs []*job
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
	list: map[string]*job{},
}

type JobMetadata struct {
	ID                      string    `json:"id"`
	Name                    string    `json:"name"`
	DomainID                string    `json:"domain_id"`
	ProcessingType          string    `json:"processing_type"`
	CreatedAt               time.Time `json:"created_at"`
	DomainServerURL         string    `json:"domain_server_url"`
	ReconstructionServerURL string    `json:"reconstruction_server_url"`
	AccessToken             string    `json:"-"`
	DataIDs                 []string  `json:"data_ids"`
	SkipManifestUpload      bool      `json:"skip_manifest_upload"` // Whether to skip uploading job manifest
	OverrideJobName         string    `json:"override_job_name"`    // Optional
	OverrideManifestID      string    `json:"override_manifest_id"` // Optional
}

type job struct {
	JobMetadata
	JobPath         string            `json:"job_path"`
	Status          string            `json:"status"`
	UploadedDataIDs map[string]string `json:"uploaded_data_ids"`
	CompletedScans  map[string]bool   `json:"completed_scans"`
	completedMutex  sync.RWMutex      `json:"-"` // Mutex for thread-safe access to CompletedScans
}

type JobRequestData struct {
	DataIDs            []string `json:"data_ids"`
	DomainID           string   `json:"domain_id"`
	AccessToken        string   `json:"access_token"`
	ProcessingType     string   `json:"processing_type"`
	DomainServerURL    string   `json:"domain_server_url"` // Optional. Default: "issuer" of the incoming request, since jobs are triggered via the domain server.
	SkipManifestUpload bool     `json:"skip_manifest_upload"`
	OverrideJobName    string   `json:"override_job_name"`
	OverrideManifestID string   `json:"override_manifest_id"`
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

func WriteScanDataSummary(datasetsRootPath string, allScanFolders []os.DirEntry, summaryJsonPath string) error {
	scanCount := 0
	totalFrameCount := 0
	totalScanDuration := 0.0
	scanDurations := []float64{}
	uniquePortalIDs := []string{}
	portalSizes := []float64{} // Size list is used when saving manifest, to output same physical size, without asking domain server
	devicesUsed := []string{}
	appVersionsUsed := []string{}

	for _, scanFolder := range allScanFolders {
		if !scanFolder.IsDir() {
			continue
		}

		manifestPath := path.Join(datasetsRootPath, scanFolder.Name(), "Manifest.json")
		if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
			continue
		}

		manifestData, err := os.ReadFile(manifestPath)
		if err != nil {
			return err
		}

		var manifest map[string]interface{}
		if err := json.Unmarshal(manifestData, &manifest); err != nil {
			return err
		}

		scanCount++

		frameCount := int(manifest["frameCount"].(float64))
		duration := manifest["duration"].(float64)
		totalFrameCount += frameCount
		totalScanDuration += duration
		scanDurations = append(scanDurations, duration)

		if portals, ok := manifest["portals"].([]interface{}); ok {
			for _, portal := range portals {
				if portalMap, ok := portal.(map[string]interface{}); ok {
					if portalID, ok := portalMap["shortId"].(string); ok {
						if !slices.Contains(uniquePortalIDs, portalID) {
							uniquePortalIDs = append(uniquePortalIDs, portalID)
							portalSizes = append(portalSizes, portalMap["physicalSize"].(float64))
						}
					}
				}
			}
		}

		device := "unknown"
		if manifest["brand"] != nil && manifest["model"] != nil && manifest["systemName"] != nil && manifest["systemVersion"] != nil {
			device = manifest["brand"].(string) + " " + manifest["model"].(string) + " " + manifest["systemName"].(string) + " " + manifest["systemVersion"].(string)
			device = strings.TrimSpace(device)
		}
		if !slices.Contains(devicesUsed, device) {
			devicesUsed = append(devicesUsed, device)
		}

		appVersion := "unknown"
		if manifest["appVersion"] != nil && manifest["buildId"] != nil {
			appVersion = manifest["appVersion"].(string) + " (build " + manifest["buildId"].(string) + ")"
		}
		if !slices.Contains(appVersionsUsed, appVersion) {
			appVersionsUsed = append(appVersionsUsed, appVersion)
		}
	}

	sort.Float64s(scanDurations)
	shortestScanDuration := scanDurations[0]
	longestScanDuration := scanDurations[len(scanDurations)-1]
	medianScanDuration := scanDurations[len(scanDurations)/2]

	averageScanDuration := totalScanDuration / float64(len(allScanFolders))
	averageScanFrameCount := float64(totalFrameCount) / float64(len(allScanFolders))
	averageScanFrameRate := float64(totalFrameCount) / totalScanDuration

	summary := map[string]interface{}{
		"scanCount":             scanCount,
		"totalFrameCount":       totalFrameCount,
		"totalScanDuration":     totalScanDuration,
		"averageScanDuration":   averageScanDuration,
		"averageScanFrameCount": averageScanFrameCount,
		"averageFrameRate":      averageScanFrameRate,
		"shortestScanDuration":  shortestScanDuration,
		"longestScanDuration":   longestScanDuration,
		"medianScanDuration":    medianScanDuration,
		"portalCount":           len(uniquePortalIDs),
		"portalIDs":             uniquePortalIDs,
		"portalSizes":           portalSizes,
		"deviceVersionsUsed":    devicesUsed,
		"appVersionsUsed":       appVersionsUsed,
	}

	summaryJson, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(summaryJsonPath, summaryJson, 0644)
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
save_failed_manifest_json('` + j.JobPath + `/job_manifest.json', '` + j.JobPath + `', '` + errorMessage + `')
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
	'` + j.JobPath + `',
	job_status='` + status + `',
	job_progress=` + strconv.Itoa(progress) + `,
	job_status_details='` + statusDetails + `'
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

	// Skip uploading manifest if flag is set
	if j.SkipManifestUpload {
		logs.WithTag("job_id", j.ID).
			WithTag("domain_id", j.DomainID).
			Infof("Skipping manifest upload due to -skip-manifest-upload flag")
		return nil
	}

	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Infof("Upload job manifest to domain, for job")

	output := ExpectedOutput{
		FilePath: "job_manifest.json",
		Name:     RefinedManifestDataName,
		DataType: RefinedManifestDataType,
		Optional: false,
	}

	return UploadOutputToDomain(j, output)
}

func UploadRefinedOutputsToDomain(j *job) (int, error) {

	if j.ProcessingType == "local_refinement" {
		return 0, nil
	}

	refinedOutput := path.Join("refined", "global")
	expectedOutputs := []ExpectedOutput{
		{
			FilePath: path.Join(refinedOutput, "refined_manifest.json"),
			Name:     RefinedManifestDataName,
			DataType: RefinedManifestDataType,
			Optional: false,
		},
		{
			FilePath: path.Join(refinedOutput, "RefinedPointCloudReduced.ply"),
			Name:     "refined_pointcloud",
			DataType: "refined_pointcloud_ply",
			Optional: false,
		},
		{
			FilePath: path.Join(refinedOutput, "RefinedPointCloud.ply.drc"),
			Name:     "refined_pointcloud_full_draco",
			DataType: "refined_pointcloud_ply_draco",
			Optional: true,
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

	if !j.SkipManifestUpload {
		if err := UploadOutputToDomain(j, expectedOutputs[0]); err != nil {
			return outputCount, errors.New("failed to upload refined manifest to domain").Wrap(err)
		}
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
	if err := UploadOutputToDomainHelper(j, output); err != nil {
		if output.Optional {
			logs.WithTag("job_id", j.ID).
				WithTag("domain_id", j.DomainID).
				Infof("Uploading output %s failed (but won't fail the job): %s", output.Name, err.Error())
			return nil
		}
		return err
	}
	return nil
}

func UploadOutputToDomainHelper(j *job, output ExpectedOutput) error {
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
	if j.OverrideJobName != "" {
		nameSuffix = j.OverrideJobName
	}
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
		case "refined_scan_zip":
			fileName = "RefinedScan.zip"
		default:
			logs.Infof("unknown domain data type: %s", meta.DataType)
			fileName = meta.Name + "." + meta.DataType
		}

		scanFolder := path.Join(j.JobPath, "datasets", scanFolderName)
		if err := os.MkdirAll(scanFolder, 0755); err != nil {
			return err
		}

		filePath := path.Join(scanFolder, fileName)
		f, err := os.Create(filePath)
		if err != nil {
			return err
		}
		defer f.Close()

		if _, err := io.Copy(f, part); err != nil {
			return err
		}

		if fileName == "RefinedScan.zip" {
			logs.WithTag("job_id", j.ID).
				WithTag("domain_id", j.DomainID).
				Infof("Unzipping refined scan for '%s'", scanFolderName)
			unzipPath := path.Join(j.JobPath, "refined", "local", scanFolderName, "sfm")
			if err := os.MkdirAll(unzipPath, 0755); err != nil {
				return err
			}
			zipReader, err := zip.OpenReader(filePath)
			if err != nil {
				return err
			}
			defer zipReader.Close()
			for _, content := range zipReader.File {
				if content.FileInfo().IsDir() {
					continue
				}
				outFile, err := os.Create(path.Join(unzipPath, content.Name))
				if err != nil {
					return err
				}
				defer outFile.Close()
				contentFile, err := content.Open()
				if err != nil {
					return err
				}
				defer contentFile.Close()

				if _, err := io.Copy(outFile, contentFile); err != nil {
					return err
				}

				logs.WithTag("job_id", j.ID).
					WithTag("domain_id", j.DomainID).
					WithTag("scan_id", scanFolderName).
					Infof("Unzipped %s to %s", content.Name, outFile.Name())
			}
		}

		i++
	}
	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Infof("downloaded %d data objects from domain", i)
	return nil
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

func CreateJobMetadata(dirPath string, requestJson string, reconstructionServerURL string, retriggerJobID string) (*job, error) {

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
	if retriggerJobID != "" {
		jobID = retriggerJobID
	}
	jobName := "job_" + jobID

	dataIDs := jobRequest.DataIDs
	if retriggerJobID != "" {
		dataIDs = []string{retriggerJobID} // Don't download again
	}

	j := job{
		JobMetadata: JobMetadata{
			CreatedAt:               startTime,
			ID:                      jobID,
			Name:                    jobName,
			DomainID:                jobRequest.DomainID,
			DataIDs:                 dataIDs,
			ProcessingType:          jobRequest.ProcessingType,
			DomainServerURL:         domainServerURL,
			ReconstructionServerURL: reconstructionServerURL,
			AccessToken:             jobRequest.AccessToken,
			SkipManifestUpload:      jobRequest.SkipManifestUpload,
			OverrideJobName:         jobRequest.OverrideJobName,
			OverrideManifestID:      jobRequest.OverrideManifestID,
		},
		Status:          "started",
		UploadedDataIDs: map[string]string{},
		CompletedScans:  map[string]bool{},
		JobPath:         path.Join(dirPath, jobRequest.DomainID, jobName),
		completedMutex:  sync.RWMutex{},
	}

	if retriggerJobID == "" {
		if err := os.MkdirAll(j.JobPath, 0755); err != nil {
			return nil, errors.New("failed to create job directory").Wrap(err).
				WithTag("job_id", j.ID).
				WithTag("domain_id", j.DomainID)
		}

		// write the requestJson to file, for debugging purposes
		requestFile := path.Join(j.JobPath, "job_request.json")
		if err := os.WriteFile(requestFile, []byte(requestJson), 0644); err != nil {
			return nil, errors.New("failed to write jobrequest json file to disk").Wrap(err).
				WithTag("job_id", j.ID).
				WithTag("domain_id", j.DomainID)
		}

		// write job metadata to file, gets added into refined manifest file
		metadataFile := path.Join(j.JobPath, "job_metadata.json")
		jobMetadataJson, err := json.Marshal(j.JobMetadata)
		if err != nil {
			return nil, errors.New("failed to marshal job metadata to json").Wrap(err).
				WithTag("job_id", j.ID).
				WithTag("domain_id", j.DomainID)
		}
		if err := os.WriteFile(metadataFile, jobMetadataJson, 0644); err != nil {
			return nil, errors.New("failed to write job metadata json file to disk").Wrap(err).
				WithTag("job_id", j.ID).
				WithTag("domain_id", j.DomainID)
		}

		logs.WithTag("job_id", j.ID).
			WithTag("domain_id", j.DomainID).
			Infof("Job Request File: %s", requestFile)

		logs.WithTag("job_id", j.ID).
			WithTag("domain_id", j.DomainID).
			Infof("Job Metadata File: %s", metadataFile)
	}

	jobs.AddJob(&j)
	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		Infof("Job added to job list: %s", j.ID)

	return &j, nil
}

func executeJob(j *job, numCpuWorkers int) {

	// When we run in parallel across many nodes,
	// the global refinement must update the existing manifest json (with PUT)
	if j.OverrideJobName != "" && j.OverrideManifestID != "" {
		key := RefinedManifestDataName + "." + RefinedManifestDataType
		logs.WithTag("job_id", j.ID).
			WithTag("domain_id", j.DomainID).
			Infof("Job requested to update existing manifest domain data: %s -> %s", key, j.OverrideManifestID)

		// Adding to this dictionary makes sure the upload will update the existing domain data.
		// Otherwise it would "POST" and fail with "409 Conflict" since manifest already exists.
		j.UploadedDataIDs[key] = j.OverrideManifestID
	}

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

	outputPath := path.Join(j.JobPath, "refined")
	logFilePath := path.Join(j.JobPath, "log.txt")

	params := []string{
		refinementPython,
		"--mode", j.ProcessingType,
		"--job_root_path", j.JobPath,
		"--output", outputPath,
		"--domain_id", j.DomainID,
		"--job_id", j.Name,
		"--local_refinement_workers", strconv.Itoa(numCpuWorkers),
		"--scans"}

	datasetsRootPath := path.Join(j.JobPath, "datasets")
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

		if j.ProcessingType == "local_and_global_refinement" || j.ProcessingType == "local_refinement" {
			// for global_refinement we don't download the scan manifests so we can't write scan data summary (and don't need it)
			scanDataSummaryPath := path.Join(j.JobPath, "scan_data_summary.json")
			WriteScanDataSummary(datasetsRootPath, allScanFolders, scanDataSummaryPath)
		}

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

	checkProgress := func() {
		refinedPath := path.Join(outputPath, "local")

		// Get total number of datasets
		datasetFolders, err := os.ReadDir(datasetsRootPath)
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
			scanID := dataset.Name()
			refinedScanPath := path.Join(refinedPath, scanID)

			if _, err := os.Stat(refinedScanPath); err == nil {
				refinedCount++

				// Check if this scan has been completed and uploaded
				j.completedMutex.RLock()
				isCompleted := j.CompletedScans[scanID]
				j.completedMutex.RUnlock()

				if !isCompleted {
					// Check if sfm directory exists
					sfmPath := path.Join(refinedScanPath, "sfm")
					if _, err := os.Stat(sfmPath); err == nil {
						// Expected outputs
						waitForFilenames := []string{
							"images.bin",
							"cameras.bin",
							"points3D.bin",
							"portals.csv",
						}

						incomplete := false
						for _, filename := range waitForFilenames {
							if _, err := os.Stat(path.Join(sfmPath, filename)); err != nil {
								incomplete = true
								break
							}
						}
						if incomplete {
							continue
						}

						// Scan finished refinement, upload results as zip
						// (but not for global_refinement jobs)
						if j.ProcessingType == "local_refinement" || j.ProcessingType == "local_and_global_refinement" {
							logs.WithTag("job_id", j.ID).
								WithTag("domain_id", j.DomainID).
								WithTag("scan_id", scanID).
								Infof("Scan completed, uploading zip file")

							if err := ZipScanFiles(j, scanID); err != nil {
								logs.WithTag("job_id", j.ID).
									WithTag("domain_id", j.DomainID).
									WithTag("scan_id", scanID).
									Error(errors.Newf("failed to upload scan zip").Wrap(err))
								continue
							} else {
								logs.WithTag("job_id", j.ID).
									WithTag("domain_id", j.DomainID).
									WithTag("scan_id", scanID).
									Infof("Scan zip file uploaded successfully")
							}
						}

						// Mark scan as completed
						j.completedMutex.Lock()
						j.CompletedScans[scanID] = true
						j.completedMutex.Unlock()
					}
				}
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

		if j.Status == "processing" || j.Status == "started" { // Avoid overwriting final manifest from python when we run the final checkProgress afterwards
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
		}
	}
	go func() {
		for {
			select {
			case <-progressDone:
				return
			default:
				checkProgress()
				time.Sleep(30 * time.Second)
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

	checkProgress() // One extra check to upload any remaining local refinement outputs

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

// ZipScanFiles creates a zip file containing all bin, csv, and txt files from the sfm directory
func ZipScanFiles(j *job, scanID string) error {
	sfmPath := path.Join(j.JobPath, "refined", "local", scanID, "sfm")

	// Check if sfm directory exists
	if _, err := os.Stat(sfmPath); os.IsNotExist(err) {
		logs.WithTag("job_id", j.ID).
			WithTag("domain_id", j.DomainID).
			WithTag("scan_id", scanID).
			Infof("sfm directory does not exist: %s", sfmPath)
		return nil
	}

	// Create zip file in memory
	var zipBuffer bytes.Buffer
	zipWriter := zip.NewWriter(&zipBuffer)

	// Walk through the sfm directory and add files with specific extensions
	err := filepath.Walk(sfmPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Check if file has the required extension
		ext := strings.ToLower(filepath.Ext(filePath))
		if ext != ".bin" && ext != ".csv" && ext != ".txt" {
			return nil
		}

		// Get relative path from sfm directory
		relPath, err := filepath.Rel(sfmPath, filePath)
		if err != nil {
			return err
		}

		// Create zip file entry
		zipFile, err := zipWriter.Create(relPath)
		if err != nil {
			return err
		}

		// Read and write file content
		fileContent, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}

		_, err = zipFile.Write(fileContent)
		return err
	})

	if err != nil {
		return errors.Newf("failed to create zip file for scan %s", scanID).Wrap(err)
	}

	// Close zip writer
	if err := zipWriter.Close(); err != nil {
		return errors.Newf("failed to close zip writer for scan %s", scanID).Wrap(err)
	}


	// Upload the zip file to domain
	zipData := bytes.NewReader(zipBuffer.Bytes())
	domainData := DomainData{
		DomainDataMetadata: DomainDataMetadata{
			EditableDomainDataMetadata: EditableDomainDataMetadata{
				Name:     fmt.Sprintf("refined_scan_%s", scanID),
				DataType: "refined_scan_zip",
			},
			DomainID: j.DomainID,
		},
		Data: io.NopCloser(zipData),
	}

	// Create multipart request body
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	if err := WriteDomainData(writer, &domainData); err != nil {
		return errors.Newf("failed to write domain data for scan %s", scanID).Wrap(err)
	}

	if err := writer.Close(); err != nil {
		return err
	}

	// Make HTTP request to upload
	reqUrl := j.DomainServerURL + "/api/v1/domains/" + j.DomainID + "/data"
	req, err := http.NewRequest(http.MethodPost, reqUrl, body)
	if err != nil {
		return errors.Newf("failed to create upload request for scan %s", scanID).Wrap(err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+j.AccessToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if resp.StatusCode == http.StatusConflict {
			logs.WithTag("job_id", j.ID).
				WithTag("domain_id", j.DomainID).
				WithTag("scan_id", scanID).
				Infof("Skip uploading refined outputs zip. Already exists in domain")
			return nil
		}
		return errors.Newf("failed to upload scan zip for scan %s", scanID).Wrap(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Newf("domain server returned status %d for scan %s", resp.StatusCode, scanID)
	}

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Newf("failed to read response for scan %s", scanID).Wrap(err)
	}

	var parsedResp PostDomainDataResponse
	if err := json.Unmarshal(responseBody, &parsedResp); err != nil {
		return errors.Newf("failed to parse response for scan %s", scanID).Wrap(err)
	}

	// Store the uploaded data ID
	j.UploadedDataIDs[fmt.Sprintf("refined_scan_%s.refined_scan_zip", scanID)] = parsedResp.Data[0].ID

	logs.WithTag("job_id", j.ID).
		WithTag("domain_id", j.DomainID).
		WithTag("scan_id", scanID).
		Infof("Successfully uploaded scan zip file with ID: %s", parsedResp.Data[0].ID)

	return nil
}
