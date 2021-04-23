package monitor

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/apex/log"
	globus "github.com/materials-commons/goglobus"
	"github.com/materials-commons/mcbridgefs/pkg/fs/mcbridgefs"
	"github.com/materials-commons/mcbridgefs/pkg/store"
	"gorm.io/gorm"
)

type GlobusTaskMonitor struct {
	client                       *globus.Client
	db                           *gorm.DB
	endpointID                   string
	lastUserProjectProcessedTime map[string]time.Time
	lastProcessedTime            time.Time
	transferRequestFileStore     *store.TransferRequestFileStore
	settlingPeriod               time.Duration
}

func NewGlobusTaskMonitor(client *globus.Client, db *gorm.DB, endpointID string) *GlobusTaskMonitor {
	return &GlobusTaskMonitor{
		client:                       client,
		db:                           db,
		endpointID:                   endpointID,
		lastUserProjectProcessedTime: make(map[string]time.Time),
		// set lastProcessedTime to a date far in the past so that we initially match all requests
		lastProcessedTime:        time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		transferRequestFileStore: store.NewTransferRequestFileStore(db),
		settlingPeriod:           getSettlingPeriod(),
	}
}

func getSettlingPeriod() time.Duration {
	d, err := time.ParseDuration(os.Getenv("MC_GLOBUS_SETTLING_PERIOD"))
	if err != nil || d.Seconds() < 10 {
		return 10 * time.Second
	}

	return d
}

func (m *GlobusTaskMonitor) Start(ctx context.Context) {
	log.Infof("Starting globus task monitor...")
	go m.monitorAndProcessTasks(ctx)
}

func (m *GlobusTaskMonitor) monitorAndProcessTasks(ctx context.Context) {
	for {
		m.retrieveAndProcessUploads(ctx)
		select {
		case <-ctx.Done():
			log.Infof("Shutting down globus monitoring...")
			return
		case <-time.After(10 * time.Second):
		}
	}
}

func (m *GlobusTaskMonitor) retrieveAndProcessUploads(c context.Context) {
	// Build a filter to get all successful tasks that completed in the last week
	lastWeek := time.Now().AddDate(0, 0, -7).Format("2006-01-02")
	taskFilter := map[string]string{
		"filter_completion_time": lastWeek,
		"filter_status":          "SUCCEEDED",
		"orderby":                "completion_time ASC",
		"limit":                  "1000",
	}
	tasks, err := m.client.GetEndpointTaskList(m.endpointID, taskFilter)

	if err != nil {
		log.Infof("globus.GetEndpointTaskList returned the following error: %s - %#v", err, m.client.GetGlobusErrorResponse())
		return
	}

	for _, task := range tasks.Tasks {
		taskCompletionTime, err := time.Parse(time.RFC3339, task.CompletionTime)
		switch {
		case err != nil:
			log.Errorf("Error parsing task time '%s': %s", task.CompletionTime, err)
			continue
		case m.lastProcessedTime.After(taskCompletionTime):
			// Task finished before the lastProcessedTime, so we've already seen it and don't need to re-process it.
			continue
		}

		//log.Infof("Getting successful transfers for Globus Task %s", task.TaskID)
		transfers, err := m.client.GetTaskSuccessfulTransfers(task.TaskID, 0)

		switch {
		case err != nil:
			log.Infof("globus.GetTaskSuccessfulTransfers(%d) returned error %s - %#v", task.TaskID, err, m.client.GetGlobusErrorResponse())
			continue
		case len(transfers.Transfers) == 0:
			// No files transferred in this request
			continue
		default:
			// Files were transferred for this request
			m.processTransfers(taskCompletionTime, &transfers)
		}

		// Check if we should stop processing requests
		select {
		case <-c.Done():
			break
		default:
		}
	}
}

func (m *GlobusTaskMonitor) processTransfers(taskCompletionTime time.Time, transfers *globus.TransferItems) {
	transferItem := transfers.Transfers[0]

	// Transfer items with a blank DestinationPath are downloads not uploads.
	if transferItem.DestinationPath == "" {
		return
	}

	// Destination path will have the following format: /__transfers/globus/<user-id>/<project-id>/...rest of path...
	// Split will return ["", "__transfers", "globus", "<user-id>", "<project-id>", ...]
	// So the 3rd entry in the array is the id in the globus_uploads table we want to look up.
	pieces := strings.Split(transferItem.DestinationPath, "/")
	if len(pieces) < 5 {
		// sanity check, because the destination path should at least be /__transfers/globus/<user-id>/<project-id>/...rest of path...
		// so it should at least have 5 entries in it (See Split return description above)
		log.Infof("Invalid globus DestinationPath: %s", transferItem.DestinationPath)
		return
	}

	// Look up the project this transfer is associated with. If the project processing time for this user id is less
	// than the taskCompletionTime then we need to process this transfer.
	id := fmt.Sprintf("%s_%s_%s", pieces[2], pieces[3], pieces[4])
	if lastProcessingTime, ok := m.lastUserProjectProcessedTime[id]; ok {
		// Found project entry for this user, lets check the time completion time
		if taskCompletionTime.Before(lastProcessingTime) {
			// Project was processed after the taskCompletionTime, so this means we've already processed this
			// user/project entry. We are looking for taskCompletionTimes that are after the projects lastProcessingTime
			// as those are tasks that haven't been processed yet.
			return
		}
	}

	// If we are here then we know that this user/project combo needs to be processed. So, first thing we have
	// to do is tell that file system instance not to accept any more requests for this user/project combo,
	c := mcbridgefs.ToTransferPathContext(transferItem.DestinationPath)
	mcbridgefs.LockFS(c)
	defer mcbridgefs.UnlockFS(c)

	if !m.userProjectFSInactive(id) {
		// There have been writes since we attempted the lock...
		return
	}

	// Processing entries simply means cleaning up all the files in the transfer, because those are the files that
	// have been completed.
	for _, transfer := range transfers.Transfers {
		m.processFileTransfer(taskCompletionTime, transfer.DestinationPath)
	}
}

func (m *GlobusTaskMonitor) userProjectFSInactive(id string) bool {
	return true
}

func (m *GlobusTaskMonitor) processFileTransfer(taskCompletionTime time.Time, path string) {
	// Look at file according to path, user, and project
	c := mcbridgefs.ToTransferPathContext(path)
	transferRequestFile, err := m.transferRequestFileStore.GetTransferFileRequestByPath(c.UserID, c.ProjectID, path)
	if err != nil {
		log.Errorf("Unable to find transfer file request for user: %d, project: %d, path: %s: %s", c.UserID, c.ProjectID, path, err)
		return
	}

	if transferRequestFile.UpdatedAt.After(taskCompletionTime) {
		// file was possibly changed since the task completed so ignore
		return
	}

	if err := m.transferRequestFileStore.DeleteTransferRequestFile(transferRequestFile); err != nil {
		log.Errorf("Unable to delete transfer file request for user: %d, project: %d, path: %s: %s", c.UserID, c.ProjectID, path, err)
	}
}
