package monitor

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apex/log"
	globus "github.com/materials-commons/goglobus"
	"github.com/materials-commons/mcbridgefs/pkg/fs/mcbridgefs"
	"github.com/materials-commons/mcbridgefs/pkg/store"
	"gorm.io/gorm"
)

type LastProcessingTimeSaveFn func(lastProcessingTime time.Time) error

type GlobusTaskMonitorCfg struct {
	EndpointID               string
	SettlingPeriod           time.Duration
	LastProcessingTimeSaveFn LastProcessingTimeSaveFn
}

type GlobusTaskMonitor struct {
	client                       *globus.Client
	db                           *gorm.DB
	transferRequestFileStore     *store.TransferRequestFileStore
	lastUserProjectProcessedTime map[string]time.Time
	taskMonitorCfg               GlobusTaskMonitorCfg
	taskMonitorInterval          time.Duration
	lastProcessedTime            time.Time
}

func NewGlobusTaskMonitor(client *globus.Client, db *gorm.DB, lastProcessedTime time.Time, taskMonitorCfg GlobusTaskMonitorCfg) *GlobusTaskMonitor {
	return &GlobusTaskMonitor{
		client:                       client,
		db:                           db,
		lastUserProjectProcessedTime: make(map[string]time.Time),
		transferRequestFileStore:     store.NewTransferRequestFileStore(db),
		taskMonitorCfg:               taskMonitorCfg,
		lastProcessedTime:            lastProcessedTime,
	}
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
		case <-time.After(m.taskMonitorInterval):
		}
	}
}

func (m *GlobusTaskMonitor) retrieveAndProcessUploads(c context.Context) {
	// Build a filter to get all successful tasks that completed in the last week
	filterCompletionTime := m.lastProcessedTime.Format("2006-01-02")
	taskFilter := map[string]string{
		"filter_completion_time": filterCompletionTime,
		"filter_status":          "SUCCEEDED",
		"orderby":                "completion_time ASC",
		"limit":                  "1000",
	}
	tasks, err := m.client.GetEndpointTaskList(m.taskMonitorCfg.EndpointID, taskFilter)

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
			m.lastProcessedTime = taskCompletionTime
			if err := m.taskMonitorCfg.LastProcessingTimeSaveFn(m.lastProcessedTime); err != nil {
				log.Errorf("Unable to save last processing time: %s", err)
			}
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

	// Processing entries simply means cleaning up all the files in the transfer, because those are the files that
	// have been completed.
	for _, transfer := range transfers.Transfers {
		m.processFileTransfer(taskCompletionTime, transfer.DestinationPath)
	}
}

func (m *GlobusTaskMonitor) processFileTransfer(taskCompletionTime time.Time, path string) {
	// Look at file according to path, user, and project
	c := mcbridgefs.ToTransferPathContext(path)
	transferRequestFile, err := m.transferRequestFileStore.GetTransferFileRequestByPath(c.UserID, c.ProjectID, path)
	switch {
	case err != nil:
		log.Errorf("Unable to find transfer file request for user: %d, project: %d, path: %s: %s", c.UserID, c.ProjectID, path, err)
		return
	case transferRequestFile.UpdatedAt.After(taskCompletionTime):
		// file was possibly changed since the task completed so ignore
		return
	case transferRequestFile.State == "open":
		// file still open, which means there is another transfer request accessing it
		return
	default:
		if err := m.transferRequestFileStore.DeleteTransferRequestFile(transferRequestFile); err != nil {
			log.Errorf("Unable to delete transfer file request for user: %d, project: %d, path: %s: %s", c.UserID, c.ProjectID, path, err)
			return
		}

		mcbridgefs.DeleteOpenFileFromTrackerByPath(path)
	}

}
