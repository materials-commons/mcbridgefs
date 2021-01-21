package mcglobusfs

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/materials-commons/mcglobusfs/pkg/globusapi"

	"github.com/apex/log"
	"gorm.io/gorm"
)

type GlobusRequestMonitor struct {
	db           *gorm.DB
	ctx          context.Context
	globusClient *globusapi.Client
}

func NewGlobusRequestMonitor(db *gorm.DB, ctx context.Context, globusClient *globusapi.Client) *GlobusRequestMonitor {
	return &GlobusRequestMonitor{db: db, ctx: ctx, globusClient: globusClient}
}

func (m *GlobusRequestMonitor) Start() {
	go m.monitorAndProcessGlobusRequests()
}

func (m *GlobusRequestMonitor) monitorAndProcessGlobusRequests() {
	for {
		m.processGlobusRequests()
		select {
		case <-m.ctx.Done():
			log.Infof("Shutting down globus request monitoring")
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (m *GlobusRequestMonitor) processGlobusRequests() {
	m.startNewRequests()
	m.processFinishedRequests()
}

func (m *GlobusRequestMonitor) startNewRequests() {
	for _, request := range m.retrieveNewRequests() {
		go handleBridgeMountAndRequestCompletion(m.db, &request, m.globusClient)
	}
}

func (m *GlobusRequestMonitor) retrieveNewRequests() []GlobusRequest {
	var requests []GlobusRequest
	result := m.db.Where("state = ?", "new").Find(&requests)
	if result.Error != nil {
		log.Warnf("No entries found")
	}

	return requests
}

func handleBridgeMountAndRequestCompletion(db *gorm.DB, request *GlobusRequest, globusClient *globusapi.Client) {
	mountPath := filepath.Join(os.Getenv("MC_GLOBUS_BRIDGE_ROOT"), request.UUID)
	if err := os.MkdirAll(mountPath, 0777); err != nil {
		log.Errorf("Unable to create root mount point (%s) for request %d: %s", mountPath, request.ID, err)
		return
	}

	cmd, err := StartMCBridgeFS(request.ID, request.ProjectID, mountPath)
	if err != nil {
		log.Errorf("Unable to start mcbridgefs for request %d: %s", request.ID, err)
		return
	}

	if err := cmd.Wait(); err != nil {
		log.Errorf("A mcbridgefs command failed while running: %s", err)
		return
	}

	// TODO: Have the bridge do this instead...
	if _, err := globusClient.DeleteEndpointACLRule(os.Getenv("MC_GLOBUS_ENDPOINT_ID"), request.GlobusAclID); err != nil {
		log.Errorf("Error deleting ACL for request %d: %s", request.ID, err)
	}

	result := db.Model(&request).Updates(GlobusRequest{Pid: 0, State: "done"})
	if result.Error != nil {
		log.Errorf("Unable to change request %d to done state: %s", request.ID, result.Error)
	}

	if result.RowsAffected != 1 {
		log.Errorf("Unable to change request %d to done state - no rows affected", request.ID)
	}
}

func StartMCBridgeFS(globusRequestID, projectID int, path string) (*exec.Cmd, error) {
	command := fmt.Sprintf("mcbridgefs mount -g %d -p %d %s", globusRequestID, projectID, path)
	cmd := exec.Command(command)
	return cmd, cmd.Start()
}

func (m *GlobusRequestMonitor) processFinishedRequests() {
	// TODO: Implement project level locking so that only request for a particular project is processed
	for _, request := range m.retrieveFinishedRequests() {
		go handleFinishedRequest(&request)
	}
}

func (m *GlobusRequestMonitor) retrieveFinishedRequests() []GlobusRequest {
	return nil
}

func handleFinishedRequest(g *GlobusRequest) {

}
