package mcglobusfs

import (
	"context"
	"os"
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
	return nil
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
	// TODO: Need to update the request with our pid

	if err := cmd.Wait(); err != nil {
		log.Errorf("A mcbridgefs command failed while running: %s", err)
		return
	}

	// TODO: Need to signal we are done so that the uploaded files can be revealed in the project by
	//   1. Setting pid to nil?
	//   2. Setting some state to signal it can be imported
	//   3. Finally we need to remove the Globus ACL giving access to the mount point
	//      a. Figure out where to get the endpoint id from
	//      b. Show should remove the acl, perhaps the mcbridgefs should instead?
	globusClient.DeleteEndpointACLRule("", request.GlobusAclID)
}

func (m *GlobusRequestMonitor) processFinishedRequests() {

}
