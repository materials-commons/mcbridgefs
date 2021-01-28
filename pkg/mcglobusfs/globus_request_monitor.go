package mcglobusfs

import (
	"context"
	"fmt"
	"github.com/materials-commons/gomcdb/mcmodel"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/materials-commons/goglobus"

	"github.com/apex/log"
	"gorm.io/gorm"
)

type GlobusRequestMonitor struct {
	db           *gorm.DB
	ctx          context.Context
	globusClient *globus.Client
}

func NewGlobusRequestMonitor(db *gorm.DB, ctx context.Context, globusClient *globus.Client) *GlobusRequestMonitor {
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

func (m *GlobusRequestMonitor) retrieveNewRequests() []mcmodel.GlobusRequest {
	var requests []mcmodel.GlobusRequest
	result := m.db.Where("state = ?", "new").Find(&requests)
	if result.Error != nil {
		log.Warnf("No entries found")
	}

	return requests
}

func handleBridgeMountAndRequestCompletion(db *gorm.DB, request *mcmodel.GlobusRequest, globusClient *globus.Client) {
	mountPath := filepath.Join(os.Getenv("MC_GLOBUS_BRIDGE_ROOT"), request.UUID)
	if err := os.MkdirAll(mountPath, 0777); err != nil {
		log.Errorf("Unable to create root mount point (%s) for request %d: %s", mountPath, request.ID, err)
		return
	}

	if err := setupGlobusMountPoint(db, request, globusClient); err != nil {
		log.Errorf("Unable to setup Globus Mount Point for request %d: %s", request.ID, err)
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

	result := db.Model(&request).Updates(mcmodel.GlobusRequest{Pid: 0, State: "done"})
	if result.Error != nil {
		log.Errorf("Unable to change request %d to done state: %s", request.ID, result.Error)
	}

	if result.RowsAffected != 1 {
		log.Errorf("Unable to change request %d to done state - no rows affected", request.ID)
	}
}

func setupGlobusMountPoint(db *gorm.DB, request *mcmodel.GlobusRequest, globusClient *globus.Client) error {
	identities, err := globusClient.GetIdentities([]string{request.Owner.GlobusUser})
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("Unable to retrieve globus user from globus api %s", request.Owner.GlobusUser))
	}

	globusIdentityID := identities.Identities[0].ID

	globusEndpointID := os.Getenv("MC_GLOBUS_ENDPOINT_ID")

	path := makeGlobusPath(request.UUID)

	rule := globus.EndpointACLRule{
		PrincipalType: globus.ACLPrincipalTypeIdentity,
		EndpointID:    globusEndpointID,
		Path:          path,
		IdentityID:    globusIdentityID,
		Permissions:   "rw",
	}

	aclRes, err := globusClient.AddEndpointACLRule(rule)
	if err != nil {
		msg := fmt.Sprintf("Unable to add endpoint rule for endpoint %s, path %s, user %s/%s", globusEndpointID, path, request.Owner.GlobusUser, globusIdentityID)
		return errors.WithMessage(err, msg)
	}

	return db.Model(request).Updates(mcmodel.GlobusRequest{
		GlobusAclID:      aclRes.AccessID,
		GlobusIdentityID: globusIdentityID,
	}).Error
}

// makeGlobusPath constructs the path as Globus expects to see it. Globus needs the path to both
// start and end with a '/', eg /__globus/abc/.
func makeGlobusPath(dir string) string {
	// We need to Sprintf the ending slash because filepath.Join removes the trailing slash.
	return fmt.Sprintf("%s/", filepath.Join("/", os.Getenv("MC_GLOBUS_ROOT"), dir))
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

func (m *GlobusRequestMonitor) retrieveFinishedRequests() []mcmodel.GlobusRequest {
	return nil
}

func handleFinishedRequest(g *mcmodel.GlobusRequest) {

}
