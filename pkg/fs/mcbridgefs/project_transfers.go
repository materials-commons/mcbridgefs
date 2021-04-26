package mcbridgefs

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apex/log"
	"github.com/hashicorp/go-uuid"
	globus "github.com/materials-commons/goglobus"
	"github.com/materials-commons/gomcdb/mcmodel"
	"github.com/materials-commons/mcbridgefs/pkg/config"
	"gorm.io/gorm"
)

var (
	ErrNoProjectTransferRequest = errors.New("no project transfer request")
	ErrNoAccessToProject        = errors.New("no access to project")
)

type GlobusContext struct {
	globusACL        string
	globusIdentityID string
}

type ProjectTransfer struct {
	setupComplete       bool
	transferType        string
	transferPathContext TransferPathContext
	transferRequest     mcmodel.TransferRequest
	globusContext       GlobusContext
	mutex               sync.Mutex
}

var projectTransfers sync.Map
var globusClient *globus.Client
var projectTransfersLoaded = false

func LoadProjectTransfers(db *gorm.DB) error {
	if projectTransfersLoaded {
		return nil
	}

	var activeTransfers []mcmodel.TransferRequest
	result := db.Preload("GlobusTransfer").Find(&activeTransfers)
	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		projectTransfersLoaded = true
		return nil
	}

	for _, transfer := range activeTransfers {
		projectTransferContext := &ProjectTransfer{
			setupComplete:   true,
			transferRequest: transfer,
		}

		transferPathContext := ToTransferPathContext(fmt.Sprintf("/globus/%d/%d", transfer.OwnerID, transfer.ProjectID))
		if transfer.GlobusTransfer != nil {
			projectTransferContext.globusContext = GlobusContext{
				globusACL:        transfer.GlobusTransfer.GlobusAclID,
				globusIdentityID: transfer.GlobusTransfer.GlobusIdentityID,
			}

		}
		projectTransferContext.transferPathContext = *transferPathContext
		projectTransfers.Store(projectTransferContext.transferPathContext.ProjectPathContext(), projectTransferContext)
	}

	projectTransfersLoaded = true
	return nil
}

func GetProjectTransferRequest(pathContext TransferPathContext) (error, mcmodel.TransferRequest) {
	projectTransferRequest := &ProjectTransfer{}
	val, ok := projectTransfers.Load(pathContext.ProjectPathContext())
	if !ok {
		return ErrNoProjectTransferRequest, projectTransferRequest.transferRequest
	}

	projectTransferRequest = val.(*ProjectTransfer)
	return nil, projectTransferRequest.transferRequest
}

func GetOrCreateProjectTransferRequest(pathContext TransferPathContext) (err error, transferRequest mcmodel.TransferRequest) {
	projectTransferContext := &ProjectTransfer{}
	val, ok := projectTransfers.LoadOrStore(pathContext.ProjectPathContext(), projectTransferContext)
	if ok {
		// There was already a ProjectTransferRequest loaded
		projectContext := val.(*ProjectTransfer)
		if projectContext.setupComplete {
			// if setupComplete then there is nothing else to do
			return nil, projectContext.transferRequest
		}
	}

	// If we are here then at this point the project transfer context has not been initialized, so we lock
	// it and then we will check again after the lock in case between checking and the lock someone slipped
	// in and initialized it.

	// Create context and setACL
	projectContext := val.(*ProjectTransfer)
	projectContext.mutex.Lock()
	defer projectContext.mutex.Unlock()

	if projectContext.setupComplete {
		// Someone slipped in before us and initialized this context so there is nothing to do
		return nil, projectContext.transferRequest
	}

	// Make sure they have access to the project
	if !canAccessProject(pathContext) {
		return ErrNoAccessToProject, projectContext.transferRequest
	}

	if pathContext.IsGlobusTransferType() {
		// if this is a globus transfer we need to validate that the user has a globus user account setup
		var user mcmodel.User
		if err := db.First(&user, pathContext.UserID).Error; err != nil {
			return err, projectContext.transferRequest
		}

		if user.GlobusUser == "" {
			// This user hasn't configured a globus account so we can't setup this transfer request
			return errors.New("no globus account configured"), projectContext.transferRequest
		}

		var err error
		if projectContext.transferRequest, err = createTransferRequest(pathContext.ProjectID, pathContext.UserID); err != nil {
			return err, projectContext.transferRequest
		}

		//if err := setupGlobus(pathContext, projectContext, user); err != nil {
		//	// TODO: Should we delete the transfer request here?
		//	return err, projectContext.transferRequest
		//}
	}
	// Add additional transfer types here
	// May need to refactor for common case calling createTransferRequest

	projectContext.setupComplete = true

	// We don't need to store the updated projectContext because projectContext is a pointer to the
	// context stored in the projectTransfers.

	return nil, projectContext.transferRequest
}

func canAccessProject(pathContext TransferPathContext) bool {
	var teamID int
	sqlQuery := "select team_id from projects where id = ?"
	if err := db.Raw(sqlQuery, pathContext.ProjectID).Scan(&teamID).Error; err != nil {
		return false
	}

	var count int64
	err := db.Table("team2admin").
		Where("team_id = ?", teamID).
		Where("user_id = ?", pathContext.UserID).
		Count(&count).Error
	if err != nil {
		return false
	}

	if count > 0 {
		return true
	}

	err = db.Table("team2member").
		Where("team_id = ?", teamID).
		Where("user_id = ?", pathContext.UserID).
		Count(&count).Error
	if err != nil {
		return false
	}

	if count > 0 {
		return true
	}

	return false
}

func createTransferRequest(projectID, userID int) (mcmodel.TransferRequest, error) {
	var err error

	transferRequest := mcmodel.TransferRequest{
		State:        "open",
		ProjectID:    projectID,
		OwnerID:      userID,
		LastActiveAt: time.Now(),
	}

	if transferRequest.UUID, err = uuid.GenerateUUID(); err != nil {
		return transferRequest, err
	}

	result := db.Create(&transferRequest)

	return transferRequest, result.Error
}

func setupGlobus(pathContext TransferPathContext, projectContext *ProjectTransfer, user mcmodel.User) error {
	identities, err := globusClient.GetIdentities([]string{user.GlobusUser})
	if err != nil {
		return nil
	}

	projectContext.globusContext.globusIdentityID = identities.Identities[0].ID

	rule := globus.EndpointACLRule{
		PrincipalType: globus.ACLPrincipalTypeIdentity,
		EndpointID:    config.MustGetGlobusEndpointID(),
		Path:          fmt.Sprintf("/__transfers/%d/%d/", pathContext.UserID, pathContext.ProjectID),
		IdentityID:    projectContext.globusContext.globusIdentityID,
		Permissions:   "rw",
	}
	aclRes, err := globusClient.AddEndpointACLRule(rule)
	if err != nil {
		return err
	}
	projectContext.globusContext.globusACL = aclRes.AccessID

	return nil
}

func cleanupTransferContext(pathContext TransferPathContext) {
	val, ok := projectTransfers.Load(pathContext.ProjectPathContext())
	if !ok {
		// Not in the store any longer
		return
	}

	transferContext := val.(*ProjectTransfer)
	transferContext.mutex.Lock()
	defer transferContext.mutex.Unlock()

	if pathContext.IsGlobusTransferType() {
		endpointID := config.MustGetGlobusEndpointID()
		if _, err := globusClient.DeleteEndpointACLRule(endpointID, transferContext.globusContext.globusACL); err != nil {
			log.Errorf("Failed deleting ACL for %s: %s", pathContext.ProjectPathContext(), err)
		}
	}

	if err := db.Delete(&transferContext.transferRequest).Error; err != nil {
		log.Errorf("Failed to delete TransferRequest %d: %s", transferContext.transferRequest.ID, err)
	}

	projectTransfers.Delete(pathContext.ProjectPathContext())

	return
}
