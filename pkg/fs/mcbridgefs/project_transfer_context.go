package mcbridgefs

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/apex/log"
	"github.com/hashicorp/go-uuid"
	globus "github.com/materials-commons/goglobus"
	"github.com/materials-commons/gomcdb/mcmodel"
	"gorm.io/gorm"
)

type GlobusContext struct {
	globusACL        string
	globusIdentityID string
}

type ProjectTransferContext struct {
	setupComplete   bool
	transferType    string
	transferContext TransferPathContext
	transferRequest mcmodel.TransferRequest
	globusContext   GlobusContext
	mutex           sync.Mutex
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
		projectTransferContext := &ProjectTransferContext{
			setupComplete:   true,
			transferRequest: transfer,
			// TODO: Construct the TransferPathContext
		}

		if transfer.GlobusTransfer != nil {
			projectTransferContext.globusContext = GlobusContext{
				globusACL:        transfer.GlobusTransfer.GlobusAclID,
				globusIdentityID: transfer.GlobusTransfer.GlobusIdentityID,
			}
		}
	}

	projectTransfersLoaded = true
	return nil
}

func setupTransferContext(pathContext TransferPathContext) error {
	projectTransferContext := &ProjectTransferContext{}
	val, ok := projectTransfers.LoadOrStore(pathContext.ProjectPathContext(), projectTransferContext)
	if ok {
		// There was already a ProjectTransferRequest loaded
		projectContext := val.(*ProjectTransferContext)
		if projectContext.setupComplete {
			// if setupComplete then there is nothing else to do
			return nil
		}
	}

	// If we are here then at this point the project transfer context has not been initialized, so we lock
	// it and then we will check again after the lock in case between checking and the lock someone slipped
	// in and initialized it.

	// Create context and setACL
	projectContext := val.(*ProjectTransferContext)
	projectContext.mutex.Lock()
	defer projectContext.mutex.Unlock()

	if projectContext.setupComplete {
		// Someone slipped in before us and initialized this context so there is nothing to do
		return nil
	}

	// if this is a globus transfer we need to validate that the user has a globus user account setup
	var user mcmodel.User
	if err := db.First(&user, pathContext.UserID).Error; err != nil {
		return err
	}

	if user.GlobusUser == "" {
		// This user hasn't configured a globus account so we can't setup this transfer request
		return errors.New("no globus account configured")
	}

	// If we are here then we are creating the transfer. We will also look at the
	// transfer type in the

	var err error
	if projectContext.transferRequest, err = createTransferRequest(pathContext.ProjectID, pathContext.UserID); err != nil {
		return err
	}

	if pathContext.IsGlobusTransferType() {
		if err := setupGlobus(pathContext, projectContext, user); err != nil {
			return err
		}
	}

	projectContext.setupComplete = true

	// We don't need to store the updated projectContext because projectContext is a pointer to the
	// context stored in the projectTransfers.

	return nil
}

func createTransferRequest(projectID, userID int) (mcmodel.TransferRequest, error) {
	var err error

	transferRequest := mcmodel.TransferRequest{
		State:     "open",
		ProjectID: projectID,
		OwnerID:   userID,
	}

	if transferRequest.UUID, err = uuid.GenerateUUID(); err != nil {
		return transferRequest, err
	}

	result := db.Create(&transferRequest)

	return transferRequest, result.Error
}

func setupGlobus(pathContext TransferPathContext, projectContext *ProjectTransferContext, user mcmodel.User) error {
	identities, err := globusClient.GetIdentities([]string{user.GlobusUser})
	if err != nil {
		return nil
	}

	projectContext.globusContext.globusIdentityID = identities.Identities[0].ID

	rule := globus.EndpointACLRule{
		PrincipalType: globus.ACLPrincipalTypeIdentity,
		EndpointID:    os.Getenv("MC_GLOBUS_ENDPOINT_ID"),
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

	transferContext := val.(*ProjectTransferContext)
	transferContext.mutex.Lock()
	defer transferContext.mutex.Unlock()

	if pathContext.IsGlobusTransferType() {
		endpointID := os.Getenv("MC_GLOBUS_ENDPOINT_ID")
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
