package mcbridgefs

import (
	"fmt"
	"os"
	"sync"

	"github.com/hashicorp/go-uuid"
	globus "github.com/materials-commons/goglobus"
	"github.com/materials-commons/gomcdb/mcmodel"
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

func setupTransferContext(pathContext TransferPathContext) error {
	projectTransferContext := ProjectTransferContext{}
	val, ok := projectTransfers.LoadOrStore(pathContext.ProjectPathContext(), &projectTransferContext)
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

	// If we are here then we are creating the transfer, which means setting up the ACL and all the
	// other context.

	// Create Transfer Request
	transferRequest, _ := createTransferRequest(pathContext.ProjectID, pathContext.UserID)
	_ = transferRequest

	rule := globus.EndpointACLRule{
		PrincipalType: globus.ACLPrincipalTypeIdentity,
		EndpointID:    os.Getenv("MC_GLOBUS_ENDPOINT_ID"),
		Path:          fmt.Sprintf("/__transfers/%d/%d/", pathContext.UserID, pathContext.ProjectID),
		IdentityID:    "",
		Permissions:   "rw",
	}
	aclRes, err := globusClient.AddEndpointACLRule(rule)
	if err != nil {
		return err
	}
	projectContext.globusContext.globusACL = aclRes.AccessID
	projectContext.setupComplete = true
	projectTransfers.Store(pathContext.ProjectPathContext(), projectContext)

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
