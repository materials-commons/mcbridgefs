package mcbridgefs

import (
	"sync"

	globus "github.com/materials-commons/goglobus"
	"github.com/materials-commons/gomcdb/mcmodel"
)

type ProjectTransferContext struct {
	GlobusACL       string
	Context         TransferPathContext
	mutex           sync.Mutex
	transferRequest mcmodel.TransferRequest
}

var projectTransfers sync.Map
var globusClient *globus.Client

func setupTransferContext(pathContext TransferPathContext) error {
	val, ok := projectTransfers.LoadOrStore(pathContext.ProjectPathContext(), pathContext)
	if !ok {
		// Create context and setACL
		projectContext := val.(ProjectTransferContext)
		projectContext.mutex.Lock()
		defer projectContext.mutex.Unlock()

		if projectContext.GlobusACL != "" {
			// Someone slipped in before us
			return nil
		}
		rule := globus.EndpointACLRule{
			PrincipalType: globus.ACLPrincipalTypeIdentity,
			EndpointID:    "",
			Path:          "",
			IdentityID:    "",
			Permissions:   "",
		}
		aclRes, err := globusClient.AddEndpointACLRule(rule)
		if err != nil {
			return err
		}
		projectContext.GlobusACL = aclRes.AccessID
		projectTransfers.Store(pathContext.ProjectPathContext(), projectContext)
		// Create Transfer
	}

	return nil
}
