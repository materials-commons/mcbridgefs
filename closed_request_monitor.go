package mcbridgefs

import (
	"context"
	"errors"
	"github.com/materials-commons/gomcdb/mcmodel"
	"time"

	"github.com/apex/log"
	"gorm.io/gorm"
)

type ClosedGlobusRequestMonitor struct {
	globusRequest         mcmodel.GlobusRequest
	db                    *gorm.DB
	ctx                   context.Context
	handleClosedRequestFn func()
}

func NewClosedGlobusRequestMonitor(db *gorm.DB, ctx context.Context, globusRequest mcmodel.GlobusRequest, handleFn func()) *ClosedGlobusRequestMonitor {
	return &ClosedGlobusRequestMonitor{db: db, ctx: ctx, globusRequest: globusRequest, handleClosedRequestFn: handleFn}
}

func (m *ClosedGlobusRequestMonitor) Start() {
	go m.monitorGlobusRequestState()
}

func (m *ClosedGlobusRequestMonitor) monitorGlobusRequestState() {
	for {
		if m.handledClosedOrDeletedRequest() {
			break
		}
		select {
		case <-m.ctx.Done():
			log.Infof("Shutting down globus request monitoring")
			return
		case <-time.After(10 * time.Second):
		}
	}
}

func (m *ClosedGlobusRequestMonitor) handledClosedOrDeletedRequest() bool {
	var request mcmodel.GlobusRequest
	result := m.db.Find(&request, m.globusRequest.ID)
	switch {
	case errors.Is(result.Error, gorm.ErrRecordNotFound):
		// Request no longer exists so break out of monitoring
		return true
	case result.Error != nil:
		// (Hopefully) transient error on database
		log.Warnf("Error querying database: %s", result.Error)
		return false
	case request.State == "closed":
		// Request state marked as closed, perform cleanup
		m.handleClosedRequestFn()
		return true
	case request.State == "unmounted":
		// This state should never occur
		log.Errorf("Monitor saw impossible state 'unmounted', shutting down bridge...")
		return true
	case request.State == "loading":
		log.Errorf("Monitor saw impossible state 'loading', shutting down bridge...")
		return true
	default:
		// If we are here then we found the request, but its state is still open and active
		return false
	}
}
