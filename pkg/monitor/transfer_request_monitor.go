package monitor

import (
	"context"
	"errors"
	"time"

	"github.com/materials-commons/gomcdb/mcmodel"

	"github.com/apex/log"
	"gorm.io/gorm"
)

type TransferRequestMonitor struct {
	transferRequest       mcmodel.TransferRequest
	db                    *gorm.DB
	ctx                   context.Context
	handleClosedRequestFn func()
}

func NewTransferRequestMonitor(db *gorm.DB, ctx context.Context, transferRequest mcmodel.TransferRequest, handleFn func()) *TransferRequestMonitor {
	return &TransferRequestMonitor{db: db, ctx: ctx, transferRequest: transferRequest, handleClosedRequestFn: handleFn}
}

func (m *TransferRequestMonitor) Start() {
	go m.monitorTransferRequestState()
}

func (m *TransferRequestMonitor) monitorTransferRequestState() {
	for {
		if m.transferRequestIsClosedOrDeleted() {
			break
		}
		select {
		case <-m.ctx.Done():
			log.Infof("Shutting down transfer request monitor")
			return
		case <-time.After(10 * time.Second):
		}
	}
}

func (m *TransferRequestMonitor) transferRequestIsClosedOrDeleted() bool {
	var request mcmodel.TransferRequest
	result := m.db.First(&request, m.transferRequest.ID)
	switch {
	case errors.Is(result.Error, gorm.ErrRecordNotFound):
		// Request no longer exists so break out of monitoring
		log.Infof("TransferRequest %d removed from database", m.transferRequest.ID)
		m.handleClosedRequestFn()
		return true
	case result.Error != nil:
		// (Hopefully) transient error on database
		log.Errorf("Error querying database: %s\n", result.Error)
		return false
	case request.State == "closed":
		// Request state marked as closed, perform cleanup
		m.handleClosedRequestFn()
		return true
	default:
		// If we are here then we found the request, but its still active
		return false
	}
}
