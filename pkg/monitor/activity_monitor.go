package monitor

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/apex/log"
	"github.com/materials-commons/gomcdb/mcmodel"
	"github.com/materials-commons/gomcdb/store"
	"gorm.io/gorm"
)

var activityCount int64
var oneWeek = 7 * time.Hour * 24

func IncrementActivity() {
	atomic.AddInt64(&activityCount, 1)
}

type ActivityMonitor struct {
	lastSeenActivityCount int64
	lastChanged           time.Time
	db                    *gorm.DB
	transferRequest       mcmodel.TransferRequest
}

func NewActivityMonitor(db *gorm.DB, transferRequest mcmodel.TransferRequest) *ActivityMonitor {
	return &ActivityMonitor{
		db:              db,
		transferRequest: transferRequest,
	}
}

func (m *ActivityMonitor) Start(ctx context.Context) {
	log.Info("Starting activity monitor...")
	go m.monitorActivity(ctx)
}

func (m *ActivityMonitor) monitorActivity(ctx context.Context) {
	for {
		if m.loadAndCheckIfBridgeInactiveForTooLong() {
			break
		}

		select {
		case <-ctx.Done():
			break
		case <-time.After(20 * time.Second):
		}
	}

	// If the bridge has been inactive for too long or told to shut down then mark the transfer as closed so
	// that we can begin cleaning it up.
	_ = store.WithTxRetryDefault(func(tx *gorm.DB) error {
		_ = tx.Model(m.transferRequest.GlobusTransfer).Updates(mcmodel.GlobusTransfer{State: "closed"}).Error
		return tx.Model(m.transferRequest).Updates(mcmodel.TransferRequest{State: "closed"}).Error
	}, m.db)
}

func (m *ActivityMonitor) loadAndCheckIfBridgeInactiveForTooLong() bool {
	currentActivityCount := atomic.LoadInt64(&activityCount)
	now := time.Now()
	if currentActivityCount == m.lastSeenActivityCount {
		oneWeekSinceLastActivity := m.lastChanged.Add(oneWeek)
		if oneWeekSinceLastActivity.After(now) {
			return true
		}
	} else {
		m.lastChanged = time.Now()
		m.lastSeenActivityCount = currentActivityCount
	}

	return false
}
