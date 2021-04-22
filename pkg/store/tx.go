package store

import (
	"os"
	"strconv"

	"gorm.io/gorm"
)

var (
	txRetryCount int
)

func WithTxRetryDefault(fn func(tx *gorm.DB) error, db *gorm.DB) error {
	if txRetryCount < 1 {
		setDefaultRetryCount()
	}

	return WithTxRetry(fn, db, txRetryCount)
}

func WithTxRetry(fn func(tx *gorm.DB) error, db *gorm.DB, retryCount int) error {
	var err error

	if retryCount < 3 {
		retryCount = 3
	}

	for i := 0; i < retryCount; i++ {
		err = db.Transaction(fn)
		if err == nil {
			break
		}
	}

	return err
}

func setDefaultRetryCount() {
	// All updates and creates to the database are wrapped in a transaction. These transactions may need to be
	// retried, especially when they fail because two transactions are deadlocked trying to acquire a lock on
	// a foreign table reference.
	txRetryCount64, err := strconv.ParseInt(os.Getenv("MC_TX_RETRY"), 10, 32)
	if err != nil || txRetryCount64 < 3 {
		txRetryCount64 = 3
	}

	txRetryCount = int(txRetryCount64)
}
