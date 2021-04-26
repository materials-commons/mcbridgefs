package store

import (
	"github.com/materials-commons/mcbridgefs/pkg/config"
	"gorm.io/gorm"
)

func WithTxRetryDefault(fn func(tx *gorm.DB) error, db *gorm.DB) error {
	return WithTxRetry(fn, db, config.GetTxRetry())
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
