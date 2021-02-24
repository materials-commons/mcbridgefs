package mcbridgefs

import "gorm.io/gorm"

func withTxRetry(fn func(tx *gorm.DB) error, db *gorm.DB, retryCount int) error {
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
