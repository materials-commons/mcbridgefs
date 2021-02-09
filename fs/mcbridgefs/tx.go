package mcbridgefs

import "gorm.io/gorm"

func withTxRetry(fn func(tx *gorm.DB) error, db *gorm.DB, retryCount int) error {
	var err error
	for i := 0; i < retryCount; i++ {
		err = DB.Transaction(fn)
		if err == nil {
			break
		}
	}

	return err
}
