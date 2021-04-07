package ops

import (
	"time"

	"github.com/apex/log"
	mcdb "github.com/materials-commons/gomcdb"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const maxDBRetries = 5

// MustConnectToDB will attempt to connect to the database maxDBRetries times. If it isn't successful
// after that number of retries then it will call log.Fatalf(), which will cause the server to exit.
// Between retry attempts it will sleep for 3 seconds.
func MustConnectToDB() *gorm.DB {
	var (
		err error
		db  *gorm.DB
	)

	gormConfig := &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	}

	retryCount := 1
	for {
		db, err = gorm.Open(mysql.Open(mcdb.MakeDSNFromEnv()), gormConfig)
		switch {
		case err == nil:
			// Connected to db, yay!
			return db
		case retryCount == maxDBRetries:
			// Retry limit exceeded :-(
			log.Fatalf("Failed to open db (%s): %s", mcdb.MakeDSNFromEnv(), err)
		default:
			// Couldn't connect, so increment count, then wait a bit before trying again.
			retryCount++
			time.Sleep(3 * time.Second)
		}
	}
}
