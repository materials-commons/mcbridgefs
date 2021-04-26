package config

import (
	"os"
	"strconv"
	"time"

	"github.com/apex/log"
)

var (
	globusSettlingPeriod time.Duration = 0
	txRetry                            = 0
	globusEndpointID     string
	mcfsDir              string
	dotenvPath           string
)

func GetGlobusSettlingPeriod() time.Duration {
	var err error
	if globusSettlingPeriod != time.Duration(0) {
		return globusSettlingPeriod
	}

	globusSettlingPeriod, err = time.ParseDuration(os.Getenv("MC_GLOBUS_SETTLING_PERIOD"))
	if err != nil || globusSettlingPeriod.Seconds() < 10 {
		globusSettlingPeriod = 10 * time.Second
	}

	return globusSettlingPeriod
}

func GetTxRetry() int {
	if txRetry != 0 {
		return txRetry
	}

	txRetryCount64, err := strconv.ParseInt(os.Getenv("MC_TX_RETRY"), 10, 32)
	if err != nil || txRetryCount64 < 3 {
		txRetryCount64 = 3
	}

	txRetry = int(txRetryCount64)

	return txRetry
}

func MustGetGlobusEndpointID() string {
	if globusEndpointID != "" {
		return globusEndpointID
	}

	globusEndpointID = os.Getenv("MC_GLOBUS_ENDPOINT_ID")
	if globusEndpointID == "" {
		log.Fatal("MC_GLOBUS_ENDPOINT_ID not set")
	}

	return globusEndpointID
}

func MustGetMCFSDir() string {
	if mcfsDir != "" {
		return mcfsDir
	}

	mcfsDir = os.Getenv("MCFS_DIR")
	if mcfsDir == "" {
		log.Fatal("MCFS_DIR not set")
	}

	return mcfsDir
}

func MustGetDotenvPath() string {
	if dotenvPath != "" {
		return dotenvPath
	}

	dotenvPath = os.Getenv("MC_DOTENV_PATH")
	if dotenvPath == "" {
		log.Fatal("MC_DOTENV_PATH not set")
	}

	return dotenvPath
}
