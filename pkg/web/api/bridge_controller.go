package api

import (
	"net/http"
	"sync"

	"github.com/labstack/echo/v4"
	"github.com/materials-commons/gomcdb/mcmodel"
	"gorm.io/gorm"
)

var (
	activeBridges sync.Map
	db            *gorm.DB
)

type ActiveBridge struct {
	TransferRequestID int    `json:"transfer_request_id"`
	MountPath         string `json:"mount_path"`
	Pid               int    `json:"pid"`
}

type StartBridgeRequest struct {
	TransferRequestID int    `json:"transfer_request_id"`
	MountPath         string `json:"mount_path"`
	LogPath           string `json:"log_path"`
}

func StartBridgeController(c echo.Context) error {
	var req StartBridgeRequest

	if err := c.Bind(&req); err != nil {
		return err
	}

	// Run in background
	go startBridge(req)

	return c.NoContent(http.StatusOK)
}

func startBridge(req StartBridgeRequest) {

}

func ListActiveBridgesController(c echo.Context) error {
	var resp []ActiveBridge

	activeBridges.Range(func(key, value interface{}) bool {
		runningMount := value.(ActiveBridge)
		resp = append(resp, runningMount)
		return true
	})

	return c.JSON(http.StatusOK, &resp)
}

func StopBridgeController(c echo.Context) error {
	var req struct {
		TransferRequestID int `json:"transfer_request_id"`
	}

	if err := c.Bind(&req); err != nil {
		return err
	}

	transferRequest := mcmodel.TransferRequest{ID: req.TransferRequestID}

	err := db.Model(&transferRequest).Update("state", "closed").Error
	if err != nil {
		return err
	}

	return c.NoContent(http.StatusOK)
}
