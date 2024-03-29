// Copyright © 2021 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sync"

	"github.com/apex/log"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	mcdb "github.com/materials-commons/gomcdb"
	"github.com/materials-commons/gomcdb/mcmodel"
	"github.com/spf13/cobra"
	"github.com/subosito/gotenv"
	"gorm.io/gorm"
)

var (
	cfgFile       string
	activeBridges sync.Map
	db            *gorm.DB
)

type ActiveBridge struct {
	TransferRequestID int    `json:"transfer_request_id"`
	MountPath         string `json:"mount_path"`
	Pid               int    `json:"pid"`
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "mcbridgefsd",
	Short: "Server for launching bridges",
	Long:  `The mcbridgefsd is responsible for launching new mcbridgefs and monitoring if they exit prematurely.`,
	Run: func(cmd *cobra.Command, args []string) {
		db := mcdb.MustConnectToDB()

		// Remove any existing globus_transfers and transfer_requests because there is no longer
		// a bridge associated with them.
		closeExistingGlobusTransfers(db)

		e := echo.New()
		e.HideBanner = true
		e.HidePort = true
		e.Use(middleware.Recover())

		g := e.Group("/api")
		g.POST("/start-bridge", startBridgeController)
		g.GET("/list-active-bridges", listActiveBridgesController)
		g.POST("/stop-bridge", stopBridgeController)
		g.GET("/stop-server", stopServerController)

		if err := e.Start("localhost:1323"); err != nil {
			log.Fatalf("Unable to start web server: %s", err)
		}
	},
}

// closeExistingGlobusTransfers will mark all existing globus transfers and transfer requests
// as closed so they can be cleaned up. When the server starts it performs this action to
// remove old requests that no longer have a bridge associated with them.
func closeExistingGlobusTransfers(db *gorm.DB) {
	_ = db.Exec("update globus_transfers set state = ?", "closed").Error
	_ = db.Exec("update transfer_requests set state = ?", "closed").Error
}

func stopServerController(_ echo.Context) error {
	os.Exit(0)
	return nil
}

func stopBridgeController(c echo.Context) error {
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

func listActiveBridgesController(c echo.Context) error {
	var resp []ActiveBridge

	activeBridges.Range(func(key, value interface{}) bool {
		runningMount := value.(ActiveBridge)
		resp = append(resp, runningMount)
		return true
	})

	return c.JSON(http.StatusOK, &resp)
}

type StartBridgeRequest struct {
	TransferRequestID int    `json:"transfer_request_id"`
	MountPath         string `json:"mount_path"`
	LogPath           string `json:"log_path"`
}

func startBridgeController(c echo.Context) error {
	var req StartBridgeRequest

	if err := c.Bind(&req); err != nil {
		return err
	}

	// Run in background
	go startBridge(req)

	return c.NoContent(http.StatusOK)
}

func startBridge(req StartBridgeRequest) {

	cmd := exec.Command("nohup", "/usr/local/bin/mcbridgefs.sh", fmt.Sprintf("%d", req.TransferRequestID),
		req.MountPath, req.LogPath)
	if err := cmd.Start(); err != nil {
		log.Errorf("Starting bridge failed (%d, %s): %s", req.TransferRequestID, req.MountPath, err)
		return
	}

	activeBridge := ActiveBridge{
		TransferRequestID: req.TransferRequestID,
		MountPath:         req.MountPath,
		Pid:               cmd.Process.Pid,
	}

	// Store running bridge so it can be queried and tracked
	activeBridges.Store(req.MountPath, activeBridge)

	if err := cmd.Wait(); err != nil {
		log.Errorf("Bridge exited with error: %s", err)
	}

	activeBridges.Delete(req.MountPath)
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.mcbridefsd.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	dotenvFilePath := os.Getenv("MC_DOTENV_PATH")
	if dotenvFilePath == "" {
		log.Fatalf("MC_DOTENV_PATH not set")
	}

	if err := gotenv.Load(dotenvFilePath); err != nil {
		log.Fatalf("Loading %s failed: %s", dotenvFilePath, err)
	}
}
