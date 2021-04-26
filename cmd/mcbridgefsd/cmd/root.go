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
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/apex/log"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/materials-commons/gomcdb/mcmodel"
	"github.com/materials-commons/mcbridgefs/pkg/config"
	"github.com/materials-commons/mcbridgefs/pkg/fs/mcbridgefs"
	"github.com/materials-commons/mcbridgefs/pkg/ops"
	"github.com/spf13/cobra"
	"github.com/subosito/gotenv"
	"gorm.io/gorm"
)

var (
	activeBridges sync.Map
	db            *gorm.DB
	mcfsDir       string
)

type ActiveBridge struct {
	TransferRequestID int    `json:"transfer_request_id"`
	MountPath         string `json:"mount_path"`
	Pid               int    `json:"pid"`
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
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if err := gotenv.Load(config.MustGetDotenvPath()); err != nil {
		log.Fatalf("Loading dotenv file path %s failed: %s", config.MustGetDotenvPath(), err)
	}
	mcfsDir = config.MustGetMCFSDir()
}

var rootCmd = &cobra.Command{
	Use:   "mcbridgefsd",
	Short: "Server for launching bridges",
	Long:  `The mcbridgefsd is responsible for launching new mcbridgefs and monitoring if they exit prematurely.`,
	Run: func(cmd *cobra.Command, args []string) {
		e := echo.New()
		e.HideBanner = true
		e.HidePort = true
		e.Use(middleware.Recover())

		db = ops.MustConnectToDB()

		g := e.Group("/api")
		g.POST("/start-bridge", startBridgeController)
		g.GET("/list-active-bridges", listActiveBridgesController)
		g.POST("/stop-bridge", stopBridgeController)
		g.GET("/stop-server", stopServerController)

		go func() {
			if err := e.Start("localhost:1323"); err != nil {
				log.Fatalf("Unable to start web server: %s", err)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		_ = ctx

		if err := mcbridgefs.LoadProjectTransfers(db); err != nil {
			log.Fatalf("Failed loading existing project transfers: %s", err)
		}

		mountPath := filepath.Join(mcfsDir, "__transfers")
		if err := os.MkdirAll(mountPath, 0777); err != nil {
			log.Fatalf("Unable to make root dir '%s' to mount fs on: %s", mountPath, err)
		}
		rootNode := mcbridgefs.CreateFS(mcfsDir, db)
		server := mustStartFuseFileServer(mountPath, rootNode)

		go server.listenForUnmount(cancel)

		log.Infof("Mounted project at %q, use ctrl+c to stop", args[0])
		server.Wait()
	},
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

var timeout = 10 * time.Second

type Server struct {
	*fuse.Server
	mountPoint string
	c          chan os.Signal
}

func mustStartFuseFileServer(mountPoint string, root *mcbridgefs.Node) *Server {
	opts := &fs.Options{
		AttrTimeout:  &timeout,
		EntryTimeout: &timeout,
		MountOptions: fuse.MountOptions{
			Debug:  false,
			FsName: "mcfs",
		},
	}

	server, err := fs.Mount(mountPoint, root, opts)
	if err != nil {
		log.Fatalf("Unable to mount project %s", err)
	}

	return &Server{
		Server:     server,
		mountPoint: mountPoint,
		c:          make(chan os.Signal, 1),
	}
}

func (s *Server) listenForUnmount(cancelFunc context.CancelFunc) {
	signal.Notify(s.c, syscall.SIGTERM, syscall.SIGINT)
	sig := <-s.c
	log.Infof("Got %s signal, unmounting %q...", sig, s.mountPoint)
	cancelFunc()
	if err := s.Unmount(); err != nil {
		log.Errorf("Failed to unmount: %s, try '/usr/bin/fusermount -u %s' manually.", err, s.mountPoint)
	}

	os.Exit(0)
}
