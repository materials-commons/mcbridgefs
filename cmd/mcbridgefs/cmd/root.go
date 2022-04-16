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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apex/log"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	mcdb "github.com/materials-commons/gomcdb"
	"github.com/materials-commons/gomcdb/mcmodel"
	"github.com/materials-commons/mcbridgefs/pkg/fs/mcbridgefs"
	"github.com/materials-commons/mcbridgefs/pkg/monitor"
	"github.com/spf13/cobra"
)

var (
	cfgFile           string
	transferRequestID int
	mcfsDir           string
)

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.mcbridgefs.yaml)")
	rootCmd.PersistentFlags().IntVarP(&transferRequestID, "transfer-request-id", "t", -1, "Transfer request this mount is associated with")

	mcfsDir = os.Getenv("MCFS_DIR")
	if mcfsDir == "" {
		log.Fatalf("MCFS_DIR environment variable not set")
	}
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	// We don't use viper because all the needed configuration variables are in the environment that Laravel
	// automatically sets up when a new process is launched.
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "mcbridgefs",
	Short: "Create a file system for file transfers to read from/write to",
	Long: `mcbridgefs creates a FUSE based file system to intercept transfers calls to the file system
and present the Materials Commons storage as a traditional hierarchical file system. It handles creating new
file versions and consistency for the project that the transfer request is associated with.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			log.Fatalf("No path specified for mount.")
		}

		if transferRequestID == -1 {
			log.Fatalf("No transfer request specified.")
		}

		db := mcdb.MustConnectToDB()

		var transferRequest mcmodel.TransferRequest

		if result := db.Preload("Owner").Preload("GlobusTransfer").Find(&transferRequest, transferRequestID); result.Error != nil {
			log.Fatalf("Unable to load TransferRequest id %d: %s", transferRequestID, result.Error)
		}

		if transferRequest.State != "open" {
			log.Infof("TransferRequest %d state is not 'open' (state = %s), aborting", transferRequest.ID, transferRequest.State)
			os.Exit(0)
		}

		ctx, cancel := context.WithCancel(context.Background())

		rootNode := mcbridgefs.CreateFS(mcfsDir, db, transferRequest)
		server := mustStartFuseFileServer(args[0], rootNode)

		onClose := func() {
			server.c <- syscall.SIGINT
		}

		transferRequestMonitor := monitor.NewTransferRequestMonitor(db, ctx, transferRequest, onClose)
		transferRequestMonitor.Start()

		activityMonitor := monitor.NewActivityMonitor(db, transferRequest)
		activityMonitor.Start(ctx)

		go server.listenForUnmount(cancel)

		log.Infof("Mounted project at %q, use ctrl+c to stop", args[0])
		server.Wait()
	},
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

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Infof("rootCmd.Execute failed:", err)
		os.Exit(1)
	}
}
