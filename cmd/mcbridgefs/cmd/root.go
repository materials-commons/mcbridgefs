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
	"github.com/materials-commons/mcbridgefs/pkg/config"
	"github.com/materials-commons/mcbridgefs/pkg/fs/mcbridgefs"
	"github.com/materials-commons/mcbridgefs/pkg/ops"
	"github.com/spf13/cobra"
)

var (
	mcfsDir string
)

func init() {
	cobra.OnInitialize(initConfig)
	mcfsDir = config.MustGetMCFSDir()
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
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

		db := ops.MustConnectToDB()

		ctx, cancel := context.WithCancel(context.Background())
		_ = ctx

		if err := mcbridgefs.LoadProjectTransfers(db); err != nil {
			log.Fatalf("Failed loading existing project transfers: %s", err)
		}

		rootNode := mcbridgefs.CreateFS(mcfsDir, db)
		server := mustStartFuseFileServer(args[0], rootNode)

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
