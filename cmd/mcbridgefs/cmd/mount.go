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
	"github.com/apex/log"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/materials-commons/mcglobusfs/mcbridgefs"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

// mountCmd represents the mount command
var mountCmd = &cobra.Command{
	Use:   "mount",
	Short: "Mount a Materials Commons project as a file system",
	Long: `The 'mount' command will mount a Materials Commons project and present the project
as a traditional file system. It will intermediate between this view and the actual underlying
CAS used by Materials Commons.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			log.Fatalf("No path specified for mount.")
		}

		if projectID == -1 {
			log.Fatalf("No project specified.")
		}

		if globusRequestId == -1 {
			log.Fatalf("No globus request specified.")
		}

		db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err != nil {
			log.Fatalf("Failed to open db: %s", err)
		}

		rootNode := mcbridgefs.RootNode(db, projectID, globusRequestId, mcfsRoot)
		server := mustMount(args[0], rootNode)
		go server.listenForUnmount()
		log.Infof("Mounted project at %q, use ctrl+c to stop", args[0])
		server.Wait()
	},
}

var (
	projectID       int
	globusRequestId int
	dsn             string
	mcfsRoot        string
)

func init() {
	rootCmd.AddCommand(mountCmd)
	mountCmd.PersistentFlags().IntVarP(&projectID, "project-id", "p", -1, "Project Id to mount")
	mountCmd.PersistentFlags().IntVarP(&globusRequestId, "globus-request-id", "g", -1, "Globus request this mount is associated with")

	mcfsRoot = os.Getenv("MCFS_ROOT")
	if mcfsRoot == "" {
		log.Fatalf("MCFS_ROOT environment variable not set")
	}

	dsn = os.Getenv("MCDB_CONNECT_STR")
	if dsn == "" {
		log.Fatalf("MCDB_CONNECT_STR environment variable not set")
	}
}

var timeout = 10 * time.Second

type Server struct {
	*fuse.Server
	mountPoint string
}

func mustMount(mountPoint string, root *mcbridgefs.Node) *Server {
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
	}
}

func (s *Server) listenForUnmount() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	sig := <-c
	log.Infof("Got %s signal, unmounting %q...", sig, s.mountPoint)
	if err := s.Unmount(); err != nil {
		log.Errorf("Failed to unmount: %s, try 'umount %s' manually.", err, s.mountPoint)
	}

	<-c
	log.Warnf("Force exiting...")
	os.Exit(1)
}
