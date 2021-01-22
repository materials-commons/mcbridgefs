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
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/materials-commons/mcglobusfs/pkg/mcglobusfs"

	"github.com/apex/log"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/materials-commons/mcglobusfs/pkg/fs/mcbridgefs"
	"github.com/materials-commons/mcglobusfs/pkg/globusapi"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/spf13/cobra"
)

var (
	projectID        int
	globusRequestID  int
	dsn              string
	mcfsRoot         string
	globusCCUser     string
	globusCCToken    string
	globusEndpointID string
	globusRoot       string
)

func init() {
	rootCmd.AddCommand(mountCmd)
	mountCmd.PersistentFlags().IntVarP(&projectID, "project-id", "p", -1, "Project Id to mount")
	mountCmd.PersistentFlags().IntVarP(&globusRequestID, "globus-request-id", "g", -1, "Globus request this mount is associated with")

	mcfsRoot = os.Getenv("MCFS_ROOT")
	if mcfsRoot == "" {
		log.Fatalf("MCFS_ROOT environment variable not set")
	}

	dsn = os.Getenv("MCDB_CONNECT_STR")
	if dsn == "" {
		log.Fatalf("MCDB_CONNECT_STR environment variable not set")
	}

	if globusCCUser = os.Getenv("MC_GLOBUS_CC_USER"); globusCCUser == "" {
		log.Fatalf("MC_GLOBUS_CC_USER environment variable not set")
	}

	if globusCCToken = os.Getenv("MC_GLOBUS_CC_TOKEN"); globusCCToken == "" {
		log.Fatalf("MC_GLOBUS_CC_TOKEN environment variable not set")
	}

	if globusEndpointID = os.Getenv("MC_GLOBUS_ENDPOINT_ID"); globusEndpointID == "" {
		log.Fatalf("MC_GLOBUS_ENDPOINT_ID environment variable not set")
	}

	if globusRoot = os.Getenv("MC_GLOBUS_ROOT"); globusRoot == "" {
		log.Fatalf("MC_GLOBUS_ROOT environment variable not set")
	}
}

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

		if globusRequestID == -1 {
			log.Fatalf("No globus request specified.")
		}

		var (
			err          error
			db           *gorm.DB
			globusClient *globusapi.Client
		)

		if db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{}); err != nil {
			log.Fatalf("Failed to open db: %s", err)
		}

		if globusClient, err = globusapi.CreateConfidentialClient(globusCCUser, globusCCToken); err != nil {
			log.Fatalf("Failed to create confidential globus client: %s", err)
		}

		var _ = globusClient

		var globusRequest mcglobusfs.GlobusRequest

		if err := db.Preload("Owner").First(&globusRequest, globusRequestID); err != nil {
			log.Fatalf("Unable to load GlobusRequest id %d: %s", globusRequestID, err)
		}

		rootNode := mcbridgefs.RootNode(db, projectID, globusRequestID, mcfsRoot)
		server := mustMount(args[0], rootNode)
		go server.listenForUnmount()
		log.Infof("Mounted project at %q, use ctrl+c to stop", args[0])
		server.Wait()
	},
}

// makeGlobusPath constructs the path as Globus expects to see it. Globus needs the path to both
// start and end with a '/', eg /__globus/abc/.
func makeGlobusPath(dir string) string {
	// We need to Sprintf the ending slash because filepath.Join removes the trailing slash.
	return fmt.Sprintf("%s/", filepath.Join("/", os.Getenv("MC_GLOBUS_ROOT"), dir))
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
