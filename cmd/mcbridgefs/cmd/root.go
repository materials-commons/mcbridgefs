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
	"github.com/apex/log"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	mcdb "github.com/materials-commons/gomcdb"
	"github.com/materials-commons/gomcdb/mcmodel"
	mcbridge "github.com/materials-commons/mcbridgefs"
	"github.com/materials-commons/mcbridgefs/fs/mcbridgefs"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

var (
	cfgFile         string
	globusRequestID int
	mcfsDir         string
)

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.mcbridgefs.yaml)")
	rootCmd.PersistentFlags().IntVarP(&globusRequestID, "globus-request-id", "g", -1, "Globus request this mount is associated with")

	mcfsDir = os.Getenv("MCFS_DIR")
	if mcfsDir == "" {
		log.Fatalf("MCFS_DIR environment variable not set")
	}
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".mcbridgefs" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".mcbridgefs")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "mcbridgefs",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			log.Fatalf("No path specified for mount.")
		}

		if globusRequestID == -1 {
			log.Fatalf("No globus request specified.")
		}

		var (
			err error
			db  *gorm.DB
		)

		gormConfig := &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		}

		if db, err = gorm.Open(mysql.Open(mcdb.MakeDSNFromEnv()), gormConfig); err != nil {
			log.Fatalf("Failed to open db (%s): %s", mcdb.MakeDSNFromEnv(), err)
		}

		var globusRequest mcmodel.GlobusRequest

		if result := db.Preload("Owner").Find(&globusRequest, globusRequestID); result.Error != nil {
			log.Fatalf("Unable to load GlobusRequest id %d: %s", globusRequestID, result.Error)
		}

		ctx, cancel := context.WithCancel(context.Background())

		rootNode := mcbridgefs.RootNode(db, globusRequest.ProjectID, globusRequestID, mcfsDir)
		server := mustMount(args[0], rootNode)

		onClose := func() {
			if err := server.Unmount(); err != nil {
				log.Errorf("Failed to unmount: %s, try 'umount %s' manually.", err, server.mountPoint)
			}

			os.Exit(0)
		}

		closedRequestMonitor := mcbridge.NewClosedGlobusRequestMonitor(db, ctx, globusRequest, onClose)
		closedRequestMonitor.Start()

		go server.listenForUnmount(cancel)

		log.Infof("Mounted project at %q, use ctrl+c to stop", args[0])
		server.Wait()
	},
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

func (s *Server) listenForUnmount(cancelFunc context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	sig := <-c
	log.Infof("Got %s signal, unmounting %q...", sig, s.mountPoint)
	cancelFunc()
	if err := s.Unmount(); err != nil {
		log.Errorf("Failed to unmount: %s, try 'umount %s' manually.", err, s.mountPoint)
	}

	<-c
	log.Warnf("Force exiting...")
	os.Exit(1)
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
