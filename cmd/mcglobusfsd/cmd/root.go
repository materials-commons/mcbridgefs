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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/labstack/echo/v4/middleware"
	"gorm.io/driver/mysql"

	"github.com/apex/log"
	"github.com/labstack/echo/v4"
	"gorm.io/gorm"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile          string
	port             int
	mcfsRoot         string
	dsn              string
	bridgeRoot       string
	globusCCUser     string
	globusCCToken    string
	globusEndpointID string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "mcglobusfsd",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: cliCmdRoot,
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

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.mcglobusfsd.yaml)")
	rootCmd.Flags().IntVarP(&port, "port", "p", 4000, "Port to listen on")

	if mcfsRoot = os.Getenv("MCFS_ROOT"); mcfsRoot == "" {
		log.Fatalf("MCFS_ROOT environment variable not set")
	}

	if dsn = os.Getenv("MCDB_CONNECT_STR"); dsn == "" {
		log.Fatalf("MCDB_CONNECT_STR environment variable not set")
	}

	if bridgeRoot = os.Getenv("MC_GLOBUS_BRIDGE_ROOT"); bridgeRoot == "" {
		log.Fatalf("MC_GLOBUS_BRIDGE_ROOT environment variable not set")
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

	// Make sure the environment variable is set, even though we don't need it spawned child processes will.
	if globusRoot := os.Getenv("MC_GLOBUS_ROOT"); globusRoot == "" {
		log.Fatalf("MC_GLOBUS_ROOT environment variable not set")
	}
}

func cliCmdRoot(cmd *cobra.Command, args []string) {
	log.Infof("Starting mcglobusfsd...")
	ctx, cancel := context.WithCancel(context.Background())
	db := connectToDB()
	e := setupEcho()

	var _ = db

	log.Infof("Listening on port %d", port)

	go func() {
		if err := e.Start(fmt.Sprintf(":%d", port)); err != nil {
			log.Infof("Shutting down mcglobusfsd: %s", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Infof("Received signal, shutting down mcglobusfsd...")
	cancel()

	select {
	case <-time.After(2 * time.Second):
	}

	if err := e.Shutdown(ctx); err != nil {
		log.Fatalf("Error shutting down server: %s", err)
	}
}

func setupEcho() *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	return e
}

func connectToDB() *gorm.DB {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to open db: %s", err)
	}

	return db
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

		// Search config in home directory with name ".mcglobusfsd" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".mcglobusfsd")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
