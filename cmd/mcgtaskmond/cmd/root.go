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
	globus "github.com/materials-commons/goglobus"
	mcdb "github.com/materials-commons/gomcdb"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"os"
	"os/signal"
	"syscall"
	"time"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile          string
	globusEndpointID string
	globusCCUser     string
	globusCCToken    string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "mcgtaskmond",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		var (
			err          error
			db           *gorm.DB
			globusClient *globus.Client
		)

		log.Infof("Starting mcgtaskmond...")
		ctx, cancel := context.WithCancel(context.Background())

		gormConfig := &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		}

		if db, err = gorm.Open(mysql.Open(mcdb.MakeDSNFromEnv()), gormConfig); err != nil {
			log.Fatalf("Failed to open db (%s): %s", mcdb.MakeDSNFromEnv(), err)
		}

		if globusClient, err = globus.CreateConfidentialClient(globusCCUser, globusCCToken); err != nil {
			log.Fatalf("Failed to create globus confidential client: %s", err)
		}

		_ = db
		_ = globusClient
		_ = ctx

		quit := make(chan os.Signal)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		log.Infof("Recevied signal, shutting down mcgtaskmond...")
		cancel()

		select {
		case <-time.After(2 * time.Second):
		}

	},
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
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.mcgtaskmond.yaml)")

	if globusEndpointID = os.Getenv("MC_CONFIDENTIAL_CLIENT_ENDPOINT"); globusEndpointID == "" {
		log.Fatalf("MC_CONFIDENTIAL_CLIENT_ENDPOINT env var is not set")
	}

	if globusCCUser = os.Getenv("MC_CONFIDENTIAL_CLIENT_USER"); globusCCUser == "" {
		log.Fatalf("MC_CONFIDENTIAL_CLIENT_USER env var is not set")
	}

	if globusCCToken = os.Getenv("MC_CONFIDENTIAL_CLIENT_PW"); globusCCToken == "" {
		log.Fatalf("MC_CONFIDENTIAL_CLIENT_PW env var is not set")
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

		// Search config in home directory with name ".mcgtaskmond" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".mcgtaskmond")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
