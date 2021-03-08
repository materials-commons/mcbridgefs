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

	"github.com/apex/log"
	"github.com/labstack/echo/v4/middleware"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/labstack/echo/v4"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "mcbridefsd",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		e := echo.New()
		e.HideBanner = true
		e.HidePort = true
		e.Use(middleware.Recover())

		g := e.Group("/api")
		g.POST("/start", startBridgeController)

		if err := e.Start("localhost:1323"); err != nil {
			log.Fatalf("Unable to start web server: %s", err)
		}
	},
}

func startBridgeController(c echo.Context) error {
	var req struct {
		TransferRequestID int    `json:"transfer_request_id"`
		MountPath         string `json:"mount_path"`
		LogPath           string `json:"log_path"`
	}

	if err := c.Bind(&req); err != nil {
		return err
	}

	cmd := exec.Command("nohup", "/usr/local/bin/mcbridgefs.sh", fmt.Sprintf("%d", req.TransferRequestID),
		req.MountPath, req.LogPath)

	// Run in background
	go func(cmd *exec.Cmd) {
		cmd.Run()
	}(cmd)

	return c.NoContent(http.StatusOK)
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

		// Search config in home directory with name ".mcbridefsd" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".mcbridefsd")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
