package mcglobusfs

import (
	"fmt"
	"os/exec"
)

func StartMCBridgeFS(globusRequestID, projectID int, path string) (*exec.Cmd, error) {
	command := fmt.Sprintf("mcbridgefs mount -g %d -p %d %s", globusRequestID, projectID, path)
	cmd := exec.Command(command)
	return cmd, cmd.Start()
}
