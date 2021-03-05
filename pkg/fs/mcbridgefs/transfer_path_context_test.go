package mcbridgefs

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPathParsing(t *testing.T) {
	tests := []struct {
		path         string
		transferType string
		userID       int
		projectID    int
		rest         string
	}{
		{path: "/globus/1/1", transferType: "globus", userID: 1, projectID: 1, rest: "/"},
		{path: "/globus/1/1/abc", transferType: "globus", userID: 1, projectID: 1, rest: "/abc"},
		{path: "/globus/1/1/abc/def/ghi.txt", transferType: "globus", userID: 1, projectID: 1, rest: "/abc/def/ghi.txt"},
		{path: "/globus/1", transferType: "globus", userID: 1, projectID: 0, rest: ""},
		{path: "/globus", transferType: "globus", userID: 0, projectID: 0, rest: ""},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("Path:%s", test.path), func(t *testing.T) {
			transferPathContext := ToTransferPathContext(test.path)
			require.Equal(t, test.transferType, transferPathContext.TransferType)
			require.Equal(t, test.userID, transferPathContext.UserID)
			require.Equal(t, test.projectID, transferPathContext.ProjectID)
			require.Equal(t, test.rest, transferPathContext.Path)
		})
	}
}
