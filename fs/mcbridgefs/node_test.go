package mcbridgefs

import (
	"fmt"
	"github.com/hashicorp/go-uuid"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"path/filepath"
	"testing"
)

func TestWrite100ThousandFiles(t *testing.T) {
	for i := 0; i < 100_000; i++ {
		fname, err := uuid.GenerateUUID()
		require.NoError(t, err, "GenerateUUID failed: %s", err)
		filename := filepath.Join("/home/gtarcea/mcdir/mcfs/data/test/tproj", fname+".txt")
		err = ioutil.WriteFile(filename, []byte(fname), 0644)
		require.NoError(t, err, "ioutil.WriteFile(%s) failed: %s", fname, err)
		if i%100 == 0 {
			fmt.Printf("Wrote %d files...\n", i)
		}
	}
}
