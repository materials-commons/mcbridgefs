package mcbridgefs

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/hashicorp/go-uuid"
	"github.com/stretchr/testify/require"
)

func TestWrite100ThousandFiles(t *testing.T) {
	timeStart := time.Now()
	for i := 0; i < 100_000; i++ {
		fname, err := uuid.GenerateUUID()
		require.NoError(t, err, "GenerateUUID failed: %s", err)
		filename := filepath.Join("/home/gtarcea/mcdir/mcfs/data/test/tproj", fname+".txt")
		err = ioutil.WriteFile(filename, []byte(fname), 0644)
		require.NoError(t, err, "ioutil.WriteFile(%s) failed: %s", fname, err)
		if i%100 == 0 {
			timeElapsed := time.Now().Sub(timeStart)
			fmt.Printf("Wrote %d files in %.0f seconds...\n", i, timeElapsed.Seconds())
			timeStart = time.Now()
		}
	}
}

func TestWrite500FilesInParallel(t *testing.T) {
	timeStart := time.Now()
	for i := 0; i < 50; i++ {
		t.Run(fmt.Sprintf("Parallel%d", i), func(t *testing.T) {
			t.Parallel()
			for j := 0; j < 10; j++ {
				fname, err := uuid.GenerateUUID()
				require.NoError(t, err, "GenerateUUID failed: %s", err)
				filename := filepath.Join("/home/gtarcea/mcdir/mcfs/data/test/tproj", fname+".txt")
				err = ioutil.WriteFile(filename, []byte(fname), 0644)
				require.NoError(t, err, "ioutil.WriteFile(%s) failed: %s", fname, err)
				if j%10 == 0 {
					timeElapsed := time.Now().Sub(timeStart)
					fmt.Printf("Wrote %d files in %.0f seconds...\n", i, timeElapsed.Seconds())
					timeStart = time.Now()
				}
			}
		})
	}
}

func TestFTruncateFile(t *testing.T) {
	fd, err := syscall.Open("/home/gtarcea/mcdir/mcfs/data/test/tproj/newfile.txt", syscall.O_RDWR, 0)
	require.NoError(t, err, "Open failed: %s", err)
	err = syscall.Ftruncate(fd, 0)
	require.NoError(t, err, "Ftruncate failed: %s", err)
	err = syscall.Close(fd)
	require.NoError(t, err, "Close failed: %s", err)
}
