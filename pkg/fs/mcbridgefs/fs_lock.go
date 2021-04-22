package mcbridgefs

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"
)

type FSLock struct {
	LockedAt time.Time
}

var fsLocks sync.Map

func LockFS(c *TransferPathContext) {
	key := lockKey(c)
	fsLock := FSLock{LockedAt: time.Now()}
	fsLocks.LoadOrStore(key, fsLock)
}

func UnlockFS(c *TransferPathContext) {
	key := lockKey(c)
	fsLocks.Delete(key)
}

func LockedFS(c *TransferPathContext) bool {
	key := lockKey(c)
	_, found := fsLocks.Load(key)
	return found
}

func lockKey(c *TransferPathContext) string {
	return filepath.Join("/", c.TransferType, fmt.Sprintf("%d", c.UserID), fmt.Sprintf("%d", c.ProjectID))
}
