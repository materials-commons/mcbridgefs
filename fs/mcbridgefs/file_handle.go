package mcbridgefs

import (
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/materials-commons/mcbridgefs/fs/bridgefs"
)

// Code based on loopback file system from github.com/hanwen/go-fuse/v2/fs/file.go

type FileHandle struct {
	*bridgefs.BridgeFileHandle
}

var _ = (fs.FileHandle)((*FileHandle)(nil))
var _ = (fs.FileReleaser)((*FileHandle)(nil))
var _ = (fs.FileGetattrer)((*FileHandle)(nil))
var _ = (fs.FileReader)((*FileHandle)(nil))
var _ = (fs.FileWriter)((*FileHandle)(nil))
var _ = (fs.FileGetlker)((*FileHandle)(nil))
var _ = (fs.FileSetlker)((*FileHandle)(nil))
var _ = (fs.FileSetlkwer)((*FileHandle)(nil))
var _ = (fs.FileLseeker)((*FileHandle)(nil))
var _ = (fs.FileFlusher)((*FileHandle)(nil))
var _ = (fs.FileFsyncer)((*FileHandle)(nil))
var _ = (fs.FileSetattrer)((*FileHandle)(nil))
var _ = (fs.FileAllocater)((*FileHandle)(nil))

func NewFileHandle(fd int) fs.FileHandle {
	return &FileHandle{bridgefs.NewBridgeFileHandle(fd).(*bridgefs.BridgeFileHandle)}
}
