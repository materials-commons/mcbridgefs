package mcbridgefs

import (
	"bytes"
	"context"
	"io"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/materials-commons/mcbridgefs/pkg/fs/bridgefs"
)

// Code based on loopback file system from github.com/hanwen/go-fuse/v2/fs/file.go

type FileHandle struct {
	*bridgefs.BridgeFileHandle
	Flags               uint32
	Path                string
	TotalBytes          int64
	transferPathContext *TransferPathContext
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

func NewFileHandle(fd int, flags uint32, path string) fs.FileHandle {
	return &FileHandle{
		BridgeFileHandle:    bridgefs.NewBridgeFileHandle(fd).(*bridgefs.BridgeFileHandle),
		Flags:               flags,
		Path:                path,
		TotalBytes:          0,
		transferPathContext: ToTransferPathContext(path),
	}
}

// Write overrides the BridgeFileHandle write to incorporate updating the checksum as bytes
// are written to the file.
func (f *FileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if LockedFS(f.transferPathContext) {
		return 0, syscall.EIO
	}

	f.Mu.Lock()
	defer f.Mu.Unlock()

	n, err := syscall.Pwrite(f.Fd, data, off)
	if err != fs.OK {
		return uint32(n), fs.ToErrno(err)
	}

	file := openedFilesTracker.Get(f.Path)
	if file != nil && n > 0 {
		_, _ = io.Copy(file.hasher, bytes.NewBuffer(data[:n]))
	}

	f.TotalBytes = f.TotalBytes + int64(n)

	return uint32(n), fs.OK
}

func (f *FileHandle) Flush(ctx context.Context) syscall.Errno {
	return fs.OK
}
