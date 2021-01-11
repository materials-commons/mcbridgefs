package bridgefs

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type FileHandle struct {
}

func (f *FileHandle) Read(ctx context.Context, buf []byte, off int64) (res fuse.ReadResult, errno syscall.Errno) {
	return nil, syscall.EIO
}

func (f *FileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	return 0, syscall.EIO
}

func (f *FileHandle) Flush(ctx context.Context) syscall.Errno {
	return syscall.EIO
}

func (f *FileHandle) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	return 0, syscall.EIO
}
