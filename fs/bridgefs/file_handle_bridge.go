// Copyright 2019 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bridgefs

import (
	"context"
	"fmt"
	"github.com/hanwen/go-fuse/v2/fs"
	"sync"

	//	"time"

	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

// NewBridgeFileHandle creates a FileHandle out of a file descriptor. All
// operations are implemented. When using the Fd from a *os.File, call
// syscall.Dup() on the Fd, to avoid os.File's finalizer from closing
// the file descriptor.
func NewBridgeFileHandle(fd int) fs.FileHandle {
	return &BridgeFileHandle{Fd: fd}
}

type BridgeFileHandle struct {
	Mu sync.Mutex
	Fd int
}

var _ = (fs.FileHandle)((*BridgeFileHandle)(nil))
var _ = (fs.FileReleaser)((*BridgeFileHandle)(nil))
var _ = (fs.FileGetattrer)((*BridgeFileHandle)(nil))
var _ = (fs.FileReader)((*BridgeFileHandle)(nil))
var _ = (fs.FileWriter)((*BridgeFileHandle)(nil))
var _ = (fs.FileGetlker)((*BridgeFileHandle)(nil))
var _ = (fs.FileSetlker)((*BridgeFileHandle)(nil))
var _ = (fs.FileSetlkwer)((*BridgeFileHandle)(nil))
var _ = (fs.FileLseeker)((*BridgeFileHandle)(nil))
var _ = (fs.FileFlusher)((*BridgeFileHandle)(nil))
var _ = (fs.FileFsyncer)((*BridgeFileHandle)(nil))
var _ = (fs.FileSetattrer)((*BridgeFileHandle)(nil))
var _ = (fs.FileAllocater)((*BridgeFileHandle)(nil))

func (f *BridgeFileHandle) Read(ctx context.Context, buf []byte, off int64) (res fuse.ReadResult, errno syscall.Errno) {
	fmt.Println("BridgeFileHandle Read")
	f.Mu.Lock()
	defer f.Mu.Unlock()
	r := fuse.ReadResultFd(uintptr(f.Fd), off, len(buf))
	return r, fs.OK
}

func (f *BridgeFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	fmt.Println("BridgeFileHandleWrite")
	f.Mu.Lock()
	defer f.Mu.Unlock()
	n, err := syscall.Pwrite(f.Fd, data, off)
	return uint32(n), fs.ToErrno(err)
}

func (f *BridgeFileHandle) Release(ctx context.Context) syscall.Errno {
	fmt.Println("BridgeFileHandle Release")
	f.Mu.Lock()
	defer f.Mu.Unlock()
	if f.Fd != -1 {
		err := syscall.Close(f.Fd)
		f.Fd = -1
		return fs.ToErrno(err)
	}
	return syscall.EBADF
}

func (f *BridgeFileHandle) Flush(ctx context.Context) syscall.Errno {
	fmt.Println("BridgeFileHandle Flush")
	f.Mu.Lock()
	defer f.Mu.Unlock()
	// Since Flush() may be called for each dup'd Fd, we don't
	// want to really close the file, we just want to flush. This
	// is achieved by closing a dup'd Fd.
	newFd, err := syscall.Dup(f.Fd)

	if err != nil {
		return fs.ToErrno(err)
	}
	err = syscall.Close(newFd)
	return fs.ToErrno(err)
}

func (f *BridgeFileHandle) Fsync(ctx context.Context, flags uint32) (errno syscall.Errno) {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	r := fs.ToErrno(syscall.Fsync(f.Fd))

	return r
}

const (
	_OFD_GETLK  = 36
	_OFD_SETLK  = 37
	_OFD_SETLKW = 38
)

func (f *BridgeFileHandle) Getlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) (errno syscall.Errno) {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	flk := syscall.Flock_t{}
	lk.ToFlockT(&flk)
	errno = fs.ToErrno(syscall.FcntlFlock(uintptr(f.Fd), _OFD_GETLK, &flk))
	out.FromFlockT(&flk)
	return
}

func (f *BridgeFileHandle) Setlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) (errno syscall.Errno) {
	return f.setLock(ctx, owner, lk, flags, false)
}

func (f *BridgeFileHandle) Setlkw(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) (errno syscall.Errno) {
	return f.setLock(ctx, owner, lk, flags, true)
}

func (f *BridgeFileHandle) setLock(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32, blocking bool) (errno syscall.Errno) {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	if (flags & fuse.FUSE_LK_FLOCK) != 0 {
		var op int
		switch lk.Typ {
		case syscall.F_RDLCK:
			op = syscall.LOCK_SH
		case syscall.F_WRLCK:
			op = syscall.LOCK_EX
		case syscall.F_UNLCK:
			op = syscall.LOCK_UN
		default:
			return syscall.EINVAL
		}
		if !blocking {
			op |= syscall.LOCK_NB
		}
		return fs.ToErrno(syscall.Flock(f.Fd, op))
	} else {
		flk := syscall.Flock_t{}
		lk.ToFlockT(&flk)
		var op int
		if blocking {
			op = _OFD_SETLKW
		} else {
			op = _OFD_SETLK
		}
		return fs.ToErrno(syscall.FcntlFlock(uintptr(f.Fd), op, &flk))
	}
}

func (f *BridgeFileHandle) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	fmt.Println("BridgeFileHandle Setattr")
	if errno := f.setAttr(ctx, in); errno != 0 {
		return errno
	}

	return f.Getattr(ctx, out)
}

func (f *BridgeFileHandle) setAttr(ctx context.Context, in *fuse.SetAttrIn) syscall.Errno {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	var errno syscall.Errno
	if mode, ok := in.GetMode(); ok {
		errno = fs.ToErrno(syscall.Fchmod(f.Fd, mode))
		if errno != 0 {
			return errno
		}
	}

	uid32, uOk := in.GetUID()
	gid32, gOk := in.GetGID()
	if uOk || gOk {
		uid := -1
		gid := -1

		if uOk {
			uid = int(uid32)
		}
		if gOk {
			gid = int(gid32)
		}
		errno = fs.ToErrno(syscall.Fchown(f.Fd, uid, gid))
		if errno != 0 {
			return errno
		}
	}

	mtime, mok := in.GetMTime()
	atime, aok := in.GetATime()

	if mok || aok {
		ap := &atime
		mp := &mtime
		if !aok {
			ap = nil
		}
		if !mok {
			mp = nil
		}
		errno = f.utimens(ap, mp)
		if errno != 0 {
			return errno
		}
	}

	if sz, ok := in.GetSize(); ok {
		errno = fs.ToErrno(syscall.Ftruncate(f.Fd, int64(sz)))
		if errno != 0 {
			return errno
		}
	}
	return fs.OK
}

func (f *BridgeFileHandle) Getattr(ctx context.Context, a *fuse.AttrOut) syscall.Errno {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	st := syscall.Stat_t{}
	err := syscall.Fstat(f.Fd, &st)
	if err != nil {
		return fs.ToErrno(err)
	}
	a.FromStat(&st)

	return fs.OK
}

func (f *BridgeFileHandle) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	n, err := unix.Seek(f.Fd, int64(off), int(whence))
	return uint64(n), fs.ToErrno(err)
}
