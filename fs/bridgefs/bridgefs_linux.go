// +build linux

// Copyright 2019 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bridgefs

import (
	"context"
	"fmt"
	"github.com/hanwen/go-fuse/v2/fs"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

func (n *BridgeNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	fmt.Println("BridgeNode Getxattr")
	sz, err := unix.Lgetxattr(n.path(""), attr, dest)
	return uint32(sz), fs.ToErrno(err)
}

func (n *BridgeNode) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	fmt.Println("BridgeNode Setxattr")
	err := unix.Lsetxattr(n.path(""), attr, data, int(flags))
	return fs.ToErrno(err)
}

func (n *BridgeNode) Removexattr(ctx context.Context, attr string) syscall.Errno {
	fmt.Println("BridgeNode Removexattr")
	err := unix.Lremovexattr(n.path(""), attr)
	return fs.ToErrno(err)
}

func (n *BridgeNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	fmt.Println("BridgeNode Listxattr")
	sz, err := unix.Llistxattr(n.path(""), dest)
	return uint32(sz), fs.ToErrno(err)
}

func (n *BridgeNode) renameExchange(name string, newparent fs.InodeEmbedder, newName string) syscall.Errno {
	fd1, err := syscall.Open(n.path(""), syscall.O_DIRECTORY, 0)
	if err != nil {
		return fs.ToErrno(err)
	}
	defer syscall.Close(fd1)
	p2 := filepath.Join(n.RootData.Path, newparent.EmbeddedInode().Path(nil))
	fd2, err := syscall.Open(p2, syscall.O_DIRECTORY, 0)
	defer syscall.Close(fd2)
	if err != nil {
		return fs.ToErrno(err)
	}

	var st syscall.Stat_t
	if err := syscall.Fstat(fd1, &st); err != nil {
		return fs.ToErrno(err)
	}

	// Double check that nodes didn't change from under us.
	inode := &n.Inode
	if inode.Root() != inode && inode.StableAttr().Ino != n.RootData.StableAttrFromStat(&st).Ino {
		return syscall.EBUSY
	}
	if err := syscall.Fstat(fd2, &st); err != nil {
		return fs.ToErrno(err)
	}

	newinode := newparent.EmbeddedInode()
	if newinode.Root() != newinode && newinode.StableAttr().Ino != n.RootData.StableAttrFromStat(&st).Ino {
		return syscall.EBUSY
	}

	return fs.ToErrno(unix.Renameat2(fd1, name, fd2, newName, unix.RENAME_EXCHANGE))
}

func (n *BridgeNode) CopyFileRange(ctx context.Context, fhIn fs.FileHandle,
	offIn uint64, out *fs.Inode, fhOut fs.FileHandle, offOut uint64,
	len uint64, flags uint64) (uint32, syscall.Errno) {
	lfIn, ok := fhIn.(*BridgeFileHandle)
	if !ok {
		return 0, syscall.ENOTSUP
	}
	lfOut, ok := fhOut.(*BridgeFileHandle)
	if !ok {
		return 0, syscall.ENOTSUP
	}

	signedOffIn := int64(offIn)
	signedOffOut := int64(offOut)
	count, err := unix.CopyFileRange(lfIn.Fd, &signedOffIn, lfOut.Fd, &signedOffOut, int(len), int(flags))
	return uint32(count), fs.ToErrno(err)
}
