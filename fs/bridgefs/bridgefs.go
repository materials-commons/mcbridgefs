// Copyright 2019 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bridgefs

import (
	"context"
	"fmt"
	"github.com/hanwen/go-fuse/v2/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// loopbackRoot holds the parameters for creating a new loopback
// filesystem. Loopback filesystem delegate their operations to an
// underlying POSIX file system.
type bridgeRoot struct {
	// The path to the root of the underlying file system.
	Path string

	// The device on which the Path resides. This must be set if
	// the underlying filesystem crosses file systems.
	Dev uint64

	// NewNode returns a new InodeEmbedder to be used to respond
	// to a LOOKUP/CREATE/MKDIR/MKNOD opcode. If not set, use a
	// LoopbackNode.
	NewNode func(rootData *bridgeRoot) fs.InodeEmbedder

	GetRealPath func(nodePath string) string
}

// NewBridgeRoot returns a root node for a loopback file system whose
// root is at the given root. This node implements all NodeXxxxer
// operations available.
func NewBridgeRoot(rootPath string, NewNode func(rootData *bridgeRoot) fs.InodeEmbedder, GetRealPath func(nodePath string) string) (fs.InodeEmbedder, error) {
	var st syscall.Stat_t
	err := syscall.Stat(rootPath, &st)
	if err != nil {
		return nil, err
	}

	root := &bridgeRoot{
		Path: rootPath,
		Dev:  uint64(st.Dev),
	}

	if NewNode != nil {
		root.NewNode = NewNode
	}

	if GetRealPath != nil {
		root.GetRealPath = GetRealPath
	}

	return root.newNode(), nil
}

func (r *bridgeRoot) newNode() fs.InodeEmbedder {
	if r.NewNode != nil {
		return r.NewNode(r)
	}

	return &BridgeNode{
		RootData: r,
	}
}

func NewBridgeNode(n *BridgeNode) fs.InodeEmbedder {
	if n.RootData.NewNode != nil {
		return n.RootData.NewNode(n.RootData)
	}

	return &BridgeNode{
		RootData: n.RootData,
	}
}

func (r *bridgeRoot) StableAttrFromStat(st *syscall.Stat_t) fs.StableAttr {
	// We compose an inode number by the underlying inode, and
	// mixing in the device number. In traditional filesystems,
	// the inode numbers are small. The device numbers are also
	// small (typically 16 bit). Finally, we mask out the root
	// device number of the root, so a loopback FS that does not
	// encompass multiple mounts will reflect the inode numbers of
	// the underlying filesystem
	swapped := (uint64(st.Dev) << 32) | (uint64(st.Dev) >> 32)
	swappedRootDev := (r.Dev << 32) | (r.Dev >> 32)
	return fs.StableAttr{
		Mode: uint32(st.Mode),
		Gen:  1,
		// This should work well for traditional backing FSes,
		// not so much for other go-fuse FS-es
		Ino: (swapped ^ swappedRootDev) ^ st.Ino,
	}
}

// LoopbackNode is a filesystem node in a loopback file system.
type BridgeNode struct {
	fs.Inode

	RootData *bridgeRoot
}

var _ = (fs.NodeStatfser)((*BridgeNode)(nil))
var _ = (fs.NodeGetattrer)((*BridgeNode)(nil))
var _ = (fs.NodeGetxattrer)((*BridgeNode)(nil))
var _ = (fs.NodeSetxattrer)((*BridgeNode)(nil))
var _ = (fs.NodeRemovexattrer)((*BridgeNode)(nil))
var _ = (fs.NodeListxattrer)((*BridgeNode)(nil))
var _ = (fs.NodeReadlinker)((*BridgeNode)(nil))
var _ = (fs.NodeOpener)((*BridgeNode)(nil))
var _ = (fs.NodeCopyFileRanger)((*BridgeNode)(nil))
var _ = (fs.NodeLookuper)((*BridgeNode)(nil))
var _ = (fs.NodeOpendirer)((*BridgeNode)(nil))
var _ = (fs.NodeReaddirer)((*BridgeNode)(nil))
var _ = (fs.NodeMkdirer)((*BridgeNode)(nil))
var _ = (fs.NodeMknoder)((*BridgeNode)(nil))
var _ = (fs.NodeLinker)((*BridgeNode)(nil))
var _ = (fs.NodeSymlinker)((*BridgeNode)(nil))
var _ = (fs.NodeUnlinker)((*BridgeNode)(nil))
var _ = (fs.NodeRmdirer)((*BridgeNode)(nil))
var _ = (fs.NodeRenamer)((*BridgeNode)(nil))

func (n *BridgeNode) GetRealPath(name string) string {
	return n.path(name)
}

func (n *BridgeNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	fmt.Println("BridgeNode Statfs")
	s := syscall.Statfs_t{}
	err := syscall.Statfs(n.path(""), &s)
	if err != nil {
		return fs.ToErrno(err)
	}
	out.FromStatfsT(&s)
	return fs.OK
}

//func (n *BridgeNode) path() string {
//	path := n.Path(n.Root())
//	return filepath.Join(n.RootData.Path, path)
//}

func (n *BridgeNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	fmt.Println("BridgeNode Lookup")
	p := n.path(name)

	st := syscall.Stat_t{}
	err := syscall.Lstat(p, &st)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	out.Attr.FromStat(&st)
	node := n.RootData.newNode()
	ch := n.NewInode(ctx, node, n.RootData.StableAttrFromStat(&st))
	return ch, fs.OK
}

// preserveOwner sets uid and gid of `path` according to the caller information
// in `ctx`.
func (n *BridgeNode) preserveOwner(ctx context.Context, path string) error {
	if os.Getuid() != 0 {
		return nil
	}
	caller, ok := fuse.FromContext(ctx)
	if !ok {
		return nil
	}
	return syscall.Lchown(path, int(caller.Uid), int(caller.Gid))
}

func (n *BridgeNode) Mknod(ctx context.Context, name string, mode, rdev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	fmt.Println("BridgeNode Mknod")
	p := n.path(name)
	err := syscall.Mknod(p, mode, int(rdev))
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	n.preserveOwner(ctx, p)
	st := syscall.Stat_t{}
	if err := syscall.Lstat(p, &st); err != nil {
		syscall.Rmdir(p)
		return nil, fs.ToErrno(err)
	}

	out.Attr.FromStat(&st)

	node := n.RootData.newNode()
	ch := n.NewInode(ctx, node, n.RootData.StableAttrFromStat(&st))

	return ch, 0
}

func (n *BridgeNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	fmt.Println("BridgeNode Mkdir")
	p := n.path(name)
	err := os.Mkdir(p, os.FileMode(mode))
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	n.preserveOwner(ctx, p)
	st := syscall.Stat_t{}
	if err := syscall.Lstat(p, &st); err != nil {
		syscall.Rmdir(p)
		return nil, fs.ToErrno(err)
	}

	out.Attr.FromStat(&st)

	node := n.RootData.newNode()
	ch := n.NewInode(ctx, node, n.RootData.StableAttrFromStat(&st))

	return ch, 0
}

func (n *BridgeNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	fmt.Println("BridgeNode Rmdir")
	p := n.path(name)
	err := syscall.Rmdir(p)
	return fs.ToErrno(err)
}

func (n *BridgeNode) Unlink(ctx context.Context, name string) syscall.Errno {
	fmt.Println("BridgeNode Unlink")
	p := n.path(name)
	err := syscall.Unlink(p)
	return fs.ToErrno(err)
}

func (n *BridgeNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	fmt.Println("BridgeNode Rename")
	if flags&fs.RENAME_EXCHANGE != 0 {
		return n.renameExchange(name, newParent, newName)
	}

	// TODO: Rename needs to be changed
	p1 := n.path(name)
	p2 := filepath.Join(n.RootData.Path, newParent.EmbeddedInode().Path(nil), newName)

	err := syscall.Rename(p1, p2)
	return fs.ToErrno(err)
}

var _ = (fs.NodeCreater)((*BridgeNode)(nil))

func (n *BridgeNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	fmt.Println("BridgeNode Create")
	p := n.path(name)
	flags = flags &^ syscall.O_APPEND
	fd, err := syscall.Open(p, int(flags)|os.O_CREATE, mode)
	if err != nil {
		return nil, nil, 0, fs.ToErrno(err)
	}
	n.preserveOwner(ctx, p)
	st := syscall.Stat_t{}
	if err := syscall.Fstat(fd, &st); err != nil {
		syscall.Close(fd)
		return nil, nil, 0, fs.ToErrno(err)
	}

	node := n.RootData.newNode()
	ch := n.NewInode(ctx, node, n.RootData.StableAttrFromStat(&st))
	lf := NewBridgeFileHandle(fd)

	out.FromStat(&st)
	return ch, lf, 0, 0
}

func (n *BridgeNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	fmt.Println("BridgeNode Symlink")
	p := n.path(name)
	err := syscall.Symlink(target, p)
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	n.preserveOwner(ctx, p)
	st := syscall.Stat_t{}
	if err := syscall.Lstat(p, &st); err != nil {
		syscall.Unlink(p)
		return nil, fs.ToErrno(err)
	}
	node := n.RootData.newNode()
	ch := n.NewInode(ctx, node, n.RootData.StableAttrFromStat(&st))

	out.Attr.FromStat(&st)
	return ch, 0
}

func (n *BridgeNode) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	fmt.Println("BridgeNode Link")
	p := n.path(name)
	err := syscall.Link(filepath.Join(n.RootData.Path, target.EmbeddedInode().Path(nil)), p)
	if err != nil {
		return nil, fs.ToErrno(err)
	}
	st := syscall.Stat_t{}
	if err := syscall.Lstat(p, &st); err != nil {
		syscall.Unlink(p)
		return nil, fs.ToErrno(err)
	}
	node := n.RootData.newNode()
	ch := n.NewInode(ctx, node, n.RootData.StableAttrFromStat(&st))

	out.Attr.FromStat(&st)
	return ch, 0
}

func (n *BridgeNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	fmt.Println("BridgeNode Readlink")
	p := n.path("")

	for l := 256; ; l *= 2 {
		buf := make([]byte, l)
		sz, err := syscall.Readlink(p, buf)
		if err != nil {
			return nil, fs.ToErrno(err)
		}

		if sz < len(buf) {
			return buf[:sz], 0
		}
	}
}

func (n *BridgeNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	fmt.Println("BridgeNode Open")
	flags = flags &^ syscall.O_APPEND
	p := n.path("")
	f, err := syscall.Open(p, int(flags), 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}
	lf := NewBridgeFileHandle(f)
	return lf, 0, 0
}

func (n *BridgeNode) Opendir(ctx context.Context) syscall.Errno {
	fmt.Printf("BridgeNode Opendir: %s\n", filepath.Join("/", n.Path(n.Root())))
	fd, err := syscall.Open(n.path(""), syscall.O_DIRECTORY, 0755)
	if err != nil {
		return fs.ToErrno(err)
	}
	syscall.Close(fd)
	return fs.OK
}

func (n *BridgeNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	fmt.Println("BridgeNode Readdir")
	return fs.NewLoopbackDirStream(n.path(""))
}

func (n *BridgeNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	getattrPath := filepath.Join("/", n.Path(n.Root()))
	fmt.Println("BridgeNode Getattr: ", getattrPath)
	if f != nil {
		return f.(fs.FileGetattrer).Getattr(ctx, out)
	}

	p := n.path("")

	var err error
	st := syscall.Stat_t{}
	if &n.Inode == n.Root() {
		err = syscall.Stat(p, &st)
	} else {
		err = syscall.Lstat(p, &st)
	}

	if err != nil {
		return fs.ToErrno(err)
	}
	out.FromStat(&st)
	return fs.OK
}

//var _ = (fs.NodeSetattrer)((*BridgeNode)(nil))

//func (n *BridgeNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
//	fmt.Println("BridgeNode Setattr")
//	p := n.path("")
//	fsa, ok := f.(fs.FileSetattrer)
//	if ok && fsa != nil {
//		fsa.Setattr(ctx, in, out)
//	} else {
//		if m, ok := in.GetMode(); ok {
//			if err := syscall.Chmod(p, m); err != nil {
//				return fs.ToErrno(err)
//			}
//		}
//
//		uid, uok := in.GetUID()
//		gid, gok := in.GetGID()
//		if uok || gok {
//			suid := -1
//			sgid := -1
//			if uok {
//				suid = int(uid)
//			}
//			if gok {
//				sgid = int(gid)
//			}
//			if err := syscall.Chown(p, suid, sgid); err != nil {
//				return fs.ToErrno(err)
//			}
//		}
//
//		mtime, mok := in.GetMTime()
//		atime, aok := in.GetATime()
//
//		if mok || aok {
//
//			ap := &atime
//			mp := &mtime
//			if !aok {
//				ap = nil
//			}
//			if !mok {
//				mp = nil
//			}
//			var ts [2]syscall.Timespec
//			ts[0] = fuse.UtimeToTimespec(ap)
//			ts[1] = fuse.UtimeToTimespec(mp)
//
//			if err := syscall.UtimesNano(p, ts[:]); err != nil {
//				return fs.ToErrno(err)
//			}
//		}
//
//		if sz, ok := in.GetSize(); ok {
//			if err := syscall.Truncate(p, int64(sz)); err != nil {
//				return fs.ToErrno(err)
//			}
//		}
//	}
//
//	fga, ok := f.(fs.FileGetattrer)
//	if ok && fga != nil {
//		fga.Getattr(ctx, out)
//	} else {
//		st := syscall.Stat_t{}
//		err := syscall.Lstat(p, &st)
//		if err != nil {
//			return fs.ToErrno(err)
//		}
//		out.FromStat(&st)
//	}
//	return fs.OK
//}

func (n *BridgeNode) path(name string) string {
	path := n.Path(n.Root())
	if n.RootData.GetRealPath == nil {
		return filepath.Join(n.RootData.Path, path, name)
	}

	return n.RootData.GetRealPath(filepath.Join(path, name))
}
