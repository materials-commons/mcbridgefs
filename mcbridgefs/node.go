package mcbridgefs

import (
	"context"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/materials-commons/mcglobusfs/bridgefs"
	"gorm.io/gorm"
	"hash/fnv"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
)

// TODO: projectID and mcfsRoot should be saved in a single place, not in every node
// TODO: Check if db is threadsafe
type Node struct {
	db        *gorm.DB
	projectID int
	file      *MCFile
	mcfsRoot  string
	*bridgefs.BridgeNode
}

var uid, gid uint32

func init() {
	u, err := user.Current()
	if err != nil {
		panic(err)
	}
	uid32, _ := strconv.ParseUint(u.Uid, 10, 32)
	gid32, _ := strconv.ParseUint(u.Gid, 10, 32)
	uid = uint32(uid32)
	gid = uint32(gid32)
}

func (n *Node) newNode() *Node {
	return &Node{
		db:         n.db,
		projectID:  n.projectID,
		mcfsRoot:   n.mcfsRoot,
		BridgeNode: bridgefs.NewBridgeNode(n.BridgeNode).(*bridgefs.BridgeNode),
	}
}

var _ = (fs.NodeReaddirer)((*Node)(nil))

func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	dir, err := n.getMCFile("")
	if err != nil {
		return nil, syscall.ENOENT
	}

	var files []MCFile
	if err := n.db.Where("directory_id = ?", dir.ID).
		Find(&files).Error; err != nil {
		return nil, syscall.ENOENT
	}

	filesList := make([]fuse.DirEntry, 0, len(files))

	for _, fileEntry := range files {
		entry := fuse.DirEntry{
			Mode: n.getMode(&fileEntry),
			Name: fileEntry.Name,
			Ino:  n.inodeHash(&fileEntry),
		}

		filesList = append(filesList, entry)
	}

	return fs.NewListDirStream(filesList), fs.OK
}

func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// TODO: Get the file from the database and then use that to compute the inode
	f, err := n.getMCFile(name)
	if err != nil {
		return nil, syscall.ENOENT
	}

	st := syscall.Stat_t{}
	if err := syscall.Lstat(f.ToPath(n.mcfsRoot), &st); err != nil {
		return nil, fs.ToErrno(err)
	}

	out.Attr.FromStat(&st)

	node := n.newNode()
	node.file = f
	return n.NewInode(ctx, node, n.RootData.StableAttrFromStat(&st)), fs.OK
}

func (n *Node) path(name string) string {
	return filepath.Join("/", n.GetRealPath(name))
}

func (n *Node) getMCFile(name string) (*MCFile, error) {
	var file MCFile
	if err := n.db.Preload("Directory").
		Where("project_id = ? and path = ?", n.projectID, n.path(name)).
		Find(&file).Error; err != nil {
		return nil, syscall.ENOENT
	}

	return &file, nil
}

func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EINVAL
}

func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	return syscall.EIO
}

func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return nil, nil, 0, syscall.EIO
}

func (n *Node) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return nil, 0, syscall.EIO
}

func (n *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return syscall.EIO
}

//func (n *Node) path(name string) string {
//	return filepath.Join("/", n.Path(n.Root()), name)
//}

func (n *Node) getMode(entry *MCFile) uint32 {
	if entry == nil {
		return 0755 | uint32(syscall.S_IFDIR)
	}

	if entry.IsDir() {
		return 0755 | uint32(syscall.S_IFDIR)
	}

	return 0644 | uint32(syscall.S_IFREG)
}

func (n *Node) inodeHash(entry *MCFile) uint64 {
	if entry == nil {
		//fmt.Printf("inodeHash entry is nil\n")
		return 1
	}

	//fmt.Printf("inodeHash entry.FullPath() = %s\n", entry.FullPath())
	h := fnv.New64a()
	_, _ = h.Write([]byte(entry.FullPath()))
	return h.Sum64()
}
