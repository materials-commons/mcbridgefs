package bridgefs

import (
	"context"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"gorm.io/gorm"
	"hash/fnv"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
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
}

func (r *bridgeRoot) newNode() fs.InodeEmbedder {
	if r.NewNode != nil {
		return r.NewNode(r)
	}
	return &Node{
		RootData: r,
	}
}

func (r *bridgeRoot) idFromStat(st *syscall.Stat_t) fs.StableAttr {
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

type Node struct {
	fs.Inode
	db        *gorm.DB
	projectID int
	file      *MCFile
	RootData  *bridgeRoot
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

func (n *Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	dir, err := n.getMCFile()
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
	p := filepath.Join(n.Path(n.Root()), name)

	st := syscall.Stat_t{}
	err := syscall.Lstat(p, &st)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	out.Attr.FromStat(&st)
	node := n.RootData.newNode()
	return n.NewInode(ctx, node, n.RootData.idFromStat(&st)), fs.OK
}

func (n *Node) getMCFile() (*MCFile, error) {
	var file MCFile
	if err := n.db.Preload("Directory").
		Where("project_id = ? and path = ?", n.projectID, n.path("")).
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

func (n *Node) path(name string) string {
	return filepath.Join("/", n.Path(n.Root()), name)
}

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
