package mcbridgefs

import (
	"context"
	"fmt"
	"github.com/apex/log"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/materials-commons/mcglobusfs/bridgefs"
	"gorm.io/gorm"
	"hash/fnv"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
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

func RootNode(db *gorm.DB, projectID int, rootPath string) *Node {
	fmt.Println("creating rootpath:", rootPath)
	bridgeRoot, err := bridgefs.NewBridgeRoot(rootPath, nil, nil)
	if err != nil {
		log.Fatalf("Failed to create root node: %s", err)
	}
	return &Node{
		db:         db,
		projectID:  projectID,
		mcfsRoot:   rootPath,
		BridgeNode: bridgeRoot.(*bridgefs.BridgeNode),
	}
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
	//readdirPath := filepath.Join("/", n.Path(n.Root()))
	//fmt.Printf("Readdir: %s\n", readdirPath)
	dir, err := n.getMCDir("")
	if err != nil {
		//fmt.Printf("   (%s) failed finding directory: %s\n", readdirPath, err)
		return nil, syscall.ENOENT
	}

	//fmt.Println("Readdir looking up files for directory: ", dir.ID)

	var files []MCFile
	err = n.db.Preload("Directory").
		Where("directory_id = ?", dir.ID).
		Where("current = true").
		Find(&files).Error

	if err != nil {
		return nil, syscall.ENOENT
	}

	filesList := make([]fuse.DirEntry, 0, len(files))

	for _, fileEntry := range files {
		//fmt.Printf("%+v\n", fileEntry)
		entry := fuse.DirEntry{
			Mode: n.getMode(&fileEntry),
			Name: fileEntry.Name,
			Ino:  n.inodeHash(&fileEntry),
		}

		filesList = append(filesList, entry)
	}

	return fs.NewListDirStream(filesList), fs.OK
}

func (n *Node) Opendir(ctx context.Context) syscall.Errno {
	return fs.OK
}

func (n *Node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	//fmt.Printf("Node Getattr: %s\n", filepath.Join("/", n.Path(n.Root())))
	if n.file != nil {
		//fmt.Println("    file for Getattr is not nil")
		if n.file.IsFile() {
			out.Size = n.file.Size
		}
	}

	out.Uid = uid
	out.Gid = gid

	now := time.Now()
	out.SetTimes(&now, &now, &now)

	return fs.OK
}

func (n *Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// TODO: Get the file from the database and then use that to compute the inode
	dir, err := n.getMCDir("")
	if err != nil {
		return nil, syscall.ENOENT
	}

	var f MCFile
	err = n.db.Preload("Directory").
		Where("directory_id = ?", dir.ID).
		Where("name = ?", name).
		Where("current = ?", true).
		First(&f).Error

	if err != nil {
		return nil, syscall.ENOENT
	}

	out.Uid = uid
	out.Gid = gid
	if f.IsFile() {
		out.Size = f.Size
	}

	now := time.Now()
	out.SetTimes(&now, &now, &now)

	node := n.newNode()
	node.file = &f
	return n.NewInode(ctx, node, fs.StableAttr{Mode: n.getMode(&f), Ino: n.inodeHash(&f)}), fs.OK

	//if f.IsDir() {
	//
	//}
	//
	//st := syscall.Stat_t{}
	//if err := syscall.Lstat(f.ToPath(n.mcfsRoot), &st); err != nil {
	//	return nil, fs.ToErrno(err)
	//}
	//
	//out.Attr.FromStat(&st)
	//
	//node := n.newNode()
	//node.file = f
	//return n.NewInode(ctx, node, n.RootData.StableAttrFromStat(&st)), fs.OK
}

/*
newNode := Node{
		mcapi:  n.mcapi,
		MCFile: file,
	}

	out.Uid = uid
	out.Gid = gid
	if file.IsFile() {
		out.Size = file.Size
	}

	now := time.Now()
	out.SetTimes(&now, &now, &now)

	return n.NewInode(ctx, &newNode, fs.StableAttr{Mode: n.getMode(file), Ino: n.inodeHash(file)}), fs.OK
*/

func (n *Node) path(name string) string {
	return filepath.Join("/", n.GetRealPath(name))
}

func (n *Node) getMCDir(name string) (*MCFile, error) {
	var file MCFile
	path := filepath.Join("/", n.Path(n.Root()), name)
	//fmt.Printf("getMCDir projectID = %d path = %s\n", n.projectID, path)
	err := n.db.Preload("Directory").
		Where("project_id = ?", n.projectID).
		Where("path = ?", path).
		First(&file).Error

	if err != nil {
		//fmt.Printf("    (%s) returning err: %s\n", path, err)
		return nil, err
	}

	//fmt.Printf("   (%s) returning: %+v\n", path, file)
	return &file, nil
}

func (n *Node) getMCFile(name string) (*MCFile, error) {
	//var file MCFile
	//path := filepath.Join("/", n.Path(n.Root()), name)
	//err := n.db.Preload("Directory").
	//	Where("project_id = ?", n.projectID).
	//	Where("current = true")
	return nil, nil
}

func (n *Node) getMCFilesInDir(directoryID int) ([]MCFile, error) {
	var files []MCFile
	err := n.db.Where("directory_id = ?", directoryID).
		Where("current = true").
		Find(&files).Error

	if err != nil {
		return nil, err
	}

	return files, nil
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
	fmt.Printf("Node Open flags = %d, path = %s\n", flags, filepath.Join("/", n.Path(n.Root())))
	if n.file != nil {
		fmt.Println("   Node Open file != nil, realpath = ", n.file.ToPath(n.mcfsRoot))
	}
	switch flags & syscall.O_ACCMODE {
	case syscall.O_RDONLY:
		fmt.Println("    Open flags O_RDONLY")
	case syscall.O_WRONLY:
		fmt.Println("    Open flags O_WRONLY")
	case syscall.O_RDWR:
		fmt.Println("    Open flags O_RDWR")
	default:
		fmt.Println("    Open flags Invalid")
		return
	}
	fd, err := syscall.Open(n.file.ToPath(n.mcfsRoot), int(flags), 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}
	fhandle := bridgefs.NewBridgeFileHandle(fd)
	return fhandle, 0, fs.OK
	//return nil, 0, syscall.EIO

	/*
		flags = flags &^ syscall.O_APPEND
			p := n.path("")
			f, err := syscall.Open(p, int(flags), 0)
			if err != nil {
				return nil, 0, fs.ToErrno(err)
			}
			lf := NewBridgeFileHandle(f)
			return lf, 0, 0
	*/
}

func (n *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return syscall.EIO
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
