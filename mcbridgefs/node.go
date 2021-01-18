package mcbridgefs

import (
	"context"
	"errors"
	"fmt"
	"github.com/apex/log"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hashicorp/go-uuid"
	"github.com/materials-commons/mcglobusfs/bridgefs"
	"gorm.io/gorm"
	"hash/fnv"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// TODO: projectID and mcfsRoot should be saved in a single place, not in every node
// TODO: Check if db is threadsafe
type Node struct {
	db              *gorm.DB
	projectID       int
	globusRequestID int
	file            *MCFile
	newFile         *MCFile
	mcfsRoot        string
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

func RootNode(db *gorm.DB, projectID, globusRequestID int, rootPath string) *Node {
	fmt.Println("creating rootpath:", rootPath)
	bridgeRoot, err := bridgefs.NewBridgeRoot(rootPath, nil, nil)
	if err != nil {
		log.Fatalf("Failed to create root node: %s", err)
	}
	return &Node{
		db:              db,
		projectID:       projectID,
		globusRequestID: globusRequestID,
		mcfsRoot:        rootPath,
		BridgeNode:      bridgeRoot.(*bridgefs.BridgeNode),
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

	// Get files that have been uploaded
	var globusUploadedFiles []GlobusRequestFile
	results := n.db.Preload("File").
		Where("directory_id = ?", dir.ID).
		Where("globus_request_id = ?", n.globusRequestID).
		Find(&globusUploadedFiles)

	filesByName := make(map[string]*MCFile)
	if results.Error == nil && len(globusUploadedFiles) != 0 {
		// convert the files into a hashtable by name
		for _, requestFile := range globusUploadedFiles {
			filesByName[requestFile.File.Name] = requestFile.File
		}
	}

	filesList := make([]fuse.DirEntry, 0, len(files))

	for _, fileEntry := range files {
		// If there is an entry in filesByName then this overrides the directory listing as it means that
		// a new version of the file has been uploaded.
		if foundEntry, ok := filesByName[fileEntry.Name]; ok {
			fileEntry = *foundEntry

			// Remove from the hash table because we are going to need to make one more pass through the
			// filesByName hash to pick up any newly uploaded files in the directory.
			delete(filesByName, fileEntry.Name)
		}

		entry := fuse.DirEntry{
			Mode: n.getMode(&fileEntry),
			Name: fileEntry.Name,
			Ino:  n.inodeHash(&fileEntry),
		}

		filesList = append(filesList, entry)
	}

	// Add any newly uploaded files
	for _, fileEntry := range filesByName {
		entry := fuse.DirEntry{
			Mode: n.getMode(fileEntry),
			Name: fileEntry.Name,
			Ino:  n.inodeHash(fileEntry),
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
	fmt.Println("Lookup: ", filepath.Join("/", n.Path(n.Root()), name))
	if n.file != nil {
		fmt.Printf("  Lookup n.file not nil name = %s, size = %d\n", n.file.Name, n.file.Size)
	}

	if n.newFile != nil {
		fmt.Printf("  Lookup n.newFile not nil name = %s, size = %d\n", n.newFile.Name, n.newFile.Size)
	}

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
}

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
	fmt.Println("Node Create: ", name)
	return nil, nil, 0, syscall.EIO
}

func (n *Node) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	var err error
	fmt.Printf("Node Open flags = %d, path = %s\n", flags, filepath.Join("/", n.Path(n.Root())))
	if n.file != nil {
		fmt.Println("   Node Open file != nil, realpath = ", n.file.ToPath(n.mcfsRoot))
	}

	switch flags & syscall.O_ACCMODE {
	case syscall.O_RDONLY:
		fmt.Println("    Open flags O_RDONLY")
	case syscall.O_WRONLY:
		n.newFile, err = n.createNewMCFileVersion()
		if err != nil {
			// TODO: What error should be returned?
			return nil, 0, syscall.EIO
		}
		flags = flags &^ syscall.O_CREAT
		fmt.Println("    Open flags O_WRONLY")
	case syscall.O_RDWR:
		// If we are here then for now return an error. Need to figure out
		// how this is handled when opening an existing file vs creating
		// a new file.
		fmt.Println("    Open flags O_RDWR")
	default:
		fmt.Println("    Open flags Invalid")
		return
	}

	path := n.file.ToPath(n.mcfsRoot)
	if n.newFile != nil {
		path = n.newFile.ToPath(n.mcfsRoot)
	}
	fd, err := syscall.Open(path, int(flags), 0)
	if err != nil {
		fmt.Printf("syscall.Open failed, err = %s\n", err)
		return nil, 0, fs.ToErrno(err)
	}
	fhandle := bridgefs.NewBridgeFileHandle(fd)
	return fhandle, 0, fs.OK
}

func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	fmt.Println("Node Setattr")
	path := n.file.ToPath(n.mcfsRoot)
	if n.newFile != nil {
		path = n.newFile.ToPath(n.mcfsRoot)
	}

	fi, err := os.Stat(path)
	if err != nil {
		fmt.Printf("os.Stat %s failed: %s\n", path, err)
	}
	if err == nil {
		fmt.Printf("   Node Setattr stat (%s) size = %d\n", path, fi.Size())
	}
	return fs.OK
}

func (n *Node) Release(ctx context.Context, f fs.FileHandle) syscall.Errno {
	fmt.Println("Node Release")
	if bridgeFH, ok := f.(fs.FileReleaser); ok {
		fmt.Println("   Handle is BridgeFileHandle")
		if err := bridgeFH.Release(ctx); err != fs.OK {
			return err
		}

		fmt.Println("   Did Release on BridgeFileHandle, now doing Stat")
		path := n.file.ToPath(n.mcfsRoot)
		mcToUpdate := n.file
		if n.newFile != nil {
			path = n.newFile.ToPath(n.mcfsRoot)
			mcToUpdate = n.newFile
		}

		fi, err := os.Stat(path)
		if err != nil {
			fmt.Printf("os.Stat %s failed: %s\n", path, err)
		}
		if err == nil {
			fmt.Printf("   Node Release stat (%s) size = %d\n", path, fi.Size())
			n.db.Model(mcToUpdate).Update("size", fi.Size())
			fmt.Printf("mcToUpdate = %+v\n", mcToUpdate)
		}

		return fs.OK
	}

	return syscall.EINVAL
}

func (n *Node) createNewMCFileVersion() (*MCFile, error) {
	// First check if there is already a version of this file being written to for this
	// globus upload context.
	var err error
	var globusRequestFile GlobusRequestFile
	path := filepath.Join("/", n.Path(n.Root()))
	err = n.db.Preload("File").
		Where("path = ?", path).
		Where("globus_request_id = ?", n.globusRequestID).
		First(&globusRequestFile).Error

	if err != nil {
		return globusRequestFile.File, nil
	}

	// There isn't an existing upload, so create a new one

	newFile := &MCFile{
		ProjectID:   n.file.ProjectID,
		Name:        n.file.Name,
		DirectoryID: n.file.DirectoryID,
		Size:        0,
		Checksum:    "",
		MimeType:    n.file.MimeType,
		OwnerID:     n.file.OwnerID,
		Current:     false,
	}

	if newFile.UUID, err = uuid.GenerateUUID(); err != nil {
		return nil, err
	}

	// Try to make the directory path where the file will go
	if err := os.MkdirAll(newFile.ToDirPath(n.mcfsRoot), 0755); err != nil {
		fmt.Printf("os.MkdirAll failed (%s): %s\n", newFile.ToDirPath(n.mcfsRoot), err)
		return nil, err
	}

	f, err := os.OpenFile(newFile.ToPath(n.mcfsRoot), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		fmt.Printf("os.OpenFile failed (%s): %s\n", newFile.ToPath(n.mcfsRoot), err)
		return nil, err
	}

	var _ = f.Close()

	result := n.db.Create(newFile)

	if result.Error != nil {
		return nil, result.Error
	}

	if result.RowsAffected != 1 {
		// TODO: Fix this error
		return nil, errors.New("incorrect rows affected")
	}

	// Create a new globus request file entry to account for the new file
	globusRequestFile = GlobusRequestFile{
		ProjectID:       n.projectID,
		OwnerID:         n.file.OwnerID,
		GlobusRequestID: n.globusRequestID,
		Path:            path,
		FileID:          newFile.ID,
	}

	if globusRequestFile.UUID, err = uuid.GenerateUUID(); err != nil {
		return nil, err
	}

	result = n.db.Create(&globusRequestFile)
	if result.Error != nil {
		return nil, result.Error
	}

	if result.RowsAffected != 1 {
		// TODO: Fix this error
		return nil, errors.New("incorrect rows affected")
	}

	fmt.Printf("createNewMCFileVersion: %+v\n", newFile)

	return newFile, nil
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
