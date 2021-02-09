package mcbridgefs

import (
	"context"
	"errors"
	"fmt"
	"github.com/apex/log"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hashicorp/go-uuid"
	"github.com/materials-commons/gomcdb/mcmodel"
	"github.com/materials-commons/mcbridgefs/fs/bridgefs"
	"gorm.io/gorm"
	"hash/fnv"
	"mime"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// TODO: projectID and mcfsRoot should be saved in a single place, not in every node
// TODO: Check if db is threadsafe
type Node struct {
	file *mcmodel.File
	*bridgefs.BridgeNode
}

var (
	uid, gid           uint32
	MCFSRoot           string
	DB                 *gorm.DB
	GlobusRequest      mcmodel.GlobusRequest
	openedFilesTracker sync.Map
)

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

func RootNode() *Node {
	bridgeRoot, err := bridgefs.NewBridgeRoot(os.Getenv("MCFS_DIR"), nil, nil)
	if err != nil {
		log.Fatalf("Failed to create root node: %s", err)
	}
	return &Node{
		BridgeNode: bridgeRoot.(*bridgefs.BridgeNode),
	}
}

func (n *Node) newNode() *Node {
	return &Node{
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

	var files []mcmodel.File
	err = DB.Preload("Directory").
		Where("directory_id = ?", dir.ID).
		Where("project_id", GlobusRequest.ProjectID).
		Where("current = true").
		Find(&files).Error

	//fmt.Println("  length of files returned for directory =", len(files))

	if err != nil {
		return nil, syscall.ENOENT
	}

	// Get files that have been uploaded
	var globusUploadedFiles []mcmodel.GlobusRequestFile
	results := DB.Preload("File.Directory").
		Where("directory_id = ?", dir.ID).
		Where("globus_request_id = ?", GlobusRequest.ID).
		Find(&globusUploadedFiles)

	filesByName := make(map[string]*mcmodel.File)
	if results.Error == nil && len(globusUploadedFiles) != 0 {
		// convert the files into a hashtable by name
		for _, requestFile := range globusUploadedFiles {
			//fmt.Println("Adding to filesByName:", requestFile.File.Name)
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

		//fmt.Println("   To directory listing adding:", fileEntry.Name)
		filesList = append(filesList, entry)
	}

	// Add any newly uploaded files
	for _, fileEntry := range filesByName {
		entry := fuse.DirEntry{
			Mode: n.getMode(fileEntry),
			Name: fileEntry.Name,
			Ino:  n.inodeHash(fileEntry),
		}

		//fmt.Println("  from filesByName adding:", fileEntry.Name)
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
	fmt.Println("Lookup:", name)
	// TODO: Get the file from the database and then use that to compute the inode
	//fmt.Println("Lookup: ", filepath.Join("/", n.Path(n.Root()), name))
	//if n.file != nil {
	//	fmt.Printf("  Lookup n.file not nil name = %s, size = %d\n", n.file.Name, n.file.Size)
	//}
	//
	//if n.newFile != nil {
	//	fmt.Printf("  Lookup n.newFile not nil name = %s, size = %d\n", n.newFile.Name, n.newFile.Size)
	//}

	f, err := n.lookupEntry(name)
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
	node.file = f
	return n.NewInode(ctx, node, fs.StableAttr{Mode: n.getMode(f), Ino: n.inodeHash(f)}), fs.OK
}

func (n *Node) lookupEntry(name string) (*mcmodel.File, error) {
	dir, err := n.getMCDir("")
	if err != nil {
		return nil, err
	}

	// First check if there is a new file being uploaded for this entry. If that is the case
	// then return that file information.
	var gf mcmodel.GlobusRequestFile
	err = DB.Preload("File.Directory").
		Where("directory_id = ?", dir.ID).
		Where("name = ?", name).
		First(&gf).Error

	if err == nil {
		// Found a version of the file that is being uploaded so return it
		// TODO: Do we need to stat the entry to get the current size?
		return gf.File, nil
	}

	// If we are here then there is not a new version of the file being written, so look up existing
	var f mcmodel.File
	err = DB.Preload("Directory").
		Where("directory_id = ?", dir.ID).
		Where("name = ?", name).
		Where("current = ?", true).
		First(&f).Error

	return &f, err
}

func (n *Node) path(name string) string {
	return filepath.Join("/", n.GetRealPath(name))
}

func (n *Node) getMCDir(name string) (*mcmodel.File, error) {
	var file mcmodel.File
	path := filepath.Join("/", n.Path(n.Root()), name)
	//fmt.Printf("getMCDir projectID = %d path = %s\n", n.projectID, path)
	err := DB.Preload("Directory").
		Where("project_id = ?", GlobusRequest.ProjectID).
		Where("path = ?", path).
		First(&file).Error

	if err != nil {
		//fmt.Printf("    (%s) returning err: %s\n", path, err)
		return nil, err
	}

	//fmt.Printf("   (%s) returning: %+v\n", path, file)
	return &file, nil
}

func (n *Node) getMCFile(name string) (*mcmodel.File, error) {
	//var file MCFile
	//path := filepath.Join("/", n.Path(n.Root()), name)
	//err := n.db.Preload("Directory").
	//	Where("project_id = ?", n.projectID).
	//	Where("current = true")
	return nil, nil
}

func (n *Node) getMCFilesInDir(directoryID int) ([]mcmodel.File, error) {
	var files []mcmodel.File
	err := DB.Where("directory_id = ?", directoryID).
		Where("current = true").
		Find(&files).Error

	if err != nil {
		return nil, err
	}

	return files, nil
}

func (n *Node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	fmt.Printf("Mkdir %s/%s\n", n.Path(n.Root()), name)
	path := filepath.Join("/", n.Path(n.Root()), name)
	var dir mcmodel.File

	parent, err := n.getMCDir("")
	if err != nil {
		fmt.Println("   Could not find parent")
		return nil, syscall.EINVAL
	}

	fmt.Printf("   parent = %+v\n", parent)

	err = DB.Transaction(func(tx *gorm.DB) error {
		err := tx.Where("path = ", path).
			Where("project_id = ", GlobusRequest.ProjectID).
			Find(&dir).Error
		if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
			// directory already exists no need to create
			fmt.Println("   Directory already exists")
			return nil
		}
		dir = mcmodel.File{
			OwnerID:              GlobusRequest.OwnerID,
			MimeType:             "directory",
			MediaTypeDescription: "directory",
			DirectoryID:          parent.ID,
			Current:              true,
			Path:                 path,
			ProjectID:            GlobusRequest.ProjectID,
			Name:                 name,
		}

		if dir.UUID, err = uuid.GenerateUUID(); err != nil {
			return err
		}

		return tx.Create(&dir).Error
	})

	if err != nil {
		fmt.Println("   Transaction returned err =", err)
		return nil, syscall.EINVAL
	}

	out.Uid = uid
	out.Gid = gid

	now := time.Now()
	out.SetTimes(&now, &now, &now)

	node := n.newNode()
	node.file = &dir
	return n.NewInode(ctx, node, fs.StableAttr{Mode: n.getMode(&dir), Ino: n.inodeHash(&dir)}), fs.OK
}

func (n *Node) Rmdir(ctx context.Context, name string) syscall.Errno {
	fmt.Printf("Rmdir %s/%s\n", n.Path(n.Root()), name)
	return syscall.EIO
}

func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	fmt.Println("Node Create: ", name)
	f, err := n.createNewMCFile(name)
	if err != nil {
		fmt.Println("   createNewMCFile failed:", err)
		return nil, nil, 0, syscall.EIO
	}

	path := filepath.Join("/", n.Path(n.Root()), name)
	openedFilesTracker.Store(path, f)
	flags = flags &^ syscall.O_APPEND
	fd, err := syscall.Open(f.ToPath(MCFSRoot), int(flags)|os.O_CREATE, mode)
	if err != nil {
		// TODO - Remove newly create file version in db
		fmt.Println("    syscall.Open failed:", err)
		return nil, nil, 0, syscall.EIO
	}
	statInfo := syscall.Stat_t{}
	if err := syscall.Fstat(fd, &statInfo); err != nil {
		// TODO - Remove newly created file version in db
		fmt.Println("   Fstat failed:", err)
		syscall.Close(fd)
		return nil, nil, 0, fs.ToErrno(err)
	}
	// Is this sequence correct?
	node := n.newNode()
	node.file = f
	out.FromStat(&statInfo)
	return n.NewInode(ctx, node, fs.StableAttr{Mode: n.getMode(f), Ino: n.inodeHash(f)}), NewFileHandle(fd, flags), 0, fs.OK
}

func (n *Node) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	var (
		err     error
		newFile *mcmodel.File
	)
	path := filepath.Join("/", n.Path(n.Root()))
	//fmt.Printf("Node Open flags = %d, path = %s\n", flags, filepath.Join("/", n.Path(n.Root())))
	//if n.file != nil {
	//	fmt.Println("   Node Open file != nil, realpath = ", n.file.ToPath(MCFSRoot))
	//}

	switch flags & syscall.O_ACCMODE {
	case syscall.O_RDONLY:
		fmt.Println("    Open flags O_RDONLY")
		newFile = getFromOpenedFiles(path)
	case syscall.O_WRONLY:
		fmt.Println("    Open flags O_WRONLY")
		newFile = getFromOpenedFiles(path)
		if newFile == nil {
			newFile, err = n.createNewMCFileVersion()
			if err != nil {
				// TODO: What error should be returned?
				fmt.Println("       createNewMCFileVersion() O_WRONLY failed:", err)
				return nil, 0, syscall.EIO
			}
			fmt.Printf("Storing into opendFilesTracker '%s'\n", path)
			openedFilesTracker.Store(path, newFile)
		}
		flags = flags &^ syscall.O_CREAT
		flags = flags &^ syscall.O_APPEND
	case syscall.O_RDWR:
		fmt.Println("    Open flags O_RDWR")
		newFile = getFromOpenedFiles(path)
		if newFile == nil {
			newFile, err = n.createNewMCFileVersion()
			if err != nil {
				// TODO: What error should be returned?
				fmt.Println("    createNewMCFileVersion() O_RDWR failed:", err)
				return nil, 0, syscall.EIO
			}
			fmt.Printf("Storing into opendFilesTracker '%s'\n", path)
			openedFilesTracker.Store(path, newFile)
		}
		flags = flags &^ syscall.O_APPEND
	default:
		fmt.Println("    Open flags Invalid")
		return
	}

	filePath := n.file.ToPath(MCFSRoot)
	if newFile != nil {
		filePath = newFile.ToPath(MCFSRoot)
	}
	fd, err := syscall.Open(filePath, int(flags), 0)
	if err != nil {
		fmt.Printf("   syscall.Open failed, err = %s\n", err)
		return nil, 0, fs.ToErrno(err)
	}
	fhandle := NewFileHandle(fd, flags)
	return fhandle, 0, fs.OK
}

func (n *Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	fmt.Println("Node Setattr")
	path := n.file.ToPath(MCFSRoot)
	fpath := filepath.Join("/", n.Path(n.Root()))
	newFile := getFromOpenedFiles(fpath)
	if newFile != nil {
		path = newFile.ToPath(MCFSRoot)
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
		//fmt.Println("   Handle is BridgeFileHandle")
		if err := bridgeFH.Release(ctx); err != fs.OK {
			return err
		}

		// If read only then no need to update size or current flag
		fh := bridgeFH.(*FileHandle)
		if fh.Flags&syscall.O_ACCMODE == syscall.O_RDONLY {
			return fs.OK
		}

		//fmt.Println("   Did Release on BridgeFileHandle, now doing Stat")
		path := n.file.ToPath(MCFSRoot)
		mcToUpdate := n.file
		fpath := filepath.Join("/", n.Path(n.Root()))
		newFile := getFromOpenedFiles(fpath)
		if newFile != nil {
			path = newFile.ToPath(MCFSRoot)
			mcToUpdate = newFile
		}

		//fmt.Printf("n.file = %+v\n", n.file)
		//fmt.Printf("mcToUpdate = %+v\n", mcToUpdate)
		fi, err := os.Stat(path)
		if err != nil {
			fmt.Printf("os.Stat %s failed: %s\n", path, err)
			return fs.ToErrno(err)
		}
		//fmt.Printf("   Node Release stat (%s) size = %d\n", path, fi.Size())
		err = DB.Transaction(func(tx *gorm.DB) error {
			err := tx.Model(&mcmodel.File{}).
				Where("directory_id = ?", n.file.DirectoryID).
				Where("name = ?", n.file.Name).
				Update("current", false).Error
			if err != nil {
				return err
			}

			//fmt.Printf("Starting updates of mcToUpdate\n")
			err = tx.Model(mcToUpdate).Updates(mcmodel.File{Size: uint64(fi.Size()), Current: true}).Error
			//fmt.Printf("Finished updates of mcToUpdate: %s\n", err)
			return err
		})

		//fmt.Println("Release after transaction err =", err)
		return fs.ToErrno(err)
	}
	return syscall.EINVAL
}

func (n *Node) createNewMCFileVersion() (*mcmodel.File, error) {
	// First check if there is already a version of this file being written to for this
	// globus upload context.

	existing := getFromOpenedFiles(filepath.Join("/", n.Path(n.Root()), n.file.Name))
	if existing != nil {
		fmt.Println("  createNewMCFileVersion found previously open - returning it")
		return existing, nil
	}

	var err error
	//var globusRequestFile mcmodel.GlobusRequestFile
	//err = DB.Preload("File").
	//	Where("name = ?", n.file.Name).
	//	Where("directory_id = ?", n.file.DirectoryID).
	//	Where("globus_request_id = ?", GlobusRequest.ID).
	//	First(&globusRequestFile).Error
	//
	//if err != nil {
	//	return globusRequestFile.File, nil
	//}

	// There isn't an existing upload, so create a new one

	newFile := &mcmodel.File{
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
	if err := os.MkdirAll(newFile.ToDirPath(MCFSRoot), 0755); err != nil {
		fmt.Printf("os.MkdirAll failed (%s): %s\n", newFile.ToDirPath(MCFSRoot), err)
		return nil, err
	}

	f, err := os.OpenFile(newFile.ToPath(MCFSRoot), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		fmt.Printf("os.OpenFile failed (%s): %s\n", newFile.ToPath(MCFSRoot), err)
		return nil, err
	}

	var _ = f.Close()

	for i := 0; i < 3; i++ {
		err = DB.Transaction(func(tx *gorm.DB) error {
			result := tx.Create(newFile)

			if result.Error != nil {
				return result.Error
			}

			if result.RowsAffected != 1 {
				// TODO: Fix this error
				return errors.New("incorrect rows affected")
			}

			// Create a new globus request file entry to account for the new file
			globusRequestFile := mcmodel.GlobusRequestFile{
				ProjectID:       GlobusRequest.ProjectID,
				OwnerID:         n.file.OwnerID,
				GlobusRequestID: GlobusRequest.ID,
				Name:            n.file.Name,
				DirectoryID:     n.file.DirectoryID,
				FileID:          newFile.ID,
			}

			if globusRequestFile.UUID, err = uuid.GenerateUUID(); err != nil {
				return err
			}

			result = tx.Create(&globusRequestFile)
			if result.Error != nil {
				return result.Error
			}

			if result.RowsAffected != 1 {
				// TODO: Fix this error
				return errors.New("incorrect rows affected")
			}

			return nil
		})
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}

	fmt.Printf("createNewMCFileVersion: %+v\n", newFile)

	return newFile, nil
}

func (n *Node) createNewMCFile(name string) (*mcmodel.File, error) {
	fmt.Println("createNewMCFile:", name)
	dir, err := n.getMCDir("")
	if err != nil {
		return nil, err
	}

	newFile := &mcmodel.File{
		ProjectID:   GlobusRequest.ProjectID,
		Name:        name,
		DirectoryID: dir.ID,
		Size:        0,
		Checksum:    "",
		MimeType:    getMimeType(name),
		OwnerID:     GlobusRequest.OwnerID,
		Current:     false,
	}

	if newFile.UUID, err = uuid.GenerateUUID(); err != nil {
		return nil, err
	}

	// Try to make the directory path where the file will go
	if err := os.MkdirAll(newFile.ToDirPath(MCFSRoot), 0755); err != nil {
		fmt.Printf("os.MkdirAll failed (%s): %s\n", newFile.ToDirPath(MCFSRoot), err)
		return nil, err
	}

	for i := 0; i < 3; i++ {
		err = DB.Transaction(func(tx *gorm.DB) error {
			result := tx.Create(newFile)

			if result.Error != nil {
				fmt.Println("    Failed create newFile")
				return result.Error
			}

			if result.RowsAffected != 1 {
				// TODO: Fix this error
				return errors.New("incorrect rows affected")
			}

			globusRequestFile := mcmodel.GlobusRequestFile{
				ProjectID:       GlobusRequest.ProjectID,
				OwnerID:         GlobusRequest.OwnerID,
				DirectoryID:     dir.ID,
				GlobusRequestID: GlobusRequest.ID,
				Name:            name,
				FileID:          newFile.ID,
			}

			if globusRequestFile.UUID, err = uuid.GenerateUUID(); err != nil {
				return err
			}

			result = tx.Create(&globusRequestFile)
			if result.Error != nil {
				fmt.Println("   failed create globusRequestFile")
				return result.Error
			}

			if result.RowsAffected != 1 {
				// TODO: Fix this error
				return errors.New("incorrect rows affected")
			}

			return nil
		})

		if err == nil {
			break
		}
	}

	if err != nil {
		return nil, err
	}

	newFile.Directory = dir
	return newFile, nil
}

func getMimeType(name string) string {
	mimeType := mime.TypeByExtension(filepath.Ext(name))
	semicolon := strings.Index(mimeType, ";")
	if semicolon == -1 {
		return strings.TrimSpace(mimeType)
	}

	return strings.TrimSpace(mimeType[:semicolon])
}

func (n *Node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	fmt.Printf("Rename: %s/%s to %s/%s\n", n.Path(n.Root()), name, newParent.EmbeddedInode().Path(n.Root()), newName)
	fromPath := filepath.Join("/", n.Path(n.Root()))
	toPath := filepath.Join("/", newParent.EmbeddedInode().Path(n.Root()))

	dir, err := n.getMCDir("")
	if err != nil {
		return syscall.ENOENT
	}

	var f mcmodel.File
	err = DB.Preload("Directory").
		Where("directory_id = ?", dir.ID).
		Where("project_id = ?", GlobusRequest.ProjectID).
		Where("name = ?", name).
		Where("current = ?", true).
		Find(&f).Error

	switch {
	case err != nil:
		return syscall.ENOENT
	case f.IsDir():
		return n.renameDir(fromPath, toPath, name, newName, f)
	default:
		// f is a file
		return n.renameFile(fromPath, toPath, name, newName, f)
	}
}

func (n *Node) renameDir(fromPath, toPath, name, toName string, f mcmodel.File) syscall.Errno {
	if fromPath == toPath {
		// not being moved to another directory. Just rename directory and all descendant directory
		// paths
	}
	return fs.OK
}

func (n *Node) renameFile(fromPath, toPath, name, toName string, f mcmodel.File) syscall.Errno {
	if fromPath == toPath {
		// not being moved to another directory. Just rename file and all its previous versions
	}
	return fs.OK
}

func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	fmt.Printf("Unlink: %s/%s\n", n.Path(n.Root()), name)
	return syscall.EINVAL
}

func (n *Node) getMode(entry *mcmodel.File) uint32 {
	if entry == nil {
		return 0755 | uint32(syscall.S_IFDIR)
	}

	if entry.IsDir() {
		return 0755 | uint32(syscall.S_IFDIR)
	}

	return 0644 | uint32(syscall.S_IFREG)
}

func (n *Node) inodeHash(entry *mcmodel.File) uint64 {
	if entry == nil {
		//fmt.Printf("inodeHash entry is nil\n")
		return 1
	}

	//fmt.Printf("inodeHash entry.FullPath() = %s\n", entry.FullPath())
	h := fnv.New64a()
	_, _ = h.Write([]byte(entry.FullPath()))
	return h.Sum64()
}

func getFromOpenedFiles(path string) *mcmodel.File {
	fmt.Printf("getFromOpenedFiles '%s'\n", path)
	val, _ := openedFilesTracker.Load(path)
	if val != nil {
		fmt.Println("   getFromOpenedFiles found path")
		return val.(*mcmodel.File)
	}

	fmt.Println("   getFromOpenedFiles DID NOT find path")
	return nil
}
