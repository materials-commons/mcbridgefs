package mcbridgefs

import (
	"errors"
	"os"

	"github.com/apex/log"
	"github.com/hashicorp/go-uuid"
	"github.com/materials-commons/gomcdb/mcmodel"
	"gorm.io/gorm"
)

type FileStore struct {
	db            *gorm.DB
	mcfsRoot      string
	globusRequest *mcmodel.GlobusRequest
}

func NewFileStore(db *gorm.DB) *FileStore {
	return &FileStore{db: db}
}

func (s *FileStore) MarkFileReleased(file *mcmodel.File, checksum string) error {
	finfo, err := os.Stat(file.ToUnderlyingFilePath(s.mcfsRoot))
	if err != nil {
		log.Errorf("MarkFileReleased Stat %s failed: %s", file.ToUnderlyingFilePath(s.mcfsRoot), err)
		return err
	}

	return withTxRetry(func(tx *gorm.DB) error {
		// To set file as the current (ie viewable) version we first need to set all its previous
		// versions to have current set to false.
		err := tx.Model(&mcmodel.File{}).
			Where("directory_id = ?", file.DirectoryID).
			Where("name = ?", file.Name).
			Update("current", false).Error

		if err != nil {
			return err
		}

		err = tx.Model(&mcmodel.GlobusRequestFile{}).
			Where("file_id = ?", file.ID).
			Update("state", "done").Error
		if err != nil {
			return err
		}

		// Now we can update the meta data on the current file. This includes, the size, current, and if there is
		// a new computed checksum, also update the checksum field.
		if checksum != "" {
			return tx.Model(file).Updates(mcmodel.File{
				Size:     uint64(finfo.Size()),
				Current:  true,
				Checksum: checksum,
			}).Error
		}

		// If we are here then the file was opened for read/write but it was never written to. In this situation there
		// is no checksum that has been computed, so don't update the field.
		return tx.Model(file).Updates(mcmodel.File{Size: uint64(finfo.Size()), Current: true}).Error
	}, s.db, txRetryCount)
}

func (s *FileStore) CreateNewFile(file, dir *mcmodel.File) (*mcmodel.File, error) {
	var err error
	if file, err = s.addFileToDatabase(file, dir.ID); err != nil {
		return file, err
	}

	if err := os.MkdirAll(file.ToUnderlyingDirPath(MCFSRoot), 0755); err != nil {
		// TODO: If this fails then we should remove the created file from the database
		log.Errorf("os.MkdirAll failed (%s): %s\n", file.ToUnderlyingDirPath(s.mcfsRoot), err)
		return nil, err
	}

	file.Directory = dir
	return file, nil
}

// addFileToDatabase will add an mcmodel.File entry and an associated mcmodel.GlobusRequestFile entry
// to the database. The file parameter must be filled out, except for the UUID which will be generated
// for the file. The GlobusRequestFile will be created based on the file entry.
// to the database. The file parameter must be filled out, except for the UUID which will be generated
// for the file. The GlobusRequestFile will be created based on the file entry.
func (s *FileStore) addFileToDatabase(file *mcmodel.File, dirID int) (*mcmodel.File, error) {
	var (
		err               error
		globusRequestUUID string
	)

	if file.UUID, err = uuid.GenerateUUID(); err != nil {
		return nil, err
	}

	if globusRequestUUID, err = uuid.GenerateUUID(); err != nil {
		return nil, err
	}

	// Wrap creation in a transaction so that both the file and the GlobusRequestFile are either
	// both created, or neither is created.
	err = withTxRetry(func(tx *gorm.DB) error {
		if result := tx.Create(file); result.Error != nil {
			return result.Error
		}

		// Create a new globus request file entry to account for the new file
		globusRequestFile := mcmodel.GlobusRequestFile{
			ProjectID:       globusRequest.ProjectID,
			OwnerID:         file.OwnerID,
			GlobusRequestID: globusRequest.ID,
			Name:            file.Name,
			DirectoryID:     dirID,
			FileID:          file.ID,
			State:           "uploading",
			UUID:            globusRequestUUID,
		}

		return tx.Create(&globusRequestFile).Error
	}, s.db, txRetryCount)

	return file, err
}

func (s *FileStore) FindDirByPath(projectID int, path string) (*mcmodel.File, error) {
	var dir mcmodel.File
	err := DB.Preload("Directory").
		Where("project_id = ?", projectID).
		Where("path = ?", path).
		First(&dir).Error
	if err != nil {
		log.Errorf("Failed looking up directory in project %d, path %s: %s", projectID, path, err)
		return nil, err
	}

	return &dir, nil
}

func (s *FileStore) CreateDirectory(parentDirID int, path, name string) (*mcmodel.File, error) {
	var dir mcmodel.File
	err := withTxRetry(func(tx *gorm.DB) error {
		err := tx.Where("path = ", path).Where("project_id = ?", globusRequest.ProjectID).Find(&dir).Error
		if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
			// directory already exists no need to create
			return nil
		}

		dir = mcmodel.File{
			OwnerID:              s.globusRequest.OwnerID,
			MimeType:             "directory",
			MediaTypeDescription: "directory",
			DirectoryID:          parentDirID,
			Current:              true,
			Path:                 path,
			ProjectID:            s.globusRequest.ProjectID,
			Name:                 name,
		}

		if dir.UUID, err = uuid.GenerateUUID(); err != nil {
			return err
		}

		return tx.Create(&dir).Error

	}, s.db, txRetryCount)

	return &dir, err
}

func (s *FileStore) ListDirectory(dir *mcmodel.File) ([]mcmodel.File, error) {
	var files []mcmodel.File

	err := DB.Where("directory_id = ?", dir.ID).
		Where("project_id", s.globusRequest.ProjectID).
		Where("current = true").
		Find(&files).Error
	if err != nil {
		return files, err
	}

	// Get files that have been uploaded
	var globusUploadedFiles []mcmodel.GlobusRequestFile
	results := DB.Where("directory_id = ?", dir.ID).
		Where("globus_request_id = ?", s.globusRequest.ID).
		Find(&globusUploadedFiles)
	uploadedFilesByName := make(map[string]mcmodel.File)
	if results.Error == nil && len(globusUploadedFiles) != 0 {
		// Convert the files into a hashtable by name. Since we don't have the underlying mcmodel.File
		// we create one on the fly only filling in the entries that will be needed to return the
		// data about the directory. In this case all that is needed are the Name and the Directory (only
		// Path off the directory). So for directory we use the single entry dirToUse. See comment at
		// start of Readdir that explains this.
		for _, requestFile := range globusUploadedFiles {
			uploadedFilesByName[requestFile.Name] = mcmodel.File{Name: requestFile.Name}
		}
	}

	for _, fileEntry := range files {
		// Keep only uploaded files that are new
		if _, ok := uploadedFilesByName[fileEntry.Name]; ok {
			// File with name already exists in files list, so skip
			continue
		}

		// Completely new file uploaded to directory (not new version of existing file)
		files = append(files, fileEntry)
	}

	return files, nil
}

//func (s *FileStore) GetFileByPath()
