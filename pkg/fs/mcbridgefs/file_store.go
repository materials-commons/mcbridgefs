package mcbridgefs

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/apex/log"
	"github.com/hashicorp/go-uuid"
	"github.com/materials-commons/gomcdb/mcmodel"
	"gorm.io/gorm"
)

type FileStore struct {
	db       *gorm.DB
	mcfsRoot string
}

func NewFileStore(db *gorm.DB, mcfsRoot string) *FileStore {
	return &FileStore{db: db, mcfsRoot: mcfsRoot}
}

// MarkFileReleased should only called for files that were created or opened with the Write flag set.
func (s *FileStore) MarkFileReleased(file *mcmodel.File, checksum string, projectID int, totalBytes int64) error {
	finfo, err := os.Stat(file.ToUnderlyingFilePath(s.mcfsRoot))
	if err != nil {
		log.Errorf("MarkFileReleased Stat %s failed: %s", file.ToUnderlyingFilePath(s.mcfsRoot), err)
		return err
	}

	return s.withTxRetry(func(tx *gorm.DB) error {
		// To set file as the current (ie viewable) version we first need to set all its previous
		// versions to have current set to false.
		err := tx.Model(&mcmodel.File{}).
			Where("directory_id = ?", file.DirectoryID).
			Where("name = ?", file.Name).
			Update("current", false).Error

		if err != nil {
			return err
		}

		err = tx.Model(&mcmodel.TransferRequestFile{}).
			Where("file_id = ?", file.ID).
			Update("state", "done").Error
		if err != nil {
			return err
		}

		// Now we can update the meta data on the current file. This includes, the size, current, and if there is
		// a new computed checksum, also update the checksum field.
		switch {
		case checksum != "":
			// If we are here then the file was written to so besides updating the file meta data we also have
			// to update the project size meta data
			fileMetadata := mcmodel.File{
				Size:     uint64(finfo.Size()),
				Current:  true,
				Checksum: checksum,
			}

			if err := tx.Model(file).Updates(&fileMetadata).Error; err != nil {
				return err
			}

			var project mcmodel.Project

			if result := db.Find(&project, projectID); result.Error != nil {
				return result.Error
			}

			return db.Model(&project).Updates(&mcmodel.Project{Size: project.Size + totalBytes}).Error
		default:
			// If we are here then the file was opened for read/write but it was never written to. In this situation there
			// is no checksum that has been computed, so don't update the field.
			return tx.Model(file).Updates(mcmodel.File{Size: uint64(finfo.Size()), Current: true}).Error
		}
	})
}

func (s *FileStore) CreateNewFileVersion(file, dir *mcmodel.File, transferRequest mcmodel.TransferRequest) (*mcmodel.File, error) {
	var err error
	if file, err = s.addFileToDatabase(file, dir.ID, transferRequest, false); err != nil {
		return file, err
	}

	if err := os.MkdirAll(file.ToUnderlyingDirPath(s.mcfsRoot), 0755); err != nil {
		// TODO: If this fails then we should remove the created file from the database
		log.Errorf("os.MkdirAll failed (%s): %s\n", file.ToUnderlyingDirPath(s.mcfsRoot), err)
		return nil, err
	}

	file.Directory = dir
	return file, nil
}

func (s *FileStore) CreateNewFile(file, dir *mcmodel.File, transferRequest mcmodel.TransferRequest) (*mcmodel.File, error) {
	var err error
	if file, err = s.addFileToDatabase(file, dir.ID, transferRequest, true); err != nil {
		return file, err
	}

	if err := os.MkdirAll(file.ToUnderlyingDirPath(s.mcfsRoot), 0755); err != nil {
		// TODO: If this fails then we should remove the created file from the database
		log.Errorf("os.MkdirAll failed (%s): %s\n", file.ToUnderlyingDirPath(s.mcfsRoot), err)
		return nil, err
	}

	file.Directory = dir
	return file, nil
}

// addFileToDatabase will add an mcmodel.File entry and an associated mcmodel.TransferRequestFile entry
// to the database. The file parameter must be filled out, except for the UUID which will be generated
// for the file. The TransferRequestFile will be created based on the file entry.
// to the database. The file parameter must be filled out, except for the UUID which will be generated
// for the file. The TransferRequestFile will be created based on the file entry.
func (s *FileStore) addFileToDatabase(file *mcmodel.File, dirID int, transferRequest mcmodel.TransferRequest, updateProject bool) (*mcmodel.File, error) {
	var (
		err                     error
		transferFileRequestUUID string
	)

	if file.UUID, err = uuid.GenerateUUID(); err != nil {
		return nil, err
	}

	if transferFileRequestUUID, err = uuid.GenerateUUID(); err != nil {
		return nil, err
	}

	// Wrap creation in a transaction so that both the file and the TransferRequestFile are either
	// both created, or neither is created.
	err = s.withTxRetry(func(tx *gorm.DB) error {
		if result := tx.Create(file); result.Error != nil {
			return result.Error
		}

		// Create a new transfer request file entry to account for the new file
		transferRequestFile := mcmodel.TransferRequestFile{
			ProjectID:         transferRequest.ProjectID,
			OwnerID:           file.OwnerID,
			TransferRequestID: transferRequest.ID,
			Name:              file.Name,
			DirectoryID:       dirID,
			FileID:            file.ID,
			State:             "uploading",
			UUID:              transferFileRequestUUID,
		}

		if err := tx.Create(&transferRequestFile).Error; err != nil {
			return err
		}

		if updateProject {
			return incrementProjectFileTypeCountAndFilesCount(db, transferRequest.ProjectID, Mime2Description(file.MimeType))
		}

		return nil
	})

	return file, err
}

func incrementProjectFileTypeCountAndFilesCount(db *gorm.DB, projectID int, fileTypeDescription string) error {
	var p mcmodel.Project
	// Get latest for project
	if result := db.Find(&p, projectID); result.Error != nil {
		return result.Error
	}

	fileTypes, err := p.GetFileTypes()
	if err != nil {
		return err
	}

	count, ok := fileTypes[fileTypeDescription]
	if !ok {
		fileTypes[fileTypeDescription] = 1
	} else {
		fileTypes[fileTypeDescription] = count + 1
	}

	fileTypesAsStr, err := p.ToFileTypeAsString(fileTypes)
	if err != nil {
		return err
	}

	return db.Model(&p).Updates(&mcmodel.Project{FileTypes: fileTypesAsStr, FileCount: p.FileCount + 1}).Error
}

func (s *FileStore) FindDirByPath(projectID int, path string) (*mcmodel.File, error) {
	var dir mcmodel.File
	err := s.db.Preload("Directory").
		Where("project_id = ?", projectID).
		Where("path = ?", path).
		First(&dir).Error
	if err != nil {
		log.Errorf("Failed looking up directory in project %d, path %s: %s", projectID, path, err)
		return nil, err
	}

	return &dir, nil
}

func (s *FileStore) CreateDirectory(parentDirID int, path, name string, transferRequest mcmodel.TransferRequest) (*mcmodel.File, error) {
	var dir mcmodel.File
	err := s.withTxRetry(func(tx *gorm.DB) error {
		err := tx.Where("path = ", path).Where("project_id = ?", transferRequest.ProjectID).Find(&dir).Error
		if err != nil && errors.Is(err, gorm.ErrRecordNotFound) {
			// directory already exists no need to create
			return nil
		}

		var project mcmodel.Project
		if result := db.Find(&project, transferRequest.ProjectID); result.Error != nil {
			return result.Error
		}

		dir = mcmodel.File{
			OwnerID:              transferRequest.OwnerID,
			MimeType:             "directory",
			MediaTypeDescription: "directory",
			DirectoryID:          parentDirID,
			Current:              true,
			Path:                 path,
			ProjectID:            transferRequest.ProjectID,
			Name:                 name,
		}

		if dir.UUID, err = uuid.GenerateUUID(); err != nil {
			return err
		}

		if err := tx.Create(&dir).Error; err != nil {
			return err
		}

		return db.Model(&project).Updates(&mcmodel.Project{DirectoryCount: project.DirectoryCount + 1}).Error
	})

	return &dir, err
}

func (s *FileStore) ListDirectory(dir *mcmodel.File, transferRequest mcmodel.TransferRequest) ([]mcmodel.File, error) {
	var files []mcmodel.File

	err := s.db.Where("directory_id = ?", dir.ID).
		Where("project_id", transferRequest.ProjectID).
		Where("current = true").
		Find(&files).Error
	if err != nil {
		return files, err
	}

	// Get files that have been uploaded
	var uploadedFiles []mcmodel.TransferRequestFile
	results := s.db.Where("directory_id = ?", dir.ID).
		Where("transfer_request_id = ?", transferRequest.ID).
		Find(&uploadedFiles)
	uploadedFilesByName := make(map[string]mcmodel.File)
	if results.Error == nil && len(uploadedFiles) != 0 {
		// Convert the files into a hashtable by name. Since we don't have the underlying mcmodel.File
		// we create one on the fly only filling in the entries that will be needed to return the
		// data about the directory. In this case all that is needed are the Name and the Directory (only
		// Path of the directory). So for directory we use the single entry dirToUse. See comment at
		// start of Readdir that explains this.
		for _, requestFile := range uploadedFiles {
			uploadedFilesByName[requestFile.Name] = mcmodel.File{Name: requestFile.Name}
		}
	}

	for _, fileEntry := range files {
		// Keep only uploaded files that are new
		if _, ok := uploadedFilesByName[fileEntry.Name]; ok {
			// File with name already exists in files list so delete
			delete(uploadedFilesByName, fileEntry.Name)
		}
	}

	// Now add in all the upload files that didn't already exist
	for _, fileEntry := range uploadedFilesByName {
		files = append(files, fileEntry)
	}

	return files, nil
}

func (s *FileStore) GetFileByPath(path string, transferRequest mcmodel.TransferRequest) (*mcmodel.File, error) {
	// Get directory so we can use its id for lookups
	dirPath := filepath.Dir(path)
	fileName := filepath.Base(path)
	var dir mcmodel.File
	err := s.db.Where("project_id = ?", transferRequest.ProjectID).
		Where("path = ?", dirPath).
		First(&dir).Error
	if err != nil {
		return nil, err
	}

	// We have the directory, so first check if there is an existing
	// upload for that file
	var transferRequestFile mcmodel.TransferRequestFile
	err = s.db.Preload("File.Directory").
		Where("directory_id = ?", dir.ID).
		Where("transfer_request_id = ?", transferRequest.ID).
		Where("name = ?", fileName).
		First(&transferRequestFile).Error
	if err == nil && transferRequestFile.File != nil {
		// Found file in the transfer request file
		return transferRequestFile.File, nil
	}

	// If we are here then lookup the file in the project
	var file mcmodel.File
	err = s.db.Preload("Directory").
		Where("directory_id = ?", dir.ID).
		Where("name = ?", fileName).
		Where("current = ?", true).
		First(&file).Error

	return &file, err
}

func (s *FileStore) UpdateFileUses(file *mcmodel.File, uuid string, fileID int) error {
	return s.withTxRetry(func(tx *gorm.DB) error {
		return tx.Model(file).Updates(mcmodel.File{
			UsesUUID: uuid,
			UsesID:   fileID,
		}).Error
	})
}

func (s *FileStore) withTxRetry(fn func(tx *gorm.DB) error) error {
	return withTxRetryDefault(fn, s.db)
}
