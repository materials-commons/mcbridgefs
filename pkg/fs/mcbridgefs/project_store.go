package mcbridgefs

import (
	"github.com/materials-commons/gomcdb/mcmodel"
	"gorm.io/gorm"
)

type ProjectStore struct {
	db *gorm.DB
}

func NewProjectStore(db *gorm.DB) *ProjectStore {
	return &ProjectStore{db: db}
}

func (s *ProjectStore) GetProjectsForUser(userID int) (error, []mcmodel.Project) {
	var projects []mcmodel.Project

	err := s.db.Where("team_id in (select team_id from team2admin where user_id = ?)", userID).
		Or("team_id in (select team_id from team2member where user_id = ?)", userID).
		Find(&projects).Error
	return err, projects
}

func (s *ProjectStore) IncrementProjectFileTypeCount(project mcmodel.Project, fileTypeIncrements map[string]int) error {
	return s.withTxRetry(func(tx *gorm.DB) error {
		var p mcmodel.Project
		// Get latest project
		if result := tx.Find(&p, project.ID); result.Error != nil {
			return result.Error
		}

		fileTypes, err := p.GetFileTypes()
		if err != nil {
			return err
		}

		for key := range fileTypeIncrements {
			count, ok := fileTypes[key]
			if !ok {
				fileTypes[key] = fileTypeIncrements[key]
			} else {
				fileTypes[key] = count + fileTypeIncrements[key]
			}
		}

		fileTypesAsStr, err := p.ToFileTypeAsString(fileTypes)
		return tx.Model(p).Update("file_types", fileTypesAsStr).Error
	})
}

func (s *ProjectStore) UpdateProjectSizeAndFileCount(project mcmodel.Project, size int64, fileCount int) error {
	return s.withTxRetry(func(tx *gorm.DB) error {
		var p mcmodel.Project
		// Get latest project
		if result := tx.Find(&p, project.ID); result.Error != nil {
			return result.Error
		}

		return tx.Model(project).Updates(mcmodel.Project{
			FileCount: p.FileCount + fileCount,
			Size: p.Size + size,
		}).Error
	})
}

func (s *ProjectStore) UpdateProjectDirectoryCount(project mcmodel.Project, directoryCount int) error {
	return s.withTxRetry(func(tx *gorm.DB) error {
		var p mcmodel.Project
		// Get latest project
		if result := tx.Find(&p, project.ID); result.Error != nil {
			return result.Error
		}

		return tx.Model(project).Updates(mcmodel.Project{
			DirectoryCount: p.DirectoryCount + directoryCount,
		}).Error
	})
}

func (s *ProjectStore) withTxRetry(fn func(tx *gorm.DB) error) error {
	return withTxRetryDefault(fn, s.db)
}
