package store

import (
	"github.com/materials-commons/gomcdb/mcmodel"
	"github.com/materials-commons/mcbridgefs/pkg/fs/mcbridgefs"
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

func (s *ProjectStore) UpdateProjectSizeAndFileCount(projectID int, size int64, fileCount int) error {
	return s.withTxRetry(func(tx *gorm.DB) error {
		var p mcmodel.Project
		// Get latest project
		if result := tx.Find(&p, projectID); result.Error != nil {
			return result.Error
		}

		return tx.Model(&p).Updates(mcmodel.Project{
			FileCount: p.FileCount + fileCount,
			Size:      p.Size + size,
		}).Error
	})
}

func (s *ProjectStore) UpdateProjectDirectoryCount(projectID int, directoryCount int) error {
	return s.withTxRetry(func(tx *gorm.DB) error {
		var p mcmodel.Project
		// Get latest project
		if result := tx.Find(&p, projectID); result.Error != nil {
			return result.Error
		}

		return tx.Model(&p).Updates(mcmodel.Project{
			DirectoryCount: p.DirectoryCount + directoryCount,
		}).Error
	})
}

func (s *ProjectStore) withTxRetry(fn func(tx *gorm.DB) error) error {
	return mcbridgefs.WithTxRetryDefault(fn, s.db)
}
