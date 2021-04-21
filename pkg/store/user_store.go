package store

import (
	"github.com/materials-commons/gomcdb/mcmodel"
	"gorm.io/gorm"
)

type UserStore struct {
	db *gorm.DB
}

func NewUserStore(db *gorm.DB) *UserStore {
	return &UserStore{db: db}
}

func (s *UserStore) GetUsersWithGlobusAccount() (error, []mcmodel.User) {
	var users []mcmodel.User
	result := s.db.Where("globus_user is not null").Find(&users)
	return result.Error, users
}
