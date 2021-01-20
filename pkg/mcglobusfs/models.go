package mcglobusfs

import (
	"path/filepath"
	"strings"
	"time"
)

type MCFile struct {
	ID          int       `json:"id"`
	UUID        string    `json:"string"`
	ProjectID   int       `json:"project_id"`
	Name        string    `json:"name"`
	OwnerID     int       `json:"owner_id"`
	Path        string    `json:"path"`
	DirectoryID int       `json:"directory_id"`
	Size        uint64    `json:"size"`
	Checksum    string    `json:"checksum"`
	MimeType    string    `json:"mime_type"`
	Current     bool      `json:"current"`
	Directory   *MCFile   `json:"directory" gorm:"foreignKey:DirectoryID;references:ID"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (MCFile) TableName() string {
	return "files"
}

func (f MCFile) IsFile() bool {
	return f.MimeType != "directory"
}

func (f MCFile) IsDir() bool {
	return f.MimeType == "directory"
}

func (f MCFile) FullPath() string {
	if f.IsDir() {
		return f.Path
	}

	// f is a file and not a directory
	if f.Directory.Path == "/" {
		return f.Directory.Path + f.Name
	}

	return f.Directory.Path + "/" + f.Name
}

func (f MCFile) ToPath(mcdir string) string {
	return filepath.Join(f.ToDirPath(mcdir), f.UUID)
}

func (f MCFile) ToDirPath(mcdir string) string {
	uuidParts := strings.Split(f.UUID, "-")
	return filepath.Join(mcdir, uuidParts[1][0:2], uuidParts[1][2:4])
}

type GlobusRequest struct {
	ID               int       `json:"id"`
	UUID             string    `json:"string"`
	ProjectID        int       `json:"project_id"`
	Name             string    `json:"name"`
	OwnerID          int       `json:"owner_id"`
	Path             string    `json:"path"`
	GlobusAclID      string    `json:"globus_acl_id"`
	GlobusPath       string    `json:"globus_path"`
	GlobusIdentityID string    `json:"globus_identity_id"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func (GlobusRequest) TableName() string {
	return "globus_requests"
}

type GlobusRequestFile struct {
	ID              int            `json:"id"`
	UUID            string         `json:"string"`
	ProjectID       int            `json:"project_id"`
	OwnerID         int            `json:"owner_id"`
	GlobusRequestID int            `json:"globus_request_id"`
	GlobusRequest   *GlobusRequest `gorm:"foreignKey:GlobusRequestID;references:ID"`
	Path            string         `json:"path"`
	FileID          int            `json:"file_id"`
	File            *MCFile        `gorm:"foreignKey:FileID;references:ID"`
	CreatedAt       time.Time      `json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
}

func (GlobusRequestFile) TableName() string {
	return "globus_request_files"
}
