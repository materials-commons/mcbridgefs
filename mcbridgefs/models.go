package mcbridgefs

import (
	"path/filepath"
	"strings"
)

type File struct {
	ID          int    `json:"id"`
	UUID        string `json:"string"`
	ProjectID   int    `json:"project_id"`
	Name        string `json:"name"`
	Path        string `json:"path"`
	DirectoryID int    `json:"directory_id"`
	Size        uint64 `json:"size"`
	Checksum    string `json:"checksum"`
	MimeType    string `json:"mime_type"`
}

type MCFile struct {
	File
	Directory File `json:"directory"`
}

func (f File) IsFile() bool {
	return f.MimeType != "directory"
}

func (f File) IsDir() bool {
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
	uuidParts := strings.Split(f.UUID, "-")
	return filepath.Join(mcdir, uuidParts[1][0:2], uuidParts[1][2:4], f.UUID)
}
