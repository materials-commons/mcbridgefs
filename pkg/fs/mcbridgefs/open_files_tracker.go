package mcbridgefs

import (
	"crypto/md5"
	"hash"
	"sync"

	"github.com/materials-commons/gomcdb/mcmodel"
)

type OpenFilesTracker struct {
	m sync.Map
}

type OpenFile struct {
	File     *mcmodel.File
	Checksum string
	hasher   hash.Hash
}

func NewOpenFilesTracker() *OpenFilesTracker {
	return &OpenFilesTracker{}
}

func (t *OpenFilesTracker) Store(path string, file *mcmodel.File) {
	openFile := &OpenFile{
		File:   file,
		hasher: md5.New(),
	}
	t.m.Store(path, openFile)
}

func (t *OpenFilesTracker) Get(path string) *OpenFile {
	val, _ := t.m.Load(path)
	if val != nil {
		return val.(*OpenFile)
	}

	return nil
}

func (t *OpenFilesTracker) Delete(path string) {
	t.m.Delete(path)
}
