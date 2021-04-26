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

// Track any files that this instance writes to/create, so that if another instance does the same
// each of them will see their versions of the file, rather than intermixing them.
var openFilesTracker = newOpenFilesTracker()

func newOpenFilesTracker() *OpenFilesTracker {
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

func AddOpenFileToTracker(path string, file *mcmodel.File) {
	openFilesTracker.Store(path, file)
}

func GetOpenFileFromTrackerByPath(path string) *OpenFile {
	return openFilesTracker.Get(path)
}

func DeleteOpenFileFromTrackerByPath(path string) {
	openFilesTracker.Delete(path)
}
