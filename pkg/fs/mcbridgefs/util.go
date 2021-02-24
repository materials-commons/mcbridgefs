package mcbridgefs

import (
	"strings"

	"github.com/materials-commons/gomcdb/mcmodel"
)

func getDirectoriesToUpdate(dir mcmodel.File, newName string) {
	directoriesToUpdate := getAllDescendents(dir)
	for _, dir2 := range directoriesToUpdate {
		dir2.Path = strings.Replace(dir2.Path, dir.Name, newName, 1)
	}
}

func getAllDescendents(dir mcmodel.File) map[string]*mcmodel.File {
	directoriesToUpdate := make(map[string]*mcmodel.File)
	count := 0

	var dirs []mcmodel.File
	err := DB.Where("directory_id = ?", dir.ID).
		Raw("where path is not null").
		Find(&dirs).Error
	if err != nil {
		return directoriesToUpdate
	}

	for {
		var ids []int
		for _, dir2 := range dirs {
			directoriesToUpdate[dir2.UUID] = &dir2
			ids = append(ids, dir2.ID)
		}
		if len(directoriesToUpdate) == count {
			break
		}

		count = len(directoriesToUpdate)

		err = DB.Where("directory_id in ?", ids).
			Raw("where path is not null").
			Find(&dirs).Error
		if err != nil {
			break
		}
	}

	return directoriesToUpdate
}
