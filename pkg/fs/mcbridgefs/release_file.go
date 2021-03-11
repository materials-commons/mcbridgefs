package mcbridgefs

import (
	"crypto/tls"
	"os"

	"github.com/apex/log"
	"github.com/go-resty/resty/v2"
	"github.com/materials-commons/gomcdb/mcmodel"
)

// TODO: Maybe this should be done when we are going to delete the transfer request. It will be done
// for all the file that were transfered. That part of the code also has to handle incrementing the
// count for the project when completely new files have been uploaded.
func releaseFile(file *mcmodel.File, checksum string) error {
	// Steps on release
	//   Mark file as complete
	if err := fileStore.MarkFileReleased(file, checksum); err != nil {
		return err
	}

	if checksum != "" {
		var foundFile mcmodel.File
		result := db.Model(file).Where("checksum = ?", checksum).
			Where("id <> ?", file.ID).
			Where("uses_uuid is null").
			First(&foundFile)

		// Found file with matching checksum
		if result.Error == nil && result.RowsAffected == 1 {
			// No need to request file be converted since its point at a file that is either converted,
			// in the process of being converted, or isn't convertible.
			return updateForExistingFile(file, foundFile.UUID, foundFile.ID)
		}
	}

	// Request file be converted, the api endpoint will determine if the file is convertible or not
	// transferRequest.Owner.ApiToken

	var fileReq struct {
		fileID    int `json:"file_id"`
		projectID int `json:"project_id"`
	}

	fileReq.fileID = file.ID
	fileReq.projectID = file.ProjectID

	c := resty.New()
	_, err := c.SetTLSClientConfig(&tls.Config{InsecureSkipVerify: true}).R().
		SetAuthToken(transferRequest.Owner.ApiToken).
		SetBody(fileReq).
		Post("http://materialscommons.org/api/convert-file")
	if err != nil {
		log.Errorf("Failed calling convert-file: %s", err)
	}

	return nil
}

func updateForExistingFile(file *mcmodel.File, uuid string, fileID int) error {
	// Point existing file at this file
	if err := fileStore.UpdateFileUses(file, uuid, fileID); err != nil {
		return err
	}

	// Delete the uploaded file
	filePath := file.ToUnderlyingFilePath(mcfsRoot)
	if err := os.Remove(filePath); err != nil {
		log.Errorf("Failed to delete file (%): %s", filePath, err)
		// TODO: Return err here?
	}

	return nil
}
