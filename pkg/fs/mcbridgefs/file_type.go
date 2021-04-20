package mcbridgefs

import "strings"

var mime2FileType = map[string]string{
	"application/msword": "office",
	"application/vnd.openxmlformats-officedocument.wordprocessingml.document":   "office",
	"application/vnd.ms-powerpoint":                                             "office",
	"application/vnd.openxmlformats-officedocument.presentationml.presentation": "office",
	"application/vnd.ms-excel":                                                  "excel",
	"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":         "excel",
	"video/webm":               "video",
	"video/mp4":                "video",
	"image/gif":                "image",
	"image/jpeg":               "image",
	"image/png":                "image",
	"image/tiff":               "image",
	"image/x-ms-bmp":           "image",
	"image/bmp":                "image",
	"application/octet-stream": "binary",
	"application/pdf":          "pdf",
	"text/plain":               "text",
	"text/csv":                 "text",
	"application/json":         "text",
}

var mime2Description = map[string]string{
	"application/msword": "MS-Word",
	"application/vnd.openxmlformats-officedocument.wordprocessingml.document":   "MS-Word",
	"application/vnd.ms-powerpoint":                                             "PowerPoint",
	"application/vnd.openxmlformats-officedocument.presentationml.presentation": "PowerPoint",
	"application/vnd.ms-excel":                                                  "Excel",
	"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":         "Excel",
	"video/webm":               "Video",
	"video/mp4":                "Video",
	"image/gif":                "Image",
	"image/jpeg":               "Image",
	"image/png":                "Image",
	"image/tiff":               "Image",
	"image/x-ms-bmp":           "Image",
	"image/bmp":                "Image",
	"application/octet-stream": "Binary",
	"application/pdf":          "PDF",
	"text/plain":               "Text",
	"text/csv":                 "Text",
	"application/json":         "Text",
}

func Mime2FileType(mimeStr string) string {
	fileType, ok := mime2FileType[mimeStr]
	if ok {
		return fileType
	}

	return "unknown"
}

func Mime2Description(mimeStr string) string {
	description, ok := mime2Description[mimeStr]
	if ok {
		return description
	}

	if strings.Contains(mimeStr, "video") {
		return "Video"
	}

	if strings.Contains(mimeStr, "zip") {
		return "Zipfile"
	}

	if strings.Contains(mimeStr, "latex") {
		return "Latex"
	}

	return "Unknown"
}
