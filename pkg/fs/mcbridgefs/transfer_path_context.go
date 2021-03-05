package mcbridgefs

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

func t() {
	path := "/gtarcea@umich.edu/1/rest/of/path"
	fmt.Printf("%q\n", strings.SplitN(path, "/", 4))
	fmt.Printf("%s: %+v\n", "/gtarcea@umich.edu/1", ToTransferPathContext("/gtarcea@umich.edu/1"))
	fmt.Printf("%s: %+v\n", "/gtarcea@umich.edu/1/abc", ToTransferPathContext("/gtarcea@umich.edu/1/abc"))
	fmt.Printf("%s: %+v\n", "/gtarcea@umich.edu/1/abc/def/ghi.txt", ToTransferPathContext("/gtarcea@umich.edu/1/abc/def/ghi.txt"))
	fmt.Printf("%s: %+v\n", "/gtarcea@umich.edu", ToTransferPathContext("/gtarcea@umich.edu"))
}

type TransferPathContext struct {
	Email     string
	ProjectID int
	Path      string
}

func (p *TransferPathContext) ProjectPathContext() string {
	return filepath.Join("/", p.Email, fmt.Sprintf("%d", p.ProjectID), "/")
}

func (p *TransferPathContext) IsRoot() bool {
	return p.Email == ""
}

func (p *TransferPathContext) IsEmail() bool {
	return p.Email != ""
}

func (p *TransferPathContext) IsProject() bool {
	return p.ProjectID != 0
}

func (n *Node) ToTransferPathContext() *TransferPathContext {
	basePath := n.Path(n.Root())
	return ToTransferPathContext(filepath.Join("/", basePath))
}

func (p *TransferPathContext) ToFilePath(name string) string {
	return filepath.Join(p.Path, name)
}

func (p *TransferPathContext) ToFSPath(name string) string {
	return filepath.Join("/", p.Email, fmt.Sprintf("%d", p.ProjectID), p.Path, name)
}

func ToTransferPathContext(p string) *TransferPathContext {
	pathParts := strings.SplitN(p, "/", 4)

	id := 0
	if len(pathParts) > 2 {
		id, _ = strconv.Atoi(pathParts[2])
	}

	rest := "/"
	if len(pathParts) == 4 {
		rest = filepath.Join("/", pathParts[3])
	}

	return &TransferPathContext{
		Email:     pathParts[1],
		ProjectID: id,
		Path:      rest,
	}
}
