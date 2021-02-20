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
	fmt.Printf("%s: %+v\n", "/gtarcea@umich.edu/1", ToPath("/gtarcea@umich.edu/1"))
	fmt.Printf("%s: %+v\n", "/gtarcea@umich.edu/1/abc", ToPath("/gtarcea@umich.edu/1/abc"))
	fmt.Printf("%s: %+v\n", "/gtarcea@umich.edu/1/abc/def/ghi.txt", ToPath("/gtarcea@umich.edu/1/abc/def/ghi.txt"))
	fmt.Printf("%s: %+v\n", "/gtarcea@umich.edu", ToPath("/gtarcea@umich.edu"))
}

type Path struct {
	Email     string
	ProjectID int
	Path      string
}

func (p *Path) IsRoot() bool {
	return p.Email == ""
}

func (p *Path) IsEmail() bool {
	return p.Email != ""
}

func (p *Path) IsProject() bool {
	return p.ProjectID != 0
}

func (n *Node) ToPath() *Path {
	basePath := n.Path(n.Root())
	return ToPath(filepath.Join("/", basePath))
}

func (p *Path) ToFilePath(name string) string {
	return filepath.Join(p.Path, name)
}

func (p *Path) ToFSPath(name string) string {
	return filepath.Join("/", p.Email, fmt.Sprintf("%d", p.ProjectID), p.Path, name)
}

func ToPath(p string) *Path {
	pathParts := strings.SplitN(p, "/", 4)

	id := 0
	if len(pathParts) > 2 {
		id, _ = strconv.Atoi(pathParts[2])
	}

	rest := "/"
	if len(pathParts) == 4 {
		rest = filepath.Join("/", pathParts[3])
	}

	return &Path{
		Email:     pathParts[1],
		ProjectID: id,
		Path:      rest,
	}
}
