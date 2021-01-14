package mcbridgefs

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"os"
	"testing"
)

/*
import (
  "gorm.io/driver/mysql"
  "gorm.io/gorm"
)

func main() {
  // refer https://github.com/go-sql-driver/mysql#dsn-data-source-name for details
  dsn := "user:pass@tcp(127.0.0.1:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local"
  db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
}
*/

func TestAccessingDB(t *testing.T) {
	dsn := os.Getenv("DB_CONNECT_STR")
	//dsn := "mc:mcpw@tcp(127.0.0.1:3306)/mc?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Errorf("Failed to open db: %s", err)
	}

	var file MCFile
	result := db.Preload("Directory").Find(&file, 339230)
	if result.Error != nil {
		t.Errorf("Failed to get file id 339230: %s", result.Error)
	}

	assert.Equal(t, file.Name, "traj.txt", "They should be equal")
	fmt.Printf("%+v\n", file)
	fmt.Printf("%+v\n", file.Directory)
}
