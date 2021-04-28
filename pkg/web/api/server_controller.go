package api

import (
	"os"

	"github.com/labstack/echo/v4"
)

func StopServerController(_ echo.Context) error {
	os.Exit(0)
	return nil
}
