package monitor

import (
	"fmt"
	"testing"
	"time"
)

func TestOneWeekAfterLogic(t *testing.T) {
	now := time.Now()

	lastChanged := time.Date(2021, 12, 13, 0, 0, 0, 0, time.UTC)

	oneWeekSinceLastChanged := lastChanged.Add(oneWeek)
	nowLongerThanOneWeek := now.Add(2 * oneWeek)

	if nowLongerThanOneWeek.After(oneWeekSinceLastChanged) {
		fmt.Println("expired")
	} else {
		fmt.Println("not expired")
	}
}
