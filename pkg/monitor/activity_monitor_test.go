package monitor

import (
	"testing"
	"time"
)

func TestOneWeekAfterLogic(t *testing.T) {
	now := time.Now().Add(2 * oneWeek)

	lastChanged := time.Date(2021, 12, 13, 0, 0, 0, 0, time.UTC)
	oneWeekSinceLastChanged := lastChanged.Add(oneWeek)

	if !now.After(oneWeekSinceLastChanged) {
		t.Fatalf("Should have been expired")
	}
}
