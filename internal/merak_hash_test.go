package internal

import (
	"os"
	"testing"
)

func BenchmarkMerakHash(b *testing.B) {
	path := "../test.iso"
	err := os.Setenv("MARK_WORKER", "128")
	if err != nil {
		return
	}
	err = os.Setenv("MARK_SIZE", "128000000")
	if err != nil {
		return
	}
	MerakHash(path)
}
