package util

import "testing"

func TestContains(t *testing.T) {
	letters := []string{"a", "b", "c", "d"}

	if !Contains(letters, "a") {
		t.Error("Expected true, got false")
	}

	if Contains(letters, "e") {
		t.Error("Expected false, got true")
	}
}
